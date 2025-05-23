package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.common.utils.HashingUtils;
import vk.itmo.teamgray.sharded.storage.node.client.NodeNodeClient;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static vk.itmo.teamgray.sharded.storage.node.service.NodeState.REARRANGE_PREPARED;
import static vk.itmo.teamgray.sharded.storage.node.service.NodeState.REARRANGE_PREPARING;
import static vk.itmo.teamgray.sharded.storage.node.service.NodeState.REARRANGE_PROCESSED;
import static vk.itmo.teamgray.sharded.storage.node.service.NodeState.REARRANGE_PROCESSING;
import static vk.itmo.teamgray.sharded.storage.node.service.NodeState.REARRANGE_ROLLING_BACK;

public class NodeManagementService {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    private final NodeStorageService nodeStorageService;

    private final DiscoveryClient discoveryClient;

    private CountDownLatch rollbackLatch;

    public NodeManagementService(NodeStorageService nodeStorageService, DiscoveryClient discoveryClient) {
        this.nodeStorageService = nodeStorageService;
        this.discoveryClient = discoveryClient;

        nodeStorageService.changeState(NodeState.INIT, NodeState.RUNNING);
    }

    public void prepareRearrange(
        Map<Integer, Long> shardToHash,
        int fullShardCount,
        StatusResponseWriter responseWriter
    ) {
        try {
            log.info("Preparing rearrange shards. [request={}, fullShardCount={}]", shardToHash, fullShardCount);

            nodeStorageService.changeState(NodeState.RUNNING, REARRANGE_PREPARING);

            ConcurrentHashMap<Integer, ShardData> stagedShards = new ConcurrentHashMap<>();

            shardToHash.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getValue))
                .forEach(shard ->
                    stagedShards.put(shard.getKey(), new ShardData())
                );

            nodeStorageService.stageShards(stagedShards, fullShardCount);

            if (failActionOnRollback()) {
                responseWriter.writeResponse(false, "Rolled back.");

                return;
            }

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(REARRANGE_PREPARING, NodeState.REARRANGE_PREPARED);

            log.info("Prepared rearrange shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    public void processRearrange(
        List<FragmentDTO> fragments,
        Map<Integer, Integer> serverByShardNumber,
        StatusResponseWriter responseWriter
    ) {
        try {
            log.info("Processing rearrange shards. [fragments={}, serverByShardNumber={}]", fragments, serverByShardNumber);

            nodeStorageService.changeState(NodeState.REARRANGE_PREPARED, NodeState.REARRANGE_PROCESSING);

            Map<Integer, ShardData> existingShards = nodeStorageService.getShards().getShardMap();
            Map<Integer, ShardData> stagedShards = nodeStorageService.getStagedShards().getShardMap();

            // Local fragments
            fragments.stream()
                .filter(fragment -> stagedShards.containsKey(fragment.newShardId()) && existingShards.containsKey(fragment.oldShardId()))
                .forEach(fragment ->
                    existingShards.get(fragment.oldShardId())
                        .getStorage().entrySet().stream()
                        //TODO At some point for perf reasons it will be nice to be able to do this with hash ranges instead of one-by-one
                        .filter(entry -> {
                            long hash = HashingUtils.calculate64BitHash(entry.getKey());
                            return hash >= fragment.rangeFrom() && hash < fragment.rangeTo();
                        })
                        .forEach(entry ->
                            stagedShards.get(fragment.newShardId()).addToStorage(entry.getKey(), entry.getValue())
                        )
                );

            List<FragmentDTO> externalFragments = fragments.stream()
                .filter(fragment -> !stagedShards.containsKey(fragment.newShardId()))
                .toList();

            Map<Integer, Integer> nodesByShard = externalFragments.isEmpty()
                ? Collections.emptyMap()
                : serverByShardNumber;

            Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(nodesByShard.values());

            externalFragments.stream()
                .filter(it -> existingShards.containsKey(it.oldShardId()))
                .forEach(fragment -> {
                    int oldShardId = fragment.oldShardId();

                    Map<String, String> fragmentStorage = existingShards.get(oldShardId).getStorage();

                    log.debug(
                        "Moving fragment [{}]-[{}] from shard {} to shard {}",
                        fragment.rangeFrom(),
                        fragment.rangeTo(),
                        oldShardId,
                        fragment.newShardId()
                    );

                    Map<String, String> fragmentsToSend = fragmentStorage.entrySet().stream()
                        .filter(entry -> {
                            long hash = HashingUtils.calculate64BitHash(entry.getKey());
                            return hash >= fragment.rangeFrom() && hash < fragment.rangeTo();
                        })
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                            )
                        );

                    Integer serverId = nodesByShard.get(fragment.newShardId());

                    DiscoverableServiceDTO node = nodes.get(serverId);

                    StatusResponseDTO moveResponse = moveShardFragment(node, fragment.newShardId(), fragmentsToSend);

                    if (!moveResponse.isSuccess()) {
                        throw new IllegalStateException(
                            "Failed to move shard fragment: "
                                + System.lineSeparator()
                                + node.getIdForLogging() + ": "
                                + moveResponse.getMessage()
                        );
                    }
                });

            if (failActionOnRollback()) {
                responseWriter.writeResponse(false, "Rolled back.");

                return;
            }

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(NodeState.REARRANGE_PROCESSING, NodeState.REARRANGE_PROCESSED);

            log.info("Processed rearrange shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    public void applyRearrange(StatusResponseWriter responseWriter) {
        try {
            log.info("Applying rearrange shards.");

            nodeStorageService.changeState(NodeState.REARRANGE_PROCESSED, NodeState.REARRANGE_APPLYING);

            nodeStorageService.swapWithStaged();

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(NodeState.REARRANGE_APPLYING, NodeState.RUNNING);

            log.info("Applied rearrange shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    public void rollbackRearrange(StatusResponseWriter responseWriter) {
        try {
            log.info("Rolling back rearrange shards.");

            nodeStorageService.changeState(
                List.of(REARRANGE_PREPARING, REARRANGE_PREPARED, REARRANGE_PROCESSING, REARRANGE_PROCESSED),
                REARRANGE_ROLLING_BACK
            );

            awaitRollback();

            nodeStorageService.clearStaged();

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(NodeState.REARRANGE_ROLLING_BACK, NodeState.RUNNING);

            log.info("Rolled back rearrange shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    private void awaitRollback() throws InterruptedException {
        rollbackLatch = new CountDownLatch(1);

        log.info("Awaiting rollback.");

        rollbackLatch.await(10, TimeUnit.SECONDS);

        rollbackLatch = null;
    }

    // TODO Find a way to stop execution mid-way on rollback.
    private boolean failActionOnRollback() {
        if (nodeStorageService.getState() == REARRANGE_ROLLING_BACK) {
            if (rollbackLatch != null) {
                rollbackLatch.countDown();
            }

            return true;
        }

        return false;
    }

    private StatusResponseDTO moveShardFragment(DiscoverableServiceDTO server, int newShardId, Map<String, String> fragmentsToSend) {
        var nodeNodeClient = GrpcClientCachingFactory
            .getInstance()
            .getClient(
                server,
                NodeNodeClient::new
            );

        log.debug("Sending fragment from shard {} to node {}", newShardId, server);

        return nodeNodeClient.sendShardFragment(newShardId, fragmentsToSend);
    }

    public void moveShard(int shardId, int targetServerId, StatusResponseWriter responseWriter) {
        DiscoverableServiceDTO targetServer = discoveryClient.getNode(targetServerId);

        log.info("Request to move shard {} to {}", shardId, targetServer);

        var existingShards = nodeStorageService.getShards();

        if (!existingShards.containsShard(shardId)) {
            var errorMessage = "Shard " + shardId + " not found in this node";

            log.error(errorMessage);

            responseWriter.writeResponse(false, errorMessage);

            return;
        }

        ShardData shardToMove = existingShards.getShardMap().get(shardId);
        Map<String, String> shardData = shardToMove.getStorage();

        StatusResponseDTO sendResponse = sendShard(shardId, shardData, targetServer);

        if (sendResponse.isSuccess()) {
            // remove shard only after a successful transfer
            nodeStorageService.getShards().clearShard(shardId);

            responseWriter.writeResponse(true, "Shard successfully moved");
        } else {
            responseWriter.writeResponse(
                false,
                "Failed to send shard to target server: "
                    + System.lineSeparator()
                    + targetServer.getIdForLogging() + ": "
                    + sendResponse.getMessage()
            );
        }
    }

    private StatusResponseDTO sendShard(int shardId, Map<String, String> shardData, DiscoverableServiceDTO targetServer) {
        var nodeNodeClient = GrpcClientCachingFactory
            .getInstance()
            .getClient(
                targetServer,
                NodeNodeClient::new
            );

        log.debug("Sending shard {} to node {}", shardId, targetServer);

        return nodeNodeClient.sendShard(shardId, shardData);
    }
}
