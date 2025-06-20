package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.node.ActionPhase;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.common.utils.HashingUtils;
import vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils;
import vk.itmo.teamgray.sharded.storage.node.client.NodeNodeClient;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static vk.itmo.teamgray.sharded.storage.common.node.NodeState.MOVE_SHARDS_PREPARING;
import static vk.itmo.teamgray.sharded.storage.common.node.NodeState.MOVE_SHARDS_ROLLING_BACK;
import static vk.itmo.teamgray.sharded.storage.common.node.NodeState.REARRANGE_SHARDS_PREPARING;
import static vk.itmo.teamgray.sharded.storage.common.node.NodeState.REARRANGE_SHARDS_ROLLING_BACK;

public class NodeManagementService {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    //90% of the size to account for overhead.
    private static final int MEMORY_THRESHOLD = Double.valueOf(PropertyUtils.getMessageMaxSize() * 0.90).intValue();

    private final NodeStorageService nodeStorageService;

    private final DiscoveryClient discoveryClient;

    private final ClientCachingFactory clientCachingFactory;

    private CountDownLatch rollbackLatch;

    public NodeManagementService(
        NodeStorageService nodeStorageService,
        DiscoveryClient discoveryClient,
        ClientCachingFactory clientCachingFactory
    ) {
        this.nodeStorageService = nodeStorageService;
        this.discoveryClient = discoveryClient;
        this.clientCachingFactory = clientCachingFactory;

        nodeStorageService.changeState(NodeState.INIT, NodeState.RUNNING);
    }

    public void prepareRearrange(
        List<FragmentDTO> fragments,
        Map<Integer, Integer> serverByShardNumber,
        Map<Integer, Long> shardToHash,
        int fullShardCount,
        StatusResponseWriter responseWriter
    ) {
        try {
            log.info(
                "Preparing rearrange shards. [fragments={}, serverByShardNumber={}, request={}, fullShardCount={}]",
                fragments,
                serverByShardNumber,
                shardToHash,
                fullShardCount
            );

            nodeStorageService.changeState(NodeState.RUNNING, REARRANGE_SHARDS_PREPARING);

            ConcurrentHashMap<Integer, ShardData> stagedShards = new ConcurrentHashMap<>();

            shardToHash.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getValue))
                .forEach(shard ->
                    stagedShards.put(shard.getKey(), new ShardData())
                );

            nodeStorageService.prepareDataForResharding(fragments, serverByShardNumber, stagedShards, fullShardCount);

            if (failActionOnRollback()) {
                responseWriter.writeResponse(false, "Rolled back.");

                return;
            }

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(REARRANGE_SHARDS_PREPARING, NodeState.REARRANGE_SHARDS_PREPARED);

            log.info("Prepared rearrange shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    private void processRearrange(StatusResponseWriter responseWriter) {
        try {
            log.info("Processing rearrange shards.");

            nodeStorageService.changeState(NodeState.REARRANGE_SHARDS_PREPARED, NodeState.REARRANGE_SHARDS_PROCESSING);

            Map<Integer, ShardData> existingShards = nodeStorageService.getShards().getShardMap();
            Map<Integer, ShardData> stagedShards = nodeStorageService.getStagedShards().getShardMap();

            List<FragmentDTO> fragments = nodeStorageService.getPreparedFragments();

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

            Action rearrangeShards = Action.REARRANGE_SHARDS;

            Map<Integer, Integer> nodesByShard = externalFragments.isEmpty()
                ? Collections.emptyMap()
                : nodeStorageService.getPreparedServerByShardNumber(rearrangeShards);

            Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(nodesByShard.values());

            Map<Integer, List<FragmentDTO>> externalFragmentsByServer = externalFragments.stream()
                .filter(it -> existingShards.containsKey(it.oldShardId()))
                .collect(
                    Collectors.groupingBy(
                        it -> nodesByShard.get(it.newShardId()),
                        Collectors.mapping(Function.identity(), Collectors.toList())
                    )
                );

            externalFragmentsByServer.forEach(
                (serverId, fragmentsForServer) -> {
                    DiscoverableServiceDTO node = nodes.get(serverId);

                    ShardAutoFlushSink shardSink = new ShardAutoFlushSink(
                        MEMORY_THRESHOLD,
                        batch -> {
                            StatusResponseDTO moveResponse = sendShardEntries(rearrangeShards, batch, node);

                            if (!moveResponse.isSuccess()) {
                                throw new IllegalStateException(
                                    "Failed to move entries fragment: "
                                        + System.lineSeparator()
                                        + node.getIdForLogging() + ": "
                                        + moveResponse.getMessage()
                                );
                            }
                        }
                    );

                    fragmentsForServer
                        .forEach(fragment -> {
                            int oldShardId = fragment.oldShardId();

                            Map<String, String> fragmentStorage = existingShards.get(oldShardId).getStorage();

                            log.debug(
                                "Moving fragment [{}]-[{}] from entries {} to entries {}",
                                fragment.rangeFrom(),
                                fragment.rangeTo(),
                                oldShardId,
                                fragment.newShardId()
                            );

                            fragmentStorage.entrySet().stream()
                                .filter(entry -> {
                                    long hash = HashingUtils.calculate64BitHash(entry.getKey());

                                    return hash >= fragment.rangeFrom() && hash < fragment.rangeTo();
                                })
                                .forEachOrdered(entry -> shardSink.addEntry(fragment.newShardId(), entry));
                        });

                    shardSink.finalFlush();
                }
            );

            if (failActionOnRollback()) {
                responseWriter.writeResponse(false, "Rolled back.");

                return;
            }

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(NodeState.REARRANGE_SHARDS_PROCESSING, NodeState.REARRANGE_SHARDS_PROCESSED);

            log.info("Processed rearrange shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    public void prepareMove(
        List<Integer> receiveShardIds,
        List<SendShardTaskDTO> sendShards,
        int fullShardCount,
        StatusResponseWriter responseWriter
    ) {
        try {
            log.info(
                "Preparing move shards. [receiveShardIds={}, sendShards={}, fullShardCount={}]",
                receiveShardIds,
                sendShards,
                fullShardCount
            );

            nodeStorageService.changeState(NodeState.RUNNING, MOVE_SHARDS_PREPARING);

            ConcurrentHashMap<Integer, ShardData> stagedShards = new ConcurrentHashMap<>();

            Set<Integer> removeShardIdsSet = new HashSet<>(receiveShardIds);

            // Reuse same shards that are not sent.
            nodeStorageService.getShards()
                .getShardMap().entrySet().stream()
                .filter(it -> !removeShardIdsSet.contains(it.getKey()))
                .forEach(entry -> stagedShards.put(entry.getKey(), entry.getValue()));

            // Add missing shards to get data
            receiveShardIds.forEach(newShardId -> stagedShards.put(newShardId, new ShardData()));

            nodeStorageService.prepareDataForMoving(sendShards, stagedShards, fullShardCount);

            if (failActionOnRollback()) {
                responseWriter.writeResponse(false, "Rolled back.");

                return;
            }

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(MOVE_SHARDS_PREPARING, NodeState.MOVE_SHARDS_PREPARED);

            log.info("Prepared move shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    private void processMove(StatusResponseWriter responseWriter) {
        try {
            log.info("Processing move shards.");

            nodeStorageService.changeState(NodeState.MOVE_SHARDS_PREPARED, NodeState.MOVE_SHARDS_PROCESSING);

            Action moveShards = Action.MOVE_SHARDS;

            Map<Integer, List<Integer>> shardsByTargetServers = nodeStorageService.getPreparedServerByShardNumber(moveShards)
                .entrySet()
                .stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                    )
                );

            ShardsContainer existingShards = nodeStorageService.getShards();

            Map<Integer, ShardData> shardMap = existingShards.getShardMap();

            shardsByTargetServers.forEach((targetServerId, shardIds) -> {
                DiscoverableServiceDTO targetServer = discoveryClient.getNode(targetServerId);

                ShardAutoFlushSink shardSink = new ShardAutoFlushSink(
                    MEMORY_THRESHOLD,
                    batch -> {
                        StatusResponseDTO sendResponse = sendShardEntries(moveShards, batch, targetServer);

                        if (!sendResponse.isSuccess()) {
                            throw new IllegalStateException(
                                "Failed to move entries: "
                                    + System.lineSeparator()
                                    + targetServer.getIdForLogging() + ": "
                                    + sendResponse.getMessage()
                            );
                        }
                    }
                );

                if (targetServer == null) {
                    throw new IllegalStateException("No server with id " + targetServerId + " found");
                }

                log.info("Request to move shards {} to {}", shardIds, targetServer);

                List<Integer> absentShards = shardIds.stream()
                    .filter(it -> !existingShards.containsShard(it))
                    .toList();

                if (!absentShards.isEmpty()) {
                    var errorMessage = "Shards " + absentShards + " not found in this node";

                    log.error(errorMessage);

                    responseWriter.writeResponse(false, errorMessage);

                    return;
                }

                shardIds
                    .forEach(shardId -> shardMap.get(shardId).getStorage().entrySet()
                        .forEach(entry ->
                            shardSink.addEntry(shardId, entry)
                        )
                    );

                shardSink.finalFlush();
            });

            if (failActionOnRollback()) {
                responseWriter.writeResponse(false, "Rolled back.");

                return;
            }

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(NodeState.MOVE_SHARDS_PROCESSING, NodeState.MOVE_SHARDS_PROCESSED);

            log.info("Processed move shards.");
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    public void processAction(Action action, StatusResponseWriter responseWriter) {
        if (action == Action.REARRANGE_SHARDS) {
            processRearrange(responseWriter);
        } else if (action == Action.MOVE_SHARDS) {
            processMove(responseWriter);
        } else {
            throw new IllegalStateException("Unknown action: " + action);
        }
    }

    public void applyAction(Action action, StatusResponseWriter responseWriter) {
        try {
            log.info("Applying {}", action);

            var processedState = NodeState.resolve(action, ActionPhase.PROCESS, true);
            var applyingState = NodeState.resolve(action, ActionPhase.APPLY);

            nodeStorageService.changeState(processedState, applyingState);

            nodeStorageService.swapWithStaged();

            nodeStorageService.processQueue(ActionPhase.APPLY);
            nodeStorageService.clear();

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(applyingState, NodeState.RUNNING);

            log.info("Applied {}", action);
        } catch (Exception e) {
            log.error("Caught exception: ", e);

            responseWriter.writeResponse(false, e.getMessage());
        }
    }

    public void rollbackAction(Action action, StatusResponseWriter responseWriter) {
        try {
            log.info("Rolling back {}", action);

            var rollbackableStates = Stream.of(ActionPhase.PREPARE, ActionPhase.PROCESS)
                .flatMap(phase ->
                    Stream.of(
                        NodeState.resolve(action, phase, true),
                        NodeState.resolve(action, phase, false)
                    )
                )
                .toList();

            var rollingBackState = NodeState.resolve(action, ActionPhase.ROLLBACK);

            nodeStorageService.changeState(rollbackableStates, rollingBackState);

            awaitRollback();

            nodeStorageService.processQueue(ActionPhase.ROLLBACK);
            nodeStorageService.clear();

            responseWriter.writeResponse(true, "");
            nodeStorageService.changeState(rollingBackState, NodeState.RUNNING);

            log.info("Rolled back {}", action);
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
        var state = nodeStorageService.getState();

        if (state == REARRANGE_SHARDS_ROLLING_BACK || state == MOVE_SHARDS_ROLLING_BACK) {
            if (rollbackLatch != null) {
                rollbackLatch.countDown();
            }

            return true;
        }

        return false;
    }

    private StatusResponseDTO sendShardEntries(Action action, List<SendShardDTO> sendShardEntries, DiscoverableServiceDTO targetServer) {
        var nodeNodeClient = clientCachingFactory
            .getClient(
                targetServer,
                NodeNodeClient.class
            );

        log.debug("Sending {} shard entries for action {} to node {}", sendShardEntries.size(), action, targetServer);

        return nodeNodeClient.sendShardEntries(sendShardEntries, action);
    }
}
