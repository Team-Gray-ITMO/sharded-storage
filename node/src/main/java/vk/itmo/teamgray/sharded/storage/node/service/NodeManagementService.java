package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

public class NodeManagementService {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    private final NodeStorageService nodeStorageService;

    private final DiscoveryClient discoveryClient;

    private Map<Integer, ShardData> originalShardsBackup;

    private int originalFullShardCount;

    public NodeManagementService(NodeStorageService nodeStorageService, DiscoveryClient discoveryClient) {
        this.nodeStorageService = nodeStorageService;
        this.discoveryClient = discoveryClient;
    }

    private Map<Integer, ShardData> deepCopyShards(Map<Integer, ShardData> original) {
        if (original == null) {
            return new ConcurrentHashMap<>();
        }

        Map<Integer, ShardData> copy = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, ShardData> entry : original.entrySet()) {
            ShardData originalData = entry.getValue();
            ShardData copiedData = new ShardData();
            if (originalData != null) {
                originalData.getStorage().forEach(copiedData::addToStorage);
            }
            copy.put(entry.getKey(), copiedData);
        }
        return copy;
    }

    public void rearrangeShards(
        Map<Integer, Long> shardToHash,
        List<FragmentDTO> fragments,
        Map<Integer, Integer> serverByShardNumber,
        int fullShardCount,
        StatusResponseWriter responseWriter) {
        this.originalShardsBackup = deepCopyShards(nodeStorageService.getShards());
        this.originalFullShardCount = nodeStorageService.getFullShardCount();

        try {
            log.info("Rearranging shards. [request={}]", shardToHash);

            Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
            Map<Integer, ShardData> newShards = new ConcurrentHashMap<>();
            List<Map.Entry<Integer, Long>> shardToHashMap = new ArrayList<>(shardToHash.entrySet());
            shardToHashMap.sort(Comparator.comparingLong(Map.Entry::getValue));

            if (shardToHashMap.isEmpty()) {
                responseWriter.writeResponse(true, "");

                return;
            }

            shardToHashMap.forEach(shard -> newShards.put(shard.getKey(), new ShardData()));

            var localFragments = fragments.stream()
                .filter(fragment -> newShards.containsKey(fragment.newShardId()))
                .toList();

            localFragments.forEach(fragment -> {
                int oldShardId = fragment.oldShardId();
                if (existingShards.containsKey(oldShardId)) {
                    Map<String, String> sourceStorage = existingShards.get(oldShardId).getStorage();

                    //TODO At some point for perf reasons it will be nice to be able to do this with hash ranges instead of one-by-one
                    sourceStorage.entrySet().stream()
                        .filter(entry -> {
                            long hash = HashingUtils.calculate64BitHash(entry.getKey());
                            return hash >= fragment.rangeFrom() && hash < fragment.rangeTo();
                        })
                        .forEach(entry -> {
                            newShards.get(fragment.newShardId()).addToStorage(entry.getKey(), entry.getValue());

                            if (!newShards.containsKey(oldShardId)) {
                                sourceStorage.remove(entry.getKey());
                            }
                        });
                }
            });

            var externalFragments = fragments.stream()
                .filter(fragment -> !newShards.containsKey(fragment.newShardId()))
                .toList();

            var keysToRemove = new HashMap<Integer, Set<String>>();

            Map<Integer, Integer> nodesByShard = externalFragments.isEmpty()
                ? Collections.emptyMap()
                : serverByShardNumber;

            Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(nodesByShard.values());

            externalFragments.stream()
                .filter(it -> existingShards.containsKey(it.oldShardId()))
                .forEach(fragment -> {
                    int oldShardId = fragment.oldShardId();

                    Map<String, String> fragmentStorage = existingShards.get(oldShardId).getStorage();

                    // TODO Set level to debug
                    log.info(
                        "Moving fragment [{}]-[{}] from shard {} to shard {}",
                        fragment.rangeFrom(),
                        fragment.rangeTo(),
                        oldShardId,
                        fragment.newShardId()
                    );

                    var fragmentsToSend = fragmentStorage.entrySet().stream()
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

                    keysToRemove.put(oldShardId, fragmentsToSend.keySet());
                });

            keysToRemove.forEach((oldShardId, keysToRemoveSet) -> {
                Map<String, String> fragmentStorage = existingShards.get(oldShardId).getStorage();

                keysToRemoveSet.forEach(fragmentStorage::remove);
            });

            nodeStorageService.replace(newShards, fullShardCount);
            responseWriter.writeResponse(true, "");

            log.info("Shards rearranged");

        } catch (Exception e) {
            String message = "Failed during rearrange: ";

            log.error(message, e);

            responseWriter.writeResponse(false, message + e.getMessage());
        }
    }

    private StatusResponseDTO moveShardFragment(DiscoverableServiceDTO server, int newShardId, Map<String, String> fragmentsToSend) {
        var nodeNodeClient = GrpcClientCachingFactory
            .getInstance()
            .getClient(
                server,
                NodeNodeClient::new
            );

        // TODO Set debug level
        log.info("Sending fragment from shard {} to node {}", newShardId, server);

        return nodeNodeClient.sendShardFragment(newShardId, fragmentsToSend);
    }

    public void moveShard(int shardId, int targetServerId, StatusResponseWriter responseWriter) {
        DiscoverableServiceDTO targetServer = discoveryClient.getNode(targetServerId);

        log.info("Request to move shard {} to {}", shardId, targetServer);

        Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
        if (!existingShards.containsKey(shardId)) {
            responseWriter.writeResponse(false, "Shard " + shardId + " not found in this node");

            return;
        }

        ShardData shardToMove = existingShards.get(shardId);
        Map<String, String> shardData = shardToMove.getStorage();

        StatusResponseDTO sendResponse = sendShard(shardId, shardData, targetServer);

        if (sendResponse.isSuccess()) {
            // remove shard only after a successful transfer
            nodeStorageService.removeShard(shardId);

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

        // TODO Set debug level
        log.info("Sending shard {} to node {}", shardId, targetServer);

        return nodeNodeClient.sendShard(shardId, shardData);
    }

    public void rollbackTopologyChange(StatusResponseWriter responseWriter) {
        if (this.originalShardsBackup != null) {
            try {
                nodeStorageService.replace(this.originalShardsBackup, this.originalFullShardCount);
                this.originalShardsBackup = null;

                responseWriter.writeResponse(true, "Rollback successful");
            } catch (Exception e) {
                log.error("Error during node rollback while replacing shards", e);

                responseWriter.writeResponse(false, "Rollback failed");
            }
        } else {
            log.warn("Node rollback called but no backup found.");

            responseWriter.writeResponse(false, "No data for rollback");
        }
    }
}
