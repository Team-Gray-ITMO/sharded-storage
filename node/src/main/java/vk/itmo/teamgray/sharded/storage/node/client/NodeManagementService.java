package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.HashingUtils;
import vk.itmo.teamgray.sharded.storage.common.NodeException;
import vk.itmo.teamgray.sharded.storage.node.client.shards.ShardData;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsResponse;

public class NodeManagementService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    private final NodeStorageService nodeStorageService;

    public NodeManagementService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void rearrangeShards(RearrangeShardsRequest request, StreamObserver<RearrangeShardsResponse> responseObserver) {
        //TODO Refactor

        Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
        Map<Integer, ShardData> newShards = new ConcurrentHashMap<>();
        List<Map.Entry<Integer, Long>> shardToHashMap = new ArrayList<>(request.getShardToHashMap().entrySet());
        shardToHashMap.sort(Comparator.comparingLong(Map.Entry::getValue));

        if (shardToHashMap.isEmpty()) {
            responseObserver.onNext(RearrangeShardsResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
            return;
        }

        shardToHashMap.forEach(shard -> newShards.put(shard.getKey(), new ShardData()));

        for (Map.Entry<Integer, ShardData> existingShardEntry : existingShards.entrySet()) {
            Map<String, String> shardStorage = existingShardEntry.getValue().getStorage();
            for (Map.Entry<String, String> entry : shardStorage.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                long hash = HashingUtils.calculate64BitHash(key);

                Integer targetShardId = null;
                long previousUpperBound = Long.MIN_VALUE;

                for (Map.Entry<Integer, Long> bound : shardToHashMap) {
                    long upperBound = bound.getValue();
                    if (hash > previousUpperBound && hash <= upperBound) {
                        targetShardId = bound.getKey();
                        break;
                    }
                    previousUpperBound = upperBound;
                }

                if (targetShardId != null) {
                    newShards.get(targetShardId).addToStorage(key, value);
                } else {
                    throw new NodeException("Shard for key " + key + " not found");
                }
            }
        }

        nodeStorageService.replace(newShards);

        responseObserver.onNext(RearrangeShardsResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }
}
