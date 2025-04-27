package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.HashingUtils;
import vk.itmo.teamgray.sharded.storage.node.client.shards.ShardData;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsResponse;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardResponse;
import vk.itmo.teamgray.sharded.storage.node.management.ServerData;

public class NodeManagementService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    private final NodeStorageService nodeStorageService;

    public NodeManagementService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void rearrangeShards(RearrangeShardsRequest request, StreamObserver<RearrangeShardsResponse> responseObserver) {
        //TODO Refactor

        var shardToHash = request.getShardToHashMap();

        log.info("Rearranging shards. [request={}]", shardToHash);

        Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
        Map<Integer, ShardData> newShards = new ConcurrentHashMap<>();
        List<Map.Entry<Integer, Long>> shardToHashMap = new ArrayList<>(shardToHash.entrySet());
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
                    Metadata metadata = new Metadata();
                    String errMessage = MessageFormat.format("Shard for key=[{0}] not found", key);
                    log.warn(errMessage);
                    responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT.withDescription(errMessage)
                            .asRuntimeException(metadata));
                    return;
                }
            }
        }

        nodeStorageService.replace(newShards);

        log.info("Rearranged shards.");

        responseObserver.onNext(RearrangeShardsResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void moveShard(MoveShardRequest request, StreamObserver<MoveShardResponse> responseObserver) {
        int shardId = request.getShardId();
        ServerData targetServer = request.getTargetServer();
        
        log.info("Request to move shard {} to {}:{}", shardId, targetServer.getHost(), targetServer.getPort());
        
        Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
        if (!existingShards.containsKey(shardId)) {
            responseObserver.onNext(MoveShardResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Shard " + shardId + " not found in this node")
                .build());
            responseObserver.onCompleted();
            return;
        }

        ShardData shardToMove = existingShards.get(shardId);
        Map<String, String> shardData = shardToMove.getStorage();

        // TODO: replace with sendShard implementation
        boolean sendSuccess = sendShardStub(shardId, shardData, targetServer);

        if (sendSuccess) {
            // remove shard only after successful transfer
            nodeStorageService.removeShard(shardId);
            
            responseObserver.onNext(MoveShardResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Shard successfully moved")
                .build());
        } else {
            responseObserver.onNext(MoveShardResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Failed to send shard to target server")
                .build());
        }
        
        responseObserver.onCompleted();
    }

    private boolean sendShardStub(int shardId, Map<String, String> shardData, ServerData targetServer) {
        // TODO: refactor this method for dynamic change host and port
        NodeNodeClient nodeNodeClient = new NodeNodeClient(targetServer.getHost(), targetServer.getPort());
        boolean result = nodeNodeClient.sendShard(shardId, shardData);
        try {
            nodeNodeClient.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
