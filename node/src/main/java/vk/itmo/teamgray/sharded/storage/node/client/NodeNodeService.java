package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardRequest;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardResponse;

import java.util.Map;

public class NodeNodeService extends NodeNodeServiceGrpc.NodeNodeServiceImplBase {

    private final NodeStorageService nodeStorageService;

    public NodeNodeService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void sendShard(SendShardRequest request, StreamObserver<SendShardResponse> responseObserver) {
        int shardId = request.getShardId();
        Map<String, String> shard = request.getShardMap();
        boolean success = true;
        String message = "SUCCESS";

        try {
            for (Map.Entry<String, String> entry : shard.entrySet()) {
                nodeStorageService.checkKeyForShard(shardId, entry.getKey());
                nodeStorageService.set(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            success = false;
            message = "ERROR: " + e.getMessage();
        }

        SendShardResponse response = SendShardResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
