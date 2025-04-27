package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.NodeException;
import vk.itmo.teamgray.sharded.storage.node.node.*;

import java.util.Map;

public class NodeNodeService extends NodeNodeServiceGrpc.NodeNodeServiceImplBase {

    private final String SUCCESS_MESSAGE = "SUCCESS";
    private final NodeStorageService nodeStorageService;

    public NodeNodeService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void sendShard(SendShardRequest request, StreamObserver<SendShardResponse> responseObserver) {
        int shardId = request.getShardId();
        Map<String, String> shard = request.getShardMap();
        boolean success = true;
        String message = SUCCESS_MESSAGE;

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

    @Override
    public void sendShardFragment(SendShardFragmentRequest request,
                                  StreamObserver<SendShardFragmentResponse> responseObserver) {
        boolean success = true;
        String message = SUCCESS_MESSAGE;

        try {
            if (!nodeStorageService.containsShard(request.getShardId())) {
                nodeStorageService.addNewShard(request.getShardId());
            }
            request.getShardFragmentsMap().forEach(nodeStorageService::set);
        } catch (NodeException e) {
            success = false;
            message = "ERROR: " + e.getMessage();
        }

        responseObserver.onNext(SendShardFragmentResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build());

        responseObserver.onCompleted();
    }
}
