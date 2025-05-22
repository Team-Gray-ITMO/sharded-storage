package vk.itmo.teamgray.sharded.storage.node.service;

import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardRequest;

// TODO Decouple to gRPC Service and Service with business logic. Example: 'HealthGrpcService' and 'HealthService'
public class NodeNodeService extends NodeNodeServiceGrpc.NodeNodeServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeNodeService.class);

    private static final String SUCCESS_MESSAGE = "SUCCESS";

    private final NodeStorageService nodeStorageService;

    public NodeNodeService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void sendShard(SendShardRequest request, StreamObserver<StatusResponse> responseObserver) {
        int shardId = request.getShardId();
        Map<String, String> shard = request.getShardMap();
        boolean success = true;
        String message = SUCCESS_MESSAGE;

        log.info("Received shard {}. Processing", request.getShardId());

        try {
            if (!nodeStorageService.containsShard(request.getShardId())) {
                throw new NodeException("Shard " + request.getShardId() + " does not exist");
            }

            shard.forEach((key, value) -> {
                nodeStorageService.checkKeyForShard(shardId, key);
                nodeStorageService.set(key, value);
            });
        } catch (Exception e) {
            success = false;
            message = "ERROR: " + e.getMessage();
            log.error(message, e);
        }

        StatusResponse response = StatusResponse.newBuilder()
            .setSuccess(success)
            .setMessage(message)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void sendShardFragment(SendShardFragmentRequest request, StreamObserver<StatusResponse> responseObserver) {
        int shardId = request.getShardId();
        Map<String, String> fragments = request.getShardFragmentsMap();
        boolean success = true;
        String message = SUCCESS_MESSAGE;

        log.info("Received shard fragment for shard {}. Processing", shardId);

        try {
            if (!nodeStorageService.getStagedShards().containsKey(shardId)) {
                throw new NodeException("Staged shard " + shardId + " does not exist");
            }

            nodeStorageService.getStagedShards().get(shardId).getStorage().putAll(fragments);
        } catch (Exception e) {
            success = false;
            message = "ERROR: " + e.getMessage();
            log.error(message, e);
        }

        StatusResponse response = StatusResponse.newBuilder()
            .setSuccess(success)
            .setMessage(message)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
