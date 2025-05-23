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
    public void sendShard(
        SendShardRequest request,
        StreamObserver<StatusResponse> responseObserver
    ) {
        Map<String, String> shard = request.getShardMap();

        boolean success = true;
        String message = SUCCESS_MESSAGE;

        int shardId = request.getShardId();

        log.debug("Received shard {}. Processing", shardId);

        try {
            if (nodeStorageService.getShards().containsShard(shardId)) {
                nodeStorageService.getShards().createShard(shardId);
            }

            shard.forEach(nodeStorageService::set);
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
    public void sendShardFragment(
        SendShardFragmentRequest request,
        StreamObserver<StatusResponse> responseObserver
    ) {
        boolean success = true;
        String message = SUCCESS_MESSAGE;
        int shardId = request.getShardId();

        log.debug("Received fragment for shard {}. Processing", shardId);

        try {
            var stagedShards = nodeStorageService.getStagedShards();

            if (!stagedShards.containsShard(request.getShardId())) {
                throw new NodeException("Staged shard " + request.getShardId() + " does not exist");
            }

            request.getShardFragmentsMap()
                .forEach((key, value) -> {
                    stagedShards.checkKeyForShard(shardId, key);
                    stagedShards.set(key, value);
                });
        } catch (Exception e) {
            success = false;
            message = "ERROR: " + e.getMessage();

            log.error(message, e);
        }

        responseObserver.onNext(
            StatusResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build()
        );

        responseObserver.onCompleted();
    }
}
