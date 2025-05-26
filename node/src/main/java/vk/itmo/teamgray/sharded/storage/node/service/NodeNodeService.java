package vk.itmo.teamgray.sharded.storage.node.service;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShard;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardsRequest;

// TODO Decouple to gRPC Service and Service with business logic. Example: 'HealthGrpcService' and 'HealthService'
public class NodeNodeService extends NodeNodeServiceGrpc.NodeNodeServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeNodeService.class);

    private static final String SUCCESS_MESSAGE = "SUCCESS";

    private final NodeStorageService nodeStorageService;

    public NodeNodeService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void sendShards(
        SendShardsRequest request,
        StreamObserver<StatusResponse> responseObserver
    ) {
        String message = SUCCESS_MESSAGE;

        //TODO Do this on addServer stage instead.
        int fullShardCount = request.getFullShardCount();

        log.debug("Received shards {}. Processing", request.getShardsList().stream().map(SendShard::getShardId).toList());

        List<String> errorMessages = request.getShardsList().stream()
            .map(sendShard -> {
                int shardId = sendShard.getShardId();
                Map<String, String> shard = sendShard.getShardMap();

                try {
                    if (!nodeStorageService.getShards().containsShard(shardId)) {
                        nodeStorageService.getShards().createShard(shardId);
                    }

                    shard.forEach(nodeStorageService::set);

                    return null;
                } catch (Exception e) {
                    log.error("Caught exception: ", e);

                    return "ERROR: " + e.getMessage();
                }
            })
            .filter(Objects::nonNull)
            .toList();

        StatusResponse response = StatusResponse.newBuilder()
            .setSuccess(errorMessages.isEmpty())
            .setMessage(
                errorMessages.isEmpty()
                    ? message
                    : StringUtil.join(System.lineSeparator(), errorMessages).toString()
            )
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
