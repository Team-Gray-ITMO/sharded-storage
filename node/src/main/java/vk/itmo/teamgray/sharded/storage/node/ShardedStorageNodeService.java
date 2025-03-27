package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.HashUtils;
import vk.itmo.teamgray.sharded.storage.node.shards.ShardData;

public class ShardedStorageNodeService extends ShardedStorageNodeServiceGrpc.ShardedStorageNodeServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageNodeService.class);

    private Map<Integer, ShardData> shards = new ConcurrentHashMap<>();

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<SetKeyResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();

        shards.computeIfAbsent(
                HashUtils.hashKey(key),
                k -> new ShardData()
        ).addToStorage(key, value);

        responseObserver.onNext(
            SetKeyResponse.newBuilder()
                .setSuccess(true)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        String key = request.getKey();

        ShardData shardData = shards.get(HashUtils.hashKey(key));
        String returnValue = null;
        if (shardData != null) {
            returnValue = shardData.getValue(key);
        }

        responseObserver.onNext(
            GetKeyResponse.newBuilder()
                .setValue(returnValue)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void setFromFile(SetFromFileRequest request, StreamObserver<SetFromFileResponse> responseObserver) {
        // TODO: Implement file import logic

        responseObserver.onNext(
            SetFromFileResponse.newBuilder()
                .setSuccess(true)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(NodeHeartbeatRequest request, StreamObserver<NodeHeartbeatResponse> responseObserver) {
        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(NodeHeartbeatResponse.newBuilder()
            .setHealthy(isHealthy)
            .setServerTimestamp(now.toEpochMilli())
            .setStatusMessage("OK")
            .build());

        responseObserver.onCompleted();
    }
}
