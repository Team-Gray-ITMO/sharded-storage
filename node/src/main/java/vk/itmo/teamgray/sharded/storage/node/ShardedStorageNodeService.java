package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardedStorageNodeService extends ShardedStorageNodeServiceGrpc.ShardedStorageNodeServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageNodeService.class);

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<SetKeyResponse> responseObserver) {
        // TODO: Implement key-value storage logic

        responseObserver.onNext(
            SetKeyResponse.newBuilder()
                .setSuccess(true)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        // TODO: Implement key-value storage logic

        responseObserver.onNext(
            GetKeyResponse.newBuilder()
                .setValue("TEST_VALUE")
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
        // TODO: Implement file import logic

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
