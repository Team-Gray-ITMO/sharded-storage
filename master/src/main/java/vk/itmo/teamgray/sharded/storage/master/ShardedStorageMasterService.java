package vk.itmo.teamgray.sharded.storage.master;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardedStorageMasterService extends ShardedStorageMasterServiceGrpc.ShardedStorageMasterServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageMasterService.class);

    @Override
    public void addServer(AddServerRequest request, StreamObserver<AddServerResponse> responseObserver) {
        // TODO: Implement actual server addition logic

        responseObserver.onNext(
            AddServerResponse.newBuilder()
                .setSuccess(true)
                .setMessage("")
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void deleteServer(DeleteServerRequest request, StreamObserver<DeleteServerResponse> responseObserver) {
        // TODO: Implement actual server deletion logic

        responseObserver.onNext(
            DeleteServerResponse.newBuilder()
                .setSuccess(true)
                .setMessage("")
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void changeShardCount(ChangeShardCountRequest request, StreamObserver<ChangeShardCountResponse> responseObserver) {
        // TODO: Implement shard count change logic

        responseObserver.onNext(
            ChangeShardCountResponse.newBuilder()
                .setSuccess(true)
                .setMessage("")
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(MasterHeartbeatRequest request, StreamObserver<MasterHeartbeatResponse> responseObserver) {
        // TODO: Implement file import logic

        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(MasterHeartbeatResponse.newBuilder()
            .setHealthy(isHealthy)
            .setServerTimestamp(now.toEpochMilli())
            .setStatusMessage("OK")
            .build());

        responseObserver.onCompleted();
    }
}
