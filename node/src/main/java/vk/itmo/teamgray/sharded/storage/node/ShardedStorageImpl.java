package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.AddServerResponse;
import vk.itmo.teamgray.sharded.storage.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.ChangeShardCountResponse;
import vk.itmo.teamgray.sharded.storage.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.DeleteServerResponse;
import vk.itmo.teamgray.sharded.storage.HeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.HeartbeatResponse;
import vk.itmo.teamgray.sharded.storage.SetFromFileRequest;
import vk.itmo.teamgray.sharded.storage.SetFromFileResponse;
import vk.itmo.teamgray.sharded.storage.SetKeyRequest;
import vk.itmo.teamgray.sharded.storage.SetKeyResponse;
import vk.itmo.teamgray.sharded.storage.ShardedStorageGrpc;

public class ShardedStorageImpl extends ShardedStorageGrpc.ShardedStorageImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageImpl.class);

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
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        // TODO: Implement file import logic

        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(HeartbeatResponse.newBuilder()
            .setHealthy(isHealthy)
            .setServerTimestamp(now.toEpochMilli())
            .setStatusMessage("OK")
            .build());

        responseObserver.onCompleted();
    }
}
