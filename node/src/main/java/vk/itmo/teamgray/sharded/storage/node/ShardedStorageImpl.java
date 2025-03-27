package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.*;

public class ShardedStorageImpl extends ShardedStorageGrpc.ShardedStorageImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageImpl.class);

    private Map<String, String> storage = new ConcurrentHashMap<>();

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
        boolean success = true;
        try {
            storage.put(request.getKey(), request.getValue());
        } catch (Exception e) {
            success = false;
        } finally {
            responseObserver.onNext(
                SetKeyResponse.newBuilder()
                    .setSuccess(success)
                    .build()
            );
            responseObserver.onCompleted();
        }
    }

    @Override
    public void containsKey(ContainsKeyRequest request, StreamObserver<ContainsKeyResponse> responseObserver) {
        responseObserver.onNext(
            ContainsKeyResponse.newBuilder()
                .setResult(storage.containsKey(request.getKey()))
                .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void getValueByKey(GetValueByKeyRequest request, StreamObserver<GetValueByKeyResponse> responseObserver) {
        boolean success = true;
        var value = "";
        
        try {
            value = storage.get(request.getKey());
        } catch (NullPointerException ex) {
            success = false;
        } finally {
            responseObserver.onNext(
                GetValueByKeyResponse.newBuilder()
                    .setSuccess(success)
                    .setValue(value)
                    .build()
            );
            responseObserver.onCompleted();
        }
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
