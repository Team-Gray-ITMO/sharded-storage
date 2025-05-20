package vk.itmo.teamgray.sharded.storage.common.health;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.health.HealthServiceGrpc;
import vk.itmo.teamgray.sharded.storage.health.HeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.health.HeartbeatResponse;

public class HealthService extends HealthServiceGrpc.HealthServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(HealthService.class);

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
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
