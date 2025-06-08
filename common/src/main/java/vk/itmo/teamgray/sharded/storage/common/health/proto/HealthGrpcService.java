package vk.itmo.teamgray.sharded.storage.common.health.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.health.HealthServiceGrpc;
import vk.itmo.teamgray.sharded.storage.health.HeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.health.HeartbeatResponse;

// Decoupling example to allow for potential swap from gRPC if required. For heavy responses, use ResponseWriters instead of intermediary objects.
public class HealthGrpcService extends HealthServiceGrpc.HealthServiceImplBase {
    private final HealthService healthService;

    public HealthGrpcService(HealthService healthService) {
        this.healthService = healthService;
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        var builder = HeartbeatResponse.newBuilder();

        healthService.heartbeat(
            request.getTimestamp(),
            (healthy, serverTimestamp, statusMessage) -> {
                builder.setHealthy(healthy);
                builder.setServerTimestamp(serverTimestamp);
                builder.setStatusMessage(statusMessage);
            }
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
