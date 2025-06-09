package vk.itmo.teamgray.sharded.storage.common.health.proto;

import io.grpc.ManagedChannel;
import java.time.Instant;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.health.client.HealthClient;
import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.health.HealthServiceGrpc;
import vk.itmo.teamgray.sharded.storage.health.HeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.health.HeartbeatResponse;

public class HealthGrpcClient extends AbstractGrpcClient<HealthServiceGrpc.HealthServiceBlockingStub> implements HealthClient {
    public HealthGrpcClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, HealthServiceGrpc.HealthServiceBlockingStub> getStubFactory() {
        return HealthServiceGrpc::newBlockingStub;
    }

    @Override
    public HeartbeatResponseDTO heartbeat() {
        HeartbeatResponse response = blockingStub.heartbeat(
            HeartbeatRequest.newBuilder()
                .setTimestamp(Instant.now().toEpochMilli())
                .build()
        );

        return new HeartbeatResponseDTO(
            response.getHealthy(),
            response.getServerTimestamp(),
            response.getStatusMessage()
        );
    }

    @Override
    protected boolean healthCheck(String host, int port) {
        return true;
    }
}
