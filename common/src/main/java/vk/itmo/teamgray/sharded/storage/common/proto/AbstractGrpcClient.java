package vk.itmo.teamgray.sharded.storage.common.proto;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.health.HealthClient;
import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;

import static vk.itmo.teamgray.sharded.storage.common.utils.RetryUtils.retryWithAttempts;

//Should be relatively computationally cheap to create clients every time, given that channels are cached.
public abstract class AbstractGrpcClient<S extends AbstractBlockingStub> {
    protected final S blockingStub;

    protected final String host;

    protected final int port;

    public AbstractGrpcClient(String host, int port) {
        this.host = host;
        this.port = port;

        var stubCreator = CachedGrpcStubCreator.getInstance();

        this.blockingStub = retryWithAttempts(
            3,
            Duration.of(3, ChronoUnit.SECONDS),
            () -> {
                try {
                    if (!healthCheck(host, port)) {
                        return Optional.empty();
                    }

                    var stub = stubCreator.getStub(
                        host,
                        port,
                        getStubFactory()
                    );

                    return Optional.of(stub);
                } catch (Exception e) {
                    return Optional.empty();
                }
            },
            "Could not initialize gRPC client for " + this.getClass().getSimpleName() + " " + host + ":" + port
        );
    }

    protected boolean healthCheck(String host, int port) {
        return heartbeat().healthy();
    }

    public HeartbeatResponseDTO heartbeat() {
        var healthClient = new HealthClient(host, port);

        return healthClient.heartbeat();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    protected abstract Function<ManagedChannel, S> getStubFactory();
}
