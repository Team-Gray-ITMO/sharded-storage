package vk.itmo.teamgray.sharded.storage.common.proto;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import java.util.function.Function;

//Should be relatively computationally cheap to create clients every time, given that channels are cached.
public abstract class AbstractGrpcClient<S extends AbstractBlockingStub> {
    protected final S blockingStub;

    private final String host;

    private final int port;

    public AbstractGrpcClient(String host, int port) {
        this.host = host;
        this.port = port;

        var stubCreator = CachedGrpcStubCreator.getInstance();

        this.blockingStub = stubCreator.getStub(
            host,
            port,
            getStubFactory()
        );
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    protected abstract Function<ManagedChannel, S> getStubFactory();
}
