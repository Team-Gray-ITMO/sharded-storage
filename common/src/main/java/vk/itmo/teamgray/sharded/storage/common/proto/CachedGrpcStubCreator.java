package vk.itmo.teamgray.sharded.storage.common.proto;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedGrpcStubCreator {
    private static final Logger log = LoggerFactory.getLogger(CachedGrpcStubCreator.class);

    private static final CachedGrpcStubCreator INSTANCE = new CachedGrpcStubCreator();

    private final Map<String, ManagedChannel> channelMap = new ConcurrentHashMap<>();

    private CachedGrpcStubCreator() {
        // private constructor
    }

    public static CachedGrpcStubCreator getInstance() {
        return INSTANCE;
    }

    public <S> S getStub(String host, int port, Function<ManagedChannel, S> stubFactory, String fallbackHost) {
        String key = host + ":" + port;

        ManagedChannel channel = channelMap.computeIfAbsent(key, k -> createChannelWithFallback(host, port, fallbackHost));

        return stubFactory.apply(channel);
    }

    // TODO Remove, absolutely uncool, replace with something more robust later.
    private ManagedChannel createChannelWithFallback(String host, int port, String fallbackHost) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        try {
            channel.getState(true);
        } catch (Exception e) {
            log.warn("Could not connect via {}:{}, falling back to {}:{}", host, port, fallbackHost, port, e);

            channel.shutdownNow();

            channel = ManagedChannelBuilder.forAddress(fallbackHost, port)
                .usePlaintext()
                .build();
        }

        return channel;
    }

    public void shutdownAll() {
        for (ManagedChannel channel : channelMap.values()) {
            channel.shutdownNow();
        }
        channelMap.clear();
    }
}
