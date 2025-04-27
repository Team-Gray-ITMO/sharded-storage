package vk.itmo.teamgray.sharded.storage.common.proto;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class CachedGrpcStubCreator {
    private static final CachedGrpcStubCreator INSTANCE = new CachedGrpcStubCreator();

    private final Map<String, ManagedChannel> channelMap = new ConcurrentHashMap<>();

    private CachedGrpcStubCreator() {
        // private constructor
    }

    public static CachedGrpcStubCreator getInstance() {
        return INSTANCE;
    }

    public <S> S getStub(String host, int port, Function<ManagedChannel, S> stubFactory) {
        String key = host + ":" + port;

        ManagedChannel channel = channelMap.computeIfAbsent(key, k -> ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build());

        return stubFactory.apply(channel);
    }

    public void shutdownAll() {
        for (ManagedChannel channel : channelMap.values()) {
            channel.shutdownNow();
        }
        channelMap.clear();
    }
}
