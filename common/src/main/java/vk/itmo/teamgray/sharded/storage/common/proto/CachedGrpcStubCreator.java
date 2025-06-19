package vk.itmo.teamgray.sharded.storage.common.proto;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.utils.MemoryUtils;

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

        ManagedChannel channel = channelMap.computeIfAbsent(
            key,
            k -> NettyChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(4 * MemoryUtils.MEBIBYTE)
                .flowControlWindow(65535)
                .keepAliveWithoutCalls(true)
                .usePlaintext()
                .build()
        );

        return stubFactory.apply(channel);
    }

    public void shutdownAll() {
        for (ManagedChannel channel : channelMap.values()) {
            channel.shutdownNow();
        }
        channelMap.clear();
    }
}
