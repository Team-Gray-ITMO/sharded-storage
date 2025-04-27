package vk.itmo.teamgray.sharded.storage.common.proto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class GrpcClientCachingFactory {
    private static final GrpcClientCachingFactory INSTANCE = new GrpcClientCachingFactory();

    private final Map<String, AbstractGrpcClient<?>> clientCache = new ConcurrentHashMap<>();

    private GrpcClientCachingFactory() {
    }

    public static GrpcClientCachingFactory getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    public <C extends AbstractGrpcClient<?>> C getClient(
        String host,
        int port,
        BiFunction<String, Integer, C> clientCreator
    ) {
        String key = clientKey(host, port, clientCreator.getClass());

        return (C)clientCache.computeIfAbsent(key, k -> clientCreator.apply(host, port));
    }

    private String clientKey(String host, int port, Class<?> clientClass) {
        return host + ":" + port + ":" + clientClass.getName();
    }

    public void clear() {
        clientCache.clear();
    }
}
