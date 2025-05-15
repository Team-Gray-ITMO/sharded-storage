package vk.itmo.teamgray.sharded.storage.common.proto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils;

public class GrpcClientCachingFactory {
    private static final GrpcClientCachingFactory INSTANCE = new GrpcClientCachingFactory();

    private final Map<GrpcClientCacheKey, AbstractGrpcClient<?>> clientCache = new ConcurrentHashMap<>();

    private GrpcClientCachingFactory() {
    }

    public static GrpcClientCachingFactory getInstance() {
        return INSTANCE;
    }

    public <C extends AbstractGrpcClient<?>> C getClient(
        DiscoverableServiceDTO server,
        Function<String, C> clientCreator
    ) {
        String hostName = resolveHostname(server);

        return getClient(hostName, clientCreator);
    }

    @SuppressWarnings("unchecked")
    public <C extends AbstractGrpcClient<?>> C getClient(
        String hostName,
        Function<String, C> clientCreator
    ) {
        return (C)clientCache.computeIfAbsent(
            new GrpcClientCacheKey(hostName, clientCreator.getClass()),
            k -> clientCreator.apply(hostName)
        );
    }

    private String resolveHostname(DiscoverableServiceDTO targetServer) {
        var thisServer = PropertyUtils.getDiscoverableService(); // your own info

        boolean thisDocker = thisServer.isDockerized();
        boolean targetDocker = targetServer.isDockerized();

        if (thisDocker) {
            if (targetDocker) {
                // Docker -> Docker = use containerName
                return targetServer.containerName();
            } else {
                // Docker -> Native = not allowed for now.
                throw new IllegalStateException("Dockerized service cannot access native service directly");
            }
        } else {
            if (targetDocker) {
                // Native -> Docker = use host.docker.internal
                return "host.docker.internal"; //TODO Maybe localhost
            } else {
                // Native -> Native = use hostname
                return targetServer.host();
            }
        }
    }

    public void clear() {
        clientCache.clear();
    }
}
