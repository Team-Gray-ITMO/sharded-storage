package vk.itmo.teamgray.sharded.storage.common.proto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class GrpcClientCachingFactory {
    private static final GrpcClientCachingFactory INSTANCE = new GrpcClientCachingFactory();

    private final Map<GrpcClientCacheKey, AbstractGrpcClient<?>> clientCache = new ConcurrentHashMap<>();

    private GrpcClientCachingFactory() {
        // No-op.
    }

    public static GrpcClientCachingFactory getInstance() {
        return INSTANCE;
    }

    public <C extends AbstractGrpcClient<?>> C getClient(
        DiscoverableServiceDTO server,
        BiFunction<String, Integer, C> clientCreator
    ) {
        String hostName = resolveHostname(server);

        int port = resolvePort(server);

        return getClient(hostName, port, clientCreator);
    }

    @SuppressWarnings("unchecked")
    public <C extends AbstractGrpcClient<?>> C getClient(
        String hostName,
        int port,
        BiFunction<String, Integer, C> clientCreator
    ) {
        return (C)clientCache.computeIfAbsent(
            new GrpcClientCacheKey(hostName, port, clientCreator.getClass()),
            k -> clientCreator.apply(hostName, port)
        );
    }

    public void clear() {
        clientCache.clear();
    }

    private int resolvePort(DiscoverableServiceDTO targetServer) {
        switch (targetServer.type()) {
            case MASTER -> {
                return getServerPort("master");
            }
            case NODE -> {
                var nodePort = getServerPort("node");

                var thisServer = PropertyUtils.getDiscoverableService();

                if (!thisServer.isDockerized() && targetServer.isDockerized()) {
                    return replaceThirdDigit(targetServer, nodePort);
                } else {
                    return getServerPort("node");
                }
            }
            case CLIENT -> throw new UnsupportedOperationException("Clients are not supported");
            default -> throw new IllegalStateException("Unexpected value: " + targetServer.type());
        }
    }

    // Uncool, but I don't see other options to distinct node replicas from outside docker.
    private static int replaceThirdDigit(DiscoverableServiceDTO targetServer, int nodePort) {
        String basePortStr = String.valueOf(nodePort);

        if (basePortStr.length() < 3) {
            throw new IllegalStateException("Base port too short to replace third digit: " + basePortStr);
        }

        return Integer.parseInt(
            basePortStr.substring(0, 2)
                + targetServer.id()
                + basePortStr.substring(3)
        );
    }

    private String resolveHostname(DiscoverableServiceDTO targetServer) {
        var thisServer = PropertyUtils.getDiscoverableService();

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
                // Native -> Docker = use localhost + port mapping in case of NODE
                return "localhost";
            } else {
                // Native -> Native = use hostname
                return targetServer.host();
            }
        }
    }
}
