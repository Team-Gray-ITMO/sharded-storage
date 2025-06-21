package vk.itmo.teamgray.sharded.storage.common.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.ClientCacheKey;
import vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class ClientCachingFactory {
    private static final ClientCachingFactory INSTANCE = new ClientCachingFactory();

    private final Map<Class<? extends Client>, ClientCreator<? extends Client>> clientCreatorsMap = new HashMap<>();

    private final Map<ClientCacheKey, Client> clientCache = new ConcurrentHashMap<>();

    private ClientCachingFactory() {
        // No-op.
    }

    public static ClientCachingFactory getInstance() {
        return INSTANCE;
    }

    public <C extends Client> boolean registerClientCreator(
        Class<C> clientClass,
        ClientCreator<? extends C> clientCreator
    ) {
        return clientCreatorsMap.putIfAbsent(clientClass, clientCreator) == null;
    }

    public <C extends Client> C getClient(
        DiscoverableServiceDTO server,
        Class<C> clientClass
    ) {
        String hostName = resolveHostname(server);

        int port = resolvePort(server);

        return getClient(hostName, port, clientClass);
    }

    @SuppressWarnings("unchecked")
    public <C extends Client> C getClient(
        String hostName,
        int port,
        Class<C> clientClass
    ) {
        return (C)clientCache.computeIfAbsent(
            new ClientCacheKey(hostName, port, clientClass),
            k -> getClientCreator(clientClass).create(hostName, port)
        );
    }

    public void clear() {
        clientCache.clear();
    }

    @SuppressWarnings("unchecked")
    private <C extends Client> ClientCreator<C> getClientCreator(Class<C> clientClass) {
        if (!clientCreatorsMap.containsKey(clientClass)) {
            throw new IllegalStateException("Client creator not registered for class: " + clientClass.getName());
        }

        return (ClientCreator<C>)clientCreatorsMap.get(clientClass);
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

    @FunctionalInterface
    public interface ClientCreator<C extends Client> {
        C create(String hostName, int port);
    }
}
