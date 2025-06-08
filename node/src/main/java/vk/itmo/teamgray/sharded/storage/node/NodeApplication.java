package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.discovery.proto.DiscoveryGrpcClient;
import vk.itmo.teamgray.sharded.storage.common.health.proto.HealthGrpcService;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.node.client.NodeNodeClient;
import vk.itmo.teamgray.sharded.storage.node.proto.NodeClientGrpcService;
import vk.itmo.teamgray.sharded.storage.node.proto.NodeManagementGrpcService;
import vk.itmo.teamgray.sharded.storage.node.proto.NodeNodeGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.proto.NodeNodeGrpcService;
import vk.itmo.teamgray.sharded.storage.node.service.NodeClientService;
import vk.itmo.teamgray.sharded.storage.node.service.NodeManagementService;
import vk.itmo.teamgray.sharded.storage.node.service.NodeNodeService;
import vk.itmo.teamgray.sharded.storage.node.service.NodeStorageService;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getDiscoverableService;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class NodeApplication {
    private static final Logger log = LoggerFactory.getLogger(NodeApplication.class);

    private final Server activeServer;

    public NodeApplication() {
        NodeStorageService nodeStorageService = new NodeStorageService();

        int serverPort = getServerPort("node");

        ClientCachingFactory clientCachingFactory = ClientCachingFactory.getInstance();

        clientCachingFactory.registerClientCreator(DiscoveryClient.class, DiscoveryGrpcClient::new);
        clientCachingFactory.registerClientCreator(NodeNodeClient.class, NodeNodeGrpcClient::new);

        var discoveryClient = clientCachingFactory
            .getClient(
                getServerHost("discovery"),
                getServerPort("discovery"),
                DiscoveryClient.class
            );

        DiscoverableServiceDTO service = getDiscoverableService();

        discoveryClient.register(service);

        activeServer = ServerBuilder.forPort(serverPort)
            .addService(new NodeClientGrpcService(new NodeClientService(nodeStorageService)))
            .addService(new NodeManagementGrpcService(new NodeManagementService(nodeStorageService, discoveryClient, clientCachingFactory)))
            .addService(new NodeNodeGrpcService(new NodeNodeService(nodeStorageService)))
            .addService(new HealthGrpcService(new HealthService()))
            .build();
    }

    public Server getActiveServer() {
        return activeServer;
    }

    public void start() throws IOException {
        activeServer.start();

        log.info("Server started, listening on {}", activeServer.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("Shutting down gRPC server");

            NodeApplication.this.stop();

            log.error("Server shut down");
        }));
    }

    public void stop() {
        if (activeServer != null && !activeServer.isShutdown()) {
            activeServer.shutdown();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        NodeApplication nodeApplication = new NodeApplication();
        nodeApplication.start();

        nodeApplication.getActiveServer().awaitTermination();
    }
}
