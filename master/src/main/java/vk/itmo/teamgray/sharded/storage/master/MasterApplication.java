package vk.itmo.teamgray.sharded.storage.master;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.proto.DiscoveryGrpcClient;
import vk.itmo.teamgray.sharded.storage.common.health.proto.HealthGrpcService;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;
import vk.itmo.teamgray.sharded.storage.master.proto.MasterClientGrpcService;
import vk.itmo.teamgray.sharded.storage.master.proto.NodeManagementGrpcClient;
import vk.itmo.teamgray.sharded.storage.master.service.MasterClientService;
import vk.itmo.teamgray.sharded.storage.master.service.topology.TopologyService;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getDiscoverableService;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class MasterApplication {
    private static final Logger log = LoggerFactory.getLogger(MasterApplication.class);

    private final int port;

    private final Server server;

    public MasterApplication(int port) {
        this.port = port;

        var clientCachingFactory = ClientCachingFactory.getInstance();

        clientCachingFactory.registerClientCreator(DiscoveryClient.class, DiscoveryGrpcClient::new);
        clientCachingFactory.registerClientCreator(NodeManagementClient.class, NodeManagementGrpcClient::new);

        DiscoveryClient discoveryClient = clientCachingFactory
            .getClient(
                getServerHost("discovery"),
                getServerPort("discovery"),
                DiscoveryClient.class
            );

        discoveryClient.register(getDiscoverableService());

        var topologyService = new TopologyService(discoveryClient, clientCachingFactory);

        var masterClientService = new MasterClientGrpcService(new MasterClientService(topologyService));

        var healthService = new HealthGrpcService(new HealthService());

        this.server = ServerBuilder.forPort(port)
            .addService(masterClientService)
            .addService(healthService)
            .build();
    }

    public Server getServer() {
        return server;
    }

    public void start() throws IOException {
        server.start();

        log.info("Server started, listening on {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("Shutting down gRPC server");

            MasterApplication.this.stop();

            log.error("Server shut down");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = getServerPort("master");

        MasterApplication storageService = new MasterApplication(port);
        storageService.start();
        storageService.getServer().awaitTermination();
    }
}
