package vk.itmo.teamgray.sharded.storage.discovery;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.health.proto.HealthGrpcService;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.discovery.proto.DiscoveryGrpcService;
import vk.itmo.teamgray.sharded.storage.discovery.service.DiscoveryService;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class DiscoveryApplication {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryApplication.class);

    private final int port;

    private final Server server;

    public DiscoveryApplication(int port) {
        this.port = port;

        var discoveryService = new DiscoveryGrpcService(new DiscoveryService());
        var healthService = new HealthGrpcService(new HealthService());

        this.server = ServerBuilder.forPort(port)
            .addService(discoveryService)
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

            DiscoveryApplication.this.stop();

            log.error("Server shut down");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = getServerPort("discovery");

        DiscoveryApplication storageService = new DiscoveryApplication(port);
        storageService.start();
        storageService.getServer().awaitTermination();
    }
}
