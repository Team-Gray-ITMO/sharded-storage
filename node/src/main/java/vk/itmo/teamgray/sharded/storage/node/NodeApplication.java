package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientService;
import vk.itmo.teamgray.sharded.storage.node.client.NodeManagementService;
import vk.itmo.teamgray.sharded.storage.node.client.NodeNodeService;
import vk.itmo.teamgray.sharded.storage.node.client.NodeStorageService;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class NodeApplication {
    private static final Logger log = LoggerFactory.getLogger(NodeApplication.class);

    private final Server activeServer;

    public NodeApplication() {
        NodeStorageService nodeStorageService = new NodeStorageService();

        int serverPort = getServerPort("node");
        activeServer = ServerBuilder.forPort(serverPort)
                .addService(new NodeClientService(nodeStorageService))
                .addService(new NodeManagementService(nodeStorageService))
                .addService(new NodeNodeService(nodeStorageService))
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
        if (activeServer != null && !activeServer.isShutdown()){
            activeServer.shutdown();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        NodeApplication storageService = new NodeApplication();
        storageService.start();

        storageService.getActiveServer().awaitTermination();
    }
}
