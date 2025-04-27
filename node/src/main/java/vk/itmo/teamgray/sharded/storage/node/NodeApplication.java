package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientService;
import vk.itmo.teamgray.sharded.storage.node.client.NodeManagementService;
import vk.itmo.teamgray.sharded.storage.node.client.NodeStorageService;

import static vk.itmo.teamgray.sharded.storage.common.PropertyUtils.getServerPort;

public class NodeApplication {
    private static final Logger log = LoggerFactory.getLogger(NodeApplication.class);

    private final List<Server> activeServers = new ArrayList<>();

    public NodeApplication() {
        NodeStorageService nodeStorageService = new NodeStorageService();

        activeServers.add(
            ServerBuilder.forPort(getServerPort("node"))
                .addService(new NodeClientService(nodeStorageService))
                .build()
        );

        activeServers.add(
            ServerBuilder.forPort(getServerPort("node.management"))
                // Add actual node-node client with server resolving
                .addService(new NodeManagementService(nodeStorageService, null))
                .build()
        );
    }

    public List<Server> getActiveServers() {
        return activeServers;
    }

    public void start() throws IOException {
        for (Server server : activeServers) {
            server.start();

            log.info("Server started, listening on {}", server.getPort());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("Shutting down gRPC server");

            NodeApplication.this.stop();

            log.error("Server shut down");
        }));
    }

    public void stop() {
        activeServers.stream()
            .filter(server -> server != null && !server.isShutdown())
            .forEach(Server::shutdown);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        NodeApplication storageService = new NodeApplication();
        storageService.start();

        for (Server server : storageService.getActiveServers()) {
            server.awaitTermination();
        }
    }
}
