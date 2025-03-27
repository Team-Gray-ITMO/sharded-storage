package vk.itmo.teamgray.sharded.storage.master;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static vk.itmo.teamgray.sharded.storage.common.Utils.getServerPort;

public class MasterApplication {
    private static final Logger log = LoggerFactory.getLogger(MasterApplication.class);

    private final int port;

    private final Server server;

    public MasterApplication(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
            .addService(new ShardedStorageMasterService())
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
