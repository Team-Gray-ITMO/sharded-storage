package vk.itmo.teamgray.sharded.storage.common.proto;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils;

public class GrpcServerRunner {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerRunner.class);

    // Singleton for injection simplicity.
    private static final GrpcServerRunner INSTANCE = new GrpcServerRunner();

    private Integer port;

    private Server server;

    // Leave this as is. This might be accessed through reflection in tests to inject failpoints.
    private final List<BindableService> services = new ArrayList<>();

    private GrpcServerRunner() {
        // No-op.
    }

    public static GrpcServerRunner getInstance() {
        return INSTANCE;
    }

    public Server getServer() {
        return server;
    }

    public void setPort(int port) {
        // Lateinit, effectively final.
        if (this.port != null) {
            throw new IllegalStateException("Port is already set.");
        }

        this.port = port;
    }

    public void registerService(BindableService service) {
        services.add(service);
    }

    public void start() throws IOException {
        if (port == null) {
            throw new IllegalStateException("Port is not set. Please set port before starting the server.");
        }

        var serverBuilder = NettyServerBuilder
            .forPort(port)
            .maxInboundMessageSize(PropertyUtils.getMessageMaxSize());

        if (services.isEmpty()) {
            throw new IllegalStateException("No gRPC services registered. Please register at least one service.");
        }

        services.forEach(serverBuilder::addService);

        server = serverBuilder
            .build()
            .start();

        log.info("Server started, listening on {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("Shutting down gRPC server");

            this.stop();

            log.error("Server shut down");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
