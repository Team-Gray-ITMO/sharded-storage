package vk.itmo.teamgray.sharded.storage.discovery;

import java.io.IOException;
import vk.itmo.teamgray.sharded.storage.common.health.proto.HealthGrpcService;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcServerRunner;
import vk.itmo.teamgray.sharded.storage.discovery.proto.DiscoveryGrpcService;
import vk.itmo.teamgray.sharded.storage.discovery.service.DiscoveryService;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class DiscoveryApplication {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = getServerPort("discovery");

        GrpcServerRunner serverRunner = GrpcServerRunner.getInstance();

        serverRunner.setPort(port);

        serverRunner.registerService(new DiscoveryGrpcService(new DiscoveryService()));
        serverRunner.registerService(new HealthGrpcService(new HealthService()));

        serverRunner.start();
        serverRunner.getServer().awaitTermination();
    }
}
