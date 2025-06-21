package vk.itmo.teamgray.sharded.storage.master;

import java.io.IOException;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.proto.DiscoveryGrpcClient;
import vk.itmo.teamgray.sharded.storage.common.health.proto.HealthGrpcService;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcServerRunner;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;
import vk.itmo.teamgray.sharded.storage.master.proto.MasterClientGrpcService;
import vk.itmo.teamgray.sharded.storage.master.proto.NodeManagementGrpcClient;
import vk.itmo.teamgray.sharded.storage.master.service.MasterClientService;
import vk.itmo.teamgray.sharded.storage.master.service.topology.TopologyService;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getDiscoverableService;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class MasterApplication {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = getServerPort("master");

        var clientFactory = ClientCachingFactory.getInstance();

        clientFactory.registerClientCreator(DiscoveryClient.class, DiscoveryGrpcClient::new);
        clientFactory.registerClientCreator(NodeManagementClient.class, NodeManagementGrpcClient::new);

        DiscoveryClient discoveryClient = clientFactory
            .getClient(
                getServerHost("discovery"),
                getServerPort("discovery"),
                DiscoveryClient.class
            );

        discoveryClient.register(getDiscoverableService());

        GrpcServerRunner serverRunner = GrpcServerRunner.getInstance();

        serverRunner.setPort(port);

        serverRunner.registerService(
            new MasterClientGrpcService(new MasterClientService(new TopologyService(discoveryClient, clientFactory))));
        serverRunner.registerService(new HealthGrpcService(new HealthService()));

        serverRunner.start();
        serverRunner.getServer().awaitTermination();
    }
}
