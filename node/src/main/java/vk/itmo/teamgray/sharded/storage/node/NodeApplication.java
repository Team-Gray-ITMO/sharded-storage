package vk.itmo.teamgray.sharded.storage.node;

import java.io.IOException;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.discovery.proto.DiscoveryGrpcClient;
import vk.itmo.teamgray.sharded.storage.common.health.proto.HealthGrpcService;
import vk.itmo.teamgray.sharded.storage.common.health.service.HealthService;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcServerRunner;
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
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = getServerPort("node");

        NodeStorageService nodeStorageService = new NodeStorageService();

        ClientCachingFactory clientFactory = ClientCachingFactory.getInstance();

        clientFactory.registerClientCreator(DiscoveryClient.class, DiscoveryGrpcClient::new);
        clientFactory.registerClientCreator(NodeNodeClient.class, NodeNodeGrpcClient::new);

        var discoveryClient = clientFactory
            .getClient(
                getServerHost("discovery"),
                getServerPort("discovery"),
                DiscoveryClient.class
            );

        DiscoverableServiceDTO service = getDiscoverableService();

        discoveryClient.register(service);

        GrpcServerRunner serverRunner = GrpcServerRunner.getInstance();

        serverRunner.setPort(port);

        serverRunner.registerService(new NodeClientGrpcService(new NodeClientService(nodeStorageService)));
        serverRunner.registerService(
            new NodeManagementGrpcService(new NodeManagementService(nodeStorageService, discoveryClient, clientFactory)));
        serverRunner.registerService(new NodeNodeGrpcService(new NodeNodeService(nodeStorageService)));
        serverRunner.registerService(new HealthGrpcService(new HealthService()));

        serverRunner.start();
        serverRunner.getServer().awaitTermination();
    }
}
