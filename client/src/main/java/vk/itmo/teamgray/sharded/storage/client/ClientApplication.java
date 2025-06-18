package vk.itmo.teamgray.sharded.storage.client;

import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.client.proto.MasterGrpcClient;
import vk.itmo.teamgray.sharded.storage.client.proto.NodeGrpcClient;
import vk.itmo.teamgray.sharded.storage.client.service.ClientService;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.proto.DiscoveryGrpcClient;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getDiscoverableService;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        ClientCachingFactory clientCachingFactory = ClientCachingFactory
            .getInstance();

        clientCachingFactory.registerClientCreator(DiscoveryClient.class, DiscoveryGrpcClient::new);
        clientCachingFactory.registerClientCreator(MasterClient.class, MasterGrpcClient::new);
        clientCachingFactory.registerClientCreator(NodeClient.class, NodeGrpcClient::new);

        DiscoveryClient discoveryClient = clientCachingFactory.getClient(
            getServerHost("discovery"),
            getServerPort("discovery"),
            DiscoveryClient.class
        );

        //TODO Later register individual clients
        discoveryClient.register(getDiscoverableService());

        MasterClient masterClient = clientCachingFactory
            .getClient(
                discoveryClient.getMasterWithRetries(),
                MasterClient.class
            );

        ClientService clientService = new ClientService(masterClient, discoveryClient, clientCachingFactory);

        CLI cli = new CLI(clientService);
        cli.start();
    }
}
