package vk.itmo.teamgray.sharded.storage.client;

import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getDiscoverableService;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        var clientFactory = GrpcClientCachingFactory
            .getInstance();

        DiscoveryClient discoveryClient = clientFactory.getClient(
            getServerHost("discovery"),
            DiscoveryClient::new
        );

        //TODO Later register individual clients
        discoveryClient.register(getDiscoverableService());

        //TODO: Use cached clients resolving here
        var node = discoveryClient.getNode(1);
        NodeClient nodeClient = clientFactory
            .getClient(
                node,
                NodeClient::new
            );

        MasterClient masterClient = clientFactory
            .getClient(
                discoveryClient.getMasterWithRetries(),
                MasterClient::new
            );

        ClientService clientService = new ClientService(masterClient, nodeClient);

        CLI cli = new CLI(clientService);
        cli.start();
    }
}
