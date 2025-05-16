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

        MasterClient masterClient = clientFactory
            .getClient(
                discoveryClient.getMasterWithRetries(),
                MasterClient::new
            );

        ClientService clientService = new ClientService(masterClient, discoveryClient);

        CLI cli = new CLI(clientService);
        cli.start();
    }
}
