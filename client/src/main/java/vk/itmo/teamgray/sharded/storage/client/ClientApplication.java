package vk.itmo.teamgray.sharded.storage.client;

import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class ClientApplication {

    public static void main(String[] args) {
        NodeClient nodeClient = GrpcClientCachingFactory
            .getInstance()
            .getClient(
                getServerHost("node"),
                getServerPort("node"),
                NodeClient::new
            );

        MasterClient masterClient = GrpcClientCachingFactory
            .getInstance()
            .getClient(
                getServerHost("master"),
                getServerPort("master"),
                MasterClient::new
            );
        ClientService clientService = new ClientService(masterClient, nodeClient);

        CLI cli = new CLI(clientService);
        cli.start();
    }
}
