package vk.itmo.teamgray.sharded.storage.client;

import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        //TODO: Use cached clients resolving here
        NodeClient nodeClient = new NodeClient(getServerHost("node"), getServerPort("node"));

        MasterClient masterClient = GrpcClientCachingFactory
            .getInstance()
            .getClient(
                getServerHost("master"),
                getServerPort("master"),
                MasterClient::new
            );

        CLI cli = new CLI(nodeClient, masterClient);
        cli.start();
    }
}
