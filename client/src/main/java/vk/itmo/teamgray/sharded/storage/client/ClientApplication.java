package vk.itmo.teamgray.sharded.storage.client;

import static vk.itmo.teamgray.sharded.storage.common.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.PropertyUtils.getServerPort;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        NodeClient nodeClient = new NodeClient(getServerHost("node"), getServerPort("node"));
        MasterClient masterClient = new MasterClient(getServerHost("master"), getServerPort("master"));

        ClientService clientService = new ClientService(masterClient, nodeClient);

        //TODO: Test logic to check gRPC, later remove
        clientService.scheduleHeartbeat();
    }
}
