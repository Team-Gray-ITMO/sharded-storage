package vk.itmo.teamgray.sharded.storage.client.client;

import java.util.Map;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;

public interface MasterClient extends Client {
    StatusResponseDTO addServer(int server, boolean forkNewInstance);

    StatusResponseDTO deleteServer(int server);

    //Doing map flipping on the client side to unload master.
    Map<Integer, Integer> getShardToServerMap();

    //Doing map flipping on client side to unload master.
    Map<Long, Integer> getHashToShardMap();

    Map<Integer, NodeState> getServerToState();

    StatusResponseDTO changeShardCount(int newShardCount);
}
