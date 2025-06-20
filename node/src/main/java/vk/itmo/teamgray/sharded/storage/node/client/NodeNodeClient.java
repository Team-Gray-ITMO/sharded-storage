package vk.itmo.teamgray.sharded.storage.node.client;

import java.util.List;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;

public interface NodeNodeClient extends Client {
    StatusResponseDTO sendShardEntries(List<SendShardDTO> shards, Action action);
}
