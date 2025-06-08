package vk.itmo.teamgray.sharded.storage.node.client;

import java.util.List;
import java.util.Map;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;

public interface NodeNodeClient extends Client {
    StatusResponseDTO sendShard(List<SendShardDTO> shards);

    StatusResponseDTO sendShardFragment(int shardId, Map<String, String> fragmentsToSend);
}
