package vk.itmo.teamgray.sharded.storage.master.client;

import java.util.List;
import java.util.Map;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;

public interface NodeManagementClient extends Client {
    StatusResponseDTO prepareMove(List<Integer> receiveShardIds, List<Integer> removeShardsIds, int fullShardCount);

    StatusResponseDTO processMove(List<SendShardTaskDTO> sendShards);

    StatusResponseDTO prepareRearrange(Map<Integer, Long> shardToHash, int fullShardCount);

    StatusResponseDTO processRearrange(List<FragmentDTO> fragments, Map<Integer, Integer> relevantNodes);

    StatusResponseDTO applyOperation(Action action);

    StatusResponseDTO rollbackOperation(Action action);
}
