package vk.itmo.teamgray.sharded.storage.common.dto;

import java.util.Map;
import vk.itmo.teamgray.sharded.storage.node.node.SendShard;

public record SendShardDTO(
    int shardId,
    Map<String, String> shard
) {
    public SendShard toGrpc() {
        return SendShard.newBuilder()
            .setShardId(shardId)
            .putAllShard(shard)
            .build();
    }
}
