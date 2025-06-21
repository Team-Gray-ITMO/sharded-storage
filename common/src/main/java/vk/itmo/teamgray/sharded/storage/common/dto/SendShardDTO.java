package vk.itmo.teamgray.sharded.storage.common.dto;

import java.util.Map;
import vk.itmo.teamgray.sharded.storage.node.node.SendShard;

public record SendShardDTO(
    int shardId,
    Map<String, String> entries
) {
    public SendShard toGrpc() {
        return SendShard.newBuilder()
            .setShardId(shardId)
            .putAllEntries(entries)
            .build();
    }

    public static SendShardDTO fromGrpc(SendShard grpc) {
        return new SendShardDTO(
            grpc.getShardId(),
            grpc.getEntriesMap()
        );
    }
}
