package vk.itmo.teamgray.sharded.storage.master.client.topology;

import vk.itmo.teamgray.sharded.storage.common.dto.ServerDataDTO;

public record ShardNodeMapping(
    int shardId,
    ServerDataDTO node
) {
    // No-op.
}
