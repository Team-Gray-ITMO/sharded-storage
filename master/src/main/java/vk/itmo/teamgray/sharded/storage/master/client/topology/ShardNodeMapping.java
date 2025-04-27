package vk.itmo.teamgray.sharded.storage.master.client.topology;

import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;

public record ShardNodeMapping(
    int shardId,
    ServerDataDTO node
) {
    // No-op.
}
