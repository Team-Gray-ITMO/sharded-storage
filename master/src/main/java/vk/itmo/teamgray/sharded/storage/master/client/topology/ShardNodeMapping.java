package vk.itmo.teamgray.sharded.storage.master.client.topology;

import vk.itmo.teamgray.sharded.storage.common.dto.ServerDataDTO;

public record ShardNodeMapping(
    int shardId,
    int serverId
) {
    // No-op.
}
