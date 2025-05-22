package vk.itmo.teamgray.sharded.storage.master.service.topology;

public record ShardNodeMapping(
    int shardId,
    int serverId
) {
    // No-op.
}
