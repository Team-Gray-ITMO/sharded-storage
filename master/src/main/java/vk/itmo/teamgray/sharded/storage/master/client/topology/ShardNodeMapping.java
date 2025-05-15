package vk.itmo.teamgray.sharded.storage.master.client.topology;

public record ShardNodeMapping(
    int shardId,
    int serverId
) {
    // No-op.
}
