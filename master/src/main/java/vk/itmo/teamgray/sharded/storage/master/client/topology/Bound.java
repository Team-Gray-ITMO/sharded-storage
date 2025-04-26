package vk.itmo.teamgray.sharded.storage.master.client.topology;

public record Bound(boolean isNew, int shardId, long upperBound) {
    // No-op.
}
