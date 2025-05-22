package vk.itmo.teamgray.sharded.storage.master.service.topology;

public record Bound(boolean isNew, int shardId, long upperBound) {
    // No-op.
}
