package vk.itmo.teamgray.sharded.storage.common.proto;

public record ClientCacheKey(
    String host,
    int port,
    Class<?> clientClass
) {
    // No-op.
}
