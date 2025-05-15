package vk.itmo.teamgray.sharded.storage.common.proto;

public record GrpcClientCacheKey(
    String host,
    Class<?> clientClass
) {
    // No-op.
}
