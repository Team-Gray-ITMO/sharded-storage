package vk.itmo.teamgray.sharded.storage.common.proto;

public record GrpcClientCacheKey(
    String host,
    int port,
    Class<?> clientClass
) {
    // No-op.
}
