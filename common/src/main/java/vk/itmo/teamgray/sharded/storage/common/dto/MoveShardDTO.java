package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.node.management.MoveShard;

public record MoveShardDTO(
    int shardId,
    int targetServer
) {
    public MoveShard toGrpc() {
        return MoveShard.newBuilder()
            .setShardId(shardId)
            .setTargetServer(targetServer)
            .build();
    }

    public static MoveShardDTO fromGrpc(final MoveShard grpc) {
        return new MoveShardDTO(grpc.getShardId(), grpc.getTargetServer());
    }
}
