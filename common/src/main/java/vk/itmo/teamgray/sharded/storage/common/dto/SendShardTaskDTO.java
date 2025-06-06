package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.node.management.SendShardTask;

public record SendShardTaskDTO(
    int shardId,
    int targetServer
) {
    public SendShardTask toGrpc() {
        return SendShardTask.newBuilder()
            .setShardId(shardId)
            .setTargetServer(targetServer)
            .build();
    }

    public static SendShardTaskDTO fromGrpc(final SendShardTask grpc) {
        return new SendShardTaskDTO(grpc.getShardId(), grpc.getTargetServer());
    }
}
