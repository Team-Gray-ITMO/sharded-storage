package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.node.management.PrepareRequest;

import java.util.Map;

public record PrepareRequestDTO (Map<Integer, Long> shardToHash, int fullShardCount) {

    public PrepareRequest toGrpc() {
        return PrepareRequest.newBuilder()
                .setFullShardCount(fullShardCount)
                .putAllShardToHash(shardToHash)
                .build();
    }

    public static PrepareRequestDTO fromGrpc(PrepareRequest grpc) {
        return new PrepareRequestDTO(
                grpc.getShardToHashMap(),
                grpc.getFullShardCount()
        );
    }

}
