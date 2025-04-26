package vk.itmo.teamgray.sharded.storage.common;

import vk.itmo.teamgray.sharded.storage.node.management.MoveFragment;

public record FragmentDTO(int oldShardId, int newShardId, long rangeFrom, long rangeTo) {
    public MoveFragment toGrpc() {
        return MoveFragment.newBuilder()
            .setShardFrom(oldShardId)
            .setShardTo(newShardId)
            .setRangeFrom(rangeFrom)
            .setRangeTo(rangeTo)
            .build();
    }

    public static FragmentDTO fromGrpc(MoveFragment grpc) {
        return new FragmentDTO(
            grpc.getShardFrom(),
            grpc.getShardTo(),
            grpc.getRangeFrom(),
            grpc.getRangeTo()
        );
    }
}
