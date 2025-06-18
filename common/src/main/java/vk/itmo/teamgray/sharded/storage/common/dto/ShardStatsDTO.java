package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.node.client.ShardStats;

public class ShardStatsDTO {
    private int size;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ShardStats toGrpc() {
        return ShardStats.newBuilder()
            .setSize(size)
            .build();
    }

    public static ShardStatsDTO fromGrpc(ShardStats value) {
        ShardStatsDTO dto = new ShardStatsDTO();
        dto.setSize(value.getSize());
        return dto;
    }
}
