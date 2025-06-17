package vk.itmo.teamgray.sharded.storage.common.dto;

import java.util.Collections;
import java.util.Map;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.node.client.NodeStatusResponse;
import vk.itmo.teamgray.sharded.storage.node.client.ShardStats;

import static java.util.stream.Collectors.toMap;

public class NodeStatusResponseDTO {
    private NodeState state;

    private Map<Integer, ShardStatsDTO> shardStats;

    private Map<Integer, ShardStatsDTO> stagedShardStats;

    private int applyQueueSize;

    private int rollbackQueueSize;

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    public Map<Integer, ShardStatsDTO> getShardStats() {
        return shardStats;
    }

    public void setShardStats(Map<Integer, ShardStatsDTO> shardStats) {
        this.shardStats = shardStats;
    }

    public Map<Integer, ShardStatsDTO> getStagedShardStats() {
        return stagedShardStats;
    }

    public void setStagedShardStats(Map<Integer, ShardStatsDTO> stagedShardStats) {
        this.stagedShardStats = stagedShardStats;
    }

    public int getApplyQueueSize() {
        return applyQueueSize;
    }

    public void setApplyQueueSize(int applyQueueSize) {
        this.applyQueueSize = applyQueueSize;
    }

    public int getRollbackQueueSize() {
        return rollbackQueueSize;
    }

    public void setRollbackQueueSize(int rollbackQueueSize) {
        this.rollbackQueueSize = rollbackQueueSize;
    }

    public NodeStatusResponse toGrpc() {
        return NodeStatusResponse.newBuilder()
            .setState(state.name())
            .putAllShardStats(shardStatsToGrpc(shardStats))
            .putAllStagedShardStats(shardStatsToGrpc(stagedShardStats))
            .setApplyQueueSize(applyQueueSize)
            .setRollbackQueueSize(rollbackQueueSize)
            .build();
    }

    public static NodeStatusResponseDTO fromGrpc(NodeStatusResponse grpc) {
        NodeStatusResponseDTO dto = new NodeStatusResponseDTO();

        dto.setState(NodeState.valueOf(grpc.getState()));
        dto.setShardStats(shardStatsFromGrpc(grpc.getShardStatsMap()));
        dto.setShardStats(shardStatsFromGrpc(grpc.getStagedShardStatsMap()));
        dto.setApplyQueueSize(grpc.getApplyQueueSize());
        dto.setRollbackQueueSize(grpc.getRollbackQueueSize());

        return dto;
    }

    private Map<Integer, ShardStats> shardStatsToGrpc(Map<Integer, ShardStatsDTO> shardStats) {
        if (shardStats == null) {
            return Collections.emptyMap();
        }

        return shardStats.entrySet().stream()
            .collect(
                toMap(
                    Map.Entry::getKey,
                    it -> it.getValue().toGrpc()
                )
            );
    }

    private static Map<Integer, ShardStatsDTO> shardStatsFromGrpc(Map<Integer, ShardStats> shardStats) {
        return shardStats.entrySet().stream()
            .collect(
                toMap(
                    Map.Entry::getKey,
                    it -> ShardStatsDTO.fromGrpc(it.getValue())
                )
            );
    }
}
