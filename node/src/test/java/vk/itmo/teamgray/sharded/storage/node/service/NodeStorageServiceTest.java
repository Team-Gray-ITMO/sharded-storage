package vk.itmo.teamgray.sharded.storage.node.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.NodeStatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeStorageServiceTest {
    private final NodeStorageService service = new NodeStorageService();

    private final String testKey = "key";

    private final String testValue = "value";

    private final Instant testTimestamp = Instant.now();

    @BeforeEach
    void setUp() {
        ShardsContainer shards = service.getShards();
        shards.setFullShardCount(1);
        shards.getShardMap().put(1, new ShardData());

        service.changeState(service.getState(), NodeState.INIT);
    }

    @Test
    void setSuccessInRunningState() {
        service.changeState(NodeState.INIT, NodeState.RUNNING);

        SetResponseDTO response = service.set(testKey, testValue, testTimestamp);

        assertThat(response.status()).isEqualTo(SetStatus.SUCCESS);
    }

    @Test
    void getReturnsSetValue() {
        service.changeState(NodeState.INIT, NodeState.RUNNING);
        service.set(testKey, testValue, testTimestamp);

        String result = service.get(testKey);

        assertThat(result).isEqualTo(testValue);
    }

    @Test
    void stateTransitions() {
        service.changeState(NodeState.INIT, NodeState.RUNNING);
        assertThat(service.getState()).isEqualTo(NodeState.RUNNING);

        service.changeState(NodeState.RUNNING, NodeState.MOVE_SHARDS_PROCESSING);
        assertThat(service.getState()).isEqualTo(NodeState.MOVE_SHARDS_PROCESSING);
    }

    @Test
    void getNodeStatusReturnsCurrentState() {
        service.changeState(NodeState.INIT, NodeState.RUNNING);

        NodeStatusResponseDTO status = service.getNodeStatus();

        assertThat(status.getState()).isEqualTo(NodeState.RUNNING);
    }

    @Test
    void setInRunningState() {
        service.changeState(NodeState.INIT, NodeState.RUNNING);

        SetResponseDTO response = service.set(testKey, testValue, testTimestamp);

        assertThat(response.status()).isEqualTo(SetStatus.SUCCESS);
    }

    @Test
    void setDuringPreparePhase() {
        service.changeState(NodeState.INIT, NodeState.REARRANGE_SHARDS_PREPARING);

        SetResponseDTO response = service.set(testKey, testValue, testTimestamp);

        assertThat(response.status()).isEqualTo(SetStatus.SUCCESS);
    }

    @Test
    void setQueuesForStagedShards() {
        service.changeState(NodeState.INIT, NodeState.MOVE_SHARDS_PREPARING);

        service.stageShards(service.getShards().getShardMap(), service.getShards().getFullShardCount());

        service.changeState(NodeState.MOVE_SHARDS_PREPARING, NodeState.MOVE_SHARDS_PREPARED);
        service.changeState(NodeState.MOVE_SHARDS_PREPARED, NodeState.MOVE_SHARDS_PROCESSING);
        service.stageShards(Map.of(0, new ShardData()), 1);

        SetResponseDTO response = service.set(testKey, testValue, testTimestamp);

        assertThat(response.status()).isEqualTo(SetStatus.QUEUED);
    }

    @Test
    void setTransfersDuringResharding() {
        service.changeState(NodeState.INIT, NodeState.REARRANGE_SHARDS_PREPARING);

        var stagedShards = new ConcurrentHashMap<Integer, ShardData>();
        stagedShards.put(0, new ShardData());

        service.prepareDataForResharding(
            List.of(new FragmentDTO(0, 1, 0L, Long.MAX_VALUE)),
            Map.of(1, 3), // Shard 1 -> Node 3
            stagedShards,
            2);
        service.changeState(NodeState.REARRANGE_SHARDS_PREPARING, NodeState.REARRANGE_SHARDS_PREPARED);
        service.changeState(NodeState.REARRANGE_SHARDS_PREPARED, NodeState.REARRANGE_SHARDS_PROCESSING);

        SetResponseDTO response = service.set(testKey, testValue, testTimestamp);

        assertThat(response.status()).isEqualTo(SetStatus.TRANSFER);
        assertThat(response.newNodeId()).isEqualTo(3);
    }

    @Test
    void setFailsInDeadState() {
        service.changeState(NodeState.INIT, NodeState.DEAD);

        assertThrows(NodeException.class, () ->
            service.set(testKey, testValue, testTimestamp)
        );
    }

    @Test
    void setFailsDuringProcessingPhase() {
        service.stageShards(service.getShards().getShardMap(), 1);

        service.changeState(NodeState.INIT, NodeState.REARRANGE_SHARDS_PREPARING);
        service.changeState(NodeState.REARRANGE_SHARDS_PREPARING, NodeState.REARRANGE_SHARDS_PREPARED);
        service.changeState(NodeState.REARRANGE_SHARDS_PREPARED, NodeState.REARRANGE_SHARDS_PROCESSING);

        assertThrows(NodeException.class, () ->
            service.set(testKey, testValue, testTimestamp)
        );
    }
}
