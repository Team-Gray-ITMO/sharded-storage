package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.toDto;

public class NodeNodeServiceTest {
    private NodeNodeService nodeNodeService;

    private NodeStorageService nodeStorageService;

    private int shardCount = 3;

    @BeforeEach
    public void setUp() {
        Map<Integer, ShardData> shards = IntStream.range(0, shardCount)
            .boxed()
            .collect(Collectors.toMap(
                it -> it,
                it -> new ShardData()
            ));

        nodeStorageService = new NodeStorageService();
        nodeStorageService.stageShards(shards, shardCount);
        nodeStorageService.swapWithStaged();

        nodeNodeService = new NodeNodeService(nodeStorageService);
    }

    @Test
    public void sendShardFragment_forStageShard_shouldReturnSuccessAndSaveFragments() {
        // create staged shards
        Map<Integer, ShardData> stagedShards = IntStream.range(0, shardCount)
            .boxed()
            .collect(Collectors.toMap(
                it -> it,
                it -> new ShardData()
            ));
        nodeStorageService.stageShards(stagedShards, shardCount);

        // key for stage shards
        String key = "key6";
        String value = "bar";
        int shardId = Objects.requireNonNull(ShardUtils.getShardIdForKey(key, shardCount));

        Map<String, String> fragments = new HashMap<>();
        fragments.put(key, value);
        var response =
            toDto(rw -> nodeNodeService.sendShardEntries(Action.REARRANGE_SHARDS, List.of(new SendShardDTO(shardId, fragments)), rw));

        assertTrue(response.isSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        // Data must be in staged entries
        assertEquals(value, nodeStorageService.getStagedShards().getShardMap().get(shardId).getValue(key));
    }

    @Test
    public void sendShards_shouldReturnSuccessAndSaveAllShards() {
        // create staged shards
        Map<Integer, ShardData> stagedShards = IntStream.range(0, shardCount)
            .boxed()
            .collect(Collectors.toMap(
                it -> it,
                it -> new ShardData()
            ));
        nodeStorageService.stageShards(stagedShards, shardCount);

        // prepare data for multiple shards
        Map<String, String> data1 = Map.of("keyA", "valA");
        Map<String, String> data2 = Map.of("keyB", "valB");
        int shardId1 = 0;
        int shardId2 = 1;

        var sendShards = List.of(
            new SendShardDTO(shardId1, data1),
            new SendShardDTO(shardId2, data2)
        );

        var response = toDto(rw -> nodeNodeService.sendShardEntries(Action.MOVE_SHARDS, sendShards, rw));

        assertTrue(response.isSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        // data must be in the correct staged shards
        assertEquals("valA", nodeStorageService.getStagedShards().getShardMap().get(shardId1).getValue("keyA"));
        assertEquals("valB", nodeStorageService.getStagedShards().getShardMap().get(shardId2).getValue("keyB"));
    }
}
