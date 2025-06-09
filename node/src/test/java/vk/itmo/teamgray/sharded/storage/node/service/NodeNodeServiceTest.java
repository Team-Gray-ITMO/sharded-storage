package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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

    //TODO Rewrite tests
    @Disabled
    @Test
    public void sendShardFragment_forExistentShard_shouldReturnSuccessAndSaveFragments() {
        Map<String, String> fragments = new HashMap<>();
        String key = "key6";

        fragments.put(key, "bar");

        int shardId = Objects.requireNonNull(ShardUtils.getShardIdForKey(key, shardCount));

        var response = toDto(rw -> nodeNodeService.sendShardFragment(shardId, fragments, rw));

        assertTrue(response.isSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        assertEquals(shardCount, nodeStorageService.getShards().getShardMap().size());
    }

    //TODO Rewrite tests
    @Disabled
    @Test
    public void sendShardFragment_forNewShard_shouldReturnSuccessAndSaveFragments() {
        Map<String, String> fragments = new HashMap<>();
        String key = "key12";
        fragments.put(key, "bar");

        int shardId = Objects.requireNonNull(ShardUtils.getShardIdForKey(key, shardCount));

        var response = toDto(rw -> nodeNodeService.sendShardFragment(shardId, fragments, rw));

        assertTrue(response.isSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        assertEquals(shardCount, nodeStorageService.getShards().getShardMap().size());
    }
}
