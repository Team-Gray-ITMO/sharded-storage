package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.node.service.NodeStorageService;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShardChangeAvailabilityTest extends BaseIntegrationTest {

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testGetSetAvailabilityDuringShardChange() throws Exception {
        // 1. Start two servers
        int serverId1 = 1;
        int serverId2 = 2;

        orchestrationApi.runNode(serverId1);
        orchestrationApi.runNode(serverId2);

        clientService.addServer(serverId1, false);
        clientService.addServer(serverId2, false);

        // Check that servers are added
        Map<Integer, NodeState> serverStates = clientService.getServerStates();
        assertEquals(2, serverStates.size());
        assertEquals(NodeState.INIT, serverStates.get(serverId1));
        assertEquals(NodeState.INIT, serverStates.get(serverId2));

        // Set two shards
        var shardResult = clientService.changeShardCount(2);
        assertTrue(shardResult.isSuccess(), "Failed to set initial shard count: " + shardResult.getMessage());

        // 2. Add initial data
        String key1 = "initial_key_1";
        String value1 = "initial_value_1";
        String key2 = "initial_key_2";
        String value2 = "initial_value_2";

        var setResult1 = clientService.setValue(key1, value1);
        assertTrue(setResult1);

        var setResult2 = clientService.setValue(key2, value2);
        assertTrue(setResult2);

        // 3. Check that data is available
        var getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);

        var getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);

        // 4. Setup failpoint to freeze stageShards method
        var testClient = getTestClient(serverId1);
        var failpointClient = testClient.getFailpointClient();

        // Freeze stageShards for 10 seconds to have time to test operations
        failpointClient.freezeFor(NodeStorageService.class, "stageShards", Duration.of(10, ChronoUnit.SECONDS));

        // 5. Start shard count change in separate thread
        try (var executor = Executors.newFixedThreadPool(1)) {
            CompletableFuture<Boolean> shardChangeFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    var result = clientService.changeShardCount(3);
                    return result.isSuccess();
                } catch (Exception e) {
                    return false;
                }
            }, executor);

            // 6. Wait for freeze to be hit
            failpointClient.awaitFreezeHit(NodeStorageService.class, "stageShards");

            // 7. Check node state during freeze
            var nodeStatus = testClient.getNodeClient().getNodeStatus();
            assertEquals(NodeState.REARRANGE_SHARDS_PREPARING, nodeStatus.getState());

            // 8. Perform get/set operations during shard change freeze
            String key3 = "during_freeze_key_1";
            String value3 = "during_freeze_value_1";
            String key4 = "during_freeze_key_2";
            String value4 = "during_freeze_value_2";
            String key5 = "during_freeze_key_3";
            String value5 = "during_freeze_value_3";

            // Set operations during freeze
            var setResult3 = clientService.setValue(key3, value3);
            assertTrue(setResult3);

            var setResult4 = clientService.setValue(key4, value4);
            assertTrue(setResult4);

            var setResult5 = clientService.setValue(key5, value5);
            assertTrue(setResult5);

            // Get operations for old data during freeze
            getResult1 = clientService.getValue(key1);
            assertEquals(value1, getResult1);

            getResult2 = clientService.getValue(key2);
            assertEquals(value2, getResult2);

            // Get operations for new data during freeze
            var getResult3 = clientService.getValue(key3);
            assertEquals(value3, getResult3);

            var getResult4 = clientService.getValue(key4);
            assertEquals(value4, getResult4);

            var getResult5 = clientService.getValue(key5);
            assertEquals(value5, getResult5);

            // Check that state is frozen
            assertEquals(NodeState.REARRANGE_SHARDS_PREPARING, nodeStatus.getState());

            // 9. Wait for shard change completion
            boolean shardChangeSuccess = shardChangeFuture.get(30, TimeUnit.SECONDS);
            assertTrue(shardChangeSuccess, "Shard change operation failed");

            // 10. Check that all data is available after shard change
            getResult1 = clientService.getValue(key1);
            assertEquals(value1, getResult1);

            getResult2 = clientService.getValue(key2);
            assertEquals(value2, getResult2);

            getResult3 = clientService.getValue(key3);
            assertEquals(value3, getResult3);

            getResult4 = clientService.getValue(key4);
            assertEquals(value4, getResult4);

            getResult5 = clientService.getValue(key5);
            assertEquals(value5, getResult5);

            // 11. Add new data after shard change
            String key6 = "after_change_key_1";
            String value6 = "after_change_value_1";
            String key7 = "after_change_key_2";
            String value7 = "after_change_value_2";

            var setResult6 = clientService.setValue(key6, value6);
            assertTrue(setResult6);

            var setResult7 = clientService.setValue(key7, value7);
            assertTrue(setResult7);

            // 12. Check new data
            var getResult6 = clientService.getValue(key6);
            assertEquals(value6, getResult6);

            var getResult7 = clientService.getValue(key7);
            assertEquals(value7, getResult7);

            // 13. Check server states
            serverStates = clientService.getServerStates();
            assertEquals(2, serverStates.size());
            for (NodeState state : serverStates.values()) {
                assertEquals(NodeState.RUNNING, state);
            }
        }
    }
} 
