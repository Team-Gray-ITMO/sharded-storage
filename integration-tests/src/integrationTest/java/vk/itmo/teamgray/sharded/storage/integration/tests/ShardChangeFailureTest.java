package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.node.service.NodeStorageService;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShardChangeFailureTest extends BaseIntegrationTest {

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
    public void testShardChangeFailureDuringStageShards() {
        // 1. Start 2 servers
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

        // Set 2 shards
        var shardResult = clientService.changeShardCount(2);
        assertTrue(shardResult.isSuccess(), "Failed to set initial shard count: " + shardResult.getMessage());

        // 2. Add test data
        String key1 = "failure_test_key_1";
        String value1 = "failure_test_value_1";
        String key2 = "failure_test_key_2";
        String value2 = "failure_test_value_2";

        var setResult1 = clientService.setValue(key1, value1);
        assertTrue(setResult1);

        var setResult2 = clientService.setValue(key2, value2);
        assertTrue(setResult2);

        // 3. Check that data is available
        var getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);

        var getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);

        // 4. Setup failpoint to throw in swapWithStaged
        var testClient = getTestClient(serverId1);
        var failpointClient = testClient.getFailpointClient();
        failpointClient.addFailpoint(NodeStorageService.class, "swapWithStaged", NodeException.class);

        // 5. Start shard change in separate thread
        try (var executor = Executors.newFixedThreadPool(1)) {
            CompletableFuture<Boolean> shardChangeFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    var result = clientService.changeShardCount(3);
                    return result.isSuccess();
                } catch (Exception e) {
                    return false;
                }
            }, executor);

            // 6. Wait for operation completion (should fail)
            boolean shardChangeSuccess = shardChangeFuture.get(30, TimeUnit.SECONDS);
            assertFalse(shardChangeSuccess, "Shard change should have failed");
        } catch (Exception e) {
            throw new AssertionError("Test failed with exception", e);
        }

        // 8. Check that all data is available after failure
        getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);

        getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);

        // TODO: fail here
        /*
        
        // 9. Check that we can successfully change shard count after failure
        shardResult = clientService.changeShardCount(3);
        assertTrue(shardResult.isSuccess());
        
        // 10. Check that data is available after successful shard change
        getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);
        
        getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);
        
        */
    }
} 
