package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiShardMultiNodeTest extends BaseIntegrationTest {

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
    public void testMultiShardMultiNodeOperations() throws InterruptedException {
        // 1. Create first server
        int serverId1 = 1;
        orchestrationApi.runNode(serverId1);
        clientService.addServer(serverId1, false);

        // Check first state
        Map<Integer, NodeState> serverStates = clientService.getServerStates();
        assertFalse(serverStates.isEmpty());
        // TODO: not running?
        assertEquals(NodeState.INIT, serverStates.get(serverId1));

        // 2. Change shards count to 1
        var shardResult = clientService.changeShardCount(1);
        assertTrue(shardResult.isSuccess(), "Failed to add shard: " + shardResult.getMessage());

        // 3. Create second server
        int serverId2 = 2;
        orchestrationApi.runNode(serverId2);
        clientService.addServer(serverId2, false);

        // Check second server state
        serverStates = clientService.getServerStates();
        assertEquals(2, serverStates.size());
        // TODO: not running?
        assertEquals(NodeState.INIT, serverStates.get(serverId2));

        // 4. Change shards count to 2
        shardResult = clientService.changeShardCount(2);
        assertTrue(shardResult.isSuccess(), "Failed to increase shard count: " + shardResult.getMessage());

        // 5. Create third server
        int serverId3 = 3;
        orchestrationApi.runNode(serverId3);
        clientService.addServer(serverId3, false);

        // Check third server state
        serverStates = clientService.getServerStates();
        assertEquals(3, serverStates.size());
        // TODO: not running?
        assertEquals(NodeState.INIT, serverStates.get(serverId3));

        // 6. Change shards count to 3
        shardResult = clientService.changeShardCount(3);
        assertTrue(shardResult.isSuccess(), "Failed to increase shard count to 3: " + shardResult.getMessage());

        // 7. Test set/get
        String key1 = "test_key_1";
        String value1 = "test_value_1";
        String key2 = "test_key_2";
        String value2 = "test_value_2";
        String key3 = "test_key_3";
        String value3 = "test_value_3";

        var setResult1 = clientService.setValue(key1, value1);
        assertTrue(setResult1);

        var setResult2 = clientService.setValue(key2, value2);
        assertTrue(setResult2);

        var setResult3 = clientService.setValue(key3, value3);
        assertTrue(setResult3);

        var getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);

        var getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);

        var getResult3 = clientService.getValue(key3);
        assertEquals(value3, getResult3);

        // 8. Add fourth server
        int serverId4 = 4;
        orchestrationApi.runNode(serverId4);
        clientService.addServer(serverId4, false);

        // Check fourth server state
        serverStates = clientService.getServerStates();
        assertEquals(4, serverStates.size());
        assertEquals(NodeState.INIT, serverStates.get(serverId4));

        // 9. Change shards count to 4
        shardResult = clientService.changeShardCount(4);
        assertTrue(shardResult.isSuccess(), "Failed to increase shard count to 4: " + shardResult.getMessage());

        // 10. Add new data
        String key4 = "test_key_4";
        String value4 = "test_value_4";
        String key5 = "test_key_5";
        String value5 = "test_value_5";

        var setResult4 = clientService.setValue(key4, value4);
        assertTrue(setResult4);

        var setResult5 = clientService.setValue(key5, value5);
        assertTrue(setResult5);

        // 11. Check old data
        getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);

        getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);

        getResult3 = clientService.getValue(key3);
        assertEquals(value3, getResult3);

        // 12. Check new data
        var getResult4 = clientService.getValue(key4);
        assertEquals(value4, getResult4);

        var getResult5 = clientService.getValue(key5);
        assertEquals(value5, getResult5);

        // 13. Check all servers
        serverStates = clientService.getServerStates();
        assertEquals(4, serverStates.size());
        for (NodeState state : serverStates.values()) {
            assertEquals(NodeState.RUNNING, state);
        }
    }
} 
