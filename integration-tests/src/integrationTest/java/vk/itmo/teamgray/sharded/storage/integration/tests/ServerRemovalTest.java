package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import static org.junit.jupiter.api.Assertions.*;

public class ServerRemovalTest extends BaseIntegrationTest {
    
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
    public void testServerRemovalDataPreservation() throws InterruptedException {
        // 1. Setup: start 3 servers and 3 shards
        int serverId1 = 1;
        int serverId2 = 2;
        int serverId3 = 3;
        
        orchestrationApi.runNode(serverId1);
        orchestrationApi.runNode(serverId2);
        orchestrationApi.runNode(serverId3);
        
        clientService.addServer(serverId1, false);
        clientService.addServer(serverId2, false);
        clientService.addServer(serverId3, false);
        
        // Check that all servers are added
        Map<Integer, NodeState> serverStates = clientService.getServerStates();
        assertEquals(3, serverStates.size());
        // TODO: not running?
        assertEquals(NodeState.INIT, serverStates.get(serverId1));
        assertEquals(NodeState.INIT, serverStates.get(serverId2));
        assertEquals(NodeState.INIT, serverStates.get(serverId3));
        
        // Set 3 shards
        var shardResult = clientService.changeShardCount(3);
        assertTrue(shardResult.isSuccess());
        
        // 2. Add test data
        String key1 = "data_key_1";
        String value1 = "data_value_1";
        String key2 = "data_key_2";
        String value2 = "data_value_2";
        String key3 = "data_key_3";
        String value3 = "data_value_3";
        String key4 = "data_key_4";
        String value4 = "data_value_4";
        String key5 = "data_key_5";
        String value5 = "data_value_5";
        
        var setResult1 = clientService.setValue(key1, value1);
        assertTrue(setResult1);
        
        var setResult2 = clientService.setValue(key2, value2);
        assertTrue(setResult2);
        
        var setResult3 = clientService.setValue(key3, value3);
        assertTrue(setResult3);
        
        var setResult4 = clientService.setValue(key4, value4);
        assertTrue(setResult4);
        
        var setResult5 = clientService.setValue(key5, value5);
        assertTrue(setResult5);
        
        // 3. Check that all data is available
        var getResult1 = clientService.getValue(key1);
        assertEquals(value1, getResult1);
        
        var getResult2 = clientService.getValue(key2);
        assertEquals(value2, getResult2);
        
        var getResult3 = clientService.getValue(key3);
        assertEquals(value3, getResult3);
        
        var getResult4 = clientService.getValue(key4);
        assertEquals(value4, getResult4);
        
        var getResult5 = clientService.getValue(key5);
        assertEquals(value5, getResult5);
        
        // 4. Remove second server
        var removeResult = clientService.deleteServer(serverId2);
        assertTrue(removeResult.isSuccess());
        
        // 5. Check that server is removed
        serverStates = clientService.getServerStates();
        assertEquals(2, serverStates.size());
        assertTrue(serverStates.containsKey(serverId1));
        assertFalse(serverStates.containsKey(serverId2));
        assertTrue(serverStates.containsKey(serverId3));
        
        // 6. Check that all data is still available after server removal
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
        
        // 7. Add new data after server removal
        String key6 = "data_key_6";
        String value6 = "data_value_6";
        String key7 = "data_key_7";
        String value7 = "data_value_7";
        
        var setResult6 = clientService.setValue(key6, value6);
        assertTrue(setResult6);
        
        var setResult7 = clientService.setValue(key7, value7);
        assertTrue(setResult7);
        
        // 8. Check new data
        var getResult6 = clientService.getValue(key6);
        assertEquals(value6, getResult6);
        
        var getResult7 = clientService.getValue(key7);
        assertEquals(value7, getResult7);
        
        // 9. Remove third server
        removeResult = clientService.deleteServer(serverId3);
        assertTrue(removeResult.isSuccess());
        
        // 10. Check that only first server remains
        serverStates = clientService.getServerStates();
        assertEquals(1, serverStates.size());
        assertTrue(serverStates.containsKey(serverId1));
        assertFalse(serverStates.containsKey(serverId2));
        assertFalse(serverStates.containsKey(serverId3));
        
        // 11. Check that all data is still available
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
        
        getResult6 = clientService.getValue(key6);
        assertEquals(value6, getResult6);
        
        getResult7 = clientService.getValue(key7);
        assertEquals(value7, getResult7);
        
        // 12. Check state of remaining server
        var testClient = getTestClient(serverId1);
        var nodeStatus = testClient.getNodeClient().getNodeStatus();
        assertEquals(NodeState.RUNNING, nodeStatus.getState());
    }
}
