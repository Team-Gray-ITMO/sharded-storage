package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;
import vk.itmo.teamgray.sharded.storage.test.api.TestClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(OrderAnnotation.class)
public class MultiServerIntegrationTest extends BaseIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(MultiServerIntegrationTest.class);

    private static final Map<String, String> storage = new HashMap<>();

    private static final List<Integer> runningNodes = new ArrayList<>();

    private static final List<Integer> addedServers = new ArrayList<>();

    private static int shardsCount = 0;

    @BeforeAll
    public static void setUpAll() {
        // Start three nodes and add as servers
        final int NODES_COUNT = 3;

        for (int i = 1; i <= NODES_COUNT; i++) {
            orchestrationApi.runNode(i);
            runningNodes.add(i);
        }

        for (int i = 1; i <= NODES_COUNT; i++) {
            assertTrue(clientService.addServer(i, false).isSuccess());
            addedServers.add(i);
        }
        // Set initial shard count
        assertTrue(clientService.changeShardCount(addedServers.size()).isSuccess());
        shardsCount = addedServers.size();
    }

    @Test
    @Order(1)
    public void testSetAndGetOnDifferentShards() {
        var key1 = "key-" + UUID.randomUUID();
        var key2 = "key-" + UUID.randomUUID();
        var key3 = "key-" + UUID.randomUUID();
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";
        assertTrue(clientService.setValue(key1, value1));
        assertTrue(clientService.setValue(key2, value2));
        assertTrue(clientService.setValue(key3, value3));
        assertEquals(value1, clientService.getValue(key1));
        assertEquals(value2, clientService.getValue(key2));
        assertEquals(value3, clientService.getValue(key3));

        storage.put(key1, value1);
        storage.put(key2, value2);
        storage.put(key3, value3);
    }

    @Test
    @Order(2)
    public void testChangeShardCountAndCheckData() {
        // Change shard count to 2
        int newShardsCount = 2;
        assertTrue(clientService.changeShardCount(newShardsCount).isSuccess());
        shardsCount = newShardsCount;
        // Data should still be available
        storage.forEach((key, value) -> {
            assertEquals(value, clientService.getValue(key));
        });

        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        // Shards-to-server map is actual by size
        assertEquals(shardsCount, shardServerMap.size());

        // Every shard inside different server because shards count < server count
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size()
        );

        // All nodes are running
        runningNodes.forEach(n -> {
            TestClient client = getTestClient(n);
            assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
        });
    }

    @Test
    @Order(3)
    public void testAddNodeAndServer() {
        // Add new node and server
        int newNodeId = 4;
        orchestrationApi.runNode(newNodeId);
        runningNodes.add(newNodeId);

        assertTrue(clientService.addServer(newNodeId, false).isSuccess());
        addedServers.add(newNodeId);

        int newShardsCount = 4;
        assertTrue(clientService.changeShardCount(newShardsCount).isSuccess());
        shardsCount = newShardsCount;

        // Add new test data
        var stepKey = "step-key-" + UUID.randomUUID();
        var stepValue = "step-value";
        assertTrue(clientService.setValue(stepKey, stepValue));
        assertEquals(stepValue, clientService.getValue(stepKey));

        storage.put(stepKey, stepValue);

        // Verify all existing data is still available
        storage.forEach((key, value) -> {
            assertEquals(value, clientService.getValue(key));
        });

        // Verify shard distribution
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Every shard inside different server because shards count < server count
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size()
        );

        // Verify all nodes are running
        runningNodes.forEach(n -> {
            TestClient client = getTestClient(n);
            assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
        });
    }

    @Test
    @Order(4)
    public void testDeleteServer() {
        // Delete server 2
        int serverToDelete = 2;
        assertTrue(clientService.deleteServer(serverToDelete).isSuccess());
        runningNodes.remove(Integer.valueOf(serverToDelete));
        addedServers.remove(Integer.valueOf(serverToDelete));

        // Verify all data is still available
        storage.forEach((key, value) -> {
            assertEquals(value, clientService.getValue(key));
        });

        // Verify shard distribution
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Shards count = 4, servers count = 3 -> one server owns 2 shards
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size() + 1
        );

        // Verify remaining nodes are running
        runningNodes.forEach(n -> {
            TestClient client = getTestClient(n);
            assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
        });
    }

    @Test
    @Order(5)
    public void testMigrationKeysOnShardCountChange() {
        // Add new test data
        var migKey = "mig-key-" + UUID.randomUUID();
        var migValue = "mig-value";
        assertTrue(clientService.setValue(migKey, migValue));
        storage.put(migKey, migValue);

        // Change shard count
        int newShardsCount = 3;
        assertTrue(clientService.changeShardCount(newShardsCount).isSuccess());
        shardsCount = newShardsCount;

        // Verify all data is still available
        storage.forEach((key, value) -> {
            assertEquals(value, clientService.getValue(key));
        });

        // Verify shard distribution
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Every shard inside different server because shards count = server count = 3
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size()
        );

        // Verify all nodes are running
        runningNodes.forEach(n -> {
            TestClient client = getTestClient(n);
            assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
        });
    }

    @Test
    @Order(6)
    public void testParallelSetGetOnDifferentShards() throws Exception {
        var parKey1 = "par-key-1-" + UUID.randomUUID();
        var parValue1 = "par-value-1";
        var parKey2 = "par-key-2-" + UUID.randomUUID();
        var parValue2 = "par-value-2";

        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var f1 = executor.submit(() -> clientService.setValue(parKey1, parValue1));
            var f2 = executor.submit(() -> clientService.setValue(parKey2, parValue2));
            assertTrue(f1.get());
            assertTrue(f2.get());

            storage.put(parKey1, parValue1);
            storage.put(parKey2, parValue2);

            var g1 = executor.submit(() -> clientService.getValue(parKey1));
            var g2 = executor.submit(() -> clientService.getValue(parKey2));
            assertEquals(parValue1, g1.get());
            assertEquals(parValue2, g2.get());

            // Verify shard distribution
            Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
            assertEquals(shardsCount, shardServerMap.size());

            // Every shard inside different server because shards count = server count = 3
            assertEquals(
                (long)shardServerMap.values().size(),
                new HashSet<>(shardServerMap.values()).size()
            );
        }
    }

    @Test
    @Order(7)
    public void testGetSetNonExistentKey() {
        var key = "nonexistent-" + UUID.randomUUID();
        assertNull(clientService.getValue(key));

        // Existing data is available
        storage.forEach((k, v) -> {
            assertEquals(v, clientService.getValue(k));
        });

        // Shards count didn't change
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Every shard inside different server because shards count = server count = 3
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size()
        );
    }

    @Test
    @Order(8)
    public void testAddServerWithExistingId() {
        // Try to add server 1 again
        var response = clientService.addServer(1, false);
        assertFalse(response.isSuccess());

        // Existing data is available
        storage.forEach((k, v) -> {
            assertEquals(v, clientService.getValue(k));
        });

        // Shards count didn't change
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Server list didn't change
        assertEquals(addedServers.size(), new HashSet<>(shardServerMap.values()).size());
        addedServers.forEach(ad -> {
            assertTrue(shardServerMap.values().stream().anyMatch(s -> s.id() == ad));
        });

        // Every shard inside different server because shards count = server count = 3
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size()
        );
    }

    @Test
    @Order(9)
    public void testCheckKeyDistributionAcrossShards() {
        var distKey1 = "dist-key-1-" + UUID.randomUUID();
        var distKey2 = "dist-key-2-" + UUID.randomUUID();
        var distValue1 = "v1";
        var distValue2 = "v2";

        assertTrue(clientService.setValue(distKey1, distValue1));
        assertTrue(clientService.setValue(distKey2, distValue2));

        storage.put(distKey1, distValue1);
        storage.put(distKey2, distValue2);

        var shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Every shard inside different server because shards count = server count = 3
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size()
        );
    }

    @Test
    @Order(10)
    public void testAddNodeAndServerAndShardWithAllDataAvailabilityAndShardsDistributionChecking() {
        // Add node 5 and server 5
        // Now 4 servers available
        int newNodeId = 5;
        orchestrationApi.runNode(newNodeId);
        runningNodes.add(newNodeId);

        assertTrue(clientService.addServer(newNodeId, false).isSuccess());
        addedServers.add(newNodeId);

        int newShardsCount = 5;
        assertTrue(clientService.changeShardCount(newShardsCount).isSuccess());
        shardsCount = newShardsCount;

        // Add sequential test data
        var seqKey1 = "seq-key-1-" + UUID.randomUUID();
        var seqKey2 = "seq-key-2-" + UUID.randomUUID();
        var seqKey3 = "seq-key-3-" + UUID.randomUUID();
        var seqValue1 = "v1";
        var seqValue2 = "v2";
        var seqValue3 = "v3";

        assertTrue(clientService.setValue(seqKey1, seqValue1));
        assertTrue(clientService.setValue(seqKey2, seqValue2));
        assertTrue(clientService.setValue(seqKey3, seqValue3));

        storage.put(seqKey1, seqValue1);
        storage.put(seqKey2, seqValue2);
        storage.put(seqKey3, seqValue3);

        // Verify all data is available
        storage.forEach((key, value) -> {
            assertEquals(value, clientService.getValue(key));
        });

        // Verify shard distribution
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Shards count = 5, servers count = 4 -> one server owns 2 shards
        assertEquals(
            (long)shardServerMap.values().size(),
            new HashSet<>(shardServerMap.values()).size() + 1
        );
    }

    @Test
    @Order(11)
    public void testDeleteTwoServers() {
        // Delete servers 5 and 4
        List<Integer> serversToDelete = List.of(5, 4);
        for (int serverId : serversToDelete) {
            assertTrue(clientService.deleteServer(serverId).isSuccess());
            runningNodes.remove(Integer.valueOf(serverId));
            addedServers.remove(Integer.valueOf(serverId));
        }
        // Now only 2 servers running

        // Verify all data is still available
        storage.forEach((key, value) -> {
            assertEquals(value, clientService.getValue(key));
        });

        // Verify shard distribution
        Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
        assertEquals(shardsCount, shardServerMap.size());

        // Shards count = 5, servers count = 2 -> one server owns 2 shards
        Map<DiscoverableServiceDTO, List<Integer>> shardsByServer = shardServerMap.entrySet()
            .stream()
            .collect(Collectors.groupingBy(
                Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toList())
            ));

        // Running servers:
        // [id = 1, id = 3]
        assertEquals(addedServers.size(), shardsByServer.size());
        assertEquals(2, addedServers.size());

        List<Integer> shardsByFirstServer = shardsByServer.get(
            shardsByServer.keySet().stream().filter(s -> s.id() == 1)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unexpected: server #1 is not found"))
        );
        List<Integer> shardsBySecondServer = shardsByServer.get(
            shardsByServer.keySet().stream().filter(s -> s.id() == 3)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unexpected: server #3 is not found"))
        );
        // One server owns 2 shards, then another one owns 3 shards and vice versa
        assertTrue(shardsByFirstServer.size() == 2 || shardsByFirstServer.size() == 3 &&
            shardsBySecondServer.size() == 2 || shardsBySecondServer.size() == 3
        );

        // Verify remaining nodes are running
        runningNodes.forEach(n -> {
            TestClient client = getTestClient(n);
            assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
        });
    }

    @Test
    @Order(12)
    public void testDataAvailabilityAfterSeveralShardCountChanges() {
        var firstKey = "first-key-" + UUID.randomUUID();
        var firstValue = "first-value";
        assertTrue(clientService.setValue(firstKey, firstValue));
        storage.put(firstKey, firstValue);

        // Test multiple shard count changes
        List<Integer> shardCounts = List.of(2, 1, 2);
        for (int count : shardCounts) {
            assertTrue(clientService.changeShardCount(count).isSuccess());
            shardsCount = count;

            // Verify all data is still available
            storage.forEach((key, value) -> {
                assertEquals(value, clientService.getValue(key));
            });

            // Verify shard distribution
            Map<Integer, DiscoverableServiceDTO> shardServerMap = clientService.getShardServerMapping();
            assertEquals(shardsCount, shardServerMap.size());

            // Every shard inside different server because shards count = server count = 2
            assertEquals(
                (long)shardServerMap.values().size(),
                new HashSet<>(shardServerMap.values()).size()
            );

            // Verify all nodes are running
            runningNodes.forEach(n -> {
                TestClient client = getTestClient(n);
                assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
            });

            addedServers.forEach(n -> {
                TestClient client = getTestClient(n);
                assertSame(NodeState.RUNNING, client.getNodeClient().getNodeStatus().getState());
            });
        }
    }
} 
