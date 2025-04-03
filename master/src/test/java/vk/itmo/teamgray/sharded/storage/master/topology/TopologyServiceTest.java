package vk.itmo.teamgray.sharded.storage.master.topology;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyServiceTest {
    private TopologyService topologyService;

    @BeforeEach
    void setUp() {
        topologyService = new TopologyService(false);
    }

    @Test
    void addServerDistributesShardsEvenly() {
        ServerDataDTO server1 = new ServerDataDTO("127.0.0.1", 8001);
        ServerDataDTO server2 = new ServerDataDTO("127.0.0.1", 8002);

        assertTrue(topologyService.addServer(server1).created());
        assertTrue(topologyService.addServer(server2).created());

        ConcurrentHashMap<ServerDataDTO, List<Integer>> serverToShards = topologyService.getServerToShards();
        assertEquals(2, serverToShards.size());
        assertTrue(serverToShards.containsKey(server1));
        assertTrue(serverToShards.containsKey(server2));
    }

    @Test
    void addServerFailsIfAlreadyExists() {
        ServerDataDTO server = new ServerDataDTO("127.0.0.1", 8001);
        topologyService.addServer(server);

        assertFalse(topologyService.addServer(server).created());
    }

    @Test
    void deleteServerRemovesServerAndRedistributesShards() {
        ServerDataDTO server1 = new ServerDataDTO("127.0.0.1", 8001);
        ServerDataDTO server2 = new ServerDataDTO("127.0.0.1", 8002);

        topologyService.addServer(server1);
        topologyService.addServer(server2);

        assertTrue(topologyService.deleteServer(server1).deleted());

        ConcurrentHashMap<ServerDataDTO, List<Integer>> serverToShards = topologyService.getServerToShards();
        assertEquals(1, serverToShards.size());
        assertFalse(serverToShards.containsKey(server1));
    }

    @Test
    void deleteServerFailsIfServerDoesNotExist() {
        ServerDataDTO server = new ServerDataDTO("127.0.0.1", 8001);
        assertFalse(topologyService.deleteServer(server).deleted());
    }

    @Test
    void changeShardCountCorrectlyRedistributesShards() {
        topologyService.changeShardCount(10);
        ConcurrentHashMap<Integer, Long> shardToHash = topologyService.getShardToHash();

        assertEquals(10, shardToHash.size());
    }

    @Test
    void changeShardCountHandlesZeroShards() {
        topologyService.changeShardCount(0);
        ConcurrentHashMap<Integer, Long> shardToHash = topologyService.getShardToHash();

        assertTrue(shardToHash.isEmpty());
    }

    @Test
    void redistributeShardsEvenlyHandlesUnevenShardDistribution() {
        ServerDataDTO server1 = new ServerDataDTO("127.0.0.1", 8001);
        ServerDataDTO server2 = new ServerDataDTO("127.0.0.1", 8002);

        topologyService.addServer(server1);
        topologyService.addServer(server2);
        topologyService.changeShardCount(7);

        ConcurrentHashMap<ServerDataDTO, List<Integer>> serverToShards = topologyService.getServerToShards();
        assertEquals(7, serverToShards.values().stream().mapToInt(List::size).sum());
    }

    //TODO Remove, once implemented
    @Test
    void testHardCodedNode() {
        var hardCodedTopologyService = new TopologyService(true);

        ServerDataDTO server = new ServerDataDTO("127.0.0.1", 8001);

        assertFalse(hardCodedTopologyService.addServer(server).created());
        assertFalse(hardCodedTopologyService.deleteServer(server).deleted());
    }
}

