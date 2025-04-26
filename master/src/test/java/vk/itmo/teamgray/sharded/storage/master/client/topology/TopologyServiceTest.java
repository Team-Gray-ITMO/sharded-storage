package vk.itmo.teamgray.sharded.storage.master.client.topology;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TopologyServiceTest {
    private TopologyService topologyService;

    @BeforeEach
    void setUp() {
        topologyService = new TopologyService(false, mock());
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
    void test23() {
        topologyService.changeShardCount(10);

        topologyService.changeShardCount(5);
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

    @Test
    void testSingleShard() {
        topologyService.changeShardCount(1);

        ConcurrentHashMap<Integer, Long> shardToHash = topologyService.getShardToHash();

        assertEquals(1, shardToHash.size());
        assertEquals(Long.MAX_VALUE, shardToHash.get(0));
    }

    @Test
    void hashDistributionEvenlyCoversFullRange() {
        int shardCount = 10;
        topologyService.changeShardCount(shardCount);

        ConcurrentHashMap<Integer, Long> shardToHash = topologyService.getShardToHash();

        assertEquals(shardCount, shardToHash.size());

        //Use BigInteger to calculate range more precisely in tests.
        BigInteger range = BigInteger
            .valueOf(Long.MAX_VALUE)
            .subtract(BigInteger.valueOf(Long.MIN_VALUE));

        long expectedInterval = range
            .divide(BigInteger.valueOf(shardCount))
            .longValue();

        long previousBoundary = Long.MIN_VALUE;

        for (int i = 0; i < shardCount; i++) {
            assertTrue(shardToHash.containsKey(i));
            long boundary = shardToHash.get(i);

            assertTrue(previousBoundary <= boundary);

            long interval = boundary - previousBoundary;

            long deviation = Math.abs(interval - expectedInterval);
            assertTrue(deviation <= expectedInterval * 0.001, "Interval " + i + " is not even: " + interval);

            previousBoundary = boundary;
        }

        assertEquals(Long.MAX_VALUE, previousBoundary);
    }

    //TODO Remove, once implemented
    @Test
    void testHardCodedNode() {
        var hardCodedTopologyService = new TopologyService(true, mock());

        ServerDataDTO server = new ServerDataDTO("127.0.0.1", 8001);

        assertFalse(hardCodedTopologyService.addServer(server).created());
        assertFalse(hardCodedTopologyService.deleteServer(server).deleted());
    }
}

