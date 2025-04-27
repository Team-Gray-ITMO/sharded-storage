package vk.itmo.teamgray.sharded.storage.master.client.topology;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.dto.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TopologyServiceTest {
    private TopologyService topologyService;

    //TODO Find a way to inject mock
    private NodeManagementClient nodeManagementClient = mock();

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
    void testFragments() {
        var serverCount = 2;

        IntStream.range(0, serverCount)
            .forEach(i -> topologyService.addServer(new ServerDataDTO("127.0.0.1", 8000 + i)));

        var shardCount = 10;
        topologyService.changeShardCount(shardCount);

        //Default shard count is 1, so 10 fragments come from 1 server to second
        verify(nodeManagementClient, times(1))
            .rearrangeShards(
                argThat(shards -> shards.size() == shardCount / serverCount),
                argThat(fragments -> fragments.size() == shardCount),
                argThat(nodes -> nodes.size() == shardCount)
            );

        //No fragments here, this server was not populated.
        verify(nodeManagementClient, times(1))
            .rearrangeShards(
                argThat(shards -> shards.size() == shardCount / serverCount),
                argThat(List::isEmpty),
                argThat(List::isEmpty)
            );

        var newShardCount = 5;

        topologyService.changeShardCount(newShardCount);

        verify(nodeManagementClient, times(1))
            .rearrangeShards(
                argThat(shards -> shards.size() == newShardCount / serverCount + 1),
                argThat(fragments -> fragments.size() == 9),
                any()
            );

        verify(nodeManagementClient, times(1))
            .rearrangeShards(
                argThat(shards -> shards.size() == newShardCount / serverCount),
                argThat(fragments -> fragments.size() == 5),
                any()
            );
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

        for (int i = 1; i <= shardCount; i++) {
            assertTrue(shardToHash.containsKey(i - 1));
            long boundary = shardToHash.get(i - 1);

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
        var hardCodedTopologyService = new TopologyService(true);

        ServerDataDTO server = new ServerDataDTO("127.0.0.1", 8001);

        assertFalse(hardCodedTopologyService.addServer(server).created());
        assertFalse(hardCodedTopologyService.deleteServer(server).deleted());
    }
}

