package vk.itmo.teamgray.sharded.storage.master.service.topology;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static vk.itmo.teamgray.sharded.storage.common.responsewriter.MapResponseWriter.Helper.toMap;
import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.toDto;
import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.voidRw;

class TopologyServiceTest {
    private TopologyService topologyService;

    private NodeManagementClient nodeManagementClient = mock();

    private ClientCachingFactory clientCachingFactory = mock();

    private DiscoveryClient discoveryClient = mock();

    @BeforeEach
    void setUp() {
        when(discoveryClient.getNodeMapWithRetries(any())).thenReturn(Map.of(
            0, new DiscoverableServiceDTO(0, DiscoverableServiceType.NODE, "test", "test"),
            1, new DiscoverableServiceDTO(1, DiscoverableServiceType.NODE, "test", "test"),
            2, new DiscoverableServiceDTO(2, DiscoverableServiceType.NODE, "test", "test")
        ));

        when(clientCachingFactory.getClient(
            argThat(service -> service.type() == DiscoverableServiceType.NODE),
            any()
        ))
            .thenReturn(nodeManagementClient);

        when(nodeManagementClient.prepareRearrange(any(), any(), any(), anyInt())).thenReturn(new StatusResponseDTO(true, ""));
        when(nodeManagementClient.processRearrange()).thenReturn(new StatusResponseDTO(true, ""));
        when(nodeManagementClient.applyOperation(any())).thenReturn(new StatusResponseDTO(true, ""));

        topologyService = new TopologyService(discoveryClient, clientCachingFactory);
    }

    @Test
    void addServerDistributesShardsEvenly() {
        assertTrue(toDto(rw -> topologyService.addServer(1, rw)).isSuccess());
        assertTrue(toDto(rw -> topologyService.addServer(2, rw)).isSuccess());

        Map<Integer, List<Integer>> serverToShards = toMap(topologyService::fillServerToShardsInSync);
        assertEquals(2, serverToShards.size());
        assertTrue(serverToShards.containsKey(1));
        assertTrue(serverToShards.containsKey(2));
    }

    @Test
    void addServerFailsIfAlreadyExists() {
        topologyService.addServer(1, voidRw());

        assertFalse(toDto(rw -> topologyService.addServer(1, rw)).isSuccess());
    }

    @Test
    void deleteServerRemovesServerAndRedistributesShards() {
        topologyService.addServer(1, voidRw());
        topologyService.addServer(2, voidRw());

        assertTrue(toDto(rw -> topologyService.deleteServer(1, rw)).isSuccess());

        Map<Integer, List<Integer>> serverToShards = toMap(topologyService::fillServerToShardsInSync);
        assertEquals(1, serverToShards.size());
        assertFalse(serverToShards.containsKey(1));
    }

    @Test
    void deleteServerFailsIfServerDoesNotExist() {
        assertFalse(toDto(rw -> topologyService.deleteServer(1, rw)).isSuccess());
    }

    // TODO Double-check test logic
    @Disabled
    @Test
    void testFragments() {
        var serverCount = 2;

        IntStream.range(0, serverCount)
            .forEach(i -> topologyService.addServer(i, voidRw()));

        var shardCount = 10;
        topologyService.changeShardCount(shardCount, voidRw());

        //The default shard count is 1, so 10 fragments come from 1 server to the second
        //verify(nodeManagementClient, times(1))
        //    .rearrangeShards(
        //        argThat(shards -> shards.size() == shardCount / serverCount),
        //        argThat(fragments -> fragments.size() == shardCount),
        //        argThat(nodes -> nodes.size() == shardCount),
        //        anyInt()
        //    );
//
        ////No fragments here, this server was not populated.
        //verify(nodeManagementClient, times(1))
        //    .rearrangeShards(
        //        argThat(shards -> shards.size() == shardCount / serverCount),
        //        argThat(List::isEmpty),
        //        argThat(List::isEmpty),
        //        anyInt()
        //    );

        var newShardCount = 5;

        topologyService.changeShardCount(newShardCount, voidRw());

        //verify(nodeManagementClient, times(1))
        //    .rearrangeShards(
        //        argThat(shards -> shards.size() == newShardCount / serverCount + 1),
        //        argThat(fragments -> fragments.size() == 9),
        //        any(),
        //        anyInt()
        //    );
//
        //verify(nodeManagementClient, times(1))
        //    .rearrangeShards(
        //        argThat(shards -> shards.size() == newShardCount / serverCount),
        //        argThat(fragments -> fragments.size() == 5),
        //        any(),
        //        anyInt()
        //    );
    }

    @Test
    void changeShardCountCorrectlyRedistributesShards() {
        topologyService.changeShardCount(10, voidRw());
        Map<Integer, Long> shardToHash = toMap(topologyService::fillShardToHashInSync);

        assertEquals(10, shardToHash.size());
    }

    @Test
    void changeShardCountHandlesZeroShards() {
        topologyService.changeShardCount(0, voidRw());
        Map<Integer, Long> shardToHash = toMap(topologyService::fillShardToHashInSync);

        assertTrue(shardToHash.isEmpty());
    }

    // TODO Rewrite test
    @Disabled
    @Test
    void redistributeShardsEvenlyHandlesUnevenShardDistribution() {
        topologyService.addServer(1, voidRw());
        topologyService.addServer(2, voidRw());
        topologyService.changeShardCount(7, voidRw());

        Map<Integer, List<Integer>> serverToShards = toMap(topologyService::fillServerToShardsInSync);
        assertEquals(7, serverToShards.values().stream().mapToInt(List::size).sum());
    }

    @Test
    void testSingleShard() {
        topologyService.changeShardCount(1, voidRw());

        Map<Integer, Long> shardToHash = toMap(topologyService::fillShardToHashInSync);

        assertEquals(1, shardToHash.size());
        assertEquals(Long.MAX_VALUE, shardToHash.get(0));
    }

    @Test
    void hashDistributionEvenlyCoversFullRange() {
        int shardCount = 10;
        topologyService.changeShardCount(shardCount, voidRw());

        Map<Integer, Long> shardToHash = toMap(topologyService::fillShardToHashInSync);

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
}

