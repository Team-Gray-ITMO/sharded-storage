package vk.itmo.teamgray.sharded.storage.master.service.topology;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
            2, new DiscoverableServiceDTO(2, DiscoverableServiceType.NODE, "test", "test"),
            3, new DiscoverableServiceDTO(2, DiscoverableServiceType.NODE, "test", "test")
        ));

        when(clientCachingFactory.getClient(
            argThat(service -> service.type() == DiscoverableServiceType.NODE),
            any()
        ))
            .thenReturn(nodeManagementClient);

        when(nodeManagementClient.prepareRearrange(any(), any(), any(), anyInt())).thenReturn(new StatusResponseDTO(true, ""));
        when(nodeManagementClient.processAction(any())).thenReturn(new StatusResponseDTO(true, ""));
        when(nodeManagementClient.prepareMove(anyList(), anyList(), anyInt())).thenReturn(new StatusResponseDTO(true, ""));
        when(nodeManagementClient.applyAction(any())).thenReturn(new StatusResponseDTO(true, ""));

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

    @Test
    void testChangeShardActions() {
        var serverCount = 2;

        IntStream.range(0, serverCount)
            .forEach(i -> topologyService.addServer(i, voidRw()));

        var shardCount = 10;
        topologyService.changeShardCount(shardCount, voidRw());

        Map<Integer, List<Integer>> serverToShards = toMap(topologyService::fillServerToShardsInSync);
        assertEquals(serverCount, serverToShards.size());
        assertEquals(shardCount, serverToShards.values().stream().mapToInt(List::size).sum());

        verify(nodeManagementClient, times(serverCount)).prepareRearrange(any(), any(), any(), anyInt());
        verify(nodeManagementClient, times(serverCount)).processAction(any());
        verify(nodeManagementClient, times(serverCount)).applyAction(any());

        var newShardCount = 5;
        topologyService.changeShardCount(newShardCount, voidRw());

        Map<Integer, List<Integer>> newServerToShards = toMap(topologyService::fillServerToShardsInSync);
        assertEquals(serverCount, newServerToShards.size());
        assertEquals(newShardCount, newServerToShards.values().stream().mapToInt(List::size).sum());

        verify(nodeManagementClient, times(serverCount * 2)).prepareRearrange(any(), any(), any(), anyInt());
        verify(nodeManagementClient, times(serverCount * 2)).processAction(any());
        verify(nodeManagementClient, times(serverCount * 2)).applyAction(any());
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

    @Test
    void redistributeShardsEvenlyHandlesUnevenShardDistribution() {
        topologyService.addServer(1, voidRw());
        topologyService.addServer(2, voidRw());
        topologyService.changeShardCount(7, voidRw());

        Map<Integer, List<Integer>> serverToShards = toMap(topologyService::fillServerToShardsInSync);

        assertEquals(7, serverToShards.values().stream().mapToInt(List::size).sum());

        int min = serverToShards.values().stream().mapToInt(List::size).min().orElse(0);
        int max = serverToShards.values().stream().mapToInt(List::size).max().orElse(0);
        assertTrue(max - min <= 1, "Shard distribution is not even enough: " + serverToShards);

        assertTrue(serverToShards.values().stream().allMatch(list -> !list.isEmpty()));
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

    @Test
    void testMovements() {
        topologyService.addServer(1, voidRw());
        topologyService.changeShardCount(37, voidRw());

        verify(nodeManagementClient).prepareRearrange(any(), any(), any(), anyInt());
        verify(nodeManagementClient).processAction(any());

        Map<Integer, List<Integer>> map = toMap(rw -> topologyService.fillServerToShardsInSync(rw));
        assertEquals(1, map.size());
        assertEquals(37, map.values().stream().mapToLong(List::size).sum());

        topologyService.addServer(2, voidRw());

        verify(nodeManagementClient, times(2)).prepareMove(any(), any(), anyInt());
        verify(nodeManagementClient, times(2)).processAction(eq(Action.MOVE_SHARDS));

        map = toMap(rw -> topologyService.fillServerToShardsInSync(rw));
        assertEquals(2, map.size());
        assertEquals(37, map.values().stream().mapToLong(List::size).sum());

        topologyService.changeShardCount(13, voidRw());

        verify(nodeManagementClient, times(3)).prepareRearrange(any(), any(), any(), anyInt());
        verify(nodeManagementClient, times(3)).processAction(eq(Action.REARRANGE_SHARDS));

        map = toMap(rw -> topologyService.fillServerToShardsInSync(rw));
        assertEquals(2, map.size());
        assertEquals(13, map.values().stream().mapToLong(List::size).sum());

        topologyService.addServer(3, voidRw());

        verify(nodeManagementClient, times(5)).prepareMove(any(), any(), anyInt());
        verify(nodeManagementClient, times(5)).processAction(eq(Action.MOVE_SHARDS));

        map = toMap(rw -> topologyService.fillServerToShardsInSync(rw));
        assertEquals(3, map.size());
        assertEquals(13, map.values().stream().mapToLong(List::size).sum());

        topologyService.deleteServer(1, voidRw());

        verify(nodeManagementClient, times(8)).prepareMove(any(), any(), anyInt());
        verify(nodeManagementClient, times(8)).processAction(eq(Action.MOVE_SHARDS));

        map = toMap(rw -> topologyService.fillServerToShardsInSync(rw));
        assertEquals(2, map.size());
        assertEquals(13, map.values().stream().mapToLong(List::size).sum());
    }
}

