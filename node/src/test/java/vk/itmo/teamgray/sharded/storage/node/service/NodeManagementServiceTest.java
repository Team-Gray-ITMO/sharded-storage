package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.MoveShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeManagementServiceTest {
    private NodeStorageService nodeStorageService;

    private NodeManagementService service;

    private DiscoveryClient discoveryClient = mock();

    @BeforeEach
    public void setUp() {
        nodeStorageService = new NodeStorageService();
        service = new NodeManagementService(nodeStorageService, discoveryClient);
    }

    @Test
    public void testPrepareRearrangeWithEmptyMapping() {
        StatusResponseDTO response = new StatusResponseDTO();

        service.prepareRearrange(
            Collections.emptyMap(),
            0,
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
        assertTrue(nodeStorageService.getStagedShards().getShardMap().isEmpty());
        assertEquals(0, nodeStorageService.getStagedShards().getFullShardCount());
    }

    @Test
    public void testPrepareRearrangeWithValidMapping() {
        Map<Integer, Long> shardToHash = Map.of(
            1, 1000L,
            2, 2000L
        );

        StatusResponseDTO response = new StatusResponseDTO();

        service.prepareRearrange(
            shardToHash,
            shardToHash.size(),
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
        assertEquals(shardToHash.size(), nodeStorageService.getStagedShards().getShardMap().size());
        assertEquals(shardToHash.size(), nodeStorageService.getStagedShards().getFullShardCount());
    }

    @Test
    public void testProcessRearrangeWithLocalFragments() {
        Map<Integer, Long> shardToHash = Map.of(1, 1000L);
        service.prepareRearrange(shardToHash, 1, (success, message) -> {
        });

        nodeStorageService.getShards().getShardMap().put(2, new ShardData());
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key1", "value1");
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key2", "value2");

        FragmentDTO fragment = new FragmentDTO(2, 1, 0, Long.MAX_VALUE);
        StatusResponseDTO response = new StatusResponseDTO();

        service.processRearrange(
            List.of(fragment),
            Collections.emptyMap(),
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
        assertEquals(2, nodeStorageService.getStagedShards().getShardMap().get(1).getStorage().size());
    }

    @Test
    public void testApplyRearrange() {
        Map<Integer, Long> shardToHash = Map.of(1, 1000L);
        service.prepareRearrange(shardToHash, 1, (success, message) -> {
        });

        nodeStorageService.getShards().getShardMap().put(2, new ShardData());
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key1", "value1");
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key2", "value2");

        FragmentDTO fragment = new FragmentDTO(2, 1, 0, Long.MAX_VALUE);
        service.processRearrange(
            List.of(fragment),
            Collections.emptyMap(),
            (success, message) -> {
            });

        StatusResponseDTO response = new StatusResponseDTO();
        service.applyRearrange((success, message) -> {
            response.setSuccess(success);
            response.setMessage(message);
        });

        assertTrue(response.isSuccess());
        assertEquals(1, nodeStorageService.getShards().getShardMap().size());
        assertTrue(nodeStorageService.getShards().getShardMap().containsKey(1));
        assertEquals(2, nodeStorageService.getShards().getShardMap().get(1).getStorage().size());
        assertNull(nodeStorageService.getStagedShards());
    }

    @Test
    public void testPrepareRearrangeWithNullShardMapping() {
        StatusResponseDTO response = new StatusResponseDTO();

        service.prepareRearrange(
            null,
            0,
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertFalse(response.isSuccess());
        assertNotNull(response.getMessage());
    }

    @Test
    public void testProcessRearrangeWithNoPreparedState() {
        FragmentDTO fragment = new FragmentDTO(1, 2, 0, Long.MAX_VALUE);
        StatusResponseDTO response = new StatusResponseDTO();

        service.processRearrange(
            List.of(fragment),
            Collections.emptyMap(),
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertFalse(response.isSuccess());
        assertNotNull(response.getMessage());
    }

    @Test
    public void testProcessRearrangeWithEmptyFragmentList() {
        service.prepareRearrange(Map.of(1, 1000L), 1, (success, message) -> {
        });

        StatusResponseDTO response = new StatusResponseDTO();
        service.processRearrange(
            Collections.emptyList(),
            Collections.emptyMap(),
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
    }

    //TODO Rewrite
    @Disabled
    @Test
    public void testRollbackDuringPrepare() throws InterruptedException {
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            AtomicBoolean rollbackTriggered = new AtomicBoolean(false);

            executor.submit(() -> {
                try {
                    service.prepareRearrange(
                        Map.of(1, 1000L),
                        1,
                        (success, message) -> {
                            if (!success) {
                                rollbackTriggered.set(true);
                            }
                        });
                } catch (Exception e) {
                    fail("Unexpected exception");
                }
            });

            Thread.sleep(100);

            StatusResponseDTO rollbackResponse = new StatusResponseDTO();
            service.rollbackRearrange((success, message) -> {
                rollbackResponse.setSuccess(success);
                rollbackResponse.setMessage(message);
            });

            assertTrue(rollbackTriggered.get());
            assertTrue(rollbackResponse.isSuccess());
            executor.shutdown();
        }
    }

    //TODO Rewrite
    @Disabled
    @Test
    public void testConcurrentPrepareAndRollback() throws InterruptedException {
        int threadCount = 5;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch latch = new CountDownLatch(threadCount);
            Map<Boolean, Integer> results = new ConcurrentHashMap<>();

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        StatusResponseDTO response = new StatusResponseDTO();
                        if (Thread.currentThread().threadId() % 2 == 0) {
                            service.prepareRearrange(
                                Map.of(1, 1000L),
                                1,
                                (success, message) -> {
                                    response.setSuccess(success);
                                    response.setMessage(message);
                                });
                        } else {
                            service.rollbackRearrange(
                                (success, message) -> {
                                    response.setSuccess(success);
                                    response.setMessage(message);
                                });
                        }
                        results.merge(response.isSuccess(), 1, Integer::sum);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertTrue(results.getOrDefault(true, 0) > 0);
            executor.shutdown();
        }
    }

    @Test
    public void testMultipleConcurrentRearranges() throws InterruptedException {
        int threadCount = 3;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicBoolean success = new AtomicBoolean(true);

            for (int i = 0; i < threadCount; i++) {
                final int shardId = i + 1;
                executor.submit(() -> {
                    try {
                        StatusResponseDTO response = new StatusResponseDTO();
                        service.prepareRearrange(
                            Map.of(shardId, (long)shardId * 1000),
                            1,
                            (s, m) -> {
                                response.setSuccess(s);
                                response.setMessage(m);
                            });

                        if (!response.isSuccess()) {
                            success.set(false);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(1, nodeStorageService.getStagedShards().getShardMap().size());
            executor.shutdown();
        }
    }

    @Test
    public void testMoveShardToNonExistentNode() {
        when(discoveryClient.getNode(anyInt())).thenReturn(null);

        StatusResponseDTO response = new StatusResponseDTO();
        service.moveShards(List.of(new MoveShardDTO(1, 99)), (success, message) -> {
            response.setSuccess(success);
            response.setMessage(message);
        });

        assertFalse(response.isSuccess());
        assertNotNull(response.getMessage());
    }

    @Test
    public void testMoveNonExistentShard() {
        StatusResponseDTO response = new StatusResponseDTO();
        service.moveShards(List.of(new MoveShardDTO(99, 1)), (success, message) -> {
            response.setSuccess(success);
            response.setMessage(message);
        });

        assertFalse(response.isSuccess());
        assertNotNull(response.getMessage());
    }
}
