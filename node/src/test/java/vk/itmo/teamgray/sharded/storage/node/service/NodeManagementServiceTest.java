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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class NodeManagementServiceTest {
    private NodeStorageService nodeStorageService;

    private NodeManagementService service;

    private DiscoveryClient discoveryClient = mock();
    
    private ClientCachingFactory clientCachingFactory = mock();

    @BeforeEach
    public void setUp() {
        nodeStorageService = new NodeStorageService();
        service = new NodeManagementService(nodeStorageService, discoveryClient, clientCachingFactory);
    }

    @Test
    public void testPrepareRearrangeWithEmptyMapping() {
        StatusResponseDTO response = new StatusResponseDTO();

        service.prepareRearrange(
            Collections.emptyList(),
            Collections.emptyMap(),
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
            Collections.emptyList(),
            Collections.emptyMap(),
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
        FragmentDTO fragment = new FragmentDTO(2, 1, 0, Long.MAX_VALUE);
        service.prepareRearrange(
            List.of(fragment),
            Collections.emptyMap(),
            shardToHash,
            1,
            (success, message) -> {
            });

        nodeStorageService.getShards().getShardMap().put(2, new ShardData());
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key1", "value1");
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key2", "value2");

        StatusResponseDTO response = new StatusResponseDTO();

        service.processRearrange(
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
        assertEquals(2, nodeStorageService.getStagedShards().getShardMap().get(1).getStorage().size());
    }

    @Test
    public void testApplyAction() {
        Map<Integer, Long> shardToHash = Map.of(1, 1000L);
        FragmentDTO fragment = new FragmentDTO(2, 1, 0, Long.MAX_VALUE);

        service.prepareRearrange(
            List.of(fragment),
            Collections.emptyMap(),
            shardToHash,
            1,
            (success, message) -> {
                // No-op
            });

        nodeStorageService.getShards().getShardMap().put(2, new ShardData());
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key1", "value1");
        nodeStorageService.getShards().getShardMap().get(2).addToStorage("key2", "value2");
        service.processRearrange(
            (success, message) -> {
            });

        StatusResponseDTO response = new StatusResponseDTO();
        service.applyAction(
            Action.REARRANGE_SHARDS,
            (success, message) -> {
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
            List.of(),
            Collections.emptyMap(),
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
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertFalse(response.isSuccess());
        assertNotNull(response.getMessage());
    }

    @Test
    public void testProcessRearrangeWithEmptyFragmentList() {
        service.prepareRearrange(
            List.of(),
            Collections.emptyMap(),
            Map.of(1, 1000L),
            1,
            (success, message) -> {
                // No-op
            });

        StatusResponseDTO response = new StatusResponseDTO();
        service.processRearrange(
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
    }

    @Test
    public void testRollbackAction() {
        // prepare
        service.prepareRearrange(
            List.of(),
            Collections.emptyMap(),
            Map.of(1, 1000L),
            1,
            (success, message) -> {
                // No-op
            });

        // test rollback
        StatusResponseDTO rollbackResponse = new StatusResponseDTO();
        service.rollbackAction(
            Action.REARRANGE_SHARDS,
            (success, message) -> {
                rollbackResponse.setSuccess(success);
                rollbackResponse.setMessage(message);
            }
        );

        assertTrue(rollbackResponse.isSuccess());
    }


    @Test
    public void testConcurrentPrepareAndRollback() throws InterruptedException {
        // Start both operations simultaneously
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(2);
        
        AtomicBoolean prepareSuccess = new AtomicBoolean(false);
        AtomicBoolean rollbackSuccess = new AtomicBoolean(false);

        // Prepare thread
        Thread prepareThread = new Thread(() -> {
            try {
                startLatch.await();
                StatusResponseDTO response = new StatusResponseDTO();
                service.prepareRearrange(
                    List.of(),
                    Collections.emptyMap(),
                    Map.of(1, 1000L),
                    1,
                    (success, message) -> {
                        response.setSuccess(success);
                        response.setMessage(message);
                    });
                prepareSuccess.set(response.isSuccess());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endLatch.countDown();
            }
        });

        // Rollback thread
        Thread rollbackThread = new Thread(() -> {
            try {
                startLatch.await();
                StatusResponseDTO response = new StatusResponseDTO();
                service.rollbackAction(
                    Action.REARRANGE_SHARDS,
                    (success, message) -> {
                        response.setSuccess(success);
                        response.setMessage(message);
                    });
                rollbackSuccess.set(response.isSuccess());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endLatch.countDown();
            }
        });

        prepareThread.start();
        rollbackThread.start();
        
        // Start both threads simultaneously
        startLatch.countDown();
        
        // Wait for both to complete
        endLatch.await(10, TimeUnit.SECONDS);
        
        // At least one should succeed
        assertTrue(prepareSuccess.get() || rollbackSuccess.get(), 
            "At least one operation should succeed");
        
        prepareThread.join(1000);
        rollbackThread.join(1000);
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
                            List.of(),
                            Collections.emptyMap(),
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
    public void testProcessMoveFailsIfTargetNodeDoesNotExist() {
        // target not exists
        when(discoveryClient.getNode(anyInt())).thenReturn(null);

        // add node for moving
        nodeStorageService.getShards().getShardMap().put(1, new ShardData());

        StatusResponseDTO prepareResponse = new StatusResponseDTO();
        StatusResponseDTO processResponse = new StatusResponseDTO();

        service.prepareMove(
            List.of(), 
            List.of(new SendShardTaskDTO(1, 99)),
            1, 
            (success, message) -> {
                prepareResponse.setSuccess(success);
                prepareResponse.setMessage(message);
            }
        );

        service.processMove((success, message) -> {
            processResponse.setSuccess(success);
            processResponse.setMessage(message);
        });
            
        assertFalse(processResponse.isSuccess());
        assertEquals("No server with id 99 found", processResponse.getMessage());
    }

    @Test
    public void testProcessMoveDoNothingIfShardDoesNotExist() {
        // some target node exists
        when(discoveryClient.getNode(anyInt())).thenReturn(mock());

        StatusResponseDTO prepareResponse = new StatusResponseDTO();
        StatusResponseDTO processResponse = new StatusResponseDTO();

        // shard 99 not in nodeStorageService, so does not exist
        service.prepareMove(
            List.of(),
            List.of(new SendShardTaskDTO(99, 1)),
            1,
            (success, message) -> {
                prepareResponse.setSuccess(success);
                prepareResponse.setMessage(message);
            }
        );

        service.processMove((success, message) -> {
            processResponse.setSuccess(success);
            processResponse.setMessage(message);
        });

        // no interactions with clientCachingFactory for sending shards
        verifyNoInteractions(clientCachingFactory);
        assertTrue(processResponse.isSuccess());
    }
}
