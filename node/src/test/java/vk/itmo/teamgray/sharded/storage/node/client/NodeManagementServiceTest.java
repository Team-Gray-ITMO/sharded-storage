package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import vk.itmo.teamgray.sharded.storage.common.HashingUtils;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NodeManagementServiceTest {

    @Test
    public void testRearrangeShardsWithEmptyMapping() {
        NodeStorageService nodeStorageService = new NodeStorageService();
        NodeManagementService service = new NodeManagementService(nodeStorageService);

        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder().build();
        StreamObserver<RearrangeShardsResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<RearrangeShardsResponse> responseCaptor = ArgumentCaptor.forClass(RearrangeShardsResponse.class);

        service.rearrangeShards(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertTrue(nodeStorageService.getShards().isEmpty());
    }

    @Test
    public void testRearrangeShardsWithEmptyStorageAndNotEmptyRequestMapping() {
        NodeStorageService nodeStorageService = new NodeStorageService();
        NodeManagementService service = new NodeManagementService(nodeStorageService);

        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder()
                .putShardToHash(1, Long.MAX_VALUE / 2)
                .putShardToHash(2, Long.MAX_VALUE)
                .build();
        StreamObserver<RearrangeShardsResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<RearrangeShardsResponse> responseCaptor = ArgumentCaptor.forClass(RearrangeShardsResponse.class);

        service.rearrangeShards(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertEquals(request.getShardToHashMap().size(), nodeStorageService.getShards().size());
        nodeStorageService.getShards().forEach((shardNum, shardData) -> {
            assertTrue(shardData.getStorage().isEmpty());
        });
    }

    //TODO Update test to new conditions
    @Disabled
    @Test
    public void testRearrangeShardsWithNotEmptyStorageAndNotEmptyRequestMapping() {
        NodeStorageService nodeStorageService = new NodeStorageService();
        Map<String, String> pairs = new HashMap<>();
        pairs.put("key1", "value1");
        pairs.put("key2", "value2");
        pairs.put("key3", "value3");
        pairs.put("key4", "value4");

        Map<String, Long> hashByKey = new HashMap<>();
        pairs.keySet().forEach(key -> hashByKey.put(key, HashingUtils.calculate64BitHash(key)));
        pairs.forEach((key, value) -> {
            nodeStorageService.set(key, value);
            hashByKey.put(key, HashingUtils.calculate64BitHash(key));
        });

        NodeManagementService service = new NodeManagementService(nodeStorageService);

        List<Map.Entry<String, Long>> sortedHashesByKey = new ArrayList<>(hashByKey.entrySet());
        sortedHashesByKey.sort(Map.Entry.comparingByValue());

        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder()
                .putShardToHash(1, sortedHashesByKey.get(1).getValue() + 1)
                .putShardToHash(2, Long.MAX_VALUE)
                .build();
        StreamObserver<RearrangeShardsResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<RearrangeShardsResponse> responseCaptor = ArgumentCaptor.forClass(RearrangeShardsResponse.class);

        service.rearrangeShards(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertEquals(request.getShardToHashMap().size(), nodeStorageService.getShards().size());

        var shards = nodeStorageService.getShards();
        assertEquals(2, shards.get(1).getStorage().size());
        assertEquals(2, shards.get(2).getStorage().size());

        assertNotNull(shards.get(1).getValue(sortedHashesByKey.getFirst().getKey()));
        assertNotNull(shards.get(1).getValue(sortedHashesByKey.get(1).getKey()));
        assertNotNull(shards.get(2).getValue(sortedHashesByKey.get(2).getKey()));
        assertNotNull(shards.get(2).getValue(sortedHashesByKey.get(3).getKey()));

    }

}
