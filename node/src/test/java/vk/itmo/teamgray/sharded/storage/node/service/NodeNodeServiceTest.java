package vk.itmo.teamgray.sharded.storage.node.service;

import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NodeNodeServiceTest {

    private NodeNodeService nodeNodeService;

    private NodeStorageService nodeStorageService;

    private int shardCount = 3;

    @BeforeEach
    public void setUp() {
        Map<Integer, ShardData> shards = IntStream.range(0, shardCount)
            .boxed()
            .collect(Collectors.toMap(
                it -> it,
                it -> new ShardData()
            ));

        nodeStorageService = new NodeStorageService();
        nodeStorageService.replace(shards, shardCount);
        nodeNodeService = new NodeNodeService(nodeStorageService);
    }

    @Test
    public void sendShardFragment_forExistentShard_shouldReturnSuccessAndSaveFragments() {
        Map<String, String> fragments = new HashMap<>();
        String key = "key6";

        fragments.put(key, "bar");

        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
            .setShardId(Objects.requireNonNull(ShardUtils.getShardIdForKey(key, shardCount)))
            .putAllShardFragments(fragments)
            .build();
        StreamObserver<StatusResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<StatusResponse> responseCaptor = ArgumentCaptor.forClass(
            StatusResponse.class
        );

        nodeNodeService.sendShardFragment(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        assertEquals(shardCount, nodeStorageService.getShards().size());
    }

    @Test
    public void sendShardFragment_forNewShard_shouldReturnSuccessAndSaveFragments() {
        Map<String, String> fragments = new HashMap<>();
        String key = "key12";
        fragments.put(key, "bar");

        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
            .setShardId(Objects.requireNonNull(ShardUtils.getShardIdForKey(key, shardCount)))
            .putAllShardFragments(fragments)
            .build();
        StreamObserver<StatusResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<StatusResponse> responseCaptor = ArgumentCaptor.forClass(
            StatusResponse.class
        );

        nodeNodeService.sendShardFragment(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        assertEquals(shardCount, nodeStorageService.getShards().size());
    }

}
