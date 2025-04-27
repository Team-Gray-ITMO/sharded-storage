package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import vk.itmo.teamgray.sharded.storage.node.client.shards.ShardData;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsResponse;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NodeClientServiceTest {

    private NodeClientService nodeClientService;
    private NodeStorageService nodeStorageService;

    @BeforeEach
    public void setUp() {
        Map<Integer, ShardData> shards = new HashMap<>();
        shards.put(0, new ShardData());
        shards.put(1, new ShardData());
        shards.put(2, new ShardData());

        nodeStorageService = new NodeStorageService();
        nodeStorageService.replace(shards);
        nodeClientService = new NodeClientService(nodeStorageService);
    }

    @Test
    public void sendShardFragment_forExistentShard_shouldReturnSuccessAndSaveFragments() {
        Map<String, String> fragments = new HashMap<>();
        fragments.put("key6", "bar");

        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
                .setShardId(2)
                .putAllShardFragments(fragments)
                .build();
        StreamObserver<SendShardFragmentResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<SendShardFragmentResponse> responseCaptor = ArgumentCaptor.forClass(
                SendShardFragmentResponse.class
        );

        nodeClientService.sendShardFragment(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        assertEquals(3, nodeStorageService.getShards().size());
    }

    @Test
    public void sendShardFragment_forNewShard_shouldReturnSuccessAndSaveFragments() {
        Map<String, String> fragments = new HashMap<>();
        fragments.put("key12", "bar");

        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
                .setShardId(3)
                .putAllShardFragments(fragments)
                .build();
        StreamObserver<SendShardFragmentResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<SendShardFragmentResponse> responseCaptor = ArgumentCaptor.forClass(
                SendShardFragmentResponse.class
        );

        nodeClientService.sendShardFragment(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        var response = responseCaptor.getValue();

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().startsWith("SUCCESS"));
        assertEquals(4, nodeStorageService.getShards().size());
    }

}
