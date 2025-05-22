package vk.itmo.teamgray.sharded.storage.node.service;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NodeManagementServiceTest {

    @Test
    public void testRearrangeShardsWithEmptyMapping() {
        NodeStorageService nodeStorageService = new NodeStorageService();
        NodeManagementService service = new NodeManagementService(nodeStorageService, mock());

        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder().build();
        StreamObserver<StatusResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<StatusResponse> responseCaptor = ArgumentCaptor.forClass(StatusResponse.class);

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
        NodeManagementService service = new NodeManagementService(nodeStorageService, mock());

        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder()
            .putShardToHash(1, Long.MAX_VALUE / 2)
            .putShardToHash(2, Long.MAX_VALUE)
            .build();
        StreamObserver<StatusResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<StatusResponse> responseCaptor = ArgumentCaptor.forClass(StatusResponse.class);

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
}
