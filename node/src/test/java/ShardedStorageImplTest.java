import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import vk.itmo.teamgray.sharded.storage.*;
import vk.itmo.teamgray.sharded.storage.node.ShardedStorageImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ShardedStorageImplTest {

    private ShardedStorageImpl storage;

    @BeforeEach
    void setUp() {
        storage = new ShardedStorageImpl();
    }

    @Test
    @Order(1)
    void setKey_withCorrectKey_shouldSavePair() {
        // set key
        SetKeyRequest request = SetKeyRequest.newBuilder()
                .setKey("testKey")
                .setValue("testValue")
                .build();
        StreamObserver<SetKeyResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<SetKeyResponse> responseCaptor = ArgumentCaptor.forClass(SetKeyResponse.class);

        storage.setKey(request, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();


        SetKeyResponse response = responseCaptor.getValue();
        assertTrue(response.getSuccess(), "Expected success to be true");

        // contains key
        ContainsKeyRequest containsKeyRequest = ContainsKeyRequest.newBuilder()
                .setKey(request.getKey())
                .build();
        StreamObserver<ContainsKeyResponse> containsResponseObserver = mock(StreamObserver.class);
        ArgumentCaptor<ContainsKeyResponse> containsResponseCaptor = ArgumentCaptor.forClass(ContainsKeyResponse.class);

        storage.containsKey(containsKeyRequest, containsResponseObserver);
        verify(containsResponseObserver).onNext(containsResponseCaptor.capture());
        verify(containsResponseObserver).onCompleted();

        ContainsKeyResponse containsKeyResponse = containsResponseCaptor.getValue();
        assertTrue(containsKeyResponse.getResult(), "Expected key to be present");

        GetValueByKeyRequest getValueByKeyRequest = GetValueByKeyRequest.newBuilder()
                .setKey(request.getKey())
                .build();
        StreamObserver<GetValueByKeyResponse> getValueResponseObserver = mock(StreamObserver.class);
        ArgumentCaptor<GetValueByKeyResponse> getValueResponseCaptor = ArgumentCaptor.forClass(GetValueByKeyResponse.class);

        storage.getValueByKey(getValueByKeyRequest, getValueResponseObserver);
        verify(getValueResponseObserver).onNext(getValueResponseCaptor.capture());
        verify(getValueResponseObserver).onCompleted();

        GetValueByKeyResponse getValueByKeyResponse = getValueResponseCaptor.getValue();
        assertEquals(request.getKey(), getValueByKeyRequest.getKey(), "Expected value by key is saved");
    }

    @Test
    @Order(2)
    void setKey_withTwoDifferentKeys_shouldSaveTwoPairs() {
        // set key
        SetKeyRequest firstRequest = SetKeyRequest.newBuilder()
                .setKey("testKey1")
                .setValue("testValue1")
                .build();
        SetKeyRequest secondRequest = SetKeyRequest.newBuilder()
                .setKey("testKey2")
                .setValue("testValue2")
                .build();

        StreamObserver<SetKeyResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<SetKeyResponse> responseCaptor = ArgumentCaptor.forClass(SetKeyResponse.class);

        storage.setKey(firstRequest, responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        SetKeyResponse response = responseCaptor.getValue();
        assertTrue(response.getSuccess(), "Expected success to be true");

        storage.setKey(secondRequest, responseObserver);
        verify(responseObserver, times(2)).onNext(responseCaptor.capture());
        verify(responseObserver, times(2)).onCompleted();

        response = responseCaptor.getValue();
        assertTrue(response.getSuccess(), "Expected success to be true");

        // contains first key
        ContainsKeyRequest containsKeyRequest = ContainsKeyRequest.newBuilder()
                .setKey(firstRequest.getKey())
                .build();
        StreamObserver<ContainsKeyResponse> containsResponseObserver = mock(StreamObserver.class);
        ArgumentCaptor<ContainsKeyResponse> containsResponseCaptor = ArgumentCaptor.forClass(ContainsKeyResponse.class);

        storage.containsKey(containsKeyRequest, containsResponseObserver);
        verify(containsResponseObserver).onNext(containsResponseCaptor.capture());
        verify(containsResponseObserver).onCompleted();

        ContainsKeyResponse containsKeyResponse = containsResponseCaptor.getValue();
        assertTrue(containsKeyResponse.getResult(), "Expected key to be present");

        // contains second key
        containsKeyRequest = ContainsKeyRequest.newBuilder()
                .setKey(secondRequest.getKey())
                .build();

        storage.containsKey(containsKeyRequest, containsResponseObserver);
        verify(containsResponseObserver, times(2)).onNext(containsResponseCaptor.capture());
        verify(containsResponseObserver, times(2)).onCompleted();

        containsKeyResponse = containsResponseCaptor.getValue();
        assertTrue(containsKeyResponse.getResult(), "Expected key to be present");
    }

}
