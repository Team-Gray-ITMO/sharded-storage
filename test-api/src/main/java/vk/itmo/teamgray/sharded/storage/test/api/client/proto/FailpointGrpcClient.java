package vk.itmo.teamgray.sharded.storage.test.api.client.proto;

import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.function.Function;
import vk.itmo.teamgray.failpoint.AddFailpointRequest;
import vk.itmo.teamgray.failpoint.FailpointServiceGrpc;
import vk.itmo.teamgray.failpoint.FreezeForRequest;
import vk.itmo.teamgray.failpoint.MethodRequest;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.test.api.client.FailpointClient;

public class FailpointGrpcClient extends AbstractGrpcClient<FailpointServiceGrpc.FailpointServiceBlockingStub> implements FailpointClient {
    public FailpointGrpcClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, FailpointServiceGrpc.FailpointServiceBlockingStub> getStubFactory() {
        return FailpointServiceGrpc::newBlockingStub;
    }

    @Override
    public StatusResponseDTO addFailpoint(Class<?> methodClass, String methodName, Class<?> exceptionClass) {
        AddFailpointRequest request = AddFailpointRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .setExceptionClass(exceptionClass.getCanonicalName())
            .build();

        return new StatusResponseDTO(blockingStub.addFailpoint(request));
    }

    @Override
    public StatusResponseDTO removeFailpoint(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return new StatusResponseDTO(blockingStub.removeFailpoint(request));
    }

    @Override
    public boolean hasFailpoint(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return blockingStub.hasFailpoint(request).getHasFailpoint();
    }

    @Override
    public StatusResponseDTO freezeMethod(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return new StatusResponseDTO(blockingStub.freezeMethod(request));
    }

    @Override
    public StatusResponseDTO unfreezeMethod(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return new StatusResponseDTO(blockingStub.unfreezeMethod(request));
    }

    @Override
    public StatusResponseDTO awaitUnfreeze(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return new StatusResponseDTO(blockingStub.awaitUnfreeze(request));
    }

    @Override
    public StatusResponseDTO awaitFreeze(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return new StatusResponseDTO(blockingStub.awaitFreeze(request));
    }

    @Override
    public StatusResponseDTO freezeFor(Class<?> methodClass, String methodName, Duration duration) {
        FreezeForRequest request = FreezeForRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .setDuration(duration.toMillis())
            .build();

        return new StatusResponseDTO(blockingStub.freezeFor(request));
    }

    @Override
    public boolean isFrozen(Class<?> methodClass, String methodName) {
        MethodRequest request = MethodRequest.newBuilder()
            .setMethodClass(methodClass.getCanonicalName())
            .setMethodName(methodName)
            .build();

        return blockingStub.isFrozen(request).getIsFrozen();
    }

    @Override
    public StatusResponseDTO clear() {
        return new StatusResponseDTO(blockingStub.clear(Empty.newBuilder().build()));
    }
}
