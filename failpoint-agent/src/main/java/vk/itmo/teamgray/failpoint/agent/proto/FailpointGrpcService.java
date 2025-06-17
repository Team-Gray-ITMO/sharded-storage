package vk.itmo.teamgray.failpoint.agent.proto;

import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.failpoint.AddFailpointRequest;
import vk.itmo.teamgray.failpoint.FailpointServiceGrpc;
import vk.itmo.teamgray.failpoint.FreezeForRequest;
import vk.itmo.teamgray.failpoint.HasFailpointResponse;
import vk.itmo.teamgray.failpoint.IsFrozenResponse;
import vk.itmo.teamgray.failpoint.MethodRequest;
import vk.itmo.teamgray.failpoint.agent.FailpointRegistry;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;

import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.fromGrpcBuilder;

public class FailpointGrpcService extends FailpointServiceGrpc.FailpointServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(FailpointGrpcService.class);

    @Override
    public void addFailpoint(AddFailpointRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();

        FailpointRegistry.addFailpoint(
            request.getMethodClass(),
            request.getMethodName(),
            request.getExceptionClass(),
            fromGrpcBuilder(response)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeFailpoint(MethodRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();

        FailpointRegistry.removeFailpoint(
            request.getMethodClass(),
            request.getMethodName(),
            fromGrpcBuilder(response)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void hasFailpoint(MethodRequest request, StreamObserver<HasFailpointResponse> responseObserver) {
        try {
            var response = HasFailpointResponse.newBuilder()
                .setHasFailpoint(FailpointRegistry.hasFailpoint(request.getMethodClass(), request.getMethodName()))
                .build();

            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error("Caught error: ", e);

            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void freezeMethod(MethodRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();
        FailpointRegistry.freeze(
            request.getMethodClass(),
            request.getMethodName(),
            fromGrpcBuilder(response)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void unfreezeMethod(MethodRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();
        FailpointRegistry.unfreeze(
            request.getMethodClass(),
            request.getMethodName(),
            fromGrpcBuilder(response)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void freezeFor(FreezeForRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();
        FailpointRegistry.freezeFor(
            request.getMethodClass(),
            request.getMethodName(),
            Duration.of(request.getDuration(), ChronoUnit.MILLIS),
            fromGrpcBuilder(response)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void awaitUnfreeze(MethodRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();
        FailpointRegistry.awaitUnfreeze(
            request.getMethodClass(),
            request.getMethodName(),
            fromGrpcBuilder(response)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void isFrozen(MethodRequest request, StreamObserver<IsFrozenResponse> responseObserver) {
        try {
            var response = IsFrozenResponse.newBuilder()
                .setIsFrozen(FailpointRegistry.isFrozen(request.getMethodClass(), request.getMethodName()))
                .build();

            responseObserver.onNext(response);
        } catch (Exception e) {
            log.error("Caught error: ", e);

            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void clear(Empty request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();
        FailpointRegistry.clear(fromGrpcBuilder(response));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}
