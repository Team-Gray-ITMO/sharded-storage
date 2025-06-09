package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyResponse;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.client.SetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.SetKeyResponse;
import vk.itmo.teamgray.sharded.storage.node.service.NodeClientService;

public class NodeClientGrpcService extends NodeClientServiceGrpc.NodeClientServiceImplBase {
    private final NodeClientService nodeClientService;

    public NodeClientGrpcService(NodeClientService nodeClientService) {
        this.nodeClientService = nodeClientService;
    }

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<SetKeyResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();

        if (nodeClientService.isBusy()) {
            responseObserver.onNext(
                SetKeyResponse.newBuilder()
                    .setStatus(SetStatus.IS_BUSY.name())
                    .build()
            );
            responseObserver.onCompleted();
            return;
        }

        var errorMessage = nodeClientService.setKey(key, value);

        if (errorMessage.isPresent()) {
            Metadata metadata = new Metadata();

            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription(errorMessage.get())
                    .asRuntimeException(metadata)
            );

            return;
        }

        responseObserver.onNext(
            SetKeyResponse.newBuilder()
                .setStatus(SetStatus.SUCCESS.name())
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        String key = request.getKey();

        var response = nodeClientService.getKey(key);

        if (!response.success()) {
            Metadata metadata = new Metadata();

            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription(response.response())
                    .asRuntimeException(metadata)
            );
        }

        responseObserver.onNext(
            GetKeyResponse.newBuilder()
                // gRPC does not handle nulls well
                .setValue(Objects.requireNonNullElse(response.response(), ""))
                .build()
        );

        responseObserver.onCompleted();
    }
}
