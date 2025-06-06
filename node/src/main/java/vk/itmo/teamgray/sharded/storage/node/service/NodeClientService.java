package vk.itmo.teamgray.sharded.storage.node.service;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.text.MessageFormat;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyResponse;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.client.SetKeyRequest;

// TODO Decouple to gRPC Service and Service with business logic. Example: 'HealthGrpcService' and 'HealthService'
public class NodeClientService extends NodeClientServiceGrpc.NodeClientServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeClientService.class);

    private final NodeStorageService nodeStorageService;

    public NodeClientService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<StatusResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();

        if (nodeStorageService.isRearranging()) {
            responseObserver.onNext(
                StatusResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(SetStatus.REARRANGE_IN_PROGRESS.name())
                    .build()
            );
            responseObserver.onCompleted();
            return;
        }

        try {
            nodeStorageService.set(key, value);
        } catch (NodeException e) {
            Metadata metadata = new Metadata();

            String errMessage = MessageFormat.format("Error while setting key=[{0}] value=[{1}]", key, value);

            log.warn(errMessage, e);

            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription(errMessage)
                    .asRuntimeException(metadata)
            );

            return;
        }

        responseObserver.onNext(
            StatusResponse.newBuilder()
                .setSuccess(true)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        String key = request.getKey();

        String returnValue;
        try {
            returnValue = nodeStorageService.get(key);
        } catch (NodeException e) {
            Metadata metadata = new Metadata();

            String errMessage = MessageFormat.format("Error while getting by key=[{0}]", key);

            log.warn(errMessage, e);

            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription(errMessage)
                    .asRuntimeException(metadata)
            );
            return;
        }

        responseObserver.onNext(
            GetKeyResponse.newBuilder()
                // gRPC does not handle nulls well
                .setValue(Objects.requireNonNullElse(returnValue, ""))
                .build()
        );

        responseObserver.onCompleted();
    }
}
