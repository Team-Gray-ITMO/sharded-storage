package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;

public class NodeClientService extends NodeClientServiceGrpc.NodeClientServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeClientService.class);

    private final NodeStorageService nodeStorageService;

    public NodeClientService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<SetKeyResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();

        try {
            nodeStorageService.set(key, value);
        } catch (NodeException e) {
            Metadata metadata = new Metadata();
            String errMessage = MessageFormat.format("Error while setting key=[{0}] value=[{1}]", key, value);
            log.warn(errMessage, e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMessage)
                .asRuntimeException(metadata));
            return;
        }

        responseObserver.onNext(
            SetKeyResponse.newBuilder()
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
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMessage)
                .asRuntimeException(metadata));
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

    @Override
    public void setFromFile(SetFromFileRequest request, StreamObserver<SetFromFileResponse> responseObserver) {
        String filePath = request.getFilePath();
        boolean success = true;
        String message = "SUCCESS";

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");

                String key = parts[0].trim();
                String value = parts[1].trim();

                try {
                    nodeStorageService.set(key, value);
                } catch (NodeException e) {
                    Metadata metadata = new Metadata();
                    String errMessage = MessageFormat.format("Error while setting key=[{0}] value=[{1}]", key, value);
                    log.warn(errMessage, e);
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMessage)
                        .asRuntimeException(metadata));
                    return;
                }
            }
        } catch (IOException e) {
            success = false;
            message = "ERROR: " + e.getMessage();
        }

        responseObserver.onNext(
            SetFromFileResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(NodeHeartbeatRequest request, StreamObserver<NodeHeartbeatResponse> responseObserver) {
        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(NodeHeartbeatResponse.newBuilder()
            .setHealthy(isHealthy)
            .setServerTimestamp(now.toEpochMilli())
            .setStatusMessage("OK")
            .build());

        responseObserver.onCompleted();
    }
}
