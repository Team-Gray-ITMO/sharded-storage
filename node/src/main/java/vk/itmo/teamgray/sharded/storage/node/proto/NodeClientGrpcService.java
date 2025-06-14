package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Objects;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyResponse;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.client.NodeStatusResponse;
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
        var dto = nodeClientService.setKey(
            request.getKey(),
            request.getValue(),
            Instant.ofEpochMilli(request.getTimestamp())
        );

        var response = SetKeyResponse.newBuilder()
            .setStatus(dto.status().name())
            .setMessage(dto.message())
            .setNewNodeId(dto.newNodeId())
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        var response = GetKeyResponse.newBuilder();

        nodeClientService.getKey(
            request.getKey(),
            (status, value) -> {
                response.setStatus(status.name());
                // gRPC does not handle nulls well
                response.setValue(Objects.requireNonNullElse(value, ""));
            }
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNodeStatus(Empty request, StreamObserver<NodeStatusResponse> responseObserver) {
        responseObserver.onNext(nodeClientService.getNodeStatus().toGrpc());
        responseObserver.onCompleted();
    }
}
