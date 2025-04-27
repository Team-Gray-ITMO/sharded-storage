package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import java.time.Instant;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.dto.NodeHeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.SetFromFileResponseDTO;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.client.NodeHeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.node.client.SetFromFileRequest;
import vk.itmo.teamgray.sharded.storage.node.client.SetKeyRequest;

public class NodeClient extends AbstractGrpcClient<NodeClientServiceGrpc.NodeClientServiceBlockingStub> {
    public NodeClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeClientServiceGrpc.NodeClientServiceBlockingStub> getStubFactory() {
        return NodeClientServiceGrpc::newBlockingStub;
    }

    public boolean setKey(String key, String value) {
        SetKeyRequest request = SetKeyRequest.newBuilder()
            .setKey(key)
            .setValue(value)
            .build();

        return blockingStub.setKey(request).getSuccess();
    }

    public String getKey(String key) {
        GetKeyRequest request = GetKeyRequest.newBuilder()
            .setKey(key)
            .build();

        var value = blockingStub.getKey(request).getValue();

        //TODO Work on better strategy to handle null values in gRPC.
        if (value.isBlank()) {
            return null;
        }

        return value;
    }

    public SetFromFileResponseDTO setFromFile(String filePath) {
        SetFromFileRequest request = SetFromFileRequest.newBuilder()
            .setFilePath(filePath)
            .build();

        var response = blockingStub.setFromFile(request);
        return new SetFromFileResponseDTO(response.getMessage(), response.getSuccess());
    }

    public NodeHeartbeatResponseDTO sendHeartbeat() {
        var response = blockingStub
            .heartbeat(
                NodeHeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
        return new NodeHeartbeatResponseDTO(
            response.getHealthy(),
            response.getServerTimestamp(),
            response.getStatusMessage()
        );
    }
}
