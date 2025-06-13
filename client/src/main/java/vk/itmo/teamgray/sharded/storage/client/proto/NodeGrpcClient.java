package vk.itmo.teamgray.sharded.storage.client.proto;

import io.grpc.ManagedChannel;
import java.time.Instant;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.common.dto.GetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.GetStatus;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.client.SetKeyRequest;

public class NodeGrpcClient extends AbstractGrpcClient<NodeClientServiceGrpc.NodeClientServiceBlockingStub> implements NodeClient {
    public NodeGrpcClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeClientServiceGrpc.NodeClientServiceBlockingStub> getStubFactory() {
        return NodeClientServiceGrpc::newBlockingStub;
    }

    @Override
    public SetResponseDTO setKey(String key, String value, Instant timestamp) {
        SetKeyRequest request = SetKeyRequest.newBuilder()
            .setKey(key)
            .setValue(value)
            .setTimestamp(timestamp.toEpochMilli())
            .build();

        var response = blockingStub.setKey(request);

        return new SetResponseDTO(SetStatus.valueOf(response.getStatus()), response.getMessage(), response.getNewNodeId());
    }

    @Override
    public GetResponseDTO getKey(String key) {
        GetKeyRequest request = GetKeyRequest.newBuilder()
            .setKey(key)
            .build();

        var response = blockingStub.getKey(request);

        return new GetResponseDTO(
            GetStatus.valueOf(response.getStatus()),
            //TODO Work on better strategy to handle null values in gRPC.
            response.getValue().isBlank()
                ? null
                : response.getValue()
        );
    }
}
