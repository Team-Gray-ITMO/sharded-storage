package vk.itmo.teamgray.sharded.storage.client.client;

import io.grpc.ManagedChannel;
import java.util.function.Function;

import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.client.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.client.NodeClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.client.SetKeyRequest;

public class NodeClient extends AbstractGrpcClient<NodeClientServiceGrpc.NodeClientServiceBlockingStub> {
    public NodeClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeClientServiceGrpc.NodeClientServiceBlockingStub> getStubFactory() {
        return NodeClientServiceGrpc::newBlockingStub;
    }

    public SetStatus setKey(String key, String value) {
        SetKeyRequest request = SetKeyRequest.newBuilder()
            .setKey(key)
            .setValue(value)
            .build();

        var result = blockingStub.setKey(request).getMessage();
        return SetStatus.valueOf(result);
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

}
