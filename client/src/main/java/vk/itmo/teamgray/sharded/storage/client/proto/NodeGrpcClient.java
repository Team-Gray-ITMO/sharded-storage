package vk.itmo.teamgray.sharded.storage.client.proto;

import io.grpc.ManagedChannel;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
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
    public SetStatus setKey(String key, String value) {
        SetKeyRequest request = SetKeyRequest.newBuilder()
            .setKey(key)
            .setValue(value)
            .build();

        var result = blockingStub.setKey(request).getStatus();

        return SetStatus.valueOf(result);
    }

    @Override
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
