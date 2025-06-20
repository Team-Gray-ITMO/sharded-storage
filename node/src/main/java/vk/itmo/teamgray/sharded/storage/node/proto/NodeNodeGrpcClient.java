package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.client.NodeNodeClient;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardsRequest;

public class NodeNodeGrpcClient extends AbstractGrpcClient<NodeNodeServiceGrpc.NodeNodeServiceBlockingStub> implements NodeNodeClient {
    public NodeNodeGrpcClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeNodeServiceGrpc.NodeNodeServiceBlockingStub> getStubFactory() {
        return NodeNodeServiceGrpc::newBlockingStub;
    }

    @Override
    public StatusResponseDTO sendShardEntries(List<SendShardDTO> shards, Action action) {
        SendShardsRequest request = SendShardsRequest.newBuilder()
            .addAllShards(shards.stream().map(SendShardDTO::toGrpc).toList())
            .setAction(action.name())
            .build();

        StatusResponse grpcResponse = blockingStub.sendShardEntries(request);

        return new StatusResponseDTO(grpcResponse);
    }
}
