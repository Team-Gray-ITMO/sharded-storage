package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.client.NodeNodeClient;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
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
    public StatusResponseDTO sendShard(List<SendShardDTO> shards) {
        SendShardsRequest request = SendShardsRequest.newBuilder()
            .addAllShards(shards.stream().map(SendShardDTO::toGrpc).toList())
            .build();

        StatusResponse grpcResponse = blockingStub.sendShards(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    @Override
    public StatusResponseDTO sendShardFragment(int shardId, Map<String, String> fragmentsToSend) {
        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
            .setShardId(shardId)
            .putAllShardFragments(fragmentsToSend)
            .build();

        StatusResponse grpcResponse = blockingStub.sendShardFragment(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }
}
