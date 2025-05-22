package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.ManagedChannel;
import java.util.Map;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardRequest;

public class NodeNodeClient extends AbstractGrpcClient<NodeNodeServiceGrpc.NodeNodeServiceBlockingStub> {
    public NodeNodeClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeNodeServiceGrpc.NodeNodeServiceBlockingStub> getStubFactory() {
        return NodeNodeServiceGrpc::newBlockingStub;
    }

    public StatusResponseDTO sendShard(int shardId, Map<String, String> shard) {
        SendShardRequest request = SendShardRequest.newBuilder()
            .setShardId(shardId)
            .putAllShard(shard)
            .build();

        StatusResponse grpcResponse = blockingStub.sendShard(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO sendShardFragment(int shardId, Map<String, String> fragmentsToSend) {
        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
            .setShardId(shardId)
            .putAllShardFragments(fragmentsToSend)
            .build();

        StatusResponse grpcResponse = blockingStub.sendShardFragment(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }
}
