package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.ManagedChannel;
import java.util.Map;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardRequest;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class NodeNodeClient extends AbstractGrpcClient<NodeNodeServiceGrpc.NodeNodeServiceBlockingStub> {
    public NodeNodeClient(String host) {
        super(host, getServerPort("node.node"));
    }

    @Override
    protected Function<ManagedChannel, NodeNodeServiceGrpc.NodeNodeServiceBlockingStub> getStubFactory() {
        return NodeNodeServiceGrpc::newBlockingStub;
    }

    public boolean sendShard(int shardId, Map<String, String> shard) {
        SendShardRequest request = SendShardRequest.newBuilder()
            .setShardId(shardId)
            .putAllShard(shard)
            .build();

        return blockingStub.sendShard(request).getSuccess();
    }

    public boolean sendShardFragment(int shardId, Map<String, String> fragmentsToSend) {
        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
            .setShardId(shardId)
            .putAllShardFragments(fragmentsToSend)
            .build();

        return blockingStub.sendShardFragment(request).getSuccess();
    }
}
