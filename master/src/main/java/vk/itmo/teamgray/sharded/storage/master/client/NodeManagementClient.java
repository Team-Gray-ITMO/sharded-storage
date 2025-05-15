package vk.itmo.teamgray.sharded.storage.master.client;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.master.client.topology.ShardNodeMapping;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardResponse;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public class NodeManagementClient extends AbstractGrpcClient<NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> {
    public NodeManagementClient(String host) {
        super(host, getServerPort("node.management"));
    }

    @Override
    protected Function<ManagedChannel, NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> getStubFactory() {
        return NodeManagementServiceGrpc::newBlockingStub;
    }

    public boolean rearrangeShards(
        Map<Integer, Long> relevantShardsToHash,
        List<FragmentDTO> relevantFragments,
        List<ShardNodeMapping> relevantNodes
    ) {
        //TODO Instead of creating this many objects, let's populate gRPC stubs right away, but let's make population method modular, so we can easily switch from gRPC.
        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder()
            .putAllShardToHash(relevantShardsToHash)
            .addAllFragments(relevantFragments.stream().map(FragmentDTO::toGrpc).toList())
            .putAllServerByShardNumber(
                relevantNodes.stream()
                    .collect(
                        Collectors.toMap(
                            ShardNodeMapping::shardId,
                            ShardNodeMapping::serverId
                        )
                    )
            )
            .build();

        return blockingStub.rearrangeShards(request).getSuccess();
    }

    public boolean moveShard(int shardId, int serverId) {
        MoveShardRequest request = MoveShardRequest.newBuilder()
            .setShardId(shardId)
            .setTargetServer(serverId)
            .build();

        MoveShardResponse response = blockingStub.moveShard(request);
        return response.getSuccess();
    }
}
