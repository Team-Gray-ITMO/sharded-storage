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
import vk.itmo.teamgray.sharded.storage.node.management.RollbackTopologyChangeRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RollbackTopologyChangeResponse;
import vk.itmo.teamgray.sharded.storage.node.management.ServerData;

public class NodeManagementClient extends AbstractGrpcClient<NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> {
    public NodeManagementClient(String host, int port) {
        super(host, port);
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
        //TODO Instead of creating this many objects, let's populate gRPC stubs right away, but let's population method modular, so we can easily switch from gRPC.
        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder()
            .putAllShardToHash(relevantShardsToHash)
            .addAllFragments(relevantFragments.stream().map(FragmentDTO::toGrpc).toList())
            .putAllServerByShardNumber(
                relevantNodes.stream()
                    .collect(
                        Collectors.toMap(
                            ShardNodeMapping::shardId,
                            it -> it.node().toGrpc()
                        )
                    )
            )
            .build();

        return blockingStub.rearrangeShards(request).getSuccess();
    }

    public boolean moveShard(int shardId, ServerDataDTO targetServer) {
        ServerData serverData = ServerData.newBuilder()
            .setHost(targetServer.host())
            .setPort(targetServer.port())
            .build();

        MoveShardRequest request = MoveShardRequest.newBuilder()
            .setShardId(shardId)
            .setTargetServer(serverData)
            .build();

        MoveShardResponse response = blockingStub.moveShard(request);
        return response.getSuccess();
    }

    public boolean rollbackTopologyChange() {
        RollbackTopologyChangeRequest request = RollbackTopologyChangeRequest.newBuilder().build();
        try {
            RollbackTopologyChangeResponse response = blockingStub.rollbackTopologyChange(request);
            return response.getSuccess();
        } catch (Exception e) {
            return false;
        }
    }
}
