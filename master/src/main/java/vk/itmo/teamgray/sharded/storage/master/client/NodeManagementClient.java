package vk.itmo.teamgray.sharded.storage.master.client;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.master.service.topology.ShardNodeMapping;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareRequest;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareResponse;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessResponse;
import vk.itmo.teamgray.sharded.storage.node.management.ApplyRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ApplyResponse;
import vk.itmo.teamgray.sharded.storage.node.management.RollbackRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RollbackResponse;

public class NodeManagementClient extends AbstractGrpcClient<NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> {
    public NodeManagementClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> getStubFactory() {
        return NodeManagementServiceGrpc::newBlockingStub;
    }

    public StatusResponseDTO rearrangeShards(
        Map<Integer, Long> relevantShardsToHash,
        List<FragmentDTO> relevantFragments,
        List<ShardNodeMapping> relevantNodes,
        int fullShardCount
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
            .setFullShardCount(fullShardCount)
            .build();

        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .rearrangeShards(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO moveShard(int shardId, int serverId) {
        MoveShardRequest request = MoveShardRequest.newBuilder()
            .setShardId(shardId)
            .setTargetServer(serverId)
            .build();

        StatusResponse response = blockingStub.moveShard(request);

        return new StatusResponseDTO(response.getSuccess(), response.getMessage());
    }

    public boolean rollbackTopologyChange() {
        Empty request = Empty.newBuilder().build();
        try {
            StatusResponse response = blockingStub.rollbackTopologyChange(request);
            return response.getSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    public StatusResponseDTO prepareRearrange(Map<Integer, Long> shardToHash, int fullShardCount) {
        PrepareRequest request = PrepareRequest.newBuilder()
            .putAllShardToHash(shardToHash)
            .setFullShardCount(fullShardCount)
            .build();

        PrepareResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .prepareRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO processRearrange(List<FragmentDTO> fragments, Map<Integer, Integer> serverByShardNumber) {
        ProcessRequest request = ProcessRequest.newBuilder()
            .addAllFragments(fragments.stream().map(FragmentDTO::toGrpc).toList())
            .putAllServerByShardNumber(serverByShardNumber)
            .build();

        ProcessResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .processRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO applyRearrange() {
        ApplyRequest request = ApplyRequest.newBuilder().build();

        ApplyResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .applyRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO rollbackRearrange() {
        RollbackRequest request = RollbackRequest.newBuilder().build();

        RollbackResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .rollbackRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }
}
