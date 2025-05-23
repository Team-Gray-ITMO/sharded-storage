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
import vk.itmo.teamgray.sharded.storage.common.dto.MoveShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessRequest;

public class NodeManagementClient extends AbstractGrpcClient<NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> {
    public NodeManagementClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> getStubFactory() {
        return NodeManagementServiceGrpc::newBlockingStub;
    }

    public StatusResponseDTO moveShards(List<MoveShardDTO> shards) {
        MoveShardsRequest request = MoveShardsRequest.newBuilder()
            .addAllShards(shards.stream().map(MoveShardDTO::toGrpc).collect(Collectors.toList()))
            .build();

        StatusResponse response = blockingStub.moveShards(request);

        return new StatusResponseDTO(response.getSuccess(), response.getMessage());
    }

    public StatusResponseDTO prepareRearrange(Map<Integer, Long> shardToHash, int fullShardCount) {
        PrepareRequest request = PrepareRequest.newBuilder()
            .putAllShardToHash(shardToHash)
            .setFullShardCount(fullShardCount)
            .build();

        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .prepareRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO processRearrange(List<FragmentDTO> fragments, Map<Integer, Integer> relevantNodes) {
        ProcessRequest request = ProcessRequest.newBuilder()
            .addAllFragments(fragments.stream().map(FragmentDTO::toGrpc).toList())
            .putAllServerByShardNumber(relevantNodes)
            .build();

        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .processRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO applyRearrange() {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .applyRearrange(Empty.newBuilder().build());

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    public StatusResponseDTO rollbackRearrange() {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .rollbackRearrange(Empty.newBuilder().build());

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }
}
