package vk.itmo.teamgray.sharded.storage.master.proto;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;
import vk.itmo.teamgray.sharded.storage.node.management.ActionRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareMoveRequest;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareRearrangeRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessMoveRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessRearrangeRequest;

public class NodeManagementGrpcClient extends AbstractGrpcClient<NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> implements
    NodeManagementClient {
    public NodeManagementGrpcClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, NodeManagementServiceGrpc.NodeManagementServiceBlockingStub> getStubFactory() {
        return NodeManagementServiceGrpc::newBlockingStub;
    }

    @Override
    public StatusResponseDTO prepareMove(List<Integer> receiveShardIds, List<Integer> removeShardsIds, int fullShardCount) {
        PrepareMoveRequest request = PrepareMoveRequest.newBuilder()
            .addAllReceiveShardIds(receiveShardIds)
            .addAllRemoveShardIds(removeShardsIds)
            .setFullShardCount(fullShardCount)
            .build();

        StatusResponse response = blockingStub.prepareMove(request);

        return new StatusResponseDTO(response.getSuccess(), response.getMessage());
    }

    @Override
    public StatusResponseDTO processMove(List<SendShardTaskDTO> sendShards) {
        ProcessMoveRequest request = ProcessMoveRequest.newBuilder()
            .addAllSendShards(sendShards.stream().map(SendShardTaskDTO::toGrpc).collect(Collectors.toList()))
            .build();

        StatusResponse response = blockingStub.processMove(request);

        return new StatusResponseDTO(response.getSuccess(), response.getMessage());
    }

    @Override
    public StatusResponseDTO prepareRearrange(Map<Integer, Long> shardToHash, int fullShardCount) {
        PrepareRearrangeRequest request = PrepareRearrangeRequest.newBuilder()
            .putAllShardToHash(shardToHash)
            .setFullShardCount(fullShardCount)
            .build();

        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .prepareRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    @Override
    public StatusResponseDTO processRearrange(List<FragmentDTO> fragments, Map<Integer, Integer> relevantNodes) {
        ProcessRearrangeRequest request = ProcessRearrangeRequest.newBuilder()
            .addAllFragments(fragments.stream().map(FragmentDTO::toGrpc).toList())
            .putAllServerByShardNumber(relevantNodes)
            .build();

        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .processRearrange(request);

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    @Override
    public StatusResponseDTO applyOperation(Action action) {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .applyOperation(
                ActionRequest.newBuilder()
                    .setAction(action.name())
                    .build()
            );

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }

    @Override
    public StatusResponseDTO rollbackOperation(Action action) {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .rollbackOperation(
                ActionRequest.newBuilder()
                    .setAction(action.name())
                    .build()
            );

        return new StatusResponseDTO(grpcResponse.getSuccess(), grpcResponse.getMessage());
    }
}
