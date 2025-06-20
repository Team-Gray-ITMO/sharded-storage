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
    public StatusResponseDTO prepareMove(List<Integer> receiveShardIds, List<SendShardTaskDTO> sendShards, int fullShardCount) {
        PrepareMoveRequest request = PrepareMoveRequest.newBuilder()
            .addAllReceiveShardIds(receiveShardIds)
            .addAllSendShards(sendShards.stream().map(SendShardTaskDTO::toGrpc).collect(Collectors.toList()))
            .setFullShardCount(fullShardCount)
            .build();

        StatusResponse response = blockingStub.prepareMove(request);

        return new StatusResponseDTO(response);
    }

    @Override
    public StatusResponseDTO prepareRearrange(
        Map<Integer, Long> shardToHash,
        List<FragmentDTO> fragments,
        Map<Integer, Integer> relevantNodes,
        int fullShardCount
    ) {
        PrepareRearrangeRequest request = PrepareRearrangeRequest.newBuilder()
            .putAllShardToHash(shardToHash)
            .addAllFragments(fragments.stream().map(FragmentDTO::toGrpc).toList())
            .putAllServerByShardNumber(relevantNodes)
            .setFullShardCount(fullShardCount)
            .build();

        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .prepareRearrange(request);

        return new StatusResponseDTO(grpcResponse);
    }

    @Override
    public StatusResponseDTO processAction(Action action) {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .processAction(
                ActionRequest.newBuilder()
                    .setAction(action.name())
                    .build()
            );

        return new StatusResponseDTO(grpcResponse);
    }

    @Override
    public StatusResponseDTO applyAction(Action action) {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .applyAction(
                ActionRequest.newBuilder()
                    .setAction(action.name())
                    .build()
            );

        return new StatusResponseDTO(grpcResponse);
    }

    @Override
    public StatusResponseDTO rollbackAction(Action action) {
        StatusResponse grpcResponse = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
            .rollbackAction(
                ActionRequest.newBuilder()
                    .setAction(action.name())
                    .build()
            );

        return new StatusResponseDTO(grpcResponse);
    }
}
