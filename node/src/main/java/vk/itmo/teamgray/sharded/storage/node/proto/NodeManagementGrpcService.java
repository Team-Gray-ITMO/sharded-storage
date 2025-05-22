package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.service.NodeManagementService;

public class NodeManagementGrpcService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private final NodeManagementService nodeManagementService;

    public NodeManagementGrpcService(NodeManagementService nodeManagementService) {
        this.nodeManagementService = nodeManagementService;
    }

    @Override
    public void rearrangeShards(RearrangeShardsRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.rearrangeShards(
            request.getShardToHashMap(),
            request.getFragmentsList().stream().map(FragmentDTO::fromGrpc).toList(),
            request.getServerByShardNumberMap(),
            request.getFullShardCount(),
            (success, message) -> {
                builder.setSuccess(success);
                builder.setMessage(message);
            }
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void moveShard(MoveShardRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.moveShard(
            request.getShardId(),
            request.getTargetServer(),
            (success, message) -> {
                builder.setSuccess(success);
                builder.setMessage(message);
            }
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void rollbackTopologyChange(
        Empty request,
        StreamObserver<StatusResponse> responseObserver
    ) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.rollbackTopologyChange(
            (success, message) -> {
                builder.setSuccess(success);
                builder.setMessage(message);
            }
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
