package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.node.management.ActionRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareMoveRequest;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareRearrangeRequest;
import vk.itmo.teamgray.sharded.storage.node.service.NodeManagementService;

import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.fromGrpcBuilder;

public class NodeManagementGrpcService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private final NodeManagementService nodeManagementService;

    public NodeManagementGrpcService(NodeManagementService nodeManagementService) {
        this.nodeManagementService = nodeManagementService;
    }

    @Override
    public void prepareRearrange(PrepareRearrangeRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.prepareRearrange(
            request.getFragmentsList().stream().map(FragmentDTO::fromGrpc).toList(),
            request.getServerByShardNumberMap(),
            request.getShardToHashMap(),
            request.getFullShardCount(),
            fromGrpcBuilder(builder)
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void processAction(ActionRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.processAction(Action.valueOf(request.getAction()), fromGrpcBuilder(builder));

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void applyAction(ActionRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.applyAction(
            Action.valueOf(request.getAction()),
            fromGrpcBuilder(builder)
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void rollbackAction(ActionRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.rollbackAction(
            Action.valueOf(request.getAction()),
            fromGrpcBuilder(builder)
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void prepareMove(PrepareMoveRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.prepareMove(
            request.getReceiveShardIdsList(),
            request.getSendShardsList().stream().map(SendShardTaskDTO::fromGrpc).toList(),
            request.getFullShardCount(),
            fromGrpcBuilder(builder)
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
