package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessRequest;
import vk.itmo.teamgray.sharded.storage.node.service.NodeManagementService;

public class NodeManagementGrpcService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private final NodeManagementService nodeManagementService;

    public NodeManagementGrpcService(NodeManagementService nodeManagementService) {
        this.nodeManagementService = nodeManagementService;
    }

    @Override
    public void prepareRearrange(PrepareRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.prepareRearrange(
            request.getShardToHashMap(),
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
    public void processRearrange(ProcessRequest request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.processRearrange(
            request.getFragmentsList().stream().map(FragmentDTO::fromGrpc).toList(),
            request.getServerByShardNumberMap(),
            (success, message) -> {
                builder.setSuccess(success);
                builder.setMessage(message);
            }
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void applyRearrange(Empty request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.applyRearrange(
            (success, message) -> {
                builder.setSuccess(success);
                builder.setMessage(message);
            }
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void rollbackRearrange(Empty request, StreamObserver<StatusResponse> responseObserver) {
        var builder = StatusResponse.newBuilder();

        nodeManagementService.rollbackRearrange(
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
}
