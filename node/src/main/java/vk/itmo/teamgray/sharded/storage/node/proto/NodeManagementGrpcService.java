package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.node.management.ApplyRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ApplyResponse;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareRequest;
import vk.itmo.teamgray.sharded.storage.node.management.PrepareResponse;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessRequest;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessResponse;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RollbackRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RollbackResponse;
import vk.itmo.teamgray.sharded.storage.node.service.NodeManagementService;

public class NodeManagementGrpcService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private final NodeManagementService nodeManagementService;

    public NodeManagementGrpcService(NodeManagementService nodeManagementService) {
        this.nodeManagementService = nodeManagementService;
    }

    @Override
    public void prepareRearrange(PrepareRequest request, StreamObserver<PrepareResponse> responseObserver) {
        var builder = PrepareResponse.newBuilder();

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
    public void processRearrange(ProcessRequest request, StreamObserver<ProcessResponse> responseObserver) {
        var builder = ProcessResponse.newBuilder();

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
    public void applyRearrange(ApplyRequest request, StreamObserver<ApplyResponse> responseObserver) {
        var builder = ApplyResponse.newBuilder();

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
    public void rollbackRearrange(RollbackRequest request, StreamObserver<RollbackResponse> responseObserver) {
        var builder = RollbackResponse.newBuilder();

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
