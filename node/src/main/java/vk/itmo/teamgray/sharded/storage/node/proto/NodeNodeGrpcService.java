package vk.itmo.teamgray.sharded.storage.node.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.service.NodeNodeService;

import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.fromGrpcBuilder;

public class NodeNodeGrpcService extends NodeNodeServiceGrpc.NodeNodeServiceImplBase {
    private final NodeNodeService nodeNodeService;

    public NodeNodeGrpcService(NodeNodeService nodeNodeService) {
        this.nodeNodeService = nodeNodeService;
    }

    @Override
    public void sendShardEntries(
        SendShardsRequest request,
        StreamObserver<StatusResponse> responseObserver
    ) {
        var builder = StatusResponse.newBuilder();

        nodeNodeService.sendShardEntries(
            Action.valueOf(request.getAction()),
            request.getShardsList().stream().map(SendShardDTO::fromGrpc).toList(),
            fromGrpcBuilder(builder)
        );

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
