package vk.itmo.teamgray.sharded.storage.master.proto;

import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.master.client.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.master.client.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToStateRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToStateResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.IntList;
import vk.itmo.teamgray.sharded.storage.master.client.MasterClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.master.service.MasterClientService;

import static vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter.Helper.fromGrpcBuilder;

public class MasterClientGrpcService extends MasterClientServiceGrpc.MasterClientServiceImplBase {
    private final MasterClientService masterClientService;

    public MasterClientGrpcService(MasterClientService masterClientService) {
        this.masterClientService = masterClientService;
    }

    @Override
    public void getServerToShard(GetServerToShardRequest request, StreamObserver<GetServerToShardResponse> responseObserver) {
        var response = GetServerToShardResponse.newBuilder();

        masterClientService.fillServerToShards((k, v) ->
            response.putServerToShard(
                k,
                IntList.newBuilder().addAllValues(v).build()
            )
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getServerToState(GetServerToStateRequest request, StreamObserver<GetServerToStateResponse> responseObserver) {
        var response = GetServerToStateResponse.newBuilder();

        masterClientService.fillServerToState((k, v) -> response.putServerToState(k, v.name()));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getShardToHash(GetShardToHashRequest request, StreamObserver<GetShardToHashResponse> responseObserver) {
        var response = GetShardToHashResponse.newBuilder();

        masterClientService.fillShardToHash(response::putShardToHash);

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void addServer(AddServerRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();

        masterClientService.addServer(request.getId(), fromGrpcBuilder(response));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteServer(DeleteServerRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();

        masterClientService.deleteServer(request.getId(), fromGrpcBuilder(response));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void changeShardCount(ChangeShardCountRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();

        masterClientService.changeShardCount(request.getNewShardCount(), fromGrpcBuilder(response));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}
