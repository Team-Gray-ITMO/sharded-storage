package vk.itmo.teamgray.sharded.storage.master.service;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.master.client.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.master.client.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.MasterClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.master.service.topology.TopologyService;

// TODO Decouple to gRPC Service and Service with business logic. Example: 'HealthGrpcService' and 'HealthService'
public class MasterClientService extends MasterClientServiceGrpc.MasterClientServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(MasterClientService.class);

    private final TopologyService topologyService;

    public MasterClientService(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    @Override
    public void getServerToShard(GetServerToShardRequest request, StreamObserver<GetServerToShardResponse> responseObserver) {
        log.info("Received ServerToShard request");

        var response = topologyService.getServerToShardsAsGrpc();

        log.info("Returning {} servers with shards", response.getServerToShardCount());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getShardToHash(GetShardToHashRequest request, StreamObserver<GetShardToHashResponse> responseObserver) {
        log.info("Received ShardToHash request");

        var response = topologyService.getShardToHashAsGrpc();

        log.info("Returning {} shards with hashes", response.getShardToHashCount());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void addServer(AddServerRequest request, StreamObserver<StatusResponse> responseObserver) {
        var result = topologyService.addServer(request.getId());

        responseObserver.onNext(
            StatusResponse.newBuilder()
                .setSuccess(result.isSuccess())
                .setMessage(result.getMessage())
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void deleteServer(DeleteServerRequest request, StreamObserver<StatusResponse> responseObserver) {
        var result = topologyService.deleteServer(request.getId());

        responseObserver.onNext(
            StatusResponse.newBuilder()
                .setSuccess(result.isSuccess())
                .setMessage(result.getMessage())
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void changeShardCount(ChangeShardCountRequest request, StreamObserver<StatusResponse> responseObserver) {
        var result = topologyService.changeShardCount(request.getNewShardCount());

        responseObserver.onNext(
            StatusResponse.newBuilder()
                .setSuccess(result.isSuccess())
                .setMessage(result.getMessage())
                .build()
        );

        responseObserver.onCompleted();
    }
}
