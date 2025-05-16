package vk.itmo.teamgray.sharded.storage.master.client;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.master.client.topology.TopologyService;

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
    public void addServer(AddServerRequest request, StreamObserver<AddServerResponse> responseObserver) {
        var result = topologyService.addServer(request.getId());

        responseObserver.onNext(
            AddServerResponse.newBuilder()
                .setSuccess(result.created())
                .setMessage(result.message())
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void deleteServer(DeleteServerRequest request, StreamObserver<DeleteServerResponse> responseObserver) {
        var result = topologyService.deleteServer(request.getId());

        responseObserver.onNext(
            DeleteServerResponse.newBuilder()
                .setSuccess(result.deleted())
                .setMessage(result.message())
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void changeShardCount(ChangeShardCountRequest request, StreamObserver<ChangeShardCountResponse> responseObserver) {
        var success = topologyService.changeShardCount(request.getNewShardCount());

        responseObserver.onNext(
            ChangeShardCountResponse.newBuilder()
                .setSuccess(success)
                .setMessage("SUCCESS")
                .build()
        );

        responseObserver.onCompleted();
    }
}
