package vk.itmo.teamgray.sharded.storage.master;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.master.topology.TopologyService;

public class ShardedStorageMasterService extends ShardedStorageMasterServiceGrpc.ShardedStorageMasterServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageMasterService.class);

    private final TopologyService topologyService;

    public ShardedStorageMasterService(TopologyService topologyService) {
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
        ServerDataDTO server = new ServerDataDTO(request.getIp(), request.getPort());

        var result = topologyService.addServer(server);

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
        ServerDataDTO server = new ServerDataDTO(request.getIp(), request.getPort());

        var result = topologyService.deleteServer(server);

        responseObserver.onNext(
            DeleteServerResponse.newBuilder()
                .setSuccess(result.deleted())
                .setMessage(result.message())
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(MasterHeartbeatRequest request, StreamObserver<MasterHeartbeatResponse> responseObserver) {
        // TODO: Implement file import logic

        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(MasterHeartbeatResponse.newBuilder()
            .setHealthy(isHealthy)
            .setServerTimestamp(now.toEpochMilli())
            .setStatusMessage("OK")
            .build());

        responseObserver.onCompleted();
    }
}
