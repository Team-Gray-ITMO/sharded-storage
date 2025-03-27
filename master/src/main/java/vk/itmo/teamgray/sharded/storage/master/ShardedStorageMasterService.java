package vk.itmo.teamgray.sharded.storage.master;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.MessageAndSuccessDTO;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.node.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.node.ChangeShardCountResponse;

public class ShardedStorageMasterService extends ShardedStorageMasterServiceGrpc.ShardedStorageMasterServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageMasterService.class);

    private final ConcurrentHashMap<ServerDataDTO, List<Integer>> serverToShards = new ConcurrentHashMap<>();
    AtomicInteger shardCount = new AtomicInteger(0);
    ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void getTopology(GetTopologyRequest request, StreamObserver<GetTopologyResponse> responseObserver) {
        log.info("Received GetTopology request");

        lock.readLock().lock();
        GetTopologyResponse.Builder responseBuilder;
        try {
            responseBuilder = GetTopologyResponse.newBuilder()
                    .setTotalShardCount(shardCount.get());

            for (var entry : serverToShards.entrySet()) {
                ServerDataDTO serverData = entry.getKey();
                String serverAddress = serverData.host() + ":" + serverData.port();

                entry.getValue().forEach(shardId -> responseBuilder.putShardToServer(shardId, serverAddress));
            }
        }
        finally {
            lock.readLock().unlock();
        }
        
        GetTopologyResponse response = responseBuilder.build();
        
        log.info("Returning topology with {} shards", response.getShardToServerCount());
        
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void addServer(AddServerRequest request, StreamObserver<AddServerResponse> responseObserver) {
        ServerDataDTO server = new ServerDataDTO(request.getIp(), request.getPort());
        String responseMessage = "SERVER ALREADY EXISTS";
        boolean created = false;
        lock.writeLock().lock();
        try {
            if (!serverToShards.containsKey(server)) {
                serverToShards.put(
                        server,
                        new ArrayList<>()
                );
                created = true;
                responseMessage = "SERVER CREATED";
            }
        } finally {
            lock.writeLock().unlock();
        }

        responseObserver.onNext(
            AddServerResponse.newBuilder()
                .setSuccess(created)
                .setMessage(responseMessage)
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void deleteServer(DeleteServerRequest request, StreamObserver<DeleteServerResponse> responseObserver) {
        ServerDataDTO server = new ServerDataDTO(request.getIp(), request.getPort());
        List<Integer> shards = serverToShards.get(server);
        String responseMessage = "SERVER NOT REMOVED";
        boolean deleted = false;
        if (shards != null) {
            shardCount.getAndAdd(-shards.size());
            serverToShards.remove(server);
            deleted = true;
            responseMessage = "SERVER DELETED";
        }

        responseObserver.onNext(
            DeleteServerResponse.newBuilder()
                .setSuccess(deleted)
                .setMessage(responseMessage)
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
