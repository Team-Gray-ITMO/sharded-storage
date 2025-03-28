package vk.itmo.teamgray.sharded.storage.master.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.master.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.IntList;

public class TopologyService {

    private final ConcurrentHashMap<ServerDataDTO, List<Integer>> serverToShards = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, Long> shardToHash = new ConcurrentHashMap<>();

    private final AtomicInteger shardCount = new AtomicInteger(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public GetServerToShardResponse getServerToShardsAsGrpc() {
        lock.readLock().lock();

        try {
            GetServerToShardResponse.Builder responseBuilder = GetServerToShardResponse.newBuilder();

            for (Map.Entry<ServerDataDTO, List<Integer>> entry : serverToShards.entrySet()) {
                ServerDataDTO serverData = entry.getKey();

                String serverAddress = serverData.host() + ":" + serverData.port();
                List<Integer> shardIds = entry.getValue();

                IntList intList = IntList.newBuilder().addAllValues(shardIds).build();

                responseBuilder.putServerToShard(serverAddress, intList);
            }

            return responseBuilder.build();
        } finally {
            lock.readLock().unlock();
        }
    }

    public GetShardToHashResponse getShardToHashAsGrpc() {
        lock.readLock().lock();

        try {
            GetShardToHashResponse.Builder responseBuilder = GetShardToHashResponse.newBuilder();

            for (Map.Entry<Integer, Long> entry : shardToHash.entrySet()) {
                responseBuilder.putShardToHash(entry.getKey(), entry.getValue());
            }

            return responseBuilder.build();
        } finally {
            lock.readLock().unlock();
        }
    }

    public AddServerResult addServer(ServerDataDTO server) {
        lock.writeLock().lock();

        try {
            if (!serverToShards.containsKey(server)) {
                serverToShards.put(
                    server,
                    new ArrayList<>()
                );

                shardCount.incrementAndGet();

                redistributeHashesEvenly(shardCount.get());

                return new AddServerResult(true, "SERVER CREATED");
            }
        } finally {
            lock.writeLock().unlock();
        }

        return new AddServerResult(false, "SERVER ALREADY EXISTS");
    }

    public DeleteServerResult deleteServer(ServerDataDTO server) {
        List<Integer> shards = serverToShards.get(server);

        if (shards != null) {
            shardCount.getAndAdd(-shards.size());
            serverToShards.remove(server);

            redistributeHashesEvenly(shardCount.get());

            return new DeleteServerResult(true, "SERVER REMOVED");
        }
        return new DeleteServerResult(false, "SERVER NOT REMOVED");
    }

    public void redistributeHashesEvenly(int shardCount) {
        shardToHash.clear();

        if (shardCount == 0) {
            return;
        }

        long stepSize = Long.MAX_VALUE / shardCount * 2;

        for (int i = 0; i < shardCount; i++) {
            long hashBoundary = Long.MIN_VALUE + i * stepSize;

            if (i == shardCount - 1) {
                hashBoundary = Long.MAX_VALUE;
            }

            shardToHash.put(i, hashBoundary);
        }
    }
}

