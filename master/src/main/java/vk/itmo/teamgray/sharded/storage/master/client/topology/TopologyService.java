package vk.itmo.teamgray.sharded.storage.master.client.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import vk.itmo.teamgray.sharded.storage.common.PropertyUtils;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.IntList;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

public class TopologyService {
    private final NodeManagementClient nodeManagementClient;

    private ConcurrentHashMap<ServerDataDTO, List<Integer>> serverToShards = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, Long> shardToHash = new ConcurrentHashMap<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private final boolean hardCodedNode;

    public TopologyService(boolean hardCodedNode, NodeManagementClient nodeManagementClient) {
        this.hardCodedNode = hardCodedNode;
        this.nodeManagementClient = nodeManagementClient;

        if (hardCodedNode) {
            //TODO For now single node
            serverToShards.put(new ServerDataDTO(PropertyUtils.getServerHost("node"), PropertyUtils.getServerPort("node")), List.of());
        }
    }

    public ConcurrentHashMap<ServerDataDTO, List<Integer>> getServerToShards() {
        return serverToShards;
    }

    public ConcurrentHashMap<Integer, Long> getShardToHash() {
        return shardToHash;
    }

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

    public AddServerResult addServer(ServerDataDTO serverToAdd) {
        // TODO Remove
        if (hardCodedNode) {
            return new AddServerResult(false, "Adding Nodes is Not Supported");
        }

        if (serverToShards.containsKey(serverToAdd)) {
            return new AddServerResult(false, "SERVER NOT ADDED");
        }

        var updatedServers = Collections.list(serverToShards.keys());

        updatedServers.add(serverToAdd);

        var newServerToShards = redistributeShardsEvenly(
            updatedServers,
            Collections.list(shardToHash.keys())
        );

        replaceServerToShards(newServerToShards);

        return new AddServerResult(true, "SERVER ADDED");
    }

    public DeleteServerResult deleteServer(ServerDataDTO serverToRemove) {
        // TODO Remove
        if (hardCodedNode) {
            return new DeleteServerResult(false, "Removing Nodes is Not Supported");
        }

        if (!serverToShards.containsKey(serverToRemove)) {
            return new DeleteServerResult(false, "SERVER NOT REMOVED");
        }

        var updatedServers = Collections.list(serverToShards.keys()).stream()
            .filter(it -> it != serverToRemove)
            .toList();

        var newServerToShards = redistributeShardsEvenly(
            updatedServers,
            Collections.list(shardToHash.keys())
        );

        replaceServerToShards(newServerToShards);

        return new DeleteServerResult(true, "SERVER REMOVED");
    }

    public boolean changeShardCount(int shardCount) {
        var newShardToHash = redistributeHashesEvenly(shardCount);
        var newServerToShards = redistributeShardsEvenly(
            Collections.list(serverToShards.keys()),
            Collections.list(newShardToHash.keys())
        );

        replaceBothMaps(newShardToHash, newServerToShards);

        serverToShards.forEach((server, shards) ->
            nodeManagementClient.rearrangeShards(shards.stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        shard -> shardToHash.get(shard)
                    )
                )
            )
        );

        return true;
    }

    private void replaceServerToShards(ConcurrentHashMap<ServerDataDTO, List<Integer>> newServerToShards) {
        lock.writeLock().lock();

        try {
            var oldServerToShards = serverToShards;

            serverToShards = newServerToShards;

            oldServerToShards.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void replaceBothMaps(ConcurrentHashMap<Integer, Long> newShardToHash,
        ConcurrentHashMap<ServerDataDTO, List<Integer>> newServerToShards) {
        lock.writeLock().lock();

        try {
            var oldShardToHash = shardToHash;
            var oldServerToShards = serverToShards;

            shardToHash = newShardToHash;
            serverToShards = newServerToShards;

            oldShardToHash.clear();
            oldServerToShards.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private ConcurrentHashMap<ServerDataDTO, List<Integer>> redistributeShardsEvenly(
        List<ServerDataDTO> serverList,
        List<Integer> shardList
    ) {
        ConcurrentHashMap<ServerDataDTO, List<Integer>> newServerToShards = new ConcurrentHashMap<>();

        if (serverList.isEmpty()) {
            return newServerToShards;
        }

        int serverCount = serverList.size();

        serverList.forEach(server -> newServerToShards.put(server, new ArrayList<>()));

        // Distribute shards in round-robin
        int serverIndex = 0;

        for (Integer shardId : shardList) {
            ServerDataDTO targetServer = serverList.get(serverIndex);
            newServerToShards.get(targetServer).add(shardId);
            serverIndex = (serverIndex + 1) % serverCount;
        }

        return newServerToShards;
    }

    private ConcurrentHashMap<Integer, Long> redistributeHashesEvenly(int shardCount) {
        ConcurrentHashMap<Integer, Long> newShardToHash = new ConcurrentHashMap<>();

        if (shardCount == 0) {
            return newShardToHash;
        }

        long stepSize = Long.MAX_VALUE / shardCount * 2;

        for (int i = 0; i < shardCount; i++) {
            long hashBoundary = Long.MIN_VALUE + i * stepSize;

            if (i == shardCount - 1) {
                hashBoundary = Long.MAX_VALUE;
            }

            newShardToHash.put(i, hashBoundary);
        }

        return newShardToHash;
    }
}
