package vk.itmo.teamgray.sharded.storage.master.client.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.PropertyUtils;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.common.ShardUtils;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.IntList;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

public class TopologyService {
    private static final Logger log = LoggerFactory.getLogger(TopologyService.class);

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
        log.info("Adding server {}", serverToAdd);

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

        log.info("Added server {}", serverToAdd);

        return new AddServerResult(true, "SERVER ADDED");
    }

    public DeleteServerResult deleteServer(ServerDataDTO serverToRemove) {
        log.info("Removing server {}", serverToRemove);

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

        log.info("Removed server {}", serverToRemove);

        replaceServerToShards(newServerToShards);

        return new DeleteServerResult(true, "SERVER REMOVED");
    }

    public boolean changeShardCount(int shardCount) {
        log.info("Changing shard count to {}", shardCount);

        ConcurrentHashMap<Integer, Long> newShardToHash = redistributeHashesEvenly(shardCount);
        ConcurrentHashMap<ServerDataDTO, List<Integer>> newServerToShards = redistributeShardsEvenly(
            Collections.list(serverToShards.keys()),
            Collections.list(newShardToHash.keys())
        );

        List<Bound> allBounds = Stream.concat(
                shardToHash.entrySet().stream().map(it -> new Bound(false, it.getKey(), it.getValue())),
                newShardToHash.entrySet().stream().map(it -> new Bound(true, it.getKey(), it.getValue()))
            )
            .sorted(Comparator.comparingLong(Bound::upperBound))
            .toList();

        List<FragmentDTO> fragments = new ArrayList<>();

        if (!shardToHash.isEmpty() && !newShardToHash.isEmpty()) {
            long prevBound = Long.MIN_VALUE;
            int currentOldShard = -1;
            int currentNewShard = -1;

            int oldShardCount = shardToHash.size();

            for (Bound bound : allBounds) {
                long newBound = bound.upperBound();
                int oldShardForRange = bound.isNew() ?
                    Objects.requireNonNull(ShardUtils.getShardIdForHash(newBound, oldShardCount)) :
                    bound.shardId();

                int newShardForRange = bound.isNew() ?
                    bound.shardId() :
                    Objects.requireNonNull(ShardUtils.getShardIdForHash(newBound, shardCount));

                if (currentOldShard != oldShardForRange || currentNewShard != newShardForRange) {
                    if (currentOldShard != -1 && oldShardForRange != newShardForRange && prevBound != newBound) {
                        fragments.add(new FragmentDTO(
                            oldShardForRange,
                            newShardForRange,
                            prevBound,
                            newBound
                        ));
                    }
                    currentOldShard = oldShardForRange;
                    currentNewShard = newShardForRange;
                    prevBound = newBound;
                }
            }
        }

        var fragmentsByOldShards = fragments.stream()
            .collect(
                Collectors.groupingBy(
                    FragmentDTO::oldShardId,
                    Collectors.mapping(Function.identity(), Collectors.toList())
                )
            );

        var newShardsToServer = newServerToShards.entrySet().stream()
            .flatMap(kv -> kv.getValue().stream().map(shard -> Map.entry(kv.getKey(), shard)))
            .collect(Collectors.toMap(
                Map.Entry::getValue,
                Map.Entry::getKey
            ));

        newServerToShards.forEach((server, shards) -> {
                var relevantSchemeSlice = shards.stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(),
                            newShardToHash::get
                        )
                    );

                var relevantFragments = serverToShards.get(server).stream()
                    .flatMap(it -> fragmentsByOldShards.get(it).stream())
                    .toList();

                var relevantNodes = relevantFragments.stream()
                    .map(it -> new ShardNodeMapping(it.newShardId(), newShardsToServer.get(it.newShardId())))
                    .toList();

                log.info("Sending rearrange request [node={}, fragments={}, nodeMapping={}]", server, relevantFragments, relevantNodes);

                nodeManagementClient.rearrangeShards(relevantSchemeSlice, relevantFragments, relevantNodes);
            }
        );

        //Replace after all rearrange shards has returned as success.
        replaceBothMaps(newShardToHash, newServerToShards);

        log.info("Changed shard count");

        return true;
    }

    private void replaceServerToShards(ConcurrentHashMap<ServerDataDTO, List<Integer>> newServerToShards) {
        lock.writeLock().lock();

        log.info("Replacing server to shard scheme {}", newServerToShards);

        try {
            var oldServerToShards = serverToShards;

            serverToShards = newServerToShards;

            oldServerToShards.clear();

            log.info("Replaced server to shard scheme");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void replaceBothMaps(
        ConcurrentHashMap<Integer, Long> newShardToHash,
        ConcurrentHashMap<ServerDataDTO, List<Integer>> newServerToShards
    ) {
        lock.writeLock().lock();

        log.info("Replacing server to shard and shard to hash schemes [sts={}, sth={}]", newServerToShards, newShardToHash);

        try {
            var oldShardToHash = shardToHash;
            var oldServerToShards = serverToShards;

            shardToHash = newShardToHash;
            serverToShards = newServerToShards;

            oldShardToHash.clear();
            oldServerToShards.clear();

            log.info("Replaced server to shard and shard to hash schemes");
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

        long previousBoundary = Long.MIN_VALUE;

        for (int i = 1; i <= shardCount; i++) {
            long hashBoundary = previousBoundary + stepSize;
            if (i == shardCount) {
                hashBoundary = Long.MAX_VALUE;
            }

            newShardToHash.put(i - 1, hashBoundary);

            previousBoundary = hashBoundary;
        }

        return newShardToHash;
    }
}
