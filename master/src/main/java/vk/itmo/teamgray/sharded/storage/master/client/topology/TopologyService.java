package vk.itmo.teamgray.sharded.storage.master.client.topology;

import java.math.BigInteger;
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
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.IntList;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

import static java.util.stream.Collectors.toMap;

public class TopologyService {
    private static final Logger log = LoggerFactory.getLogger(TopologyService.class);

    private final DiscoveryClient discoveryClient;

    private ConcurrentHashMap<Integer, List<Integer>> serverToShards = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, Long> shardToHash = new ConcurrentHashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public TopologyService(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;

        //TODO add a real await for all nodes to be available.
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (shardToHash.isEmpty()) {
            changeShardCount(1);
        }
    }

    public ConcurrentHashMap<Integer, List<Integer>> getServerToShards() {
        return serverToShards;
    }

    public ConcurrentHashMap<Integer, Long> getShardToHash() {
        return shardToHash;
    }

    public GetServerToShardResponse getServerToShardsAsGrpc() {
        lock.readLock().lock();

        try {
            GetServerToShardResponse.Builder responseBuilder = GetServerToShardResponse.newBuilder();

            for (Map.Entry<Integer, List<Integer>> entry : serverToShards.entrySet()) {
                int serverId = entry.getKey();

                List<Integer> shardIds = entry.getValue();

                IntList intList = IntList.newBuilder().addAllValues(shardIds).build();

                responseBuilder.putServerToShard(serverId, intList);
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

    public AddServerResult addServer(int serverId) {
        lock.writeLock().lock();
        try {
            log.info("Adding server {}", serverId);

            if (serverToShards.containsKey(serverId)) {
                return new AddServerResult(false, "SERVER NOT ADDED");
            }

            var updatedServers = Collections.list(serverToShards.keys());

            updatedServers.add(serverId);

            var oldServerToShards = new ConcurrentHashMap<>(serverToShards);
            var newServerToShards = redistributeShardsEvenly(
                updatedServers,
                Collections.list(shardToHash.keys())
            );

            handleShardMovement(oldServerToShards, newServerToShards);

            replaceServerToShards(newServerToShards);

            log.info("Added server {}", serverId);

            return new AddServerResult(true, "SERVER ADDED");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public DeleteServerResult deleteServer(int serverId) {
        lock.writeLock().lock();
        try {
            log.info("Removing server {}", serverId);

            if (!serverToShards.containsKey(serverId)) {
                return new DeleteServerResult(false, "SERVER NOT REMOVED");
            }

            var updatedServers = Collections.list(serverToShards.keys()).stream()
                .filter(it -> it != serverId)
                .toList();

            var oldServerToShards = new ConcurrentHashMap<>(serverToShards);
            var newServerToShards = redistributeShardsEvenly(
                updatedServers,
                Collections.list(shardToHash.keys())
            );

            // move shards to new server
            handleShardMovement(oldServerToShards, newServerToShards);

            log.info("Removed server {}", serverId);

            replaceServerToShards(newServerToShards);

            return new DeleteServerResult(true, "SERVER REMOVED");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void handleShardMovement(
        ConcurrentHashMap<Integer, List<Integer>> oldMapping,
        ConcurrentHashMap<Integer, List<Integer>> newMapping
    ) {
        Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(oldMapping.keySet());

        // find shards that need to be moved
        for (Map.Entry<Integer, List<Integer>> oldEntry : oldMapping.entrySet()) {
            int sourceServerId = oldEntry.getKey();
            List<Integer> oldShards = oldEntry.getValue();

            var sourceServer = nodes.get(sourceServerId);

            for (Integer shardId : oldShards) {
                Integer targetServer = findServerForShard(newMapping, shardId);

                if (targetServer != null && !targetServer.equals(sourceServerId)) {
                    log.info("Moving shard {} from {} to {}", shardId, sourceServer, targetServer);

                    NodeManagementClient nodeManagementClient = GrpcClientCachingFactory
                        .getInstance()
                        .getClient(
                            sourceServer,
                            NodeManagementClient::new
                        );

                    boolean success = nodeManagementClient.moveShard(shardId, targetServer);
                    if (!success) {
                        log.error("Failed to move shard {} from {} to {}", shardId, sourceServer, targetServer);
                    }
                }
            }
        }
    }

    private Integer findServerForShard(
        ConcurrentHashMap<Integer, List<Integer>> mapping,
        Integer shardId
    ) {
        for (Map.Entry<Integer, List<Integer>> entry : mapping.entrySet()) {
            if (entry.getValue().contains(shardId)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public boolean changeShardCount(int shardCount) {
        lock.writeLock().lock();
        try {
            log.info("Changing shard count to {}", shardCount);

            ConcurrentHashMap<Integer, Long> originalShardToHash = new ConcurrentHashMap<>(this.shardToHash);
            ConcurrentHashMap<Integer, List<Integer>> originalServerToShards = new ConcurrentHashMap<>(this.serverToShards);

            ConcurrentHashMap<Integer, Long> newShardToHash = redistributeHashesEvenly(shardCount);
            ConcurrentHashMap<Integer, List<Integer>> newServerToShards = redistributeShardsEvenly(
                Collections.list(serverToShards.keys()),
                Collections.list(newShardToHash.keys())
            );

            List<Bound> allBounds = Stream.concat(
                    shardToHash.entrySet().stream().map(it -> new Bound(false, it.getKey(), it.getValue())),
                    newShardToHash.entrySet().stream().map(it -> new Bound(true, it.getKey(), it.getValue()))
                )
                .sorted(Comparator.comparingLong(Bound::upperBound))
                .toList();

            List<FragmentDTO> fragments = findFragmentsToMove(shardCount, newShardToHash, allBounds);

            var fragmentsByOldShards = fragments.stream()
                .collect(
                    Collectors.groupingBy(
                        FragmentDTO::oldShardId,
                        Collectors.mapping(Function.identity(), Collectors.toList())
                    )
                );

            var newShardsToServer = newServerToShards.entrySet().stream()
                .flatMap(kv -> kv.getValue().stream().map(shard -> Map.entry(kv.getKey(), shard)))
                .collect(toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey
                ));

            var nodes = discoveryClient.getNodeMapWithRetries(newServerToShards.keySet());

            boolean oneNodeFailed = false;
            List<Integer> attemptedNodes = new ArrayList<>();

            for (var entry : newServerToShards.entrySet()) {
                var serverId = entry.getKey();
                var shards = entry.getValue();

                var relevantSchemeSlice = shards.stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(),
                            newShardToHash::get
                        )
                    );

                var relevantFragments = serverToShards.get(serverId).stream()
                    .flatMap(it -> fragmentsByOldShards.get(it).stream())
                    .toList();

                var relevantNodes = relevantFragments.stream()
                    .map(FragmentDTO::newShardId)
                    .distinct()
                    .map(it -> new ShardNodeMapping(it, newShardsToServer.get(it)))
                    .toList();

                log.info(
                    "Sending rearrange request [node={}, relevantScheme={}, fragments={}, nodeMapping={}]",
                    serverId,
                    relevantSchemeSlice,
                    relevantFragments,
                    relevantNodes
                );

                var server = nodes.get(serverId);

                //TODO Invert mapping and revert on the node side.
                NodeManagementClient nodeManagementClient = GrpcClientCachingFactory
                    .getInstance()
                    .getClient(
                        server,
                        NodeManagementClient::new
                    );

                attemptedNodes.add(serverId);
                boolean success = nodeManagementClient.rearrangeShards(relevantSchemeSlice, relevantFragments, relevantNodes);

                if (!success) {
                    log.error("RearrangeShards failed on node: {}. Initiating rollback.", server);
                    oneNodeFailed = true;
                    break;
                }
            }

            if (oneNodeFailed) {
                log.warn("One or more nodes failed during rearrange. Rolling back.");
                for (Integer nodeIdToRollback : attemptedNodes) {
                    var nodeToRollback = nodes.get(nodeIdToRollback);

                    NodeManagementClient client = GrpcClientCachingFactory.getInstance()
                        .getClient(
                            nodeToRollback,
                            NodeManagementClient::new
                        );

                    boolean rollbackSuccess = client.rollbackTopologyChange();
                    if (!rollbackSuccess) {
                        log.error("Rollback failed for node {}. System may be inconsistent.", nodeToRollback);
                    }
                }
                this.shardToHash = originalShardToHash;
                this.serverToShards = originalServerToShards;

                log.info("changeShardCount failed");
                return false;
            } else {
                replaceBothMaps(newShardToHash, newServerToShards);

                log.info("Changed shard count");

                return true;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private List<FragmentDTO> findFragmentsToMove(
        int shardCount,
        ConcurrentHashMap<Integer, Long> newShardToHash,
        List<Bound> allBounds
    ) {
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
                    fragments.add(new FragmentDTO(
                        oldShardForRange,
                        newShardForRange,
                        prevBound,
                        newBound
                    ));

                    currentOldShard = oldShardForRange;
                    currentNewShard = newShardForRange;
                    prevBound = newBound;
                }
            }
        }

        return fragments;
    }

    private void replaceServerToShards(ConcurrentHashMap<Integer, List<Integer>> newServerToShards) {
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
        ConcurrentHashMap<Integer, List<Integer>> newServerToShards
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

    private ConcurrentHashMap<Integer, List<Integer>> redistributeShardsEvenly(
        List<Integer> serverList,
        List<Integer> shardList
    ) {
        ConcurrentHashMap<Integer, List<Integer>> newServerToShards = new ConcurrentHashMap<>();

        if (serverList.isEmpty()) {
            return newServerToShards;
        }

        int serverCount = serverList.size();

        serverList.forEach(server -> newServerToShards.put(server, new ArrayList<>()));

        // Distribute shards in round-robin
        int serverIndex = 0;

        for (Integer shardId : shardList) {
            int targetServer = serverList.get(serverIndex);
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

        // Precision here is more important than a small performance benefit.
        BigInteger range = BigInteger
            .valueOf(Long.MAX_VALUE)
            .subtract(BigInteger.valueOf(Long.MIN_VALUE));

        long stepSize = range
            .divide(BigInteger.valueOf(shardCount))
            .longValue();

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
