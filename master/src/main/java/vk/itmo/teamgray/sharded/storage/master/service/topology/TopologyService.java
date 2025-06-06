package vk.itmo.teamgray.sharded.storage.master.service.topology;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
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
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.MoveShardDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.master.client.*;
import vk.itmo.teamgray.sharded.storage.node.service.NodeState;

import static java.util.stream.Collectors.toMap;

public class TopologyService {
    private static final Logger log = LoggerFactory.getLogger(TopologyService.class);

    private final DiscoveryClient discoveryClient;

    private final GrpcClientCachingFactory clientCachingFactory;

    private ConcurrentHashMap<Integer, List<Integer>> serverToShards = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, NodeState> serverToState = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, Long> shardToHash = new ConcurrentHashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public TopologyService(DiscoveryClient discoveryClient, GrpcClientCachingFactory clientCachingFactory) {
        this.discoveryClient = discoveryClient;
        this.clientCachingFactory = clientCachingFactory;
    }

    public ConcurrentHashMap<Integer, List<Integer>> getServerToShards() {
        return serverToShards;
    }

    public ConcurrentHashMap<Integer, NodeState> getServerToState() {
        return serverToState;
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

    public GetServerToStateResponse getServerToStateAsGrpc() {
        lock.readLock().lock();

        try {
            GetServerToStateResponse.Builder responseBuilder = GetServerToStateResponse.newBuilder();

            for (Map.Entry<Integer, NodeState> entry : serverToState.entrySet()) {
                int serverId = entry.getKey();
                NodeState nodeState = entry.getValue();

                responseBuilder.putServerToState(serverId, nodeState.name());
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

    public StatusResponseDTO addServer(int serverId) {
        lock.writeLock().lock();
        try {
            log.info("Adding server {}", serverId);

            if (serverToShards.containsKey(serverId)) {
                return new StatusResponseDTO(false, "Could not add server " + serverId + " because it is already registered.");
            }

            var updatedServers = Collections.list(serverToShards.keys());

            updatedServers.add(serverId);

            var oldServerToShards = new ConcurrentHashMap<>(serverToShards);
            var newServerToShards = redistributeShardsEvenly(
                updatedServers,
                Collections.list(shardToHash.keys())
            );

            StatusResponseDTO result = handleShardMovement(oldServerToShards, newServerToShards);

            var newServerToState = new ConcurrentHashMap<>(serverToState);
            if (result.isSuccess()) {
                newServerToState.put(serverId, NodeState.RUNNING);
                replaceServerToShardsAndState(newServerToShards, newServerToState);

                log.info("Added server {}", serverId);

                return new StatusResponseDTO(true, "Server Added");
            } else {
                newServerToState.put(serverId, NodeState.DEAD);
                replaceServerToState(newServerToState);
                return new StatusResponseDTO(false, "Could not add new server: " + System.lineSeparator() + result.getMessage());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public StatusResponseDTO deleteServer(int serverId) {
        lock.writeLock().lock();
        try {
            log.info("Removing server {}", serverId);

            if (!serverToShards.containsKey(serverId)) {
                return new StatusResponseDTO(false, "Could not remove server " + serverId + " because it is not registered.");
            }

            var updatedServers = Collections.list(serverToShards.keys()).stream()
                .filter(it -> it != serverId)
                .toList();

            var oldServerToShards = new ConcurrentHashMap<>(serverToShards);
            var newServerToShards = redistributeShardsEvenly(
                updatedServers,
                Collections.list(shardToHash.keys())
            );

            // move shards to the new server
            StatusResponseDTO result = handleShardMovement(oldServerToShards, newServerToShards);

            var newServerToState = new ConcurrentHashMap<>(serverToState);
            if (result.isSuccess()) {
                log.info("Removed server {}", serverId);

                newServerToState.remove(serverId);
                replaceServerToShardsAndState(newServerToShards, newServerToState);

                return new StatusResponseDTO(true, "Server Removed");
            } else {
                newServerToState.put(serverId, NodeState.DEAD);
                replaceServerToState(newServerToState);
                return new StatusResponseDTO(false, "Could not remove server: " + System.lineSeparator() + result.getMessage());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private StatusResponseDTO handleShardMovement(
        ConcurrentHashMap<Integer, List<Integer>> oldMapping,
        ConcurrentHashMap<Integer, List<Integer>> newMapping
    ) {
        Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(oldMapping.keySet());

        List<String> errorMessages = new ArrayList<>();

        // find shards that need to be moved
        for (Map.Entry<Integer, List<Integer>> oldEntry : oldMapping.entrySet()) {
            int sourceServerId = oldEntry.getKey();
            List<Integer> oldShards = oldEntry.getValue();

            var sourceServer = nodes.get(sourceServerId);

            List<MoveShardDTO> moveShards = new ArrayList<>();

            for (Integer shardId : oldShards) {
                Integer targetServer = findServerForShard(newMapping, shardId);

                if (targetServer != null && !targetServer.equals(sourceServerId)) {
                    log.info("Preparing to move shard {} from {} to {}", shardId, sourceServer, targetServer);

                    moveShards.add(new MoveShardDTO(shardId, targetServer));
                }
            }

            if (moveShards.isEmpty()) {
                continue;
            }

            NodeManagementClient nodeManagementClient = clientCachingFactory
                .getClient(
                    sourceServer,
                    NodeManagementClient::new
                );

            StatusResponseDTO response = nodeManagementClient.moveShards(moveShards);

            var newServerToState = new ConcurrentHashMap<>(serverToState);
            if (!response.isSuccess()) {
                //TODO Consider rollback
                String message = "Failed to move shards " + moveShards
                    + " from " + sourceServer + ":"
                    + System.lineSeparator()
                    + sourceServer.getIdForLogging() + ": "
                    + response.getMessage();

                log.error(message);

                errorMessages.add(message);
                newServerToState.put(sourceServerId, NodeState.DEAD);
                replaceServerToState(newServerToState);
            }
        }

        if (errorMessages.isEmpty()) {
            return new StatusResponseDTO(true, "Shards Moved");
        } else {
            return new StatusResponseDTO(false, StringUtil.join(System.lineSeparator(), errorMessages).toString());
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

    public StatusResponseDTO changeShardCount(int shardCount) {
        lock.writeLock().lock();
        try {
            log.info("Changing shard count to {}", shardCount);

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

            var newShardsToServer = newServerToShards.entrySet().stream()
                .flatMap(kv -> kv.getValue().stream().map(shard -> Map.entry(kv.getKey(), shard)))
                .collect(toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey
                ));

            var nodes = discoveryClient.getNodeMapWithRetries(newServerToShards.keySet());
            List<String> errorMessages = new ArrayList<>();

            // Phase 1: Prepare
            log.info("Starting prepare phase");
            var newServerToState = new ConcurrentHashMap<>(serverToState);
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

                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient::new);

                StatusResponseDTO response = client.prepareRearrange(relevantSchemeSlice, newShardToHash.size());
                if (!response.isSuccess()) {
                    String message = server.getIdForLogging() + ": " + response.getMessage();
                    log.error("Preparation stage failed on node: {}. Error message: {}", server, response.getMessage());
                    errorMessages.add(message);
                    newServerToState.put(serverId, NodeState.DEAD);
                    return rollbackAndReturnError(nodes, errorMessages);
                }
                newServerToState.put(serverId, NodeState.REARRANGE_PREPARED);
            }
            replaceServerToState(newServerToState);

            // Phase 2: Process
            log.info("Starting process phase");
            newServerToState = new ConcurrentHashMap<>(serverToState);
            for (var serverId : newServerToShards.keySet()) {
                var relevantFragments = serverToShards.get(serverId).stream()
                    .flatMap(it -> fragments.stream().filter(f -> f.oldShardId() == it))
                    .toList();

                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient::new);

                var relevantNodes = relevantFragments.stream()
                    .map(FragmentDTO::newShardId)
                    .distinct()
                    .collect(toMap(
                        Function.identity(),
                        newShardsToServer::get
                    ));

                StatusResponseDTO response = client.processRearrange(relevantFragments, relevantNodes);

                if (!response.isSuccess()) {
                    String message = server.getIdForLogging() + ": " + response.getMessage();
                    log.error("Process stage failed on node: {}. Error message: {}", server, response.getMessage());
                    errorMessages.add(message);
                    newServerToState.put(serverId, NodeState.DEAD);
                    return rollbackAndReturnError(nodes, errorMessages);
                }
                newServerToState.put(serverId, NodeState.REARRANGE_PROCESSED);
            }
            replaceServerToState(newServerToState);

            // Phase 3: Apply
            log.info("Starting apply phase");
            newServerToState = new ConcurrentHashMap<>(serverToState);
            for (var entry : newServerToShards.entrySet()) {
                var serverId = entry.getKey();
                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient::new);

                StatusResponseDTO response = client.applyRearrange();
                if (!response.isSuccess()) {
                    String message = server.getIdForLogging() + ": " + response.getMessage();
                    log.error("Apply failed on node, System is in inconsistent state: {}. Error message: {}", server,
                        response.getMessage());
                    errorMessages.add(message);
                    newServerToState.put(serverId, NodeState.DEAD);
                    // No compensation or recovery actions on involved nodes
                    // End up change shards process and return error response
                    return new StatusResponseDTO(
                        false,
                        StringUtil.join(System.lineSeparator(), errorMessages).toString()
                    );
                }
                newServerToState.put(serverId, NodeState.RUNNING);
            }
            replaceState(newShardToHash, newServerToShards, newServerToState);

            log.info("Changed shard count successfully");
            return new StatusResponseDTO(true, "Changed shard count successfully");

        } finally {
            lock.writeLock().unlock();
        }
    }

    private StatusResponseDTO rollbackAndReturnError(
        Map<Integer, DiscoverableServiceDTO> nodes,
        List<String> errorMessages
    ) {
        String rollbackMessage = "One or more nodes failed during rearrange. Rolling back.";
        log.warn(rollbackMessage);
        errorMessages.add(rollbackMessage);

        var newServerToState = new ConcurrentHashMap<>(serverToState);
        for (var node : nodes.values()) {
            NodeManagementClient client = clientCachingFactory.getClient(node, NodeManagementClient::new);
            StatusResponseDTO response = client.rollbackRearrange();
            if (!response.isSuccess()) {
                // Just logging error, no compensation moves - continuing rollback of other nodes
                String message = node.getIdForLogging() + ": " + response.getMessage();
                log.error("Rollback failed for node: {}. Error message: {}", node, response.getMessage());
                errorMessages.add(message);
                newServerToState.put(node.id(), NodeState.DEAD);
            } else {
                newServerToState.put(node.id(), NodeState.RUNNING);
            }
        }

        replaceServerToState(newServerToState);
        // No need to swap maps, old ones are still intact, we just do not apply old ones.
        log.info("Change Shard Count Failed");
        return new StatusResponseDTO(false, StringUtil.join(System.lineSeparator(), errorMessages).toString());
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

    private void replaceServerToState(ConcurrentHashMap<Integer, NodeState> newServerToState) {
        lock.writeLock().lock();

        log.info("Replacing server to state scheme {}", newServerToState);

        try {

            var oldServerToState = serverToState;

            serverToState = newServerToState;

            oldServerToState.clear();

            log.info("Replaced server to state scheme");

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void replaceServerToShardsAndState(ConcurrentHashMap<Integer, List<Integer>> newServerToShards,
                                               ConcurrentHashMap<Integer, NodeState> newServerToState) {
        lock.writeLock().lock();

        try {
            log.info("Replacing server to shard scheme {}", newServerToShards);
            var oldServerToShards = serverToShards;

            serverToShards = newServerToShards;

            oldServerToShards.clear();

            log.info("Replaced server to shard scheme");

            log.info("Replacing server to state scheme {}", newServerToState);

            var oldServerToState = serverToState;

            serverToState = newServerToState;

            oldServerToState.clear();

            log.info("Replaced server to state scheme");

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void replaceState(
        ConcurrentHashMap<Integer, Long> newShardToHash,
        ConcurrentHashMap<Integer, List<Integer>> newServerToShards,
        ConcurrentHashMap<Integer, NodeState> newServerToState
    ) {
        lock.writeLock().lock();

        log.info("Replacing server to shard, shard to hash and server to state schemes [serverToShard={}, serverToHash={}, serverToState={}]",
                newServerToShards,
                newShardToHash,
                newServerToState
        );

        try {
            var oldShardToHash = shardToHash;
            var oldServerToShards = serverToShards;
            var oldServerToState = serverToState;

            shardToHash = newShardToHash;
            serverToShards = newServerToShards;
            serverToState = newServerToState;

            oldShardToHash.clear();
            oldServerToShards.clear();
            oldServerToState.clear();

            log.info("Replaced server to shard, shard to hash and server to states schemes schemes");
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
