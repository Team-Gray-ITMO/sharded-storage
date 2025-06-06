package vk.itmo.teamgray.sharded.storage.master.service.topology;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
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

    private final GrpcClientCachingFactory clientCachingFactory;

    private ConcurrentHashMap<Integer, List<Integer>> serverToShards = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, Long> shardToHash = new ConcurrentHashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private record ShardFromTo(int sourceServer, int targetServer, int shardId) {
        // No-op.
    }

    public TopologyService(DiscoveryClient discoveryClient, GrpcClientCachingFactory clientCachingFactory) {
        this.discoveryClient = discoveryClient;
        this.clientCachingFactory = clientCachingFactory;
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

            StatusResponseDTO result = handleShardMovement(oldServerToShards, newServerToShards, shardToHash.size());

            if (result.isSuccess()) {
                log.info("Added server {}", serverId);

                return new StatusResponseDTO(true, "Server Added");
            } else {
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
            StatusResponseDTO result = handleShardMovement(oldServerToShards, newServerToShards, shardToHash.size());

            if (result.isSuccess()) {
                log.info("Removed server {}", serverId);

                return new StatusResponseDTO(true, "Server Removed");
            } else {
                return new StatusResponseDTO(false, "Could not remove server: " + System.lineSeparator() + result.getMessage());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private StatusResponseDTO handleShardMovement(
        ConcurrentHashMap<Integer, List<Integer>> oldMapping,
        ConcurrentHashMap<Integer, List<Integer>> newMapping,
        int fullShardCount
    ) {
        var action = Action.MOVE_SHARDS;

        Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(oldMapping.keySet());

        List<String> errorMessages = new ArrayList<>();

        List<ShardFromTo> moveShards = new ArrayList<>();

        Map<Integer, DiscoverableServiceDTO> usedNodes = new HashMap<>();

        // find shards that need to be moved
        for (Map.Entry<Integer, List<Integer>> oldEntry : oldMapping.entrySet()) {
            int sourceServerId = oldEntry.getKey();
            List<Integer> oldShards = oldEntry.getValue();

            for (Integer shardId : oldShards) {
                Integer targetServerId = findServerForShard(newMapping, shardId);

                if (targetServerId != null && !targetServerId.equals(sourceServerId)) {
                    log.debug("Preparing to move shard {} from {} to {}", shardId, sourceServerId, targetServerId);

                    moveShards.add(new ShardFromTo(sourceServerId, targetServerId, shardId));
                }
            }
        }

        var shardMovesByReceivers = moveShards.stream()
            .collect(
                Collectors.groupingBy(
                    ShardFromTo::targetServer,
                    Collectors.mapping(Function.identity(), Collectors.toList())
                )
            );

        var shardMovesBySenders = moveShards.stream()
            .collect(
                Collectors.groupingBy(
                    ShardFromTo::sourceServer,
                    Collectors.mapping(Function.identity(), Collectors.toList())
                )
            );

        var allServers = moveShards.stream()
            .flatMap(it -> Stream.of(it.sourceServer(), it.targetServer()))
            .distinct()
            .toList();

        // Phase 1: Prepare
        log.info("Starting prepare phase for {}", action);
        for (Integer serverId : allServers) {
            var receiveShardMoves = shardMovesByReceivers.getOrDefault(serverId, List.of()).stream()
                .map(ShardFromTo::shardId)
                .toList();

            var removeShardMoves = shardMovesBySenders.getOrDefault(serverId, List.of()).stream()
                .map(ShardFromTo::shardId)
                .toList();

            var server = nodes.get(serverId);

            // Cached, perf ok
            NodeManagementClient managementClient = clientCachingFactory
                .getClient(server, NodeManagementClient::new);

            usedNodes.put(serverId, server);

            var response = managementClient.prepareMove(
                receiveShardMoves,
                removeShardMoves,
                fullShardCount
            );

            if (!response.isSuccess()) {
                String message = "Failed to Prepare Move on "
                    + server + ":"
                    + System.lineSeparator()
                    + server.getIdForLogging() + ": "
                    + response.getMessage();

                log.error(message);

                errorMessages.add(message);

                return rollbackAndReturnError(usedNodes, errorMessages, Action.MOVE_SHARDS);
            }
        }

        // Phase 2: Process
        log.info("Starting process phase for {}", action);

        for (Integer serverId : allServers) {
            var sendShardMoves = shardMovesBySenders.getOrDefault(serverId, List.of());

            var server = nodes.get(serverId);

            // Cached, perf ok
            NodeManagementClient managementClient = clientCachingFactory
                .getClient(server, NodeManagementClient::new);

            usedNodes.put(serverId, server);

            var response = managementClient.processMove(
                sendShardMoves.stream()
                    .map(shardFromTo -> new SendShardTaskDTO(shardFromTo.shardId(), shardFromTo.targetServer()))
                    .toList()
            );

            if (!response.isSuccess()) {
                String message = "Failed to Process Move on "
                    + server + ":"
                    + System.lineSeparator()
                    + server.getIdForLogging() + ": "
                    + response.getMessage();

                log.error(message);

                errorMessages.add(message);

                return rollbackAndReturnError(usedNodes, errorMessages, Action.MOVE_SHARDS);
            }
        }

        // Phase 3: Apply
        log.info("Starting apply phase for {}", action);

        for (Integer serverIdToApply : allServers) {
            var server = nodes.get(serverIdToApply);

            // Cached, perf ok
            NodeManagementClient managementClient = clientCachingFactory
                .getClient(server, NodeManagementClient::new);

            var response = managementClient.applyOperation(Action.MOVE_SHARDS);

            if (!response.isSuccess()) {
                String message = server.getIdForLogging() + ": " + response.getMessage();
                log.error("Apply failed on node, System is in inconsistent state: {}. Error message: {}", server, response.getMessage());
                errorMessages.add(message);
                // No compensation or recovery actions on involved nodes
                // End up change shards process and return error response
                return new StatusResponseDTO(
                    false,
                    StringUtil.join(System.lineSeparator(), errorMessages).toString()
                );
            }
        }

        replaceServerToShards(newMapping);

        log.info("Moved shards successfully");

        return new StatusResponseDTO(true, "Shards Moved");
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

            var action = Action.REARRANGE_SHARDS;

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
            log.info("Starting prepare phase for {}", action);
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

                    return rollbackAndReturnError(nodes, errorMessages, Action.REARRANGE_SHARDS);
                }
            }

            // Phase 2: Process
            log.info("Starting process phase for {}", action);
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

                    return rollbackAndReturnError(nodes, errorMessages, Action.REARRANGE_SHARDS);
                }
            }

            // Phase 3: Apply
            log.info("Starting apply phase for {}", action);
            for (var entry : newServerToShards.entrySet()) {
                var serverId = entry.getKey();
                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient::new);

                StatusResponseDTO response = client.applyOperation(Action.REARRANGE_SHARDS);

                if (!response.isSuccess()) {
                    String message = server.getIdForLogging() + ": " + response.getMessage();
                    log.error("Apply failed on node, System is in inconsistent state: {}. Error message: {}", server,
                        response.getMessage());
                    errorMessages.add(message);
                    // No compensation or recovery actions on involved nodes
                    // End up change shards process and return error response
                    return new StatusResponseDTO(
                        false,
                        StringUtil.join(System.lineSeparator(), errorMessages).toString()
                    );
                }
            }

            replaceBothMaps(newShardToHash, newServerToShards);
            log.info("Changed shard count successfully");
            return new StatusResponseDTO(true, "Changed shard count successfully");

        } finally {
            lock.writeLock().unlock();
        }
    }

    private StatusResponseDTO rollbackAndReturnError(
        Map<Integer, DiscoverableServiceDTO> nodes,
        List<String> errorMessages,
        Action action
    ) {
        String rollbackMessage = "One or more nodes failed during " + action + ". Rolling back.";
        log.warn(rollbackMessage);
        errorMessages.add(rollbackMessage);

        for (var node : nodes.values()) {
            NodeManagementClient client = clientCachingFactory.getClient(node, NodeManagementClient::new);
            StatusResponseDTO response = client.rollbackOperation(action);
            if (!response.isSuccess()) {
                // Just logging error, no compensation moves - continuing rollback of other nodes
                String message = node.getIdForLogging() + ": " + response.getMessage();
                log.error("Rollback {} failed for node: {}. Error message: {}", action, node, response.getMessage());
                errorMessages.add(message);
            }
        }

        // No need to swap maps, old ones are still intact, we just do not apply old ones.
        log.info("{} Failed", action);

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
