package vk.itmo.teamgray.sharded.storage.master.service.topology;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.node.ActionPhase;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.MapResponseWriter;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.master.client.NodeManagementClient;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class TopologyService {
    private static final Logger log = LoggerFactory.getLogger(TopologyService.class);

    private final DiscoveryClient discoveryClient;

    private final ClientCachingFactory clientCachingFactory;

    private Map<Integer, List<Integer>> serverToShards = new HashMap<>();

    private Map<Integer, NodeState> serverToState = new HashMap<>();

    private Map<Integer, Long> shardToHash = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private record ShardFromTo(int sourceServer, int targetServer, int shardId) {
        // No-op.
    }

    private enum ServerAction {
        ADD, REMOVE
    }

    public TopologyService(DiscoveryClient discoveryClient, ClientCachingFactory clientCachingFactory) {
        this.discoveryClient = discoveryClient;
        this.clientCachingFactory = clientCachingFactory;
    }

    public int fillServerToShardsInSync(MapResponseWriter<Integer, List<Integer>> responseWriter) {
        lock.readLock().lock();

        try {
            serverToShards.forEach(responseWriter::writeMapEntry);

            return serverToShards.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int fillShardToHashInSync(MapResponseWriter<Integer, Long> responseWriter) {
        lock.readLock().lock();

        try {
            shardToHash.forEach(responseWriter::writeMapEntry);

            return shardToHash.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    //No Sync to be transparent on states.
    public int fillServerToState(MapResponseWriter<Integer, NodeState> responseWriter) {
        serverToState.forEach(responseWriter::writeMapEntry);

        return serverToState.size();
    }

    public boolean addServer(int serverId, StatusResponseWriter responseWriter) {
        lock.writeLock().lock();
        try {
            setServerState(serverId, NodeState.INIT);

            if (serverToShards.containsKey(serverId)) {
                responseWriter.writeResponse(false, "Could not add server " + serverId + " because it is already registered.");

                return false;
            }

            var updatedServers = new ArrayList<>(serverToShards.keySet());

            updatedServers.add(serverId);

            var oldServerToShards = new HashMap<>(serverToShards);
            var newServerToShards = redistributeShardsEvenly(
                updatedServers,
                new ArrayList<>(shardToHash.keySet())
            );

            return handleShardMovement(
                ServerAction.ADD,
                serverId,
                oldServerToShards,
                newServerToShards,
                shardToHash.size(),
                responseWriter
            );
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean deleteServer(int serverId, StatusResponseWriter responseWriter) {
        lock.writeLock().lock();
        try {
            if (!serverToShards.containsKey(serverId)) {
                responseWriter.writeResponse(false, "Could not remove server " + serverId + " because it is not registered.");

                return false;
            }

            var updatedServers = serverToShards.keySet().stream()
                .filter(it -> it != serverId)
                .toList();

            var oldServerToShards = new HashMap<>(serverToShards);
            var newServerToShards = redistributeShardsEvenly(
                updatedServers,
                new ArrayList<>(shardToHash.keySet())
            );

            // move shards to the new server
            return handleShardMovement(
                ServerAction.REMOVE,
                serverId,
                oldServerToShards,
                newServerToShards,
                shardToHash.size(),
                responseWriter
            );
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean handleShardMovement(
        ServerAction serverAction,
        int actionServerId,
        Map<Integer, List<Integer>> oldMapping,
        Map<Integer, List<Integer>> newMapping,
        int fullShardCount,
        StatusResponseWriter responseWriter
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

            var sendShardMoves = shardMovesBySenders.getOrDefault(serverId, List.of()).stream()
                .map(it -> new SendShardTaskDTO(it.shardId(), it.targetServer()))
                .toList();

            var server = nodes.get(serverId);

            // Cached, perf ok
            NodeManagementClient managementClient = clientCachingFactory
                .getClient(server, NodeManagementClient.class);

            usedNodes.put(serverId, server);

            setServerState(serverId, NodeState.MOVE_SHARDS_PREPARING);

            var response = managementClient.prepareMove(
                receiveShardMoves,
                sendShardMoves,
                fullShardCount
            );

            if (response.isSuccess()) {
                setServerState(serverId, NodeState.MOVE_SHARDS_PREPARED);
            } else {
                String message = "Failed to Prepare Move on "
                    + server + ":"
                    + System.lineSeparator()
                    + server.getIdForLogging() + ": "
                    + response.getMessage();

                log.error(message);

                errorMessages.add(message);

                rollbackAndReturnError(usedNodes, errorMessages, action, responseWriter);

                return false;
            }
        }

        // Phase 2: Process
        log.info("Starting process phase for {}", action);

        for (Integer serverId : allServers) {

            var server = nodes.get(serverId);

            // Cached, perf ok
            NodeManagementClient managementClient = clientCachingFactory
                .getClient(server, NodeManagementClient.class);

            usedNodes.put(serverId, server);

            setServerState(serverId, NodeState.MOVE_SHARDS_PROCESSING);

            var response = managementClient.processAction(action);

            if (response.isSuccess()) {
                setServerState(serverId, NodeState.MOVE_SHARDS_PROCESSED);
            } else {
                String message = "Failed to Process Move on "
                    + server + ":"
                    + System.lineSeparator()
                    + server.getIdForLogging() + ": "
                    + response.getMessage();

                log.error(message);

                errorMessages.add(message);

                rollbackAndReturnError(usedNodes, errorMessages, action, responseWriter);

                return false;
            }
        }

        // Phase 3: Apply
        log.info("Starting apply phase for {}", action);

        for (Integer serverIdToApply : allServers) {
            var server = nodes.get(serverIdToApply);

            // Cached, perf ok
            NodeManagementClient managementClient = clientCachingFactory.getClient(server, NodeManagementClient.class);

            setServerState(serverIdToApply, NodeState.MOVE_SHARDS_APPLYING);

            var response = managementClient.applyAction(action);

            if (response.isSuccess()) {
                // For removed server state needs to be removed, instead of set RUNNING again.
                if (serverAction == ServerAction.REMOVE && serverIdToApply == actionServerId) {
                    removeServerState(actionServerId);
                } else {
                    setServerState(serverIdToApply, NodeState.RUNNING);
                }
            } else {
                applyActionFailed(
                    responseWriter,
                    response.getMessage(),
                    server,
                    errorMessages,
                    "Could not " + serverAction + " server " + actionServerId + ": "
                );

                return false;
            }
        }

        replaceServerToShards(newMapping);

        String message = "Moved shards successfully";

        responseWriter.writeResponse(true, message);
        log.info(message);

        return true;
    }

    private Integer findServerForShard(
        Map<Integer, List<Integer>> mapping,
        Integer shardId
    ) {
        for (Map.Entry<Integer, List<Integer>> entry : mapping.entrySet()) {
            if (entry.getValue().contains(shardId)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public boolean changeShardCount(int shardCount, StatusResponseWriter responseWriter) {
        lock.writeLock().lock();
        try {
            var action = Action.REARRANGE_SHARDS;

            Map<Integer, Long> newShardToHash = redistributeHashesEvenly(shardCount);
            Map<Integer, List<Integer>> newServerToShards = redistributeShardsEvenly(
                new ArrayList<>(serverToShards.keySet()),
                new ArrayList<>(newShardToHash.keySet())
            );

            List<Bound> allBounds = Stream.concat(
                    shardToHash.entrySet().stream().map(it -> new Bound(false, it.getKey(), it.getValue())),
                    newShardToHash.entrySet().stream().map(it -> new Bound(true, it.getKey(), it.getValue()))
                )
                .sorted(Comparator.comparingLong(Bound::upperBound))
                .toList();

            List<FragmentDTO> fragments = findFragmentsToMove(shardCount, newShardToHash, allBounds);

            Map<Integer, List<FragmentDTO>> fragmentsByOldShardIds = fragments.stream()
                .collect(groupingBy(
                    FragmentDTO::oldShardId,
                    mapping(Function.identity(), toList())
                ));

            Map<Integer, List<FragmentDTO>> fragmentsByOldServerIds = serverToShards.entrySet().stream()
                .collect(toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream()
                        .map(fragmentsByOldShardIds::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .toList()
                ));

            Map<Integer, Integer> newShardsToServer = newServerToShards.entrySet().stream()
                .flatMap(kv -> kv.getValue().stream().map(shard -> Map.entry(kv.getKey(), shard)))
                .collect(toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey
                ));

            Map<Integer, DiscoverableServiceDTO> nodes = discoveryClient.getNodeMapWithRetries(newServerToShards.keySet());

            List<String> errorMessages = new ArrayList<>();

            // Phase 1: Prepare
            log.info("Starting prepare phase for {}", action);
            for (var entry : newServerToShards.entrySet()) {
                Integer serverId = entry.getKey();
                List<Integer> shards = entry.getValue();

                Map<Integer, Long> relevantSchemeSlice = shards.stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(),
                            newShardToHash::get
                        )
                    );

                List<FragmentDTO> relevantFragments = fragmentsByOldServerIds.get(serverId);

                Map<Integer, Integer> relevantNodes = relevantFragments.stream()
                    .map(FragmentDTO::newShardId)
                    .distinct()
                    .collect(toMap(
                        Function.identity(),
                        newShardsToServer::get
                    ));

                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient.class);

                setServerState(serverId, NodeState.REARRANGE_SHARDS_PREPARING);
                StatusResponseDTO response =
                    client.prepareRearrange(relevantSchemeSlice, relevantFragments, relevantNodes, newShardToHash.size());

                if (response.isSuccess()) {
                    setServerState(serverId, NodeState.REARRANGE_SHARDS_PREPARED);
                } else {
                    String message = server.getIdForLogging() + ": " + response.getMessage();
                    log.error("Preparation stage failed on node: {}. Error message: {}", server, response.getMessage());
                    errorMessages.add(message);

                    rollbackAndReturnError(nodes, errorMessages, action, responseWriter);

                    return false;
                }
            }

            // Phase 2: Process
            log.info("Starting process phase for {}", action);
            for (var serverId : newServerToShards.keySet()) {
                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient.class);
                setServerState(serverId, NodeState.REARRANGE_SHARDS_PROCESSING);

                StatusResponseDTO response = client.processAction(action);

                if (response.isSuccess()) {
                    setServerState(serverId, NodeState.REARRANGE_SHARDS_PROCESSED);
                } else {
                    String message = server.getIdForLogging() + ": " + response.getMessage();
                    log.error("Process stage failed on node: {}. Error message: {}", server, response.getMessage());
                    errorMessages.add(message);

                    rollbackAndReturnError(nodes, errorMessages, action, responseWriter);

                    return false;
                }
            }

            // Phase 3: Apply
            log.info("Starting apply phase for {}", action);
            for (var entry : newServerToShards.entrySet()) {
                var serverId = entry.getKey();
                DiscoverableServiceDTO server = nodes.get(serverId);
                NodeManagementClient client = clientCachingFactory.getClient(server, NodeManagementClient.class);

                setServerState(serverId, NodeState.REARRANGE_SHARDS_APPLYING);
                StatusResponseDTO response = client.applyAction(action);

                if (response.isSuccess()) {
                    setServerState(serverId, NodeState.RUNNING);
                } else {
                    applyActionFailed(
                        responseWriter,
                        response.getMessage(),
                        server,
                        errorMessages,
                        "Could not process rearrange on server " + serverId + ": "
                    );

                    return false;
                }
            }

            replaceBothMaps(newShardToHash, newServerToShards);
            responseWriter.writeResponse(true, "Changed shard count successfully");

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void applyActionFailed(
        StatusResponseWriter responseWriter,
        String message,
        DiscoverableServiceDTO server,
        List<String> errorMessages,
        String actionPrefix
    ) {
        String serverMessage = server.getIdForLogging() + ": " + message;
        log.error("Apply failed on node, Marking it as {}: {}. Error message: {}", NodeState.DEAD, server, serverMessage);
        errorMessages.add(message);

        // No compensation or recovery actions on involved nodes
        // End up change shards process and return error response
        // Marking Node as DEAD
        setServerState(server.id(), NodeState.DEAD);
        responseWriter.writeResponse(
            false,
            actionPrefix + StringUtil.join(System.lineSeparator(), errorMessages).toString()
        );
    }

    private void rollbackAndReturnError(
        Map<Integer, DiscoverableServiceDTO> nodes,
        List<String> errorMessages,
        Action action,
        StatusResponseWriter responseWriter
    ) {
        String rollbackMessage = "One or more nodes failed during " + action + ". Rolling back.";
        log.warn(rollbackMessage);
        errorMessages.add(rollbackMessage);

        for (var node : nodes.values()) {
            NodeManagementClient client = clientCachingFactory.getClient(node, NodeManagementClient.class);

            setServerState(node.id(), NodeState.resolve(action, ActionPhase.ROLLBACK));

            StatusResponseDTO response = client.rollbackAction(action);
            if (response.isSuccess()) {
                setServerState(node.id(), NodeState.RUNNING);
            } else {
                // Just logging error, no compensation moves - continuing rollback of other nodes
                String message = node.getIdForLogging() + ": " + response.getMessage();
                log.error("Rollback {} failed for node: {}. Error message: {}", action, node, response.getMessage());
                errorMessages.add(message);

                // Marking node as DEAD
                setServerState(node.id(), NodeState.DEAD);
            }
        }

        // No need to swap maps, old ones are still intact, we just do not apply old ones.
        log.info("{} Failed", action);

        responseWriter.writeResponse(false, StringUtil.join(System.lineSeparator(), errorMessages).toString());
    }

    private List<FragmentDTO> findFragmentsToMove(
        int shardCount,
        Map<Integer, Long> newShardToHash,
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

    private void replaceServerToShards(Map<Integer, List<Integer>> newServerToShards) {
        lock.writeLock().lock();

        log.info("Replacing server to shard scheme {}", newServerToShards);

        try {
            log.info("Replacing server to shard scheme {}", newServerToShards);
            var oldServerToShards = serverToShards;

            serverToShards = newServerToShards;

            oldServerToShards.clear();

            log.info("Replaced server to shard scheme");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void replaceBothMaps(
        Map<Integer, Long> newShardToHash,
        Map<Integer, List<Integer>> newServerToShards
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

    private Map<Integer, List<Integer>> redistributeShardsEvenly(
        List<Integer> serverList,
        List<Integer> shardList
    ) {
        Map<Integer, List<Integer>> newServerToShards = new HashMap<>();

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

    private Map<Integer, Long> redistributeHashesEvenly(int shardCount) {
        Map<Integer, Long> newShardToHash = new HashMap<>();

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

    private void setServerState(Integer serverId, NodeState state) {
        log.debug("Setting server {} state to {}", serverId, state);

        serverToState.put(serverId, state);
    }

    private void removeServerState(Integer serverId) {
        log.debug("Removing server {} state", serverId);

        serverToState.remove(serverId);
    }
}
