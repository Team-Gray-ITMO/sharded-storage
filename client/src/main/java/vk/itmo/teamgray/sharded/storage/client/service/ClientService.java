package vk.itmo.teamgray.sharded.storage.client.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;

import static vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils.getShardIdForKey;

public class ClientService {
    private record Entry(String key, String value) {
        // No-op.
    }

    private static final Logger log = LoggerFactory.getLogger(ClientService.class);

    private static final Duration CACHE_EXPIRATION = Duration.of(30, ChronoUnit.MINUTES);

    private final MasterClient masterClient;

    private final DiscoveryClient discoveryClient;

    private final Map<Integer, DiscoverableServiceDTO> shardToServer = new HashMap<>();

    private final Map<Long, Integer> hashToShard = new HashMap<>();

    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Entry>> retryQueuesForServers = new ConcurrentHashMap<>();

    private final ExecutorService retryingExecutor = Executors.newSingleThreadExecutor();

    private Instant cacheLastUpdate;

    public ClientService(
        MasterClient masterClient,
        DiscoveryClient discoveryClient
    ) {
        this.masterClient = masterClient;
        this.discoveryClient = discoveryClient;

        retryingExecutor.submit(() -> {
            while (true) {
                for (var serverQueueEntry : retryQueuesForServers.entrySet()) {
                    var serverQueue = serverQueueEntry.getValue();
                    if (serverQueue.isEmpty()) {
                        continue;
                    }

                    var currentValue = serverQueue.peek();
                    var nodeClient = getNodeClient(currentValue.key);

                    while (nodeClient.setKey(currentValue.key, currentValue.value) != SetStatus.IS_BUSY) {
                        // remove currently set value
                        serverQueue.poll();

                        if (serverQueue.isEmpty()) {
                            continue;
                        }

                        currentValue = serverQueue.peek();
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        updateCaches();
    }

    /**
     * Gets value by key from node
     *
     * @return value by key
     */
    public String getValue(String key) {
        var nodeClient = getNodeClient(key);

        return nodeClient.getKey(key);
    }

    private NodeClient getNodeClient(String key) {
        if (cacheLastUpdate == null || cacheLastUpdate.isBefore(Instant.now().minus(CACHE_EXPIRATION))) {
            updateCaches();
        }

        var shardId = getShardIdForKey(key, hashToShard.size());

        var server = shardToServer.get(shardId);

        if (server == null) {
            updateCaches();

            server = shardToServer.get(shardId);
        }

        if (shardToServer.isEmpty()) {
            throw new IllegalStateException("No shards are created yet.");
        }

        if (server == null) {
            throw new IllegalStateException("Could not find server for key: " + key + " in shard " + shardId);
        }

        log.debug("Found shard {} for key '{}' in server {} ", shardId, key, server.id());

        return GrpcClientCachingFactory.getInstance()
            .getClient(
                server,
                NodeClient::new
            );
    }

    /**
     * Puts value by key
     *
     * @return Returns success of operation
     */
    public boolean setValue(String key, String value) {
        var nodeClient = getNodeClient(key);

        var shardId = getShardIdForKey(key, Integer.parseInt(String.valueOf(getTotalShardCount())));
        var server = shardToServer.get(shardId);
        if (server == null) {
            return false;
        }

        var serverQueue = retryQueuesForServers.computeIfAbsent(server.id(), serverId -> new ConcurrentLinkedQueue<>());
        // if we have some values in the queue, need to set them first
        if (!serverQueue.isEmpty()) {
            serverQueue.add(new Entry(key, value));
            return true;
        }

        var setResult = nodeClient.setKey(key, value);
        if (setResult == SetStatus.IS_BUSY) {
            serverQueue.add(new Entry(key, value));
            return true;
        } else if (setResult == SetStatus.SUCCESS) {
            return true;
        } else if (setResult == SetStatus.ERROR) {
            return false;
        }

        throw new RuntimeException("Unsupported SetStatus");
    }

    /**
     * Puts value by key pairs from file
     *
     * @param filePath path to file where to get pairs to save
     *
     * @return result of set operation
     */
    public StatusResponseDTO setFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");

                String key = parts[0].trim();
                String value = parts[1].trim();

                try {
                    var nodeClient = getNodeClient(key);
                    nodeClient.setKey(key, value);
                } catch (NodeException e) {
                    String errMessage = MessageFormat.format("Error while setting key=[{0}] value=[{1}].", key, value);
                    log.warn(errMessage, e);
                    return new StatusResponseDTO(false, errMessage);
                }
            }
        } catch (IOException e) {
            return new StatusResponseDTO(false, e.getMessage());
        }
        return new StatusResponseDTO(true, "Keys from file successfully set.");
    }

    /**
     * Adds server
     *
     * @return result of add operation
     */
    public StatusResponseDTO addServer(int serverId, boolean fork) {
        StatusResponseDTO result = masterClient.addServer(serverId, fork);
        updateCaches();
        return result;
    }

    /**
     * Deletes server
     *
     * @return result of delete operation
     */
    public StatusResponseDTO deleteServer(int serverId) {
        StatusResponseDTO result = masterClient.deleteServer(serverId);
        updateCaches();
        return result;
    }

    /**
     * Updates count of shards
     *
     * @param newCount new count
     *
     * @return result of change shards operation
     */
    public StatusResponseDTO changeShardCount(int newCount) {
        StatusResponseDTO result = masterClient.changeShardCount(newCount);
        updateCaches();
        return result;
    }

    public String getMasterHost() {
        return masterClient.getHost();
    }

    public int getMasterPort() {
        return masterClient.getPort();
    }

    private void updateCaches() {
        shardToServer.clear();
        hashToShard.clear();

        var shardToServerId = masterClient.getShardToServerMap();

        var nodeMap = discoveryClient.getNodeMapWithRetries(
            new HashSet<>(shardToServerId.values())
        );

        shardToServerId.forEach((shardId, serverId) -> {
            shardToServer.put(shardId, nodeMap.get(serverId));
        });

        hashToShard.putAll(masterClient.getHashToShardMap());

        cacheLastUpdate = Instant.now();
    }

    public HeartbeatResponseDTO sendMasterHeartbeat() {
        return masterClient.heartbeat();
    }

    /**
     * Get the current shard-to-server mapping as a Map
     *
     * @return Map from shard ID to server address (ip:port)
     */
    public Map<Integer, DiscoverableServiceDTO> getShardServerMapping() {
        return shardToServer;
    }

    /**
     * Get cached hash-to-shard mapping as a Map
     *
     * @return Map of pairs (Max available hash value -> Shard number)
     */
    public Map<Long, Integer> getHashToShardMapping() {
        return hashToShard;
    }

    // Getting uncached to get always transparent values.
    public Map<Integer, NodeState> getServerStates() {
        return masterClient.getServerToState();
    }

    /**
     * Get the total shard count
     *
     * @return the total number of shards
     */
    public long getTotalShardCount() {
        return shardToServer.size();
    }
}
