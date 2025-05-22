package vk.itmo.teamgray.sharded.storage.client.service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.utils.HashingUtils;

import static vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils.getShardIdForKey;

public class ClientService {
    private record ShardOnServer(int server, int shard) {
        // No-op.
    }

    private static final Logger log = LoggerFactory.getLogger(ClientService.class);

    private static final Duration CACHE_EXPIRATION = Duration.of(30, ChronoUnit.MINUTES);

    private final MasterClient masterClient;

    private final DiscoveryClient discoveryClient;

    private final Map<Integer, DiscoverableServiceDTO> shardToServer = new HashMap<>();

    private final Map<Long, Integer> hashToShard = new HashMap<>();

    private Instant cacheLastUpdate;

    public ClientService(
        MasterClient masterClient,
        DiscoveryClient discoveryClient
    ) {
        this.masterClient = masterClient;
        this.discoveryClient = discoveryClient;

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

        //TODO Debug level
        log.info("Found shard {} for key '{}' in server {} ", shardId, key, server.id());

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

        return nodeClient.setKey(key, value);
    }

    /**
     * Puts value by key pairs from file
     *
     * @param filePath path to file where to get pairs to save
     *
     * @return result of set operation
     */
    public StatusResponseDTO setFromFile(String filePath) {
        //var nodeClient = getNodeClient(key);

        throw new UnsupportedOperationException("Not implemented yet");

        //TODO Rework to resolve node clients based on different keys in file
        //return nodeClient.setFromFile(filePath);
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

    //TODO Ultraslow, store sorted and do a binary search
    private ShardOnServer getShardAndServerForKey(String key) {
        long hash = HashingUtils.calculate64BitHash(key);

        List<Long> sortedHashes = hashToShard.keySet().stream()
            .sorted()
            .toList();

        long previous = Long.MIN_VALUE;
        long next = Long.MAX_VALUE;
        Integer shard = null;

        for (Long upperHash : sortedHashes) {
            if (hash <= upperHash) {
                next = upperHash;
                shard = hashToShard.get(upperHash);
                break;
            }

            previous = upperHash;
        }

        // Last shard must be marked as MAX_VALUE
        if (next == Long.MAX_VALUE) {
            shard = hashToShard.get(next);
        }

        // If no previous key is found return shard from next key
        if (previous == Long.MIN_VALUE) {
            shard = hashToShard.get(next);
        }

        if (shard == null) {
            throw new IllegalStateException("Shard not found for hash: " + hash);
        }

        var server = shardToServer.get(shard);

        return new ShardOnServer(server.id(), shard);
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

    /**
     * Get the total shard count
     *
     * @return the total number of shards
     */
    public long getTotalShardCount() {
        return shardToServer.size();
    }
}
