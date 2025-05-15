package vk.itmo.teamgray.sharded.storage.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.utils.HashingUtils;
import vk.itmo.teamgray.sharded.storage.dto.*;

public class ClientService {
    private record ShardOnServer(String server, int shard) {
    }

    private static final Logger log = LoggerFactory.getLogger(ClientService.class);

    private static final Duration CACHE_EXPIRATION = Duration.of(30, ChronoUnit.MINUTES);

    private final MasterClient masterClient;

    private final NodeClient nodeClient;

    private final Cache<Integer, String> shardToServer = CacheBuilder.newBuilder()
        .expireAfterWrite(CACHE_EXPIRATION)
        .maximumSize(1000)
        .build(new CacheLoader<>() {
            @Override
            public String load(Integer shard) {
                Map<Integer, String> allMappings = masterClient.getShardToServerMap();

                //TODO Later optimize, now all cache reloads on missing value.
                shardToServer.putAll(allMappings);
                return allMappings.getOrDefault(shard, "UNKNOWN");
            }
        });

    private final Cache<Long, Integer> hashToShard = CacheBuilder.newBuilder()
        .expireAfterWrite(CACHE_EXPIRATION)
        .maximumSize(100_000)
        .build(new CacheLoader<>() {
            @Override
            public Integer load(Long hash) {
                Map<Long, Integer> allHashMappings = masterClient.getHashToShardMap();

                //TODO Later optimize, now all cache reloads on missing value.
                hashToShard.putAll(allHashMappings);
                return allHashMappings.getOrDefault(hash, -1);
            }
        });

    public ClientService(MasterClient masterClient, NodeClient nodeClient) {
        this.masterClient = masterClient;
        this.nodeClient = nodeClient;
    }

    /**
     * Gets value by key from node
     * @return value by key
     */
    public String getValue(String key) {
        return nodeClient.getKey(key);
    }

    /**
     * Puts value by key
     * @return Returns success of operation
     */
    public boolean setValue(String key, String value) {
        return nodeClient.setKey(key, value);
    }

    /**
     * Puts value by key pairs from file
     * @param filePath path to file where to get pairs to save
     * @return result of set operation
     */
    public SetFromFileResponseDTO setFromFile(String filePath) {
        return nodeClient.setFromFile(filePath);
    }

    /**
     * Adds server
     * @return result of add operation
     */
    public AddServerResponseDTO addServer(String ip, int port, boolean fork) {
        AddServerResponseDTO result = masterClient.addServer(ip, port, fork);
        updateCaches();
        return result;
    }

    /**
     * Deletes server
     * @return result of delete operation
     */
    public DeleteServerResponseDTO deleteServer(String ip, int port) {
        DeleteServerResponseDTO result = masterClient.deleteServer(ip, port);
        updateCaches();
        return result;
    }

    /**
     * Updates count of shards
     * @param newCount new count
     * @return result of change shards operation
     */
    public ChangeShardCountResponseDTO changeShardCount(int newCount) {
        ChangeShardCountResponseDTO result = masterClient.changeShardCount(newCount);
        updateCaches();
        return result;
    }

    public String getMasterHost() {
        return masterClient.getHost();
    }

    public int getMasterPort() {
        return masterClient.getPort();
    }

    public String getNodeHost() {
        return nodeClient.getHost();
    }

    public int getNodePort() {
        return nodeClient.getPort();
    }

    private void updateCaches() {
        shardToServer.putAll(masterClient.getShardToServerMap());
        hashToShard.putAll(masterClient.getHashToShardMap());
    }

    //TODO Ultraslow, store sorted and do a binary search
    private ShardOnServer getShardAndServerForKey(String key) {
        long hash = HashingUtils.calculate64BitHash(key);

        List<Long> sortedHashes = hashToShard.asMap().keySet().stream()
            .sorted()
            .toList();

        long previous = Long.MIN_VALUE;
        long next = Long.MAX_VALUE;
        Integer shard = null;

        for (Long upperHash : sortedHashes) {
            if (hash <= upperHash) {
                next = upperHash;
                shard = hashToShard.getIfPresent(upperHash);
                break;
            }

            previous = upperHash;
        }

        // Last shard must be marked as MAX_VALUE
        if (next == Long.MAX_VALUE) {
            shard = hashToShard.getIfPresent(next);
        }

        // If no previous key is found return shard from next key
        if (previous == Long.MIN_VALUE) {
            shard = hashToShard.getIfPresent(next);
        }

        if (shard == null) {
            throw new IllegalStateException("Shard not found for hash: " + hash);
        }

        var server = shardToServer.getIfPresent(shard);

        return new ShardOnServer(server, shard);
    }

    public MasterHeartbeatResponseDTO sendMasterHeartbeat() {
        return masterClient.sendHeartbeat();
    }

    public NodeHeartbeatResponseDTO sendNodeHeartbeat() {
        return nodeClient.sendHeartbeat();
    }

    //TODO: Remove later
    public void scheduleHeartbeat() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);

        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.scheduleAtFixedRate(() -> {
                log.info("Heartbeat to {}:{} sent at {}", masterClient.getHost(), masterClient.getPort(), Instant.now());

                var masterResponse = masterClient.sendHeartbeat();

                log.info("Heartbeat to {}:{} returned. Healthy: {}, Timestamp: {} ",
                    masterClient.getHost(),
                    masterClient.getPort(),
                    masterResponse.healthy(),
                    Instant.ofEpochMilli(masterResponse.serverTimestamp()));

                log.info("Heartbeat to {}:{} sent at {}", nodeClient.getHost(), nodeClient.getPort(), Instant.now());

                var nodeResponse = nodeClient.sendHeartbeat();

                log.info("Heartbeat to {}:{} returned. Healthy: {}, Timestamp: {} ",
                    nodeClient.getHost(),
                    nodeClient.getPort(),
                    nodeResponse.healthy(),
                    Instant.ofEpochMilli(nodeResponse.serverTimestamp()));

                latch.countDown();
            }, 0, 1, TimeUnit.SECONDS);

            latch.await();
        }

        masterClient.addServer("0.0.0.0", 9090, true);
        masterClient.addServer("0.0.0.1", 9091, true);

        log.info(String.valueOf(masterClient.getHashToShardMap()));
        log.info(String.valueOf(masterClient.getShardToServerMap()));
    }

    /**
     * Get the current shard-to-server mapping as a Map
     *
     * @return Map from shard ID to server address (ip:port)
     */
    public Map<Integer, String> getShardServerMapping() {
        return shardToServer.asMap();
    }

    /**
     * Get cached hash-to-shard mapping as a Map
     *
     * @return Map of pairs (Max available hash value -> Shard number)
     */
    public Map<Long, Integer> getHashToShardMapping() {
        return hashToShard.asMap();
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
