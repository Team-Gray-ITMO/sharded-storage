package vk.itmo.teamgray.sharded.storage.client.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.client.exception.ClientException;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.GetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;

import static vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils.getShardIdForKey;

public class ClientService {
    public static final int RETRIES = 3;

    private static final Logger log = LoggerFactory.getLogger(ClientService.class);

    private static final Duration CACHE_EXPIRATION = Duration.of(30, ChronoUnit.MINUTES);

    private final MasterClient masterClient;

    private final DiscoveryClient discoveryClient;

    private final ClientCachingFactory clientCachingFactory;

    private final FileReaderProvider fileReaderProvider;

    private TopologyCache topologyCache;

    public ClientService(
        MasterClient masterClient,
        DiscoveryClient discoveryClient,
        ClientCachingFactory clientCachingFactory,
        FileReaderProvider fileReaderProvider
    ) {
        this.masterClient = masterClient;
        this.discoveryClient = discoveryClient;
        this.clientCachingFactory = clientCachingFactory;
        this.fileReaderProvider = fileReaderProvider;

        updateCaches();
    }

    /**
     * Gets value by key from node
     *
     * @return value by key
     */
    public String getValue(String key) {
        NodeClient nodeClient = getNodeClient(key);

        for (int i = 0; i < RETRIES; i++) {
            GetResponseDTO result = nodeClient.getKey(key);

            switch (result.status()) {
                case ERROR -> throw new ClientException(result.value());
                case WRONG_NODE -> {
                    log.debug("Wrong node. Refreshing caches and retrying for key: {}", key);

                    updateCaches();

                    nodeClient = getNodeClient(key);
                }
                case SUCCESS -> {
                    return result.value();
                }
                case null, default -> throw new IllegalStateException("Unexpected value: " + result.status());
            }
        }

        throw new ClientException("Wrong node. Retries exhausted.");
    }

    /**
     * Puts value by key
     *
     * @return Returns success of operation
     */
    public boolean setValue(String key, String value) {
        NodeClient nodeClient = getNodeClient(key);

        for (int i = 0; i < RETRIES; i++) {
            Instant timestamp = Instant.now();

            SetResponseDTO result = nodeClient.setKey(key, value, timestamp);

            switch (result.status()) {
                // If node is resharding or moving shards and the key is within the moved ones, node will respond with new node ID to retry sending pair to.
                // Also node will add the pair to Rollback queue, that will be applied in case of a rollback.
                case TRANSFER -> {
                    var newServer = topologyCache.getServerById(result.newNodeId());

                    log.debug(
                        "Node {}:{} is transferring key {} to node {}. Retrying on destination node.",
                        nodeClient.getHost(),
                        nodeClient.getPort(),
                        newServer.id(),
                        key
                    );

                    nodeClient = clientCachingFactory
                        .getClient(
                            newServer,
                            NodeClient.class
                        );
                }
                // QUEUED means that node has added a pair to the queue, that will be applied at the end of successful resharding/shard moving.
                case QUEUED, SUCCESS -> {
                    return true;
                }
                case ERROR -> {
                    return false;
                }
                case null, default -> throw new IllegalStateException("Unexpected value: " + result.status());
            }
        }

        throw new ClientException("Retries exhausted.");
    }

    /**
     * Puts value by key pairs from file
     *
     * @param filePath path to file where to get pairs to save
     *
     * @return result of set operation
     */
    public StatusResponseDTO setFromFile(String filePath) {
        try (BufferedReader reader = fileReaderProvider.getReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");

                String key = parts[0].trim();
                String value = parts[1].trim();

                try {
                    var nodeClient = getNodeClient(key);

                    //TODO Think about applying retry logic here also somehow.
                    nodeClient.setKey(key, value, Instant.now());
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

    private NodeClient getNodeClient(String key) {
        if (topologyCache == null || topologyCache.getLastUpdate().isBefore(Instant.now().minus(CACHE_EXPIRATION))) {
            updateCaches();
        }

        var shardId = getShardIdForKey(key, topologyCache.getShardCount());

        var server = topologyCache.getServerByShardId(shardId);

        if (server == null) {
            updateCaches();

            server = topologyCache.getServerByShardId(shardId);
        }

        if (topologyCache.getShardCount() == 0) {
            throw new IllegalStateException("No shards are created yet.");
        }

        if (server == null) {
            throw new IllegalStateException("Could not find server for key: " + key + " in shard " + shardId);
        }

        log.debug("Found shard {} for key '{}' in server {} ", shardId, key, server.id());

        return clientCachingFactory
            .getClient(
                server,
                NodeClient.class
            );
    }

    public void updateCaches() {
        var shardToServerId = masterClient.getShardToServerMap();

        topologyCache = new TopologyCache(
            shardToServerId,
            discoveryClient.getNodeMapWithRetries(new HashSet<>(shardToServerId.values())),
            masterClient.getHashToShardMap()
        );
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
        return topologyCache.getShardToServer();
    }

    /**
     * Get cached hash-to-shard mapping as a Map
     *
     * @return Map of pairs (Max available hash value -> Shard number)
     */
    public Map<Long, Integer> getHashToShardMapping() {
        return topologyCache.getHashToShard();
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
        return topologyCache.getShardCount();
    }
}
