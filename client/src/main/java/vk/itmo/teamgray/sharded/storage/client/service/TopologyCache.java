package vk.itmo.teamgray.sharded.storage.client.service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;

public class TopologyCache {
    private final int serverCount;

    private final int shardCount;

    private final Map<Integer, DiscoverableServiceDTO> shardToServer;

    private final Map<Long, Integer> hashToShard;

    private final Map<Integer, DiscoverableServiceDTO> serverById;

    private final Instant lastUpdate;

    public TopologyCache(
        Map<Integer, Integer> shardToServerId,
        Map<Integer, DiscoverableServiceDTO> serverById,
        Map<Long, Integer> hashToShard
    ) {
        this.shardToServer = new HashMap<>();

        shardToServerId.forEach((shardId, serverId) -> shardToServer.put(shardId, serverById.get(serverId)));

        this.hashToShard = hashToShard;
        this.serverById = serverById;
        this.serverCount = serverById.size();
        this.shardCount = hashToShard.size();
        this.lastUpdate = Instant.now();
    }

    public DiscoverableServiceDTO getServerById(Integer id) {
        if (id == null) {
            return null;
        }

        return serverById.get(id);
    }

    public DiscoverableServiceDTO getServerByShardId(Integer shardId) {
        if (shardId == null) {
            return null;
        }

        return shardToServer.get(shardId);
    }

    public Map<Integer, DiscoverableServiceDTO> getShardToServer() {
        return shardToServer;
    }

    public Map<Long, Integer> getHashToShard() {
        return hashToShard;
    }

    public int getServerCount() {
        return serverCount;
    }

    public int getShardCount() {
        return shardCount;
    }

    public Instant getLastUpdate() {
        return lastUpdate;
    }
}
