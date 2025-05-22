package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

public class ShardsContainer {
    private static final Logger log = LoggerFactory.getLogger(ShardsContainer.class);

    private final Map<Integer, ShardData> shardMap;

    private final int fullShardCount;

    public ShardsContainer(int fullShardCount) {
        this.shardMap = new ConcurrentHashMap<>();
        this.fullShardCount = fullShardCount;
    }

    public ShardsContainer(Map<Integer, ShardData> shardMap, int fullShardCount) {
        this.shardMap = shardMap;
        this.fullShardCount = fullShardCount;
    }

    public Map<Integer, ShardData> getShardMap() {
        return shardMap;
    }

    public int getFullShardCount() {
        return fullShardCount;
    }

    public void set(String key, String value) {
        var shardId = getAndValidateShardId(key);

        shardMap
            .computeIfAbsent(shardId, k -> new ShardData())
            .addToStorage(key, value);
    }

    public String get(String key) {
        var shardId = getAndValidateShardId(key);

        ShardData shardData = shardMap.get(shardId);

        String returnValue = null;

        if (shardData != null) {
            returnValue = shardData.getValue(key);
        }

        return returnValue;
    }

    public void removeShard(int shardId) {
        shardMap.remove(shardId);

        log.info("Shard {} removed", shardId);
    }

    public boolean containsShard(int shardId) {
        return shardMap.containsKey(shardId);
    }

    public void checkKeyForShard(int shardId, String key) {
        Integer shardIdForKey = ShardUtils.getShardIdForKey(key, fullShardCount);

        if (shardIdForKey == null || shardIdForKey.compareTo(shardId) != 0) {
            throw new NodeException("Incorrect shard for key: " + key);
        }
    }

    private Integer getAndValidateShardId(String key) {
        var shardId = ShardUtils.getShardIdForKey(key, fullShardCount);

        if (shardId == null) {
            throw new NodeException("No shard found for key: " + key);
        }

        if (!shardMap.containsKey(shardId)) {
            throw new NodeException("Shard " + shardId + " is not found on this node. Existing shards: " + shardMap.keySet());
        }

        return shardId;
    }
}
