package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.exception.ShardNotExistsException;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

public class ShardsContainer {
    private static final Logger log = LoggerFactory.getLogger(ShardsContainer.class);

    private final Map<Integer, ShardData> shardMap;

    private int fullShardCount;

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

    //TODO Make final later
    public void setFullShardCount(int fullShardCount) {
        this.fullShardCount = fullShardCount;
    }

    public void set(String key, String value) {
        var shardId = getAndValidateShardId(key, false);

        shardMap
            .computeIfAbsent(shardId, k -> new ShardData())
            .addToStorage(key, value);
    }

    public String get(String key) {
        var shardId = getAndValidateShardId(key, true);

        ShardData shardData = shardMap.get(shardId);

        String returnValue = null;

        if (shardData != null) {
            returnValue = shardData.getValue(key);
        }

        return returnValue;
    }

    public void createShard(int shardId) {
        shardMap.put(shardId, new ShardData());

        log.info("Shard {} created", shardId);
    }

    public void clearShard(int shardId) {
        shardMap.get(shardId).clearStorage();

        log.info("Shard {} cleared", shardId);
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

    public boolean hasShardForKey(String key) {
        return shardMap.containsKey(ShardUtils.getShardIdForKey(key, fullShardCount));
    }

    private Integer getAndValidateShardId(String key, boolean checkShardExists) {
        var shardId = ShardUtils.getShardIdForKey(key, fullShardCount);

        if (shardId == null) {
            throw new NodeException("No shard found for key: " + key);
        }

        if (checkShardExists && !shardMap.containsKey(shardId)) {
            throw new ShardNotExistsException("Shard " + shardId + " is not found on this node. Existing shards: " + shardMap.keySet());
        }

        return shardId;
    }
}
