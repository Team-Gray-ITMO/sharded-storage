package vk.itmo.teamgray.sharded.storage.node.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.client.shards.ShardData;

public class NodeStorageService {
    private static final Logger log = LoggerFactory.getLogger(NodeStorageService.class);

    private Map<Integer, ShardData> shards = new ConcurrentHashMap<>();

    private int fullShardCount;

    public void set(String key, String value) {
        var shardId = getAndValidateShardId(key);

        //TODO Change to debug
        log.info("Setting key {} on shard {} to {}", key, shardId, value);

        shards
            .computeIfAbsent(shardId, k -> new ShardData())
            .addToStorage(key, value);
    }

    public String get(String key) {
        var shardId = getAndValidateShardId(key);

        //TODO Change to debug
        log.info("Getting value for key {} on shard {}", key, shardId);

        ShardData shardData = shards.get(shardId);
        String returnValue = null;

        if (shardData != null) {
            returnValue = shardData.getValue(key);
        }

        return returnValue;
    }

    private Integer getAndValidateShardId(String key) {
        var shardId = ShardUtils.getShardIdForKey(key, fullShardCount);

        if (shardId == null) {
            throw new NodeException("No shard found for key: " + key);
        }

        if (!shards.containsKey(shardId)) {
            throw new NodeException("Shard " + shardId + " is not found on this node. Existing shards: " + shards.keySet());
        }

        return shardId;
    }

    public Map<Integer, ShardData> getShards() {
        return shards;
    }

    public void removeShard(int shardId) {
        shards.remove(shardId);
        log.info("Shard {} removed", shardId);
    }

    public boolean containsShard(int shardId) {
        return shards.containsKey(shardId);
    }

    public void addNewShard(int shardId) {
        if (shards.containsKey(shardId)) {
            throw new NodeException("Shard already exists for id: " + shardId);
        }

        shards.put(shardId, new ShardData());
    }

    //TODO Add locks?
    public void replace(Map<Integer, ShardData> newStorage, int fullShardCount) {
        log.info("Replacing shard scheme {}", newStorage);

        shards.clear();
        shards.putAll(newStorage);

        this.fullShardCount = fullShardCount;

        log.info("Replaced shard scheme.");
    }

    public void checkKeyForShard(int shardId, String key) {
        Integer shardIdForKey = ShardUtils.getShardIdForKey(key, fullShardCount);
        if (shardIdForKey == null || shardIdForKey.compareTo(shardId) != 0) {
            throw new NodeException("Incorrect shard for key: " + key);
        }
    }

    public int getFullShardCount() {
        return fullShardCount;
    }
}
