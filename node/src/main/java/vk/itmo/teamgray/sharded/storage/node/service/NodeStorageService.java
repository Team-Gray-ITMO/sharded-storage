package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

public class NodeStorageService {
    private static final Logger log = LoggerFactory.getLogger(NodeStorageService.class);

    public enum NodeState {
        ACTIVE,
        FROZEN
    }

    private final AtomicReference<NodeState> nodeState = new AtomicReference<>(NodeState.ACTIVE);
    private final Map<Integer, ShardData> shards = new ConcurrentHashMap<>();
    private final Map<Integer, ShardData> stagedShards = new ConcurrentHashMap<>();
    private int fullShardCount;

    public NodeState getNodeState() {
        return nodeState.get();
    }

    public boolean compareAndSetNodeState(NodeState expect, NodeState update) {
        return nodeState.compareAndSet(expect, update);
    }

    public Map<Integer, ShardData> getShards() {
        return shards;
    }

    public Map<Integer, ShardData> getStagedShards() {
        return stagedShards;
    }

    public int getFullShardCount() {
        return fullShardCount;
    }

    public void setFullShardCount(int fullShardCount) {
        this.fullShardCount = fullShardCount;
    }

    public void checkKeyForShard(int shardId, String key) {
        if (!shards.containsKey(shardId)) {
            throw new NodeException("Shard " + shardId + " not found");
        }
    }

    public boolean containsShard(int shardId) {
        return shards.containsKey(shardId);
    }

    public void set(String key, String value) {
        if (nodeState.get() == NodeState.FROZEN) {
            throw new NodeException("Node is frozen, cannot perform operations");
        }
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

    public void removeShard(int shardId) {
        shards.remove(shardId);
        log.info("Shard {} removed", shardId);
    }

    public void replace(Map<Integer, ShardData> newShards, int newFullShardCount) {
        shards.clear();
        shards.putAll(newShards);
        fullShardCount = newFullShardCount;
    }

    public void clearStagedShards() {
        stagedShards.clear();
    }

    public void swapShards() {
        shards.clear();
        shards.putAll(stagedShards);
        stagedShards.clear();
    }
}
