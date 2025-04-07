package vk.itmo.teamgray.sharded.storage.node.client;

import vk.itmo.teamgray.sharded.storage.common.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.client.shards.ShardData;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeStorageService {

    private Map<Integer, ShardData> shards = new ConcurrentHashMap<>();

    public void setKeyValue(String key, String value) {
        shards.computeIfAbsent(
                ShardUtils.getLocalShardKey(key, shards.size()),
                k -> new ShardData()
        ).addToStorage(key, value);
    }

    public String getValueByKey(String key) {
        ShardData shardData = shards.get(ShardUtils.getLocalShardKey(key, shards.size()));
        String returnValue = null;
        if (shardData != null) {
            returnValue = shardData.getValue(key);
        }
        return returnValue;
    }

    public Map<Integer, ShardData> getShards() {
        return shards;
    }

    public void updateStorage(Map<Integer, ShardData> newStorage) {
        shards.clear();
        shards.putAll(newStorage);
    }

}
