package vk.itmo.teamgray.sharded.storage.node.service.shards;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import vk.itmo.teamgray.sharded.storage.common.dto.ShardStatsDTO;

public class ShardData {
    private ConcurrentHashMap<String, String> storage;

    public ShardData(ConcurrentHashMap<String, String> storage) {
        Objects.requireNonNull(storage);
        this.storage = storage;
    }

    public ShardData() {
        storage = new ConcurrentHashMap<>();
    }

    public Map<String, String> getStorage() {
        return storage;
    }

    public void addToStorage(String key, String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        storage.put(key, value);
    }

    public void removeFromStorage(String key) {
        Objects.requireNonNull(key);
        storage.remove(key);
    }

    public void clearStorage() {
        storage.clear();
    }

    public ShardStatsDTO getShardStats() {
        var stats = new ShardStatsDTO();

        stats.setSize(storage.size());

        return stats;
    }

    public String getValue(String key) {
        Objects.requireNonNull(key);
        return storage.get(key);
    }

    @Override
    public String toString() {
        return "ShardData{" +
            "storage=" + storage +
            '}';
    }
}
