package vk.itmo.teamgray.sharded.storage.node.shards;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ShardData{
    private ConcurrentHashMap<String, String> storage;

    public ShardData(@NotNull ConcurrentHashMap<String, String> storage) {
        Objects.requireNonNull(storage);
        this.storage = storage;
    }

    public ShardData(){
        storage = new ConcurrentHashMap<>();
    }

    @NotNull
    public Map<String, String> getStorage() {
        return storage;
    }

    public void addToStorage(@NotNull String key, @NotNull String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        storage.put(key, value);
    }

    public void removeFromStorage(@NotNull String key) {
        Objects.requireNonNull(key);
        storage.remove(key);
    }

    public void clearStorage() {
        storage.clear();
    }

    @Nullable
    public String getValue(@NotNull String key) {
        Objects.requireNonNull(key);
        return storage.get(key);
    }
}
