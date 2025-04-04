package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ShardUtils {
    public static int hashKey(@NotNull String key){
        Objects.requireNonNull(key);

        return key.hashCode();
    }

    public static int getLocalShardKey(@NotNull String key, int shardCount){
        Objects.requireNonNull(key);

        return key.hashCode() % shardCount;
    }
}
