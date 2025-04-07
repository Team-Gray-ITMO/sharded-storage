package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ShardUtils {

    public static int getLocalShardKey(@NotNull String key, int shardCount){
        return shardCount == 0 ? 0 : hashKey(key) % shardCount;
    }

    public static int hashKey(@NotNull String key){
        Objects.requireNonNull(key);

        return key.hashCode();
    }
}
