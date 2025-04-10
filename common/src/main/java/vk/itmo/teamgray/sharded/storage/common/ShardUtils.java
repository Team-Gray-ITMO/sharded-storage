package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ShardUtils {
    public static int hashKey(@NotNull String key){
        Objects.requireNonNull(key);

        return key.hashCode();
    }

    public static long getLocalShardKey(@NotNull String key, int shardCount){
        Objects.requireNonNull(key);

        return HashingUtils.calculate64BitHash(key);
    }
}
