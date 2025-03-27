package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class HashUtils {
    public static int hashKey(@NotNull String key){
        Objects.requireNonNull(key);

        return key.hashCode();
    }
}
