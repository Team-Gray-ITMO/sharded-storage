package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ShardUtils {
    public static int getShardIdForKey(@NotNull String key, int shardsCount) {
        if (shardsCount <= 0) {
            return 0;
        }

        long hash = HashingUtils.calculate64BitHash(key);
        long step = Long.MAX_VALUE / shardsCount - Long.MIN_VALUE / shardsCount;
        long shardId = hash / step;
        return (int) shardId;
    }


}
