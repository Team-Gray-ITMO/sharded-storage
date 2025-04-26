package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardUtils {
    private static final Logger log = LoggerFactory.getLogger(ShardUtils.class);

    public static Integer getShardIdForKey(@NotNull String key, int shardsCount) {
        if (shardsCount <= 0) {
            return null;
        }

        if (shardsCount == 1) {
            //Shard calculation does not work well for 1 shard. Returning first (0) shard
            return 0;
        }

        long hash = HashingUtils.calculate64BitHash(key);

        return getShardIdForHash(hash, shardsCount);
    }

    public static Integer getShardIdForHash(long hash, int shardsCount) {
        if (shardsCount <= 0) {
            return null;
        }

        if (shardsCount == 1) {
            //Shard calculation does not work well for 1 shard. Returning first (0) shard
            return 0;
        }

        //TODO Set to debug
        log.info("Finding shard for hash: {}", hash);

        long step = Long.MAX_VALUE / shardsCount * 2;

        long previousBoundary = Long.MIN_VALUE;

        for (int i = 0; i < shardsCount; i++) {
            long hashBoundary = previousBoundary + step;

            if (hashBoundary >= hash && previousBoundary < hash) {
                return i;
            }

            previousBoundary = hashBoundary;
        }

        //Due to truncation on division, loop on top may not reach the end, so we are putting last shard here also.
        return shardsCount - 1;

        //TODO Dividing is not working properly, find more efficient approach than checking one-by-one later
        //long shardId = hash / step;

        //if (shardId < 0 || shardId > shardsCount) {
        //    throw new NodeException("Invalid shard id: " + shardId);
        //}

        //return (int) shardId;
    }
}
