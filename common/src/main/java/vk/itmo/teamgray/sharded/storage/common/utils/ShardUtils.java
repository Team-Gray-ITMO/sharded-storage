package vk.itmo.teamgray.sharded.storage.common.utils;

import java.math.BigInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardUtils {
    private static final Logger log = LoggerFactory.getLogger(ShardUtils.class);

    public static Integer getShardIdForKey(String key, int shardsCount) {
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

    public static Integer getShardIdForHash(long hash, int shardCount) {
        if (shardCount <= 0) {
            return null;
        }

        if (shardCount == 1) {
            //Shard calculation does not work well for 1 shard. Returning first (0) shard
            return 0;
        }

        log.debug("Finding shard for hash: {}", hash);

        BigInteger range = BigInteger
            .valueOf(Long.MAX_VALUE)
            .subtract(BigInteger.valueOf(Long.MIN_VALUE));

        long stepSize = range
            .divide(BigInteger.valueOf(shardCount))
            .longValue();

        long previousBoundary = Long.MIN_VALUE;

        for (int i = 1; i <= shardCount; i++) {
            long hashBoundary = previousBoundary + stepSize;

            if (i == shardCount) {
                hashBoundary = Long.MAX_VALUE;
            }

            if (hashBoundary >= hash && previousBoundary < hash) {
                return i - 1;
            }

            previousBoundary = hashBoundary;
        }

        //Due to truncation on division, loop on top may not reach the end, so we are putting last shard here also.
        return shardCount - 1;

        //TODO Dividing is not working properly, find more efficient approach than checking one-by-one later
        //long shardId = hash / step;

        //if (shardId < 0 || shardId > shardCount) {
        //    throw new NodeException("Invalid shard id: " + shardId);
        //}

        //return (int) shardId;
    }
}
