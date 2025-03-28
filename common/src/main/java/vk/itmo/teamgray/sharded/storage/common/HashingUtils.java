package vk.itmo.teamgray.sharded.storage.common;

import com.google.common.hash.Hashing;

public class HashingUtils {
    private HashingUtils() {
        // No-op.
    }

    public static long calculate64BitHash(String key) {
        return Hashing.murmur3_128().hashUnencodedChars(key).asLong();
    }
}
