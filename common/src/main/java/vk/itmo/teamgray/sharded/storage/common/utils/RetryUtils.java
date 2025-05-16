package vk.itmo.teamgray.sharded.storage.common.utils;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

public class RetryUtils {
    private RetryUtils() {
        // No-op.
    }

    public static <T> T retryWithAttempts(
        int attempts,
        Duration delay,
        Supplier<Optional<T>> supplier,
        String errorMessage
    ) {
        for (int i = 0; i < attempts; i++) {
            Optional<T> result = supplier.get();

            if (result.isPresent()) {
                return result.get();
            }

            if (i < attempts - 1) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry delay", e);
                }
            }
        }

        throw new IllegalStateException(errorMessage);
    }
}
