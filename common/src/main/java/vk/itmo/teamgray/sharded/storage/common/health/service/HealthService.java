package vk.itmo.teamgray.sharded.storage.common.health.service;

import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Decoupling example, to allow for potential swap from gRPC if required. For heavy responses, use ResponseWriters instead of intermediary objects.
public class HealthService {
    private static final Logger log = LoggerFactory.getLogger(HealthService.class);

    public void heartbeat(long clientTimestamp, HeartbeatResponseWriter writer) {
        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(clientTimestamp), now);

        boolean isHealthy = true;

        writer.writeResponse(
            isHealthy,
            now.toEpochMilli(),
            "OK"
        );
    }

    // Response writer allows avoiding creating intermediary objects and DTOs, directly populating the final response instead.
    // It might help us regain some perf on heavy responses, such as maps and alike.
    // This one is just for example purposes.
    @FunctionalInterface
    public interface HeartbeatResponseWriter {
        void writeResponse(boolean healthy, long serverTimestamp, String statusMessage);
    }
}
