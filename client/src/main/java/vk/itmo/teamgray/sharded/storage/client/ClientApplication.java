package vk.itmo.teamgray.sharded.storage.client;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static vk.itmo.teamgray.sharded.storage.common.Utils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.Utils.getServerPort;

public class ClientApplication {
    private static final Logger log = LoggerFactory.getLogger(ClientApplication.class);

    public static void main(String[] args) throws InterruptedException {
        ShardedStorageClient client = new ShardedStorageClient(getServerHost(), getServerPort());

        //TODO: Test logic to check gRPC, later remove
        scheduleHeartbeat(client);
    }

    private static void scheduleHeartbeat(ShardedStorageClient client) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);

        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.scheduleAtFixedRate(() -> {
                sendHeartbeat(client);
                latch.countDown();
            }, 0, 1, TimeUnit.SECONDS);

            latch.await();
        }
    }

    private static void sendHeartbeat(ShardedStorageClient client) {
        log.info("Heartbeat sent at {}", Instant.now());

        var response = client.sendHeartbeat();

        log.info("Heartbeat Success. Healthy: {}, Timestamp: {} ", response.getHealthy(),
            Instant.ofEpochMilli(response.getServerTimestamp()));
    }
}
