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
        ShardedStorageNodeClient nodeClient = new ShardedStorageNodeClient(getServerHost("node"), getServerPort("node"));
        ShardedStorageMasterClient masterClient = new ShardedStorageMasterClient(getServerHost("master"), getServerPort("master"));

        //TODO: Test logic to check gRPC, later remove
        scheduleHeartbeat(masterClient);
        scheduleHeartbeat(nodeClient);
    }

    //TODO: Remove later
    private static void scheduleHeartbeat(ShardedStorageNodeClient client) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);

        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.scheduleAtFixedRate(() -> {
                log.info("Heartbeat to {}:{} sent at {}", client.getHost(), client.getPort(), Instant.now());

                var response = client.sendHeartbeat();

                log.info("Heartbeat to {}:{} returned. Healthy: {}, Timestamp: {} ",
                    client.getHost(),
                    client.getPort(),
                    response.healthy(),
                    Instant.ofEpochMilli(response.serverTimestamp()));
                latch.countDown();
            }, 0, 1, TimeUnit.SECONDS);

            latch.await();
        }
    }

    //TODO: Remove later
    private static void scheduleHeartbeat(ShardedStorageMasterClient client) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);

        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.scheduleAtFixedRate(() -> {
                log.info("Heartbeat to {}:{} sent at {}", client.getHost(), client.getPort(), Instant.now());

                var response = client.sendHeartbeat();

                log.info("Heartbeat to {}:{} returned. Healthy: {}, Timestamp: {} ",
                    client.getHost(),
                    client.getPort(),
                    response.healthy(),
                    Instant.ofEpochMilli(response.serverTimestamp()));
                latch.countDown();
            }, 0, 1, TimeUnit.SECONDS);

            latch.await();
        }
    }

}
