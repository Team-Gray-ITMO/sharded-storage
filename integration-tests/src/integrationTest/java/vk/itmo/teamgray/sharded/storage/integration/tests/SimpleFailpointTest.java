package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.node.service.NodeStorageService;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;
import vk.itmo.teamgray.sharded.storage.test.api.client.FailpointClient;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleFailpointTest extends BaseIntegrationTest {
    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void simpleFailpointTest() {
        int serverId = 1;
        orchestrationApi.runNode(serverId);

        clientService.addServer(serverId, false);

        var testClient = getTestClient(serverId);

        try (var executor = Executors.newFixedThreadPool(1)) {
            FailpointClient failpointClient = testClient.getFailpointClient();

            failpointClient.freezeFor(NodeStorageService.class, "stageShards", Duration.of(3, ChronoUnit.SECONDS));
            failpointClient.addFailpoint(NodeStorageService.class, "replaceBothMaps", NodeException.class);

            var future = executor.submit(() -> {
                var result = clientService.changeShardCount(2);

                assertFalse(result.isSuccess());
                assertTrue(result.getMessage().contains("Failed to stage shards"));
            });

            failpointClient.awaitFreezeHit(NodeStorageService.class, "stageShards");

            assertSame(NodeState.REARRANGE_SHARDS_PREPARING, testClient.getNodeClient().getNodeStatus().getState());

            future.get();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
