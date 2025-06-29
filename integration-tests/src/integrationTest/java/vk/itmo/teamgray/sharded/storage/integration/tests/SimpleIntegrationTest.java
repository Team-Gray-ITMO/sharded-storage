package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

public class SimpleIntegrationTest extends BaseIntegrationTest {
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
    public void simpleIntegrationTest() {
        int serverId = 1;
        orchestrationApi.runNode(serverId);

        clientService.addServer(serverId, false);
        clientService.changeShardCount(1);

        Map<Integer, NodeState> serverStates = clientService.getServerStates();

        assertFalse(serverStates.isEmpty());
        serverStates.forEach((server, state) -> assertSame(NodeState.RUNNING, state));

        var testClient = getTestClient(serverId);

        var nodeStatus = testClient.getNodeClient().getNodeStatus();
        assertSame(NodeState.RUNNING, nodeStatus.getState());
    }
}
