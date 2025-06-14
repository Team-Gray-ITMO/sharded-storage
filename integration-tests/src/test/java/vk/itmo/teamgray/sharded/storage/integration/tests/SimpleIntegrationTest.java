package vk.itmo.teamgray.sharded.storage.integration.tests;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        orchestrationApi.runNode(1);

        clientService.addServer(1, false);

        Map<Integer, NodeState> serverStates = clientService.getServerStates();

        assertNotNull(serverStates);
    }
}
