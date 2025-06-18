package vk.itmo.teamgray.sharded.storage.integration.tests;

import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.test.api.BaseOrchestratedTest;

public class SimpleOrchestrationTest extends BaseOrchestratedTest {
    @Test
    public void test() {
        orchestrationApi.runDiscovery();
        orchestrationApi.runMaster();

        orchestrationApi.runNode(1);
        orchestrationApi.runNode(2);
        orchestrationApi.runNode(3);

        orchestrationApi.stopNode(3);
        orchestrationApi.stopNode(2);
        orchestrationApi.stopNode(1);

        orchestrationApi.stopMaster();
        orchestrationApi.stopDiscovery();
    }
}
