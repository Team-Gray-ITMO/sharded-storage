package vk.itmo.teamgray.sharded.storage.test.api;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import vk.itmo.teamgray.sharded.storage.test.api.console.TestOrchestrationApi;

public abstract class BaseOrchestratedTest {
    protected static TestOrchestrationApi orchestrationApi = new TestOrchestrationApi();

    @BeforeAll
    public static void build() {
        orchestrationApi.buildProject();
    }

    @AfterAll
    public static void clean() {
        orchestrationApi.purge();
    }

    @BeforeEach
    public void setUp() {
        orchestrationApi.purge();
    }

    @AfterEach
    public void tearDown() {
        // No-op.
    }
}
