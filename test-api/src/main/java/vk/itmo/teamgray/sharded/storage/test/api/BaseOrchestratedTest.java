package vk.itmo.teamgray.sharded.storage.test.api;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.test.api.client.FailpointClient;
import vk.itmo.teamgray.sharded.storage.test.api.client.proto.FailpointGrpcClient;
import vk.itmo.teamgray.sharded.storage.test.api.console.TestOrchestrationApi;

public abstract class BaseOrchestratedTest {
    private static final Logger log = LoggerFactory.getLogger(BaseOrchestratedTest.class);

    /**
     * Orchestration API.
     *
     * @see TestOrchestrationApi
     */
    protected static TestOrchestrationApi orchestrationApi = new TestOrchestrationApi();

    @BeforeAll
    public static void build() {
        ClientCachingFactory clientCachingFactory = ClientCachingFactory.getInstance();
        clientCachingFactory.registerClientCreator(FailpointClient.class, FailpointGrpcClient::new);
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
