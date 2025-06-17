package vk.itmo.teamgray.sharded.storage.test.api;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.client.proto.MasterGrpcClient;
import vk.itmo.teamgray.sharded.storage.client.proto.NodeGrpcClient;
import vk.itmo.teamgray.sharded.storage.client.service.ClientService;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.proto.DiscoveryGrpcClient;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.test.api.client.FailpointClient;
import vk.itmo.teamgray.sharded.storage.test.api.client.proto.FailpointGrpcClient;

import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getDiscoverableService;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerHost;
import static vk.itmo.teamgray.sharded.storage.common.utils.PropertyUtils.getServerPort;

public abstract class BaseIntegrationTest extends BaseOrchestratedTest {
    private static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

    /**
     * Provides an instance of {@link ClientService} used for managing client interactions in integration tests.
     * Can be used to test correct flow of things, as this includes all the caching steps and client logic.
     */
    protected ClientService clientService;

    protected DiscoveryClient discoveryClient;

    protected ClientCachingFactory clientCachingFactory = ClientCachingFactory.getInstance();

    private Map<Integer, TestClient> testClientCache = new HashMap<>();

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();

        orchestrationApi.runDiscovery();
        orchestrationApi.runMaster();

        clientCachingFactory.registerClientCreator(DiscoveryClient.class, DiscoveryGrpcClient::new);
        clientCachingFactory.registerClientCreator(MasterClient.class, MasterGrpcClient::new);
        clientCachingFactory.registerClientCreator(NodeClient.class, NodeGrpcClient::new);
        clientCachingFactory.registerClientCreator(FailpointClient.class, FailpointGrpcClient::new);

        discoveryClient = clientCachingFactory.getClient(
            getServerHost("discovery"),
            getServerPort("discovery"),
            DiscoveryClient.class
        );

        //TODO Later register individual clients
        discoveryClient.register(getDiscoverableService());

        MasterClient masterClient = clientCachingFactory
            .getClient(
                discoveryClient.getMasterWithRetries(),
                MasterClient.class
            );

        clientService = new ClientService(masterClient, discoveryClient, clientCachingFactory);
    }

    /**
     * Retrieves or creates a {@link TestClient} instance for the given server ID.
     * Has two components:
     * - {@link TestClient#getNodeClient()} can perform uncached operations directly on nodes, avoiding topology checks.
     * - {@link TestClient#getFailpointClient()} can perform failing or freezing and unfreezing of the specified methods on the node.
     */
    protected TestClient getTestClient(int serverId) {
        return testClientCache.computeIfAbsent(serverId, key -> {
            var server = discoveryClient.getNode(serverId);

            if (server == null) {
                throw new IllegalStateException("Server with id " + serverId + " not found.");
            }

            var nodeClient = clientCachingFactory.getClient(server, NodeClient.class);

            if (nodeClient == null) {
                throw new IllegalStateException("Client for server with id " + serverId + " not found.");
            }

            var failpointClient = clientCachingFactory.getClient(server, FailpointClient.class);

            if (failpointClient == null) {
                throw new IllegalStateException("Failpoint Client for server with id " + serverId + " not found.");
            }

            return new TestClient(server, nodeClient, failpointClient);
        });
    }

    @AfterEach
    @Override
    public void tearDown() {
        log.info("Cleaning up test data and failpoints.");

        var clearErrors = testClientCache.values().stream()
            .map(TestClient::getFailpointClient)
            .map(FailpointClient::clear)
            .filter(it -> !it.isSuccess())
            .toList();

        if (!clearErrors.isEmpty()) {
            throw new AssertionError(
                "Failed to clear failpoints." +
                    clearErrors.stream()
                        .map(StatusResponseDTO::getMessage)
                        .collect(Collectors.joining(System.lineSeparator(), System.lineSeparator(), ""))
            );
        }

        clientCachingFactory.clear();
        testClientCache.clear();

        super.tearDown();
    }
}
