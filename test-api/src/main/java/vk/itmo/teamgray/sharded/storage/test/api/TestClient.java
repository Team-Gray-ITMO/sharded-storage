package vk.itmo.teamgray.sharded.storage.test.api;

import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.test.api.client.FailpointClient;

/**
 * Test Client is a facade for two Node clients:
 * - {@link TestClient#getNodeClient()} can perform uncached operations directly on nodes, avoiding topology checks.
 * - {@link TestClient#getFailpointClient()} can perform failing or freezing and unfreezing of the specified methods on the node.
 */
public class TestClient {
    private final DiscoverableServiceDTO node;

    private final NodeClient nodeClient;

    private final FailpointClient failpointClient;

    public TestClient(
        DiscoverableServiceDTO node,
        NodeClient nodeClient,
        FailpointClient failpointClient
    ) {
        this.node = node;
        this.nodeClient = nodeClient;
        this.failpointClient = failpointClient;
    }

    public DiscoverableServiceDTO getNode() {
        return node;
    }

    public NodeClient getNodeClient() {
        return nodeClient;
    }

    public FailpointClient getFailpointClient() {
        return failpointClient;
    }
}
