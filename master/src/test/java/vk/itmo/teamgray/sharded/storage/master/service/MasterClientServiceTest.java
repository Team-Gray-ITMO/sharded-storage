package vk.itmo.teamgray.sharded.storage.master.service;

import java.util.List;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.MapResponseWriter;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.master.service.topology.TopologyService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MasterClientServiceTest {
    private final TopologyService topologyService = mock(TopologyService.class);

    private final MasterClientService service = new MasterClientService(topologyService);

    @Test
    void fillServerToShardsDelegatesToTopologyService() {
        MapResponseWriter<Integer, List<Integer>> writer = mock();

        service.fillServerToShards(writer);

        verify(topologyService).fillServerToShardsInSync(writer);
    }

    @Test
    void fillServerToStateDelegatesToTopologyService() {
        MapResponseWriter<Integer, NodeState> writer = mock();

        service.fillServerToState(writer);

        verify(topologyService).fillServerToState(writer);
    }

    @Test
    void fillShardToHashDelegatesToTopologyService() {
        MapResponseWriter<Integer, Long> writer = mock();

        service.fillShardToHash(writer);

        verify(topologyService).fillShardToHashInSync(writer);
    }

    @Test
    void addServerDelegatesToTopologyService() {
        StatusResponseWriter writer = mock();

        service.addServer(1, writer);

        verify(topologyService).addServer(1, writer);
    }

    @Test
    void deleteServerDelegatesToTopologyService() {
        StatusResponseWriter writer = mock();

        service.deleteServer(1, writer);

        verify(topologyService).deleteServer(1, writer);
    }

    @Test
    void changeShardCountDelegatesToTopologyService() {
        StatusResponseWriter writer = mock();

        service.changeShardCount(5, writer);

        verify(topologyService).changeShardCount(5, writer);
    }
}
