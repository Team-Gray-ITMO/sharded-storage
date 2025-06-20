package vk.itmo.teamgray.sharded.storage.master.service;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.MapResponseWriter;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.master.service.topology.TopologyService;

public class MasterClientService {
    private static final Logger log = LoggerFactory.getLogger(MasterClientService.class);

    private final TopologyService topologyService;

    public MasterClientService(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    public void fillServerToShards(MapResponseWriter<Integer, List<Integer>> responseWriter) {
        log.info("Received ServerToShard request");

        var size = topologyService.fillServerToShardsInSync(responseWriter);

        log.info("Returning {} servers with shards", size);
    }

    public void fillServerToState(MapResponseWriter<Integer, NodeState> responseWriter) {
        log.info("Received ServerToState request");

        var size = topologyService.fillServerToState(responseWriter);

        log.info("Returning {} servers with states", size);
    }

    public void fillShardToHash(MapResponseWriter<Integer, Long> responseWriter) {
        log.info("Received ShardToHash request");

        var size = topologyService.fillShardToHashInSync(responseWriter);

        log.info("Returning {} shards with hashes", size);
    }

    public void addServer(int serverId, StatusResponseWriter responseWriter) {
        log.info("Adding server {}", serverId);

        var success = topologyService.addServer(serverId, responseWriter);

        if (success) {
            log.info("Added server {}", serverId);
        }
    }

    public void deleteServer(int serverId, StatusResponseWriter responseWriter) {
        log.info("Removing server {}", serverId);

        var success = topologyService.deleteServer(serverId, responseWriter);

        if (success) {
            log.info("Removed server {}", serverId);
        }
    }

    public void changeShardCount(int shardCount, StatusResponseWriter responseWriter) {
        log.info("Changing entries count to {}", shardCount);

        topologyService.changeShardCount(shardCount, responseWriter);

        log.info("Changed entries count successfully");
    }
}
