package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import vk.itmo.teamgray.sharded.storage.master.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.master.AddServerResponse;
import vk.itmo.teamgray.sharded.storage.master.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.master.DeleteServerResponse;
import vk.itmo.teamgray.sharded.storage.master.GetTopologyRequest;
import vk.itmo.teamgray.sharded.storage.master.GetTopologyResponse;
import vk.itmo.teamgray.sharded.storage.master.MasterHeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.master.MasterHeartbeatResponse;
import vk.itmo.teamgray.sharded.storage.master.ShardedStorageMasterServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.node.ChangeShardCountResponse;

public class ShardedStorageMasterClient {
    private final ManagedChannel channel;

    private final ShardedStorageMasterServiceGrpc.ShardedStorageMasterServiceBlockingStub blockingStub;

    private final String host;

    private final int port;

    public ShardedStorageMasterClient(String host, int port) {
        this.host = host;
        this.port = port;

        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        this.blockingStub = ShardedStorageMasterServiceGrpc.newBlockingStub(channel);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    //TODO Return POJO class instead of gRPC response
    public AddServerResponse addServer(String ip, int port, boolean forkNewInstance) {
        AddServerRequest request = AddServerRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .setForkNewInstance(forkNewInstance)
            .build();

        return blockingStub.addServer(request);
    }

    //TODO Return POJO class instead of gRPC response
    public DeleteServerResponse deleteServer(String ip, int port) {
        DeleteServerRequest request = DeleteServerRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .build();

        return blockingStub.deleteServer(request);
    }

    //TODO Return POJO class instead of gRPC response
    public MasterHeartbeatResponse sendHeartbeat() {
        return blockingStub
            .heartbeat(
                MasterHeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
    }

    //TODO Return POJO class instead of gRPC response
    public GetTopologyResponse getTopology() {
        GetTopologyRequest request = GetTopologyRequest.newBuilder().build();
        return blockingStub.getTopology(request);
    }
    
    /**
     * Get the current shard-to-server mapping as a Map
     * @return Map from shard ID to server address (ip:port)
     */
    public Map<Integer, String> getShardServerMapping() {
        GetTopologyResponse response = getTopology();
        return response.getShardToServerMap();
    }
    
    /**
     * Get the total shard count
     * @return the total number of shards
     */
    public int getTotalShardCount() {
        GetTopologyResponse response = getTopology();
        return response.getTotalShardCount();
    }
}
