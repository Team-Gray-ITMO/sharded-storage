package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import vk.itmo.teamgray.sharded.storage.dto.AddServerResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.DeleteServerResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.GetTopologyResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.MasterHeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.master.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.master.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.master.GetTopologyRequest;
import vk.itmo.teamgray.sharded.storage.master.MasterHeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.master.ShardedStorageMasterServiceGrpc;

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

    public AddServerResponseDTO addServer(String ip, int port, boolean forkNewInstance) {
        AddServerRequest request = AddServerRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .setForkNewInstance(forkNewInstance)
            .build();

        var response = blockingStub.addServer(request);
        return new AddServerResponseDTO(response.getMessage(), response.getSuccess());
    }

    public DeleteServerResponseDTO deleteServer(String ip, int port) {
        DeleteServerRequest request = DeleteServerRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .build();

        var response = blockingStub.deleteServer(request);
        return new DeleteServerResponseDTO(response.getMessage(), response.getSuccess());
    }

    public MasterHeartbeatResponseDTO sendHeartbeat() {
        var response = blockingStub
            .heartbeat(
                MasterHeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
        return new MasterHeartbeatResponseDTO(
            response.getHealthy(),
            response.getServerTimestamp(),
            response.getStatusMessage()
        );
    }

    public GetTopologyResponseDTO getTopology() {
        GetTopologyRequest request = GetTopologyRequest.newBuilder().build();
        var response = blockingStub.getTopology(request);
        return new GetTopologyResponseDTO(response.getShardToServerMap(), response.getTotalShardCount());
    }
    
    /**
     * Get the current shard-to-server mapping as a Map
     * @return Map from shard ID to server address (ip:port)
     */
    public Map<Integer, String> getShardServerMapping() {
        GetTopologyResponseDTO response = getTopology();
        return response.shardToServer();
    }
    
    /**
     * Get the total shard count
     * @return the total number of shards
     */
    public int getTotalShardCount() {
        GetTopologyResponseDTO response = getTopology();
        return response.totalShardCount();
    }
}
