package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import vk.itmo.teamgray.sharded.storage.dto.NodeHeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.SetFromFileResponseDTO;
import vk.itmo.teamgray.sharded.storage.node.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.NodeHeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.node.SetFromFileRequest;
import vk.itmo.teamgray.sharded.storage.node.SetKeyRequest;
import vk.itmo.teamgray.sharded.storage.node.ShardedStorageNodeServiceGrpc;

public class ShardedStorageNodeClient {
    private final ManagedChannel channel;

    private final ShardedStorageNodeServiceGrpc.ShardedStorageNodeServiceBlockingStub blockingStub;

    private final String host;

    private final int port;

    public ShardedStorageNodeClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        this.blockingStub = ShardedStorageNodeServiceGrpc.newBlockingStub(channel);
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

    public boolean setKey(String key, String value) {
        SetKeyRequest request = SetKeyRequest.newBuilder()
            .setKey(key)
            .setValue(value)
            .build();

        return blockingStub.setKey(request).getSuccess();
    }

    public String getKey(String key) {
        GetKeyRequest request = GetKeyRequest.newBuilder()
            .setKey(key)
            .build();

        return blockingStub.getKey(request).getValue();
    }

    public SetFromFileResponseDTO setFromFile(String filePath) {
        SetFromFileRequest request = SetFromFileRequest.newBuilder()
            .setFilePath(filePath)
            .build();

        var response = blockingStub.setFromFile(request);
        return new SetFromFileResponseDTO(response.getMessage(), response.getSuccess());
    }

    public NodeHeartbeatResponseDTO sendHeartbeat() {
        var response = blockingStub
            .heartbeat(
                NodeHeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
        return new NodeHeartbeatResponseDTO(
            response.getHealthy(),
            response.getServerTimestamp(),
            response.getStatusMessage()
        );
    }
}
