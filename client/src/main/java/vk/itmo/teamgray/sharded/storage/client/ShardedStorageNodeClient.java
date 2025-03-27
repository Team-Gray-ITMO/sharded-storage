package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import vk.itmo.teamgray.sharded.storage.node.*;

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

    //TODO add getKey

    //TODO Return POJO class instead of gRPC response
    public SetFromFileResponse setFromFile(String filePath) {
        SetFromFileRequest request = SetFromFileRequest.newBuilder()
            .setFilePath(filePath)
            .build();

        return blockingStub.setFromFile(request);
    }

    //TODO Return POJO class instead of gRPC response
    public NodeHeartbeatResponse sendHeartbeat() {
        return blockingStub
            .heartbeat(
                NodeHeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
    }

    //TODO Return POJO class instead of gRPC response
    public ChangeShardCountResponse changeShardCount(int newShardCount) {
        ChangeShardCountRequest request = ChangeShardCountRequest.newBuilder()
                .setNewShardCount(newShardCount)
                .build();

        return blockingStub.changeShardCount(request);
    }
}
