package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import vk.itmo.teamgray.sharded.storage.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.AddServerResponse;
import vk.itmo.teamgray.sharded.storage.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.ChangeShardCountResponse;
import vk.itmo.teamgray.sharded.storage.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.DeleteServerResponse;
import vk.itmo.teamgray.sharded.storage.HeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.HeartbeatResponse;
import vk.itmo.teamgray.sharded.storage.SetFromFileRequest;
import vk.itmo.teamgray.sharded.storage.SetFromFileResponse;
import vk.itmo.teamgray.sharded.storage.SetKeyRequest;
import vk.itmo.teamgray.sharded.storage.ShardedStorageGrpc;

public class ShardedStorageClient {
    private final ManagedChannel channel;

    private final ShardedStorageGrpc.ShardedStorageBlockingStub blockingStub;

    public ShardedStorageClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        this.blockingStub = ShardedStorageGrpc.newBlockingStub(channel);
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
    public ChangeShardCountResponse changeShardCount(int newShardCount) {
        ChangeShardCountRequest request = ChangeShardCountRequest.newBuilder()
            .setNewShardCount(newShardCount)
            .build();

        return blockingStub.changeShardCount(request);
    }

    public boolean setKey(String key, String value) {
        SetKeyRequest request = SetKeyRequest.newBuilder()
            .setKey(key)
            .setValue(value)
            .build();

        return blockingStub.setKey(request).getSuccess();
    }

    //TODO Return POJO class instead of gRPC response
    public SetFromFileResponse setFromFile(String filePath) {
        SetFromFileRequest request = SetFromFileRequest.newBuilder()
            .setFilePath(filePath)
            .build();

        return blockingStub.setFromFile(request);
    }

    //TODO Return POJO class instead of gRPC response
    public HeartbeatResponse sendHeartbeat() {
        return blockingStub
            .heartbeat(
                HeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
    }
}
