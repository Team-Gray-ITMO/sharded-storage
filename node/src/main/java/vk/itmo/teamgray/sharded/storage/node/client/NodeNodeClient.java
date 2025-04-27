package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import vk.itmo.teamgray.sharded.storage.node.node.NodeNodeServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardFragmentRequest;
import vk.itmo.teamgray.sharded.storage.node.node.SendShardRequest;

public class NodeNodeClient {

    private final ManagedChannel channel;

    private final NodeNodeServiceGrpc.NodeNodeServiceBlockingStub blockingStub;

    private final String host;

    private final int port;

    public NodeNodeClient(String host, int port) {
        this.host = host;
        this.port = port;

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        this.blockingStub = NodeNodeServiceGrpc.newBlockingStub(channel);
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

    public boolean sendShard(int shardId, Map<String, String> shard) {
        SendShardRequest request = SendShardRequest.newBuilder()
                .setShardId(shardId)
                .putAllShard(shard)
                .build();

        return blockingStub.sendShard(request).getSuccess();
    }

    public boolean sendShardFragment(int shardId, Map<String, String> fragmentsToSend) {
        SendShardFragmentRequest request = SendShardFragmentRequest.newBuilder()
            .setShardId(shardId)
            .putAllShardFragments(fragmentsToSend)
            .build();

        return blockingStub.sendShardFragment(request).getSuccess();
    }
}
