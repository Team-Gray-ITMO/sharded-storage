package vk.itmo.teamgray.sharded.storage.master.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;

public class NodeManagementClient {
    private final ManagedChannel channel;

    private final NodeManagementServiceGrpc.NodeManagementServiceBlockingStub blockingStub;

    private final String host;

    private final int port;

    public NodeManagementClient(String host, int port) {
        this.host = host;
        this.port = port;

        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        this.blockingStub = NodeManagementServiceGrpc.newBlockingStub(channel);
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

    public boolean rearrangeShards(Map<Integer, Long> relevantShardsToHash) {
        RearrangeShardsRequest request = RearrangeShardsRequest.newBuilder()
            .putAllShardToHash(relevantShardsToHash)
            .build();

        return blockingStub.rearrangeShards(request).getSuccess();
    }
}
