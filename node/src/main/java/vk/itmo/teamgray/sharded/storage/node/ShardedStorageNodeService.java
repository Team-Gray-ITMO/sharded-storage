package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.MessageAndSuccessDTO;
import vk.itmo.teamgray.sharded.storage.common.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.shards.ShardData;

public class ShardedStorageNodeService extends ShardedStorageNodeServiceGrpc.ShardedStorageNodeServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageNodeService.class);

    private Map<Integer, ShardData> shards = new ConcurrentHashMap<>();

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<SetKeyResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();

        shards.computeIfAbsent(
                ShardUtils.getLocalShardKey(key, shards.size()),
                k -> new ShardData()
        ).addToStorage(key, value);

        responseObserver.onNext(
                SetKeyResponse.newBuilder()
                        .setSuccess(true)
                        .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        String key = request.getKey();

        ShardData shardData = shards.get(ShardUtils.getLocalShardKey(key, shards.size()));
        String returnValue = null;
        if (shardData != null) {
            returnValue = shardData.getValue(key);
        }

        responseObserver.onNext(
                GetKeyResponse.newBuilder()
                        .setValue(returnValue)
                        .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void setFromFile(SetFromFileRequest request, StreamObserver<SetFromFileResponse> responseObserver) {
        // TODO: Implement file import logic

        responseObserver.onNext(
                SetFromFileResponse.newBuilder()
                        .setSuccess(true)
                        .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void changeShardCount(ChangeShardCountRequest request, StreamObserver<ChangeShardCountResponse> responseObserver) {
        MessageAndSuccessDTO result = changeShardsCount(request.getNewShardCount());

        responseObserver.onNext(
                ChangeShardCountResponse.newBuilder()
                        .setSuccess(result.success())
                        .setMessage(result.message())
                        .build()
        );

        responseObserver.onCompleted();
    }

    @NotNull
    private MessageAndSuccessDTO changeShardsCount(int newShardCount) {
        if (newShardCount <= 0) {
            return new MessageAndSuccessDTO(
                    "NEW COUNT IS NEGATIVE OR ZERO",
                    false
            );
        }

        if (newShardCount == shards.size()) {
            return new MessageAndSuccessDTO(
                    "SAME SHARD COUNT",
                    false
            );
        }

        List<ShardData> allData = new ArrayList<>(shards.values());
        shards.clear();
        for (int localShardIndex = 0; localShardIndex < newShardCount; localShardIndex++) {
            shards.put(localShardIndex, new ShardData());
        }

        allData.forEach(shardData -> {
            shardData.getStorage().forEach((key, value) -> {
                int localShardIndex = ShardUtils.getLocalShardKey(key, newShardCount);
                shards.get(localShardIndex)
                        .addToStorage(key, value);
            });
        });

        return new MessageAndSuccessDTO(
                "COUNT OF SHARDS CHANGED",
                true
        );
    }

    @Override
    public void heartbeat(NodeHeartbeatRequest request, StreamObserver<NodeHeartbeatResponse> responseObserver) {
        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(NodeHeartbeatResponse.newBuilder()
                .setHealthy(isHealthy)
                .setServerTimestamp(now.toEpochMilli())
                .setStatusMessage("OK")
                .build());

        responseObserver.onCompleted();
    }
}
