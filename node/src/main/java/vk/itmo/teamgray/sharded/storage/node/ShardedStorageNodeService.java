package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        String filePath = request.getFilePath();
        boolean success = true;
        String message = "SUCCESS";

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");

                String key = parts[0].trim();
                String value = parts[1].trim();

                shards.computeIfAbsent(
                    ShardUtils.getLocalShardKey(key, shards.size()),
                    k -> new ShardData()
                ).addToStorage(key, value);
            }
        } catch (IOException e) {
            success = false;
            message = "ERROR: " + e.getMessage();
        }

        responseObserver.onNext(
            SetFromFileResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build()
        );

        responseObserver.onCompleted();
    }

    // TODO Should be moved to a different service that will handle a separate .proto for master-node communication.
    // Should resolve a shard mapping sent by master, check if existing shards comply.
    // If not - should create a new map of shards according to topology, transfer data there and in the end swap maps and kill outdated one.
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
