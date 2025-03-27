package vk.itmo.teamgray.sharded.storage.node;

import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.AddServerResponse;
import vk.itmo.teamgray.sharded.storage.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.ChangeShardCountResponse;
import vk.itmo.teamgray.sharded.storage.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.DeleteServerResponse;
import vk.itmo.teamgray.sharded.storage.GetKeyRequest;
import vk.itmo.teamgray.sharded.storage.GetKeyResponse;
import vk.itmo.teamgray.sharded.storage.HeartbeatRequest;
import vk.itmo.teamgray.sharded.storage.HeartbeatResponse;
import vk.itmo.teamgray.sharded.storage.SetFromFileRequest;
import vk.itmo.teamgray.sharded.storage.SetFromFileResponse;
import vk.itmo.teamgray.sharded.storage.SetKeyRequest;
import vk.itmo.teamgray.sharded.storage.SetKeyResponse;
import vk.itmo.teamgray.sharded.storage.ShardedStorageGrpc;

public class ShardedStorageImpl extends ShardedStorageGrpc.ShardedStorageImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardedStorageImpl.class);

    private final AtomicInteger shardCount = new AtomicInteger(1);

    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> shards = new ConcurrentHashMap<>();
    
    private volatile boolean reshardingInProgress = false;
    
    public ShardedStorageImpl() {
        
        shards.put(0, new ConcurrentHashMap<>());
    }
    
    /**
     * Calculate the shard index for a given key
     */
    private int getShardIndex(String key) {
        return Math.abs(key.hashCode() % shardCount.get());
    }

    @Override
    public void addServer(AddServerRequest request, StreamObserver<AddServerResponse> responseObserver) {
        // TODO: Implement actual server addition logic

        responseObserver.onNext(
            AddServerResponse.newBuilder()
                .setSuccess(true)
                .setMessage("")
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void deleteServer(DeleteServerRequest request, StreamObserver<DeleteServerResponse> responseObserver) {
        // TODO: Implement actual server deletion logic

        responseObserver.onNext(
            DeleteServerResponse.newBuilder()
                .setSuccess(true)
                .setMessage("")
                .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void changeShardCount(ChangeShardCountRequest request, StreamObserver<ChangeShardCountResponse> responseObserver) {
        int newShardCount = request.getNewShardCount();
        int currentShardCount = shardCount.get();
        
        log.info("Changing shard count from {} to {}", currentShardCount, newShardCount);
        
        if (newShardCount <= 0) {
            responseObserver.onNext(
                ChangeShardCountResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Shard count must be positive")
                    .build()
            );
            responseObserver.onCompleted();
            return;
        }
        
        if (newShardCount == currentShardCount) {
            responseObserver.onNext(
                ChangeShardCountResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Shard count already set to " + newShardCount)
                    .build()
            );
            responseObserver.onCompleted();
            return;
        }
        
        try {
            reshardingInProgress = true;

            ConcurrentHashMap<String, String> allData = new ConcurrentHashMap<>();

            if (newShardCount > currentShardCount) {
                for (int i = currentShardCount; i < newShardCount; i++) {
                    shards.put(i, new ConcurrentHashMap<>());
                }
            }
            
            for (var shard : shards.values()) {
                allData.putAll(shard);
            }
            
            shardCount.set(newShardCount);
            
            for (int i = 0; i < Math.min(currentShardCount, newShardCount); i++) {
                shards.get(i).clear();
            }
            
            if (newShardCount < currentShardCount) {
                for (int i = newShardCount; i < currentShardCount; i++) {
                    shards.remove(i);
                }
            }
            
            for (Map.Entry<String, String> entry : allData.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                int newShardIndex = getShardIndex(key);
                
                if (!shards.containsKey(newShardIndex)) {
                    shards.putIfAbsent(newShardIndex, new ConcurrentHashMap<>());
                }
                
                shards.get(newShardIndex).put(key, value);
            }
            
            log.info("Successfully changed shard count to {}", newShardCount);
            
            responseObserver.onNext(
                ChangeShardCountResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Successfully changed shard count to " + newShardCount)
                    .build()
            );
        } catch (Exception e) {
            log.error("Error changing shard count", e);
            responseObserver.onNext(
                ChangeShardCountResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error changing shard count: " + e.getMessage())
                    .build()
            );
        } finally {
            reshardingInProgress = false;
        }
        
        responseObserver.onCompleted();
    }

    @Override
    public void setKey(SetKeyRequest request, StreamObserver<SetKeyResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();
        
        if (key == null || key.isEmpty()) {
            responseObserver.onNext(
                SetKeyResponse.newBuilder()
                    .setSuccess(false)
                    .build()
            );
            responseObserver.onCompleted();
            return;
        }
        
        try {
            int shardIndex = getShardIndex(key);
            
            ConcurrentHashMap<String, String> shard = shards.get(shardIndex);
            
            if (shard == null) {
                shard = new ConcurrentHashMap<>();
                ConcurrentHashMap<String, String> existingShard = shards.putIfAbsent(shardIndex, shard);
                if (existingShard != null) {
                    shard = existingShard;
                }
            }
            
            shard.put(key, value);
            
            log.debug("Set key '{}' to '{}' in shard {}", key, value, shardIndex);
            
            responseObserver.onNext(
                SetKeyResponse.newBuilder()
                    .setSuccess(true)
                    .build()
            );
        } catch (Exception e) {
            log.error("Error setting key '{}'", key, e);
            responseObserver.onNext(
                SetKeyResponse.newBuilder()
                    .setSuccess(false)
                    .build()
            );
        }
        
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
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        // TODO: Implement file import logic

        var now = Instant.now();

        log.info("Received heartbeat request for: {} Sending heartbeat at {}", Instant.ofEpochMilli(request.getTimestamp()), now);

        boolean isHealthy = true;

        responseObserver.onNext(HeartbeatResponse.newBuilder()
            .setHealthy(isHealthy)
            .setServerTimestamp(now.toEpochMilli())
            .setStatusMessage("OK")
            .build());

        responseObserver.onCompleted();
    }

    @Override
    public void getKey(GetKeyRequest request, StreamObserver<GetKeyResponse> responseObserver) {
        String key = request.getKey();
        
        if (key == null || key.isEmpty()) {
            responseObserver.onNext(
                GetKeyResponse.newBuilder()
                    .setFound(false)
                    .build()
            );
            responseObserver.onCompleted();
            return;
        }
        
        try {
            
            String value = null;
            boolean found = false;
            
            
            int shardIndex = getShardIndex(key);
            ConcurrentHashMap<String, String> shard = shards.get(shardIndex);
            
            if (shard != null) {
                value = shard.get(key);
                found = (value != null);
            }
            
            
            
            if (!found && reshardingInProgress) {
                log.debug("Key '{}' not found in primary shard during resharding, checking all shards", key);
                
                for (ConcurrentHashMap<String, String> s : shards.values()) {
                    if (s != shard) { 
                        value = s.get(key);
                        if (value != null) {
                            found = true;
                            log.debug("Found key '{}' in another shard during resharding", key);
                            break;
                        }
                    }
                }
            }
            
            if (found) {
                log.debug("Retrieved key '{}' with value '{}'", key, value);
                responseObserver.onNext(
                    GetKeyResponse.newBuilder()
                        .setFound(true)
                        .setValue(value)
                        .build()
                );
            } else {
                log.debug("Key '{}' not found", key);
                responseObserver.onNext(
                    GetKeyResponse.newBuilder()
                        .setFound(false)
                        .build()
                );
            }
        } catch (Exception e) {
            log.error("Error getting key '{}'", key, e);
            responseObserver.onNext(
                GetKeyResponse.newBuilder()
                    .setFound(false)
                    .build()
            );
        }
        
        responseObserver.onCompleted();
    }
}
