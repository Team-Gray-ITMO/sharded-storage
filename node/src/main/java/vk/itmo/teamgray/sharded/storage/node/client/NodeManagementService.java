package vk.itmo.teamgray.sharded.storage.node.client;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.HashingUtils;
import vk.itmo.teamgray.sharded.storage.common.ServerDataDTO;
import vk.itmo.teamgray.sharded.storage.node.client.shards.ShardData;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardRequest;
import vk.itmo.teamgray.sharded.storage.node.management.MoveShardResponse;
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsResponse;
import vk.itmo.teamgray.sharded.storage.node.management.ServerData;

public class NodeManagementService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    private final NodeStorageService nodeStorageService;

    private final NodeNodeClient nodeNodeClient;

    public NodeManagementService(NodeStorageService nodeStorageService, NodeNodeClient nodeNodeClient) {
        this.nodeStorageService = nodeStorageService;
        this.nodeNodeClient = nodeNodeClient;
    }

    @Override
    public void rearrangeShards(RearrangeShardsRequest request, StreamObserver<RearrangeShardsResponse> responseObserver) {
        //TODO Refactor
        var shardToHash = request.getShardToHashMap();

        log.info("Rearranging shards. [request={}]", shardToHash);

        Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
        Map<Integer, ShardData> newShards = new ConcurrentHashMap<>();
        List<Map.Entry<Integer, Long>> shardToHashMap = new ArrayList<>(shardToHash.entrySet());
        shardToHashMap.sort(Comparator.comparingLong(Map.Entry::getValue));

        if (shardToHashMap.isEmpty()) {
            responseObserver.onNext(RearrangeShardsResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
            return;
        }

        shardToHashMap.forEach(shard -> newShards.put(shard.getKey(), new ShardData()));

        var fragments = request.getFragmentsList().stream()
            .map(FragmentDTO::fromGrpc)
            .toList();

        var localFragments = fragments.stream()
            .filter(fragment -> newShards.containsKey(fragment.newShardId()))
            .toList();

        localFragments.forEach(fragment -> {
            int oldShardId = fragment.oldShardId();
            if (existingShards.containsKey(oldShardId)) {
                Map<String, String> sourceStorage = existingShards.get(oldShardId).getStorage();

                //TODO At some point for perf reasons it will be nice to be able to do this with hash ranges instead of one-by-one
                sourceStorage.entrySet().stream()
                    .filter(entry -> {
                        long hash = HashingUtils.calculate64BitHash(entry.getKey());
                        return hash >= fragment.rangeFrom() && hash < fragment.rangeTo();
                    })
                    .forEach(entry -> {
                        newShards.get(fragment.newShardId()).addToStorage(entry.getKey(), entry.getValue());

                        if (!newShards.containsKey(oldShardId)) {
                            sourceStorage.remove(entry.getKey());
                        }
                    });
            }
        });

        var externalFragments = fragments.stream()
            .filter(fragment -> !newShards.containsKey(fragment.newShardId()))
            .toList();

        var keysToRemove = new HashMap<Integer, Set<String>>();

        Map<Integer, ServerDataDTO> nodesByShard = externalFragments.isEmpty()
            ? Collections.emptyMap()
            : request.getServerByShardNumberMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        it -> ServerDataDTO.fromGrpc(it.getValue())
                    )
                );

        externalFragments.stream()
            .filter(it -> existingShards.containsKey(it.oldShardId()))
            .forEach(fragment -> {
                int oldShardId = fragment.oldShardId();

                Map<String, String> fragmentStorage = existingShards.get(oldShardId).getStorage();

                var fragmentsToSend = fragmentStorage.entrySet().stream()
                    .filter(entry -> {
                        long hash = HashingUtils.calculate64BitHash(entry.getKey());
                        return hash >= fragment.rangeFrom() && hash < fragment.rangeTo();
                    })
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                        )
                    );

                if (!moveShardFragment(nodesByShard.get(fragment.newShardId()), fragment.newShardId(), fragmentsToSend)) {
                    throw new IllegalStateException("Failed to move shard fragment");
                }

                keysToRemove.put(oldShardId, fragmentsToSend.keySet());
            });

        keysToRemove.forEach((oldShardId, keysToRemoveSet) -> {
            Map<String, String> fragmentStorage = existingShards.get(oldShardId).getStorage();

            keysToRemoveSet.forEach(fragmentStorage::remove);
        });

        nodeStorageService.replace(newShards);

        log.info("Rearranged shards.");

        responseObserver.onNext(RearrangeShardsResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    private boolean moveShardFragment(ServerDataDTO serverDataDTO, int newShardId, Map<String, String> fragmentsToSend) {
        //TODO Add shard resolving
        return nodeNodeClient.sendShard(newShardId, fragmentsToSend);
    }

    @Override
    public void moveShard(MoveShardRequest request, StreamObserver<MoveShardResponse> responseObserver) {
        int shardId = request.getShardId();
        ServerData targetServer = request.getTargetServer();

        log.info("Request to move shard {} to {}:{}", shardId, targetServer.getHost(), targetServer.getPort());

        Map<Integer, ShardData> existingShards = nodeStorageService.getShards();
        if (!existingShards.containsKey(shardId)) {
            responseObserver.onNext(MoveShardResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Shard " + shardId + " not found in this node")
                .build());
            responseObserver.onCompleted();
            return;
        }

        ShardData shardToMove = existingShards.get(shardId);
        Map<String, String> shardData = shardToMove.getStorage();

        // TODO: replace with sendShard implementation
        boolean sendSuccess = sendShardStub(shardId, shardData, targetServer);

        if (sendSuccess) {
            // remove shard only after successful transfer
            nodeStorageService.removeShard(shardId);

            responseObserver.onNext(MoveShardResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Shard successfully moved")
                .build());
        } else {
            responseObserver.onNext(MoveShardResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Failed to send shard to target server")
                .build());
        }

        responseObserver.onCompleted();
    }

    private boolean sendShardStub(int shardId, Map<String, String> shardData, ServerData targetServer) {
        // TODO: refactor this method for dynamic change host and port
        NodeNodeClient nodeNodeClient = new NodeNodeClient(targetServer.getHost(), targetServer.getPort());
        boolean result = nodeNodeClient.sendShard(shardId, shardData);
        try {
            nodeNodeClient.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
