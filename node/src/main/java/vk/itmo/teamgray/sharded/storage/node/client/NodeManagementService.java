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
import vk.itmo.teamgray.sharded.storage.node.management.NodeManagementServiceGrpc;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsRequest;
import vk.itmo.teamgray.sharded.storage.node.management.RearrangeShardsResponse;

public class NodeManagementService extends NodeManagementServiceGrpc.NodeManagementServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NodeManagementService.class);

    private final NodeStorageService nodeStorageService;

    public NodeManagementService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
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

                if (!moveShardFragment(nodesByShard.get(fragment.newShardId()), fragmentsToSend)) {
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

    private boolean moveShardFragment(ServerDataDTO serverDataDTO, Map<String, String> fragmentsToSend) {
        //TODO Implement
        return true;
    }
}
