package vk.itmo.teamgray.sharded.storage.node.service;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;

public class NodeNodeService {
    private static final Logger log = LoggerFactory.getLogger(NodeNodeService.class);

    private static final String SUCCESS_MESSAGE = "SUCCESS";

    private final NodeStorageService nodeStorageService;

    public NodeNodeService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    public void sendShards(
        List<SendShardDTO> sendShards,
        StatusResponseWriter responseWriter
    ) {

        log.debug("Received shards {}. Processing", sendShards);

        List<String> errorMessages = sendShards.stream()
            .map(sendShard -> {
                int shardId = sendShard.shardId();
                Map<String, String> shard = sendShard.shard();

                try {
                    var stagedShards = nodeStorageService.getStagedShards();

                    if (!stagedShards.containsShard(shardId)) {
                        throw new NodeException("Staged shard " + shardId + " does not exist");
                    }

                    shard
                        .forEach((key, value) -> {
                            stagedShards.checkKeyForShard(shardId, key);
                            stagedShards.set(key, value);
                        });

                    return null;
                } catch (Exception e) {
                    log.error("Caught exception: ", e);

                    return "ERROR: " + e.getMessage();
                }
            })
            .filter(Objects::nonNull)
            .toList();

        responseWriter.writeResponse(
            errorMessages.isEmpty(),
            errorMessages.isEmpty()
                ? SUCCESS_MESSAGE
                : StringUtil.join(System.lineSeparator(), errorMessages).toString()
        );
    }

    public void sendShardFragment(
        int shardId,
        Map<String, String> shardFragments,
        StatusResponseWriter responseWriter
    ) {
        boolean success = true;
        String message = SUCCESS_MESSAGE;

        log.debug("Received fragment for shard {}. Processing", shardId);

        try {
            var stagedShards = nodeStorageService.getStagedShards();

            if (!stagedShards.containsShard(shardId)) {
                throw new NodeException("Staged shard " + shardId + " does not exist");
            }

            shardFragments
                .forEach((key, value) -> {
                    stagedShards.checkKeyForShard(shardId, key);
                    stagedShards.set(key, value);
                });
        } catch (Exception e) {
            success = false;
            message = "ERROR: " + e.getMessage();

            log.error(message, e);

            responseWriter.writeResponse(false, message);
        }

        responseWriter.writeResponse(success, message);
    }
}
