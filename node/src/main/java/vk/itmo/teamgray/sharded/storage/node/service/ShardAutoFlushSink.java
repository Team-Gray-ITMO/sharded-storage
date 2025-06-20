package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardDTO;
import vk.itmo.teamgray.sharded.storage.common.utils.MemoryUtils;

public class ShardAutoFlushSink {
    private static final Logger log = LoggerFactory.getLogger(ShardAutoFlushSink.class);

    private final long maxByteSize;

    private final Consumer<List<SendShardDTO>> flushHandler;

    private final Map<Integer, Map<String, String>> currentBatch = new HashMap<>();

    private long currentBatchByteSize = 0;

    public ShardAutoFlushSink(
        long maxByteSize,
        Consumer<List<SendShardDTO>> flushHandler
    ) {
        this.maxByteSize = maxByteSize;
        this.flushHandler = flushHandler;
    }

    public void addEntry(int shardId, Map.Entry<String, String> entry) {
        currentBatch.computeIfAbsent(shardId, k -> new HashMap<>())
            .put(entry.getKey(), entry.getValue());

        var entrySize = getEntryByteSize(entry);

        currentBatchByteSize += entrySize;

        if (currentBatchByteSize >= maxByteSize) {
            log.info("Shard Elements Batch Is Bigger Than {}. Flushing.", maxByteSize);

            flush();
        }
    }

    public void finalFlush() {
        if (!currentBatch.isEmpty()) {
            log.debug("Sending final batch.");

            flush();
        }
    }

    private long getEntryByteSize(Map.Entry<String, String> entry) {
        var keySize = MemoryUtils.utf8Size(entry.getKey());
        var valueSize = MemoryUtils.utf8Size(entry.getValue());

        return keySize + valueSize;
    }

    private void flush() {
        List<SendShardDTO> batch = new ArrayList<>();

        currentBatch.forEach((shardId, entries) -> {
            if (!entries.isEmpty()) {
                batch.add(new SendShardDTO(shardId, entries));
            }
        });

        if (!batch.isEmpty()) {
            flushHandler.accept(batch);
        }

        currentBatch.clear();
        currentBatchByteSize = 0;
    }
}
