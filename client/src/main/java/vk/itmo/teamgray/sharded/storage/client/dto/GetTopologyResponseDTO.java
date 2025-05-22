package vk.itmo.teamgray.sharded.storage.client.dto;

import java.util.Map;
import org.jetbrains.annotations.NotNull;

public record GetTopologyResponseDTO(
    @NotNull Map<Integer, String> shardToServer,
    @NotNull Map<Integer, Long> shardToHash,
    int totalShardCount
) {
} 
