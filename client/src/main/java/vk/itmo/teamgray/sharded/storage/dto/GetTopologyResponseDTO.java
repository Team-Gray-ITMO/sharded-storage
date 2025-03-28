package vk.itmo.teamgray.sharded.storage.dto;

import org.jetbrains.annotations.NotNull;
import java.util.Map;

public record GetTopologyResponseDTO(
    @NotNull Map<Integer, String> shardToServer,
    int totalShardCount
) {
} 