package vk.itmo.teamgray.sharded.storage.dto;

import org.jetbrains.annotations.NotNull;

public record NodeHeartbeatResponseDTO(
    boolean healthy,
    long serverTimestamp,
    @NotNull String statusMessage
) {
} 