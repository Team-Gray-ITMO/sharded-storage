package vk.itmo.teamgray.sharded.storage.common.health.dto;

import org.jetbrains.annotations.NotNull;

public record HeartbeatResponseDTO(
    boolean healthy,
    long serverTimestamp,
    @NotNull String statusMessage
) {
    // No-op.
} 
