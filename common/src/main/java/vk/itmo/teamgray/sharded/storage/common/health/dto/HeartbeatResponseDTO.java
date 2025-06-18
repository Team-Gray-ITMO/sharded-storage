package vk.itmo.teamgray.sharded.storage.common.health.dto;

public record HeartbeatResponseDTO(
    boolean healthy,
    long serverTimestamp,
    String statusMessage
) {
    // No-op.
} 
