package vk.itmo.teamgray.sharded.storage.common.dto;

public record StatusResponseDTO(
    boolean success,
    String message
) {
    // No-op.
}
