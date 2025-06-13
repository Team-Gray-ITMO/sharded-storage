package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.common.enums.GetStatus;

public record GetResponseDTO(
    GetStatus status,
    String value
) {
    // No-op.
}
