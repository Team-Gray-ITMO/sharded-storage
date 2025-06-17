package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;

public record SetResponseDTO(
    SetStatus status,
    String message,
    int newNodeId
) {
    public SetResponseDTO(SetStatus status, String message) {
        this(status, message, 0);
    }
}
