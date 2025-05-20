package vk.itmo.teamgray.sharded.storage.common.dto;

import org.jetbrains.annotations.NotNull;

public record ServerDataDTO(@NotNull String host, int port) {
    // No-op.
}
