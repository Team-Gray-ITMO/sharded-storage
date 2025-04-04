package vk.itmo.teamgray.sharded.storage.dto;

import org.jetbrains.annotations.NotNull;

public record ChangeShardCountResponseDTO(@NotNull String message, boolean success) {
} 