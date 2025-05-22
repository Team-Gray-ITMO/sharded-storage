package vk.itmo.teamgray.sharded.storage.client.dto;

import org.jetbrains.annotations.NotNull;

public record ChangeShardCountResponseDTO(@NotNull String message, boolean success) {
} 
