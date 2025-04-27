package vk.itmo.teamgray.sharded.storage.common.dto;

import org.jetbrains.annotations.NotNull;

public record MessageAndSuccessDTO(@NotNull String message, boolean success) {
}
