package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

public record MessageAndSuccessDTO(@NotNull String message, boolean success) {
}
