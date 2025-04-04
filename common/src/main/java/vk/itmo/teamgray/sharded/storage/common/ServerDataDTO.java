package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;

public record ServerDataDTO(@NotNull String host, int port) {
}
