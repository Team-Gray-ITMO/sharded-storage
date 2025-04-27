package vk.itmo.teamgray.sharded.storage.common;

import org.jetbrains.annotations.NotNull;
import vk.itmo.teamgray.sharded.storage.node.management.Server;

public record ServerDataDTO(@NotNull String host, int port) {
    public Server toGrpc() {
        return Server.newBuilder()
            .setIp(host)
            .setPort(port)
            .build();
    }

    public static ServerDataDTO fromGrpc(Server server) {
        return new ServerDataDTO(server.getIp(), server.getPort());
    }
}
