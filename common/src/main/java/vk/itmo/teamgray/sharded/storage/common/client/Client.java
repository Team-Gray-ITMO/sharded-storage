package vk.itmo.teamgray.sharded.storage.common.client;

import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;

public interface Client {
    String getHost();

    int getPort();

    HeartbeatResponseDTO heartbeat();
}
