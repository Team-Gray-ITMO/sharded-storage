package vk.itmo.teamgray.sharded.storage.common.health.client;

import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.health.dto.HeartbeatResponseDTO;

public interface HealthClient extends Client {
    @Override
    HeartbeatResponseDTO heartbeat();
}
