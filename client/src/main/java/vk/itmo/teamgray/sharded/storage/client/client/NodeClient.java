package vk.itmo.teamgray.sharded.storage.client.client;

import java.time.Instant;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.dto.GetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;

public interface NodeClient extends Client {
    SetResponseDTO setKey(String key, String value, Instant timestamp);

    GetResponseDTO getKey(String key);
}
