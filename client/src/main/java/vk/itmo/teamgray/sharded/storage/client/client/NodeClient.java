package vk.itmo.teamgray.sharded.storage.client.client;

import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;

public interface NodeClient extends Client {
    SetStatus setKey(String key, String value);

    String getKey(String key);
}
