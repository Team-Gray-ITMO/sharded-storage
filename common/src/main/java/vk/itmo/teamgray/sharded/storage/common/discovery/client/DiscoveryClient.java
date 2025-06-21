package vk.itmo.teamgray.sharded.storage.common.discovery.client;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;

public interface DiscoveryClient extends Client {
    void register(DiscoverableServiceDTO service);

    DiscoverableServiceDTO getNode(int id);

    List<DiscoverableServiceDTO> getNodes();

    DiscoverableServiceDTO getClient(int id);

    List<DiscoverableServiceDTO> getClients();

    DiscoverableServiceDTO getMaster();

    DiscoverableServiceDTO getMasterWithRetries();

    Map<Integer, DiscoverableServiceDTO> getNodeMapWithRetries(Collection<Integer> requiredServerIds);
}
