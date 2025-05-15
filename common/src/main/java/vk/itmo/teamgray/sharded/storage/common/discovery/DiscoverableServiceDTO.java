package vk.itmo.teamgray.sharded.storage.common.discovery;

import vk.itmo.teamgray.sharded.storage.discovery.ServiceInfo;

public record DiscoverableServiceDTO(
    int id,
    DiscoverableServiceType type,
    String host,
    String containerName
) {
    public static DiscoverableServiceDTO fromServiceInfo(ServiceInfo serviceInfo) {
        return new DiscoverableServiceDTO(
            serviceInfo.getId(),
            DiscoverableServiceType.valueOf(serviceInfo.getType()),
            serviceInfo.getHost(),
            serviceInfo.getContainerName()
        );
    }

    public boolean isDockerized() {
        return containerName != null && !containerName.isBlank();
    }
}
