package vk.itmo.teamgray.sharded.storage.common.discovery.dto;

import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;
import vk.itmo.teamgray.sharded.storage.discovery.ServiceInfo;

public record DiscoverableServiceDTO(
    int id,
    DiscoverableServiceType type,
    String host,
    String containerName
) {
    public static DiscoverableServiceDTO fromGrpc(ServiceInfo serviceInfo) {
        return new DiscoverableServiceDTO(
            serviceInfo.getId(),
            DiscoverableServiceType.valueOf(serviceInfo.getType()),
            serviceInfo.getHost(),
            serviceInfo.getContainerName()
        );
    }

    public ServiceInfo toGrpc() {
        return ServiceInfo.newBuilder()
            .setId(id)
            .setType(type.name())
            .setHost(host)
            .setContainerName(containerName)
            .build();
    }

    public boolean isDockerized() {
        return containerName != null && !containerName.isBlank();
    }

    public String getIdForLogging() {
        return "[Node " + id() + "]";
    }
}
