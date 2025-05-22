package vk.itmo.teamgray.sharded.storage.common.discovery;

import io.grpc.ManagedChannel;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.discovery.DiscoveryServiceGrpc;
import vk.itmo.teamgray.sharded.storage.discovery.IdRequest;
import vk.itmo.teamgray.sharded.storage.discovery.ServiceInfo;

import static java.util.stream.Collectors.toMap;
import static vk.itmo.teamgray.sharded.storage.common.utils.RetryUtils.retryWithAttempts;

public class DiscoveryClient extends AbstractGrpcClient<DiscoveryServiceGrpc.DiscoveryServiceBlockingStub> {
    public DiscoveryClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, DiscoveryServiceGrpc.DiscoveryServiceBlockingStub> getStubFactory() {
        return DiscoveryServiceGrpc::newBlockingStub;
    }

    public void register(DiscoverableServiceDTO service) {
        ServiceInfo info = ServiceInfo.newBuilder()
            .setId(service.id())
            .setType(service.type().name())
            .setHost(service.host())
            .setContainerName(service.containerName() == null ? "" : service.containerName())
            .build();

        StatusResponse response = blockingStub.registerService(info);

        if (!response.getSuccess()) {
            throw new IllegalStateException(
                "Failed to register service: " + service.type() + System.lineSeparator() + response.getMessage()
            );
        }
    }

    public DiscoverableServiceDTO getNode(int id) {
        return DiscoverableServiceDTO.fromServiceInfo(
            blockingStub.getNode(IdRequest.newBuilder().setId(id).build())
        );
    }

    public List<DiscoverableServiceDTO> getNodes() {
        return blockingStub.getNodes(Empty.newBuilder().build()).getNodesList().stream()
            .map(DiscoverableServiceDTO::fromServiceInfo)
            .toList();
    }

    public DiscoverableServiceDTO getClient(int id) {
        return DiscoverableServiceDTO.fromServiceInfo(
            blockingStub.getClient(IdRequest.newBuilder().setId(id).build())
        );
    }

    public List<DiscoverableServiceDTO> getClients() {
        return blockingStub.getClients(Empty.newBuilder().build()).getClientsList().stream()
            .map(DiscoverableServiceDTO::fromServiceInfo)
            .toList();
    }

    public DiscoverableServiceDTO getMaster() {
        return DiscoverableServiceDTO.fromServiceInfo(
            blockingStub.getMaster(Empty.newBuilder().build())
        );
    }

    public DiscoverableServiceDTO getMasterWithRetries() {
        return retryWithAttempts(
            3,
            Duration.of(3, ChronoUnit.SECONDS),
            () -> {
                try {
                    ServiceInfo masterInfo = blockingStub.getMaster(Empty.newBuilder().build());

                    return Optional.of(DiscoverableServiceDTO.fromServiceInfo(masterInfo));
                } catch (Exception e) {
                    return Optional.empty();
                }
            },
            "Failed to retrieve master after retries"
        );
    }

    public Map<Integer, DiscoverableServiceDTO> getNodeMapWithRetries(Collection<Integer> requiredServerIds) {
        return retryWithAttempts(
            3,
            Duration.of(3, ChronoUnit.SECONDS),
            () -> {
                Map<Integer, DiscoverableServiceDTO> nodes = getNodes().stream()
                    .collect(
                        toMap(
                            DiscoverableServiceDTO::id,
                            Function.identity()
                        )
                    );

                Set<Integer> presentIds = nodes.keySet();

                if (presentIds.containsAll(requiredServerIds)) {
                    return Optional.of(nodes);
                }

                return Optional.empty();
            },
            "Failed to discover all required nodes after retries: " + requiredServerIds
        );
    }
}
