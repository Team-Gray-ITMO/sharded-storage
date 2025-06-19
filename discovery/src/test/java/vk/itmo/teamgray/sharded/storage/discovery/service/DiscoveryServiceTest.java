package vk.itmo.teamgray.sharded.storage.discovery.service;

import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.discovery.exception.DiscoveryException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DiscoveryServiceTest {
    private final DiscoveryService service = new DiscoveryService();

    private final StatusResponseWriter statusWriter = mock(StatusResponseWriter.class);

    private final DiscoveryService.DiscoverableServiceResponseWriter serviceWriter =
        mock(DiscoveryService.DiscoverableServiceResponseWriter.class);

    private DiscoverableServiceDTO createService(int id, DiscoverableServiceType type) {
        return new DiscoverableServiceDTO(id, type, "host-" + id, "container-" + id);
    }

    @Test
    void registerMaster() {
        DiscoverableServiceDTO master = createService(1, DiscoverableServiceType.MASTER);

        service.registerService(master, statusWriter);

        verify(statusWriter).writeResponse(true, "Registered 1");
        assertThat(service.getMaster()).isEqualTo(master);
    }

    @Test
    void registerNode() {
        DiscoverableServiceDTO node = createService(2, DiscoverableServiceType.NODE);

        service.registerService(node, statusWriter);

        verify(statusWriter).writeResponse(true, "Registered 2");
        assertThat(service.getNode(2)).isEqualTo(node);
    }

    @Test
    void registerClient() {
        DiscoverableServiceDTO client = createService(3, DiscoverableServiceType.CLIENT);

        service.registerService(client, statusWriter);

        verify(statusWriter).writeResponse(true, "Registered 3");
        assertThat(service.getClient(3)).isEqualTo(client);
    }

    @Test
    void getNonexistentNode() {
        assertThrows(DiscoveryException.class, () -> service.getNode(99));
    }

    @Test
    void getNonexistentMaster() {
        assertThrows(DiscoveryException.class, service::getMaster);
    }

    @Test
    void getNonexistentClient() {
        assertThrows(DiscoveryException.class, () -> service.getClient(99));
    }

    @Test
    void getAllNodes() {
        DiscoverableServiceDTO node1 = createService(1, DiscoverableServiceType.NODE);
        DiscoverableServiceDTO node2 = createService(2, DiscoverableServiceType.NODE);

        service.registerService(node1, statusWriter);
        service.registerService(node2, statusWriter);

        service.getNodes(serviceWriter);

        verify(serviceWriter).write(node1);
        verify(serviceWriter).write(node2);
    }

    @Test
    void getAllClients() {
        DiscoverableServiceDTO client1 = createService(1, DiscoverableServiceType.CLIENT);
        DiscoverableServiceDTO client2 = createService(2, DiscoverableServiceType.CLIENT);

        service.registerService(client1, statusWriter);
        service.registerService(client2, statusWriter);

        service.getClients(serviceWriter);

        verify(serviceWriter).write(client1);
        verify(serviceWriter).write(client2);
    }

    @Test
    void masterOverwrite() {
        DiscoverableServiceDTO master1 = createService(1, DiscoverableServiceType.MASTER);
        DiscoverableServiceDTO master2 = createService(2, DiscoverableServiceType.MASTER);

        service.registerService(master1, statusWriter);
        service.registerService(master2, statusWriter);

        assertThat(service.getMaster()).isEqualTo(master2);
    }
}
