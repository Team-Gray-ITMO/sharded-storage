package vk.itmo.teamgray.sharded.storage.client.service;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.client.client.NodeClient;
import vk.itmo.teamgray.sharded.storage.client.exception.ClientException;
import vk.itmo.teamgray.sharded.storage.common.client.ClientCachingFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;
import vk.itmo.teamgray.sharded.storage.common.discovery.client.DiscoveryClient;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.GetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.GetStatus;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;
import static vk.itmo.teamgray.sharded.storage.client.service.ClientService.RETRIES;

class ClientServiceTest {
    private ClientService clientService;

    private NodeClient nodeClient1 = mock();

    private NodeClient nodeClient2 = mock();

    private MasterClient masterClient = mock();

    @BeforeEach
    void setUp() {
        DiscoveryClient discoveryClient = mock();
        ClientCachingFactory clientCachingFactory = mock();

        //2 Shards on 2 Servers
        when(masterClient.getHashToShardMap())
            .thenReturn(Map.of(0L, 1, Long.MAX_VALUE, 2));
        when(masterClient.getShardToServerMap())
            .thenReturn(Map.of(0, 1, 1, 2));
        when(discoveryClient.getNodeMapWithRetries(any()))
            .thenReturn(Map.of(
                1, new DiscoverableServiceDTO(1, DiscoverableServiceType.NODE, "test1", "test1"),
                2, new DiscoverableServiceDTO(2, DiscoverableServiceType.NODE, "test2", "test2"))
            );

        when(clientCachingFactory.getClient(argThat(server -> server != null && server.id() == 1), eq(NodeClient.class)))
            .thenReturn(nodeClient1);
        when(clientCachingFactory.getClient(argThat(server -> server != null && server.id() == 2), eq(NodeClient.class)))
            .thenReturn(nodeClient2);

        clientService = new ClientService(
            masterClient,
            discoveryClient,
            clientCachingFactory
        );
    }

    @Test
    void testGetValueRetries() {
        when(nodeClient1.getKey(any()))
            .thenReturn(new GetResponseDTO(GetStatus.WRONG_NODE, null));
        when(nodeClient2.getKey(any()))
            .thenReturn(new GetResponseDTO(GetStatus.WRONG_NODE, null));

        assertThrows(ClientException.class, () -> clientService.getValue(UUID.randomUUID().toString()));

        assertEquals(
            RETRIES,
            (int)Stream.concat(
                    mockingDetails(nodeClient1).getInvocations().stream(),
                    mockingDetails(nodeClient2).getInvocations().stream()
                )
                .filter(i -> i.getMethod().getName().equals("getKey"))
                .count()
        );
    }

    @Test
    void testSetValueRetries() {
        when(nodeClient1.setKey(any(), any(), any()))
            .thenReturn(new SetResponseDTO(SetStatus.TRANSFER, null, 2));
        when(nodeClient2.setKey(any(), any(), any()))
            .thenReturn(new SetResponseDTO(SetStatus.TRANSFER, null, 1));

        assertThrows(ClientException.class, () -> clientService.setValue(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        assertEquals(
            RETRIES,
            (int)Stream.concat(
                    mockingDetails(nodeClient1).getInvocations().stream(),
                    mockingDetails(nodeClient2).getInvocations().stream()
                )
                .filter(i -> i.getMethod().getName().equals("setKey"))
                .count()
        );
    }

    @Test
    void testGetFail() {
        when(nodeClient1.getKey(any()))
            .thenReturn(new GetResponseDTO(GetStatus.ERROR, null));
        when(nodeClient2.getKey(any()))
            .thenReturn(new GetResponseDTO(GetStatus.ERROR, null));

        assertThrows(ClientException.class, () -> clientService.getValue(UUID.randomUUID().toString()));
    }

    @Test
    void testSetFail() {
        when(nodeClient1.setKey(any(), any(), any()))
            .thenReturn(new SetResponseDTO(SetStatus.ERROR, null, 0));
        when(nodeClient2.setKey(any(), any(), any()))
            .thenReturn(new SetResponseDTO(SetStatus.ERROR, null, 0));

        assertFalse(clientService.setValue(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    }

    @Test
    void testGetValueEventuallySuccessful() {
        clientService.updateCaches();

        //Flip shard map, but old one is already cached
        when(masterClient.getShardToServerMap())
            .thenReturn(Map.of(0, 2, 1, 1));

        String testValue = "test_value";

        when(nodeClient1.getKey(any()))
            .thenReturn(new GetResponseDTO(GetStatus.WRONG_NODE, null))
            .thenReturn(new GetResponseDTO(GetStatus.SUCCESS, testValue));
        when(nodeClient2.getKey(any()))
            .thenReturn(new GetResponseDTO(GetStatus.WRONG_NODE, null))
            .thenReturn(new GetResponseDTO(GetStatus.SUCCESS, testValue));

        assertEquals(testValue, clientService.getValue(UUID.randomUUID().toString()));
    }

    @Test
    void testSetValueEventuallySuccessful() {
        when(nodeClient1.setKey(any(), any(), any()))
            .thenReturn(new SetResponseDTO(SetStatus.TRANSFER, null, 2))
            .thenReturn(new SetResponseDTO(SetStatus.SUCCESS, "Success", 0));
        when(nodeClient2.setKey(any(), any(), any()))
            .thenReturn(new SetResponseDTO(SetStatus.TRANSFER, null, 1))
            .thenReturn(new SetResponseDTO(SetStatus.SUCCESS, "Success", 0));

        assertTrue(clientService.setValue(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    }
}
