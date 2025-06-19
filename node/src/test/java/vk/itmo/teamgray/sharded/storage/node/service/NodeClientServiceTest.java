package vk.itmo.teamgray.sharded.storage.node.service;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.dto.GetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.GetStatus;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.exception.ShardNotExistsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static vk.itmo.teamgray.sharded.storage.node.service.NodeClientService.GetResponseWriter.toDto;

class NodeClientServiceTest {
    private NodeStorageService nodeStorageService = mock();

    private NodeClientService nodeClientService = new NodeClientService(nodeStorageService);

    private final String testKey = "testKey";

    private final String testValue = "testValue";

    private final Instant testTimestamp = Instant.now();

    @Test
    void successfulSet() throws NodeException {
        SetResponseDTO expected = new SetResponseDTO(SetStatus.SUCCESS, "OK", 1);
        when(nodeStorageService.set(testKey, testValue, testTimestamp)).thenReturn(expected);

        SetResponseDTO result = nodeClientService.setKey(testKey, testValue, testTimestamp);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void failedSet() throws NodeException {
        when(nodeStorageService.set(testKey, testValue, testTimestamp))
            .thenThrow(new NodeException("error"));

        SetResponseDTO result = nodeClientService.setKey(testKey, testValue, testTimestamp);

        assertThat(result.status()).isEqualTo(SetStatus.ERROR);
    }

    @Test
    void successfulGet() throws NodeException {
        when(nodeStorageService.get(testKey)).thenReturn(testValue);

        GetResponseDTO dto = toDto(rw -> nodeClientService.getKey(testKey, rw));

        assertThat(dto.status()).isEqualTo(GetStatus.SUCCESS);
        assertThat(dto.value()).isEqualTo(testValue);
    }

    @Test
    void shardNotExists() throws NodeException {
        when(nodeStorageService.get(testKey))
            .thenThrow(new ShardNotExistsException("error"));

        GetResponseDTO dto = toDto(rw -> nodeClientService.getKey(testKey, rw));

        assertThat(dto.status()).isEqualTo(GetStatus.WRONG_NODE);
    }

    @Test
    void failedGet() throws NodeException {
        when(nodeStorageService.get(testKey))
            .thenThrow(new NodeException("error"));

        GetResponseDTO dto = toDto(rw -> nodeClientService.getKey(testKey, rw));

        assertThat(dto.status()).isEqualTo(GetStatus.ERROR);
    }
}
