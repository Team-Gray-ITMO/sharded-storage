package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class NodeManagementServiceTest {
    @Test
    public void testRearrangeShardsWithEmptyMapping() {
        NodeStorageService nodeStorageService = new NodeStorageService();
        NodeManagementService service = new NodeManagementService(nodeStorageService, mock());

        var response = new StatusResponseDTO();

        //TODO Do a TestService that will do a plain DTO Conversions
        service.rearrangeShards(
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyMap(),
            0,
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            });

        assertTrue(response.isSuccess());
        assertTrue(nodeStorageService.getShards().isEmpty());
    }

    @Test
    public void testRearrangeShardsWithEmptyStorageAndNotEmptyRequestMapping() {
        NodeStorageService nodeStorageService = new NodeStorageService();
        NodeManagementService service = new NodeManagementService(nodeStorageService, mock());

        Map<Integer, Long> shardToHash = Map.of(
            1, Long.MAX_VALUE / 2,
            2, Long.MAX_VALUE
        );

        var response = new StatusResponseDTO();

        service.rearrangeShards(
            shardToHash,
            Collections.emptyList(),
            Collections.emptyMap(),
            shardToHash.size(),
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            }
        );

        assertTrue(response.isSuccess());
        assertEquals(shardToHash.size(), nodeStorageService.getShards().size());
        nodeStorageService.getShards().forEach((shardNum, shardData) -> assertTrue(shardData.getStorage().isEmpty()));
    }
}
