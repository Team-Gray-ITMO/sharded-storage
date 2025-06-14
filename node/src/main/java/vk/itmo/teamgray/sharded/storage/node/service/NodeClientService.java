package vk.itmo.teamgray.sharded.storage.node.service;

import java.text.MessageFormat;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.dto.NodeStatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.GetStatus;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.exception.ShardNotExistsException;

public class NodeClientService {
    private static final Logger log = LoggerFactory.getLogger(NodeClientService.class);

    private final NodeStorageService nodeStorageService;

    public NodeClientService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    public SetResponseDTO setKey(String key, String value, Instant timestamp) {
        try {
            return nodeStorageService.set(key, value, timestamp);
        } catch (NodeException e) {
            String errMessage = MessageFormat.format("Error while setting key=[{0}] value=[{1}]", key, value);

            log.warn(errMessage, e);

            return new SetResponseDTO(SetStatus.ERROR, errMessage, 0);
        }
    }

    public void getKey(String key, GetResponseWriter responseWriter) {
        try {
            String value = nodeStorageService.get(key);

            responseWriter.writeResponse(GetStatus.SUCCESS, value);
        } catch (ShardNotExistsException e) {
            String errMessage = MessageFormat.format("Error while getting by key=[{0}]", key);

            log.warn(errMessage, e);

            responseWriter.writeResponse(GetStatus.WRONG_NODE, errMessage);
        } catch (NodeException e) {
            String errMessage = MessageFormat.format("Error while getting by key=[{0}]", key);

            log.warn(errMessage, e);

            responseWriter.writeResponse(GetStatus.ERROR, errMessage);
        }
    }

    public NodeStatusResponseDTO getNodeStatus() {
        return nodeStorageService.getNodeStatus();
    }

    @FunctionalInterface
    public interface GetResponseWriter {
        void writeResponse(GetStatus status, String value);
    }
}
