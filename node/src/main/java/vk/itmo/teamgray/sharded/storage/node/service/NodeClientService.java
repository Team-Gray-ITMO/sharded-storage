package vk.itmo.teamgray.sharded.storage.node.service;

import java.text.MessageFormat;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.dto.GetResponse;

public class NodeClientService {
    private static final Logger log = LoggerFactory.getLogger(NodeClientService.class);

    private final NodeStorageService nodeStorageService;

    public NodeClientService(NodeStorageService nodeStorageService) {
        this.nodeStorageService = nodeStorageService;
    }

    public Optional<String> setKey(String key, String value) {
        try {
            nodeStorageService.set(key, value);

            return Optional.empty();
        } catch (NodeException e) {
            String errMessage = MessageFormat.format("Error while setting key=[{0}] value=[{1}]", key, value);

            log.warn(errMessage, e);

            return Optional.of(errMessage);
        }
    }

    public GetResponse getKey(String key) {
        try {
            return new GetResponse(nodeStorageService.get(key), true);
        } catch (NodeException e) {
            String errMessage = MessageFormat.format("Error while getting by key=[{0}]", key);

            log.warn(errMessage, e);

            return new GetResponse(errMessage, false);
        }
    }

    public boolean isBusy() {
        return nodeStorageService.isBusy();
    }
}
