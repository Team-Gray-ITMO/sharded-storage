package vk.itmo.teamgray.sharded.storage.discovery.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;
import vk.itmo.teamgray.sharded.storage.discovery.DiscoveryServiceGrpc;
import vk.itmo.teamgray.sharded.storage.discovery.exception.DiscoveryException;

public class DiscoveryService {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryService.class);

    private final Map<Integer, DiscoverableServiceDTO> nodes = new ConcurrentHashMap<>();

    private final Map<Integer, DiscoverableServiceDTO> clients = new ConcurrentHashMap<>();

    private volatile DiscoverableServiceDTO master;

    public void registerService(DiscoverableServiceDTO request, StatusResponseWriter responseWriter) {
        var type = request.type();

        switch (type) {
            case MASTER -> master = request;
            case NODE -> nodes.put(request.id(), request);
            case CLIENT -> clients.put(request.id(), request);
            default -> {
                responseWriter.writeResponse(false, "Unknown role: " + request.type());

                return;
            }
        }

        log.info("Registered {} service: {}{}", type, System.lineSeparator(), request);

        responseWriter.writeResponse(true, "Registered " + request.id());
    }

    public DiscoverableServiceDTO getNode(int id) {
        var node = nodes.get(id);

        if (node == null) {
            throw new DiscoveryException("No node registered with id " + id);
        }

        log.info("Returning node: {}", node);

        return node;
    }

    public void getNodes(DiscoverableServiceResponseWriter responseWriter) {
        var nodeValues = nodes.values();

        nodeValues.forEach(responseWriter::write);

        log.info("Returning nodes: {}", nodeValues);
    }

    public DiscoverableServiceDTO getMaster() {
        if (master == null) {
            throw new DiscoveryException("No master registered");
        }

        log.info("Returning master: {}", master);

        return master;
    }

    public DiscoverableServiceDTO getClient(int id) {
        var client = clients.get(id);

        if (client == null) {
            throw new DiscoveryException("No client registered with id " + id);
        }

        log.info("Returning client: {}", client);

        return client;
    }

    public void getClients(DiscoverableServiceResponseWriter responseWriter) {
        var clientValues = clients.values();

        log.info("Returning clients: {}", clientValues);

        clientValues.forEach(responseWriter::write);
    }

    @FunctionalInterface
    public interface DiscoverableServiceResponseWriter {
        void write(DiscoverableServiceDTO discoverableService);
    }
}
