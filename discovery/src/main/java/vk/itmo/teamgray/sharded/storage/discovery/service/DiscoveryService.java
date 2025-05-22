package vk.itmo.teamgray.sharded.storage.discovery.service;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;
import vk.itmo.teamgray.sharded.storage.discovery.ClientList;
import vk.itmo.teamgray.sharded.storage.discovery.DiscoveryServiceGrpc;
import vk.itmo.teamgray.sharded.storage.discovery.IdRequest;
import vk.itmo.teamgray.sharded.storage.discovery.NodeList;
import vk.itmo.teamgray.sharded.storage.discovery.ServiceInfo;

public class DiscoveryService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryService.class);

    private final Map<Integer, ServiceInfo> nodes = new ConcurrentHashMap<>();

    private final Map<Integer, ServiceInfo> clients = new ConcurrentHashMap<>();

    private volatile ServiceInfo master;

    @Override
    public void registerService(ServiceInfo request, StreamObserver<StatusResponse> responseObserver) {
        var type = DiscoverableServiceType.valueOf(request.getType());

        switch (type) {
            case MASTER -> master = request;
            case NODE -> nodes.put(request.getId(), request);
            case CLIENT -> clients.put(request.getId(), request);
            default -> {
                responseObserver.onNext(StatusResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Unknown role: " + request.getType())
                    .build());

                responseObserver.onCompleted();
                return;
            }
        }

        log.info("Registered {} service: {}{}", type, System.lineSeparator(), request);

        StatusResponse response = StatusResponse.newBuilder()
            .setSuccess(true)
            .setMessage("Registered " + request.getId())
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getNode(IdRequest request, StreamObserver<ServiceInfo> responseObserver) {
        var node = nodes.get(request.getId());

        if (node == null) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("No node registered with id " + request.getId()).asRuntimeException()
            );

            return;
        }

        log.info("Returning node: {}", node);

        responseObserver.onNext(node);
        responseObserver.onCompleted();
    }

    @Override
    public void getNodes(Empty request, StreamObserver<NodeList> responseObserver) {
        NodeList nodeList = NodeList.newBuilder()
            .addAllNodes(nodes.values())
            .build();

        log.info("Returning nodes: {}", nodeList);

        responseObserver.onNext(nodeList);
        responseObserver.onCompleted();
    }

    @Override
    public void getMaster(Empty request, StreamObserver<ServiceInfo> responseObserver) {
        if (master == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("No master registered").asRuntimeException());
            return;
        }

        log.info("Returning master: {}", master);

        responseObserver.onNext(master);
        responseObserver.onCompleted();
    }

    @Override
    public void getClient(IdRequest request, StreamObserver<ServiceInfo> responseObserver) {
        var client = clients.get(request.getId());

        if (client == null) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("No client registered with id " + request.getId()).asRuntimeException()
            );

            return;
        }

        log.info("Returning client: {}", client);

        responseObserver.onNext(client);
        responseObserver.onCompleted();
    }

    @Override
    public void getClients(Empty request, StreamObserver<ClientList> responseObserver) {
        ClientList clientList = ClientList.newBuilder()
            .addAllClients(clients.values())
            .build();

        log.info("Returning clients: {}", clients);

        responseObserver.onNext(clientList);
        responseObserver.onCompleted();
    }
}
