package vk.itmo.teamgray.sharded.storage.discovery.proto;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import vk.itmo.teamgray.sharded.storage.common.Empty;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.discovery.DiscoveryServiceGrpc;
import vk.itmo.teamgray.sharded.storage.discovery.IdRequest;
import vk.itmo.teamgray.sharded.storage.discovery.ServiceInfo;
import vk.itmo.teamgray.sharded.storage.discovery.ServiceList;
import vk.itmo.teamgray.sharded.storage.discovery.exception.DiscoveryException;
import vk.itmo.teamgray.sharded.storage.discovery.service.DiscoveryService;

public class DiscoveryGrpcService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase {
    private final DiscoveryService discoveryService;

    public DiscoveryGrpcService(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public void registerService(ServiceInfo request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder();

        discoveryService.registerService(
            DiscoverableServiceDTO.fromGrpc(request),
            (success, message) -> {
                response.setSuccess(success);
                response.setMessage(message);
            }
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNode(IdRequest request, StreamObserver<ServiceInfo> responseObserver) {
        try {
            ServiceInfo response = discoveryService.getNode(request.getId())
                .toGrpc();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DiscoveryException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getNodes(Empty request, StreamObserver<ServiceList> responseObserver) {
        var response = ServiceList.newBuilder();

        discoveryService.getNodes(service -> response.addServices(service.toGrpc()));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMaster(Empty request, StreamObserver<ServiceInfo> responseObserver) {
        try {
            ServiceInfo response = discoveryService.getMaster().toGrpc();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DiscoveryException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getClient(IdRequest request, StreamObserver<ServiceInfo> responseObserver) {
        try {
            ServiceInfo response = discoveryService.getClient(request.getId())
                .toGrpc();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DiscoveryException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getClients(Empty request, StreamObserver<ServiceList> responseObserver) {
        var response = ServiceList.newBuilder();

        discoveryService.getClients(service -> response.addServices(service.toGrpc()));

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}
