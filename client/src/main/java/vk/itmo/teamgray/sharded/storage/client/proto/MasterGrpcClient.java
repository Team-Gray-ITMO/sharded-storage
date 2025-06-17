package vk.itmo.teamgray.sharded.storage.client.proto;

import io.grpc.ManagedChannel;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import vk.itmo.teamgray.sharded.storage.client.client.MasterClient;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.master.client.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.master.client.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToStateRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.MasterClientServiceGrpc;

public class MasterGrpcClient extends AbstractGrpcClient<MasterClientServiceGrpc.MasterClientServiceBlockingStub> implements MasterClient {
    public MasterGrpcClient(String host, int port) {
        super(host, port);
    }

    @Override
    protected Function<ManagedChannel, MasterClientServiceGrpc.MasterClientServiceBlockingStub> getStubFactory() {
        return MasterClientServiceGrpc::newBlockingStub;
    }

    @Override
    public StatusResponseDTO addServer(int server, boolean forkNewInstance) {
        AddServerRequest request = AddServerRequest.newBuilder()
            .setId(server)
            .setForkNewInstance(forkNewInstance)
            .build();

        var response = blockingStub.addServer(request);
        return new StatusResponseDTO(response);
    }

    @Override
    public StatusResponseDTO deleteServer(int server) {
        DeleteServerRequest request = DeleteServerRequest.newBuilder()
            .setId(server)
            .build();

        var response = blockingStub.deleteServer(request);
        return new StatusResponseDTO(response);
    }

    //Doing map flipping on the client side to unload master.
    @Override
    public Map<Integer, Integer> getShardToServerMap() {
        GetServerToShardRequest request = GetServerToShardRequest.newBuilder().build();
        GetServerToShardResponse response = blockingStub.getServerToShard(request);

        return response.getServerToShardMap().entrySet().stream()
            .flatMap(
                entry -> entry.getValue().getValuesList().stream()
                    .map(shardId -> Map.entry(shardId, entry.getKey()))
            )
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue)
            );
    }

    //Doing map flipping on client side to unload master.
    @Override
    public Map<Long, Integer> getHashToShardMap() {
        GetShardToHashRequest request = GetShardToHashRequest.newBuilder().build();
        GetShardToHashResponse response = blockingStub.getShardToHash(request);

        return response.getShardToHashMap().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey)
            );
    }

    @Override
    public Map<Integer, NodeState> getServerToState() {
        GetServerToStateRequest request = GetServerToStateRequest.newBuilder().build();

        var response = blockingStub.getServerToState(request);

        return response.getServerToStateMap().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    it -> NodeState.valueOf(it.getValue())
                )
            );
    }

    @Override
    public StatusResponseDTO changeShardCount(int newShardCount) {
        ChangeShardCountRequest request = ChangeShardCountRequest.newBuilder()
            .setNewShardCount(newShardCount)
            .build();

        var response = blockingStub.changeShardCount(request);
        return new StatusResponseDTO(response);
    }
}
