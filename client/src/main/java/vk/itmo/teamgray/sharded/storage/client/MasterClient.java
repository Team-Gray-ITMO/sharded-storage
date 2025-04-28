package vk.itmo.teamgray.sharded.storage.client;

import io.grpc.ManagedChannel;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import vk.itmo.teamgray.sharded.storage.common.proto.AbstractGrpcClient;
import vk.itmo.teamgray.sharded.storage.dto.AddServerResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.ChangeShardCountResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.DeleteServerResponseDTO;
import vk.itmo.teamgray.sharded.storage.dto.MasterHeartbeatResponseDTO;
import vk.itmo.teamgray.sharded.storage.master.client.AddServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.ChangeShardCountRequest;
import vk.itmo.teamgray.sharded.storage.master.client.DeleteServerRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetServerToShardResponse;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashRequest;
import vk.itmo.teamgray.sharded.storage.master.client.GetShardToHashResponse;
import vk.itmo.teamgray.sharded.storage.master.client.MasterClientServiceGrpc;
import vk.itmo.teamgray.sharded.storage.master.client.MasterHeartbeatRequest;

public class MasterClient extends AbstractGrpcClient<MasterClientServiceGrpc.MasterClientServiceBlockingStub> {
    public MasterClient(String host, int port) {
        //TODO Do a normal host resolving instead of this abomination.
        super(host, port, "master");
    }

    @Override
    protected Function<ManagedChannel, MasterClientServiceGrpc.MasterClientServiceBlockingStub> getStubFactory() {
        return MasterClientServiceGrpc::newBlockingStub;
    }

    public AddServerResponseDTO addServer(String ip, int port, boolean forkNewInstance) {
        AddServerRequest request = AddServerRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .setForkNewInstance(forkNewInstance)
            .build();

        var response = blockingStub.addServer(request);
        return new AddServerResponseDTO(response.getMessage(), response.getSuccess());
    }

    public DeleteServerResponseDTO deleteServer(String ip, int port) {
        DeleteServerRequest request = DeleteServerRequest.newBuilder()
            .setIp(ip)
            .setPort(port)
            .build();

        var response = blockingStub.deleteServer(request);
        return new DeleteServerResponseDTO(response.getMessage(), response.getSuccess());
    }

    public MasterHeartbeatResponseDTO sendHeartbeat() {
        var response = blockingStub
            .heartbeat(
                MasterHeartbeatRequest.newBuilder()
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build()
            );
        return new MasterHeartbeatResponseDTO(
            response.getHealthy(),
            response.getServerTimestamp(),
            response.getStatusMessage()
        );
    }

    //Doing map flipping on client side to unload master.
    public Map<Integer, String> getShardToServerMap() {
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

    public ChangeShardCountResponseDTO changeShardCount(int newShardCount) {
        ChangeShardCountRequest request = ChangeShardCountRequest.newBuilder()
            .setNewShardCount(newShardCount)
            .build();

        var response = blockingStub.changeShardCount(request);
        return new ChangeShardCountResponseDTO(response.getMessage(), response.getSuccess());
    }
}
