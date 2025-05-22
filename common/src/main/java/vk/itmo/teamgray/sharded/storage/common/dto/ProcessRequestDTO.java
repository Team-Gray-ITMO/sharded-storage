package vk.itmo.teamgray.sharded.storage.common.dto;

import vk.itmo.teamgray.sharded.storage.node.management.MoveFragment;
import vk.itmo.teamgray.sharded.storage.node.management.ProcessRequest;

import java.util.List;
import java.util.Map;

public record ProcessRequestDTO (List<MoveFragment> fragments, Map<Integer, Integer> serverByShardNumber) {

    public ProcessRequest toGrpc() {
        return ProcessRequest.newBuilder()
                .addAllFragments(fragments)
                .putAllServerByShardNumber(serverByShardNumber)
                .build();
    }

    public static ProcessRequestDTO fromGrpc(ProcessRequest grpc) {
        return new ProcessRequestDTO(
                grpc.getFragmentsList(),
                grpc.getServerByShardNumberMap()
        );
    }

}
