package vk.itmo.teamgray.sharded.storage.node.service;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.concurrency.AtomicEnum;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

//TODO Move maps and full counts behind a decorators, incapsulate some of the methods from here into it.
public class NodeStorageService {
    private static final Logger log = LoggerFactory.getLogger(NodeStorageService.class);

    private final AtomicEnum<NodeState> state = new AtomicEnum<>(NodeState.INIT);

    private ShardsContainer shards = new ShardsContainer(0);

    private ShardsContainer stagedShards;

    public ShardsContainer getShards() {
        return shards;
    }

    public ShardsContainer getStagedShards() {
        return stagedShards;
    }

    public void set(String key, String value) {
        if (state.get() != NodeState.RUNNING) {
            throw new NodeException("Node is frozen, cannot perform operations");
        }

        //TODO Change to debug
        log.debug("Setting key {} to {}", key, value);

        shards.set(key, value);
    }

    public String get(String key) {
        //TODO Change to debug
        log.debug("Getting value for key {}", key);

        return shards.get(key);
    }

    public void stageShards(Map<Integer, ShardData> stagedShards, int stagedFullShardCount) {
        this.stagedShards = new ShardsContainer(stagedShards, stagedFullShardCount);
    }

    public void clearStaged() {
        stagedShards = null;
    }

    public void swapWithStaged() {
        log.info("Replacing shard scheme {}", stagedShards);

        shards = stagedShards;

        stagedShards = null;

        log.info("Replaced shard scheme.");
    }

    public NodeState getState() {
        return state.get();
    }

    public void changeState(NodeState expectedState, NodeState newState) {
        if (!state.compareAndSet(expectedState, newState)) {
            throw new NodeException("Could not change node state " + expectedState + "->" + newState + ". Actual state was " + state.get());
        }
    }

    public void changeState(List<NodeState> expectedStates, NodeState newState) {
        var iterator = expectedStates.iterator();

        boolean success = false;

        while (iterator.hasNext()) {
            var expectedState = iterator.next();

            if (state.compareAndSet(expectedState, newState)) {
                success = true;

                break;
            }
        }

        if (!success) {
            throw new NodeException(
                "Could not change node state " + expectedStates + "->" + newState + ". Actual state was " + state.get());
        }
    }
}
