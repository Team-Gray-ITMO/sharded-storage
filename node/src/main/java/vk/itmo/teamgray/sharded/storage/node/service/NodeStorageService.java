package vk.itmo.teamgray.sharded.storage.node.service;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.sharded.storage.common.concurrency.AtomicEnum;
import vk.itmo.teamgray.sharded.storage.common.dto.FragmentDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.NodeStatusResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SendShardTaskDTO;
import vk.itmo.teamgray.sharded.storage.common.dto.SetResponseDTO;
import vk.itmo.teamgray.sharded.storage.common.enums.SetStatus;
import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;
import vk.itmo.teamgray.sharded.storage.common.node.Action;
import vk.itmo.teamgray.sharded.storage.common.node.ActionPhase;
import vk.itmo.teamgray.sharded.storage.common.node.NodeState;
import vk.itmo.teamgray.sharded.storage.common.utils.HashingUtils;
import vk.itmo.teamgray.sharded.storage.common.utils.ShardUtils;
import vk.itmo.teamgray.sharded.storage.node.service.shards.ShardData;

import static java.util.stream.Collectors.toMap;

public class NodeStorageService {
    private static final Logger log = LoggerFactory.getLogger(NodeStorageService.class);

    private final AtomicEnum<NodeState> state = new AtomicEnum<>(NodeState.INIT);

    private ShardsContainer shards = new ShardsContainer(0);

    private ShardsContainer stagedShards;

    private Queues queues;

    private PreparedData preparedData;

    public ShardsContainer getShards() {
        return shards;
    }

    public ShardsContainer getStagedShards() {
        return stagedShards;
    }

    public SetResponseDTO set(String key, String value, Instant timestamp) {
        var startState = state.get();

        //This means that we either resharding or moving shards
        if (startState.getAction() != null) {

            // Optimistic write, if state will move forward after preparation, we return transfer.
            if (startState.getActionPhase() == ActionPhase.PREPARE) {
                shards.set(key, value);

                var newState = state.get();
                if (newState.getActionPhase() == ActionPhase.PREPARE) {
                    return new SetResponseDTO(SetStatus.SUCCESS, "Added entry to storage.");
                }
            }

            // If target is this node, we queue it for later.
            if (stagedShards.hasShardForKey(key)) {
                queues.add(ActionPhase.APPLY, new QueueEntry(timestamp, key, value));

                return new SetResponseDTO(SetStatus.QUEUED, "Entry queued and will be applied later.");
            } else {
                // Add to rollback queue and apply in case of a rollback, so that data won't be lost.
                queues.add(ActionPhase.ROLLBACK, new QueueEntry(timestamp, key, value));

                Integer newNodeId = findPreparedServerIdByKey(key);

                if (newNodeId == null) {
                    throw new NodeException("Could not find a new node for moved key: " + key);
                }

                return new SetResponseDTO(SetStatus.TRANSFER, "Entry should be applied to another node.", newNodeId);
            }
        }

        if (state.get() != NodeState.RUNNING) {
            throw new NodeException("Node is frozen, cannot perform operations");
        }

        log.debug("Setting key {} to {}", key, value);

        shards.set(key, value);

        return new SetResponseDTO(SetStatus.SUCCESS, "Added entry to storage.");
    }

    public String get(String key) {
        log.debug("Getting value for key {}", key);

        return shards.get(key);
    }

    public void stageShards(Map<Integer, ShardData> stagedShards, int stagedFullShardCount) {
        this.stagedShards = new ShardsContainer(stagedShards, stagedFullShardCount);
    }

    public void swapWithStaged() {
        log.info("Replacing shard scheme {}", stagedShards);

        shards = stagedShards;

        log.info("Replaced shard scheme.");
    }

    public void processQueue(ActionPhase phase) {
        var applyQueue = queues.getQueue(phase);

        log.info("Processing {} queue. Size: {}", phase, applyQueue.size());

        processQueue(applyQueue);

        log.info("Processed {} queue.", phase);
    }

    public void clear() {
        preparedData = null;
        queues = null;
        stagedShards = null;
    }

    public NodeState getState() {
        return state.get();
    }

    public List<FragmentDTO> getPreparedFragments() {
        if (preparedData == null || preparedData.getAction() != Action.REARRANGE_SHARDS
        ) {
            throw new IllegalStateException("Fragments were not prepared");
        }

        return preparedData.getPreparedFragments();
    }

    public Map<Integer, Integer> getPreparedServerByShardNumber(Action action) {
        if (preparedData == null || preparedData.getAction() != action) {
            throw new IllegalStateException("Server and shard data was not prepared");
        }

        return preparedData.getPreparedServerByShardNumber();
    }

    public void prepareDataForResharding(List<FragmentDTO> fragments, Map<Integer, Integer> serverByShardNumber) {
        this.preparedData = new PreparedData();

        preparedData.setAction(Action.REARRANGE_SHARDS);

        // Sorting for future searches.
        preparedData.setPreparedFragments(
            fragments.stream()
                .sorted(Comparator.comparing(FragmentDTO::rangeTo))
                .toList()
        );
        preparedData.setPreparedServerByShardNumber(serverByShardNumber);
    }

    public void prepareDataForMoving(List<SendShardTaskDTO> sendShards) {
        this.preparedData = new PreparedData();

        preparedData.setAction(Action.MOVE_SHARDS);

        preparedData.setPreparedServerByShardNumber(
            sendShards.stream()
                .collect(toMap(
                    SendShardTaskDTO::shardId,
                    SendShardTaskDTO::targetServer
                ))
        );
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

    public NodeStatusResponseDTO getNodeStatus() {
        var dto = new NodeStatusResponseDTO();

        dto.setState(state.get());

        dto.setShardStats(
            shards.getShardMap().entrySet().stream()
                .collect(
                    toMap(
                        Map.Entry::getKey,
                        kv -> kv.getValue().getShardStats()
                    )
                )
        );

        dto.setShardStats(
            stagedShards == null
                ? Collections.emptyMap()
                : stagedShards.getShardMap().entrySet().stream()
                    .collect(
                        toMap(
                            Map.Entry::getKey,
                            kv -> kv.getValue().getShardStats()
                        )
                    )
        );

        dto.setApplyQueueSize(
            queues == null
                ? 0
                : queues.getQueue(ActionPhase.APPLY).size()
        );
        dto.setRollbackQueueSize(
            queues == null
                ? 0
                : queues.getQueue(ActionPhase.ROLLBACK).size()
        );

        return dto;
    }

    private Integer findPreparedServerIdByKey(String key) {
        Integer preparedShard = null;

        Action action = preparedData.getAction();

        if (action == Action.REARRANGE_SHARDS) {
            var hash = HashingUtils.calculate64BitHash(key);

            for (FragmentDTO fragment : preparedData.getPreparedFragments()) {
                if (fragment.rangeTo() >= hash && fragment.rangeFrom() < hash) {
                    preparedShard = fragment.newShardId();
                }
            }
        } else if (action != Action.MOVE_SHARDS) {
            preparedShard = ShardUtils.getShardIdForKey(key, shards.getFullShardCount());
        }

        if (preparedShard == null) {
            return null;
        } else {
            return preparedData
                .getPreparedServerByShardNumber()
                .get(preparedShard);
        }
    }

    private void processQueue(Queue<QueueEntry> queueToProcess) {
        queueToProcess.stream()
            .sorted(Comparator.comparing(QueueEntry::timestamp))
            .forEach(queueEntry -> shards.set(queueEntry.key(), queueEntry.value()));
    }

    private record QueueEntry(
        Instant timestamp,
        String key,
        String value
    ) {
        // No-op.
    }

    private static class Queues {
        // Queues of inserted entries, that are not processed, due to Resharding or moving shards. Order is not that relevant, given that we have to sort by timestamps in the end.
        private Queue<QueueEntry> applyQueue = new ConcurrentLinkedQueue<>();

        private Queue<QueueEntry> rollbackQueue = new ConcurrentLinkedQueue<>();

        public void add(ActionPhase phase, QueueEntry entry) {
            getQueue(phase).add(entry);
        }

        public Queue<QueueEntry> getQueue(ActionPhase phase) {
            if (phase == ActionPhase.APPLY) {
                return applyQueue;
            } else if (phase == ActionPhase.ROLLBACK) {
                return rollbackQueue;
            } else {
                throw new IllegalStateException("Unexpected phase: " + phase);
            }
        }
    }

    private static class PreparedData {
        private Action action;

        private List<FragmentDTO> preparedFragments;

        private Map<Integer, Integer> preparedServerByShardNumber;

        public Action getAction() {
            return action;
        }

        public void setAction(Action action) {
            this.action = action;
        }

        public List<FragmentDTO> getPreparedFragments() {
            return preparedFragments;
        }

        public void setPreparedFragments(List<FragmentDTO> preparedFragments) {
            this.preparedFragments = preparedFragments;
        }

        public Map<Integer, Integer> getPreparedServerByShardNumber() {
            return preparedServerByShardNumber;
        }

        public void setPreparedServerByShardNumber(Map<Integer, Integer> preparedServerByShardNumber) {
            this.preparedServerByShardNumber = preparedServerByShardNumber;
        }
    }
}
