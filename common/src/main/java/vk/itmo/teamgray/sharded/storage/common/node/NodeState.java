package vk.itmo.teamgray.sharded.storage.common.node;

import vk.itmo.teamgray.sharded.storage.common.exception.NodeException;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum NodeState {
    INIT,
    RUNNING,
    REARRANGE_SHARDS_PREPARING(Action.REARRANGE_SHARDS, ActionPhase.PREPARE, false),
    REARRANGE_SHARDS_PREPARED(Action.REARRANGE_SHARDS, ActionPhase.PREPARE, true),
    REARRANGE_SHARDS_PROCESSING(Action.REARRANGE_SHARDS, ActionPhase.PROCESS, false),
    REARRANGE_SHARDS_PROCESSED(Action.REARRANGE_SHARDS, ActionPhase.PROCESS, true),
    REARRANGE_SHARDS_APPLYING(Action.REARRANGE_SHARDS, ActionPhase.APPLY),
    REARRANGE_SHARDS_ROLLING_BACK(Action.REARRANGE_SHARDS, ActionPhase.ROLLBACK),
    MOVE_SHARDS_PREPARING(Action.MOVE_SHARDS, ActionPhase.PREPARE, false),
    MOVE_SHARDS_PREPARED(Action.MOVE_SHARDS, ActionPhase.PREPARE, true),
    MOVE_SHARDS_PROCESSING(Action.MOVE_SHARDS, ActionPhase.PROCESS, false),
    MOVE_SHARDS_PROCESSED(Action.MOVE_SHARDS, ActionPhase.PROCESS, true),
    MOVE_SHARDS_APPLYING(Action.MOVE_SHARDS, ActionPhase.APPLY),
    MOVE_SHARDS_ROLLING_BACK(Action.MOVE_SHARDS, ActionPhase.ROLLBACK),
    DEAD;

    private final Action action;

    private final ActionPhase actionPhase;

    private final Boolean phaseFinalized;

    private record Key(Action action, ActionPhase actionPhase, Boolean phaseFinalized) {
        // No-op.
    }

    private static final Map<Key, NodeState> STATE_BY_PARAMS = Arrays.stream(values())
            .filter(s -> s.getAction() != null || s.getActionPhase() != null || s.getPhaseFinalized() != null)
            .collect(Collectors.toMap(
                    s -> new Key(s.getAction(), s.getActionPhase(), s.getPhaseFinalized()),
                    s -> s
            ));

    NodeState(Action action, ActionPhase actionPhase, Boolean phaseFinalized) {
        this.action = action;
        this.actionPhase = actionPhase;
        this.phaseFinalized = phaseFinalized;
    }

    NodeState(Action action, ActionPhase actionPhase) {
        this(action, actionPhase, null);
    }

    NodeState() {
        this(null, null, null);
    }

    public static NodeState resolve(Action action, ActionPhase actionPhase, Boolean phaseFinalized) {
        var state = STATE_BY_PARAMS.get(new Key(action, actionPhase, phaseFinalized));

        if (state == null) {
            throw new NodeException(
                    "Could not resolve state from action " + action + " phase " + actionPhase + " finalized " + phaseFinalized
            );
        }

        return state;
    }

    public static NodeState resolve(Action action, ActionPhase actionPhase) {
        return resolve(action, actionPhase, null);
    }

    public Action getAction() {
        return action;
    }

    public ActionPhase getActionPhase() {
        return actionPhase;
    }

    public Boolean getPhaseFinalized() {
        return phaseFinalized;
    }
}
