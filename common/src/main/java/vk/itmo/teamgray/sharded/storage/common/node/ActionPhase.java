package vk.itmo.teamgray.sharded.storage.common.node;

public enum ActionPhase {
    PREPARE,
    PROCESS,
    APPLY,
    ROLLBACK
}
