package vk.itmo.teamgray.sharded.storage.node.service;

public enum NodeState {
    INIT,
    RUNNING,
    REARRANGE_PREPARING,
    REARRANGE_PREPARED,
    REARRANGE_PROCESSING,
    REARRANGE_PROCESSED,
    REARRANGE_APPLYING,
    REARRANGE_ROLLING_BACK,
    DEAD
}
