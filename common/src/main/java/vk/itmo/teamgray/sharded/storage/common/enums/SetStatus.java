package vk.itmo.teamgray.sharded.storage.common.enums;

public enum SetStatus {
    SUCCESS,
    ERROR,
    // Renamed, due to other actions now possible, other than rearrangement.
    IS_BUSY
}
