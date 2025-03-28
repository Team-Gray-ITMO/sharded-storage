package vk.itmo.teamgray.sharded.storage.master.topology;

public record AddServerResult(
    boolean created,
    String message
) {
}
