package vk.itmo.teamgray.sharded.storage.master.topology;

public record DeleteServerResult(
    boolean deleted,
    String message
) {
}
