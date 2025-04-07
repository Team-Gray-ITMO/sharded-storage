package vk.itmo.teamgray.sharded.storage.master.client.topology;

public record DeleteServerResult(
    boolean deleted,
    String message
) {
}
