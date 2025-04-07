package vk.itmo.teamgray.sharded.storage.master.client.topology;

public record AddServerResult(
    boolean created,
    String message
) {
}
