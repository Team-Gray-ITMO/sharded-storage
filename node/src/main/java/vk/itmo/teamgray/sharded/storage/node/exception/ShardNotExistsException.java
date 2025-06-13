package vk.itmo.teamgray.sharded.storage.node.exception;

public class ShardNotExistsException extends RuntimeException {
    public ShardNotExistsException(String message) {
        super(message);
    }
}
