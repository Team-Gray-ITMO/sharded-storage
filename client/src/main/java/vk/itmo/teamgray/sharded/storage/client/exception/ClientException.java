package vk.itmo.teamgray.sharded.storage.client.exception;

public class ClientException extends RuntimeException {
    public ClientException(String message) {
        super(message);
    }
}
