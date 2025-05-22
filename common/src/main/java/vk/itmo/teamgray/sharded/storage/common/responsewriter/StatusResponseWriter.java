package vk.itmo.teamgray.sharded.storage.common.responsewriter;

@FunctionalInterface
public interface StatusResponseWriter {
    void writeResponse(boolean success, String message);
}
