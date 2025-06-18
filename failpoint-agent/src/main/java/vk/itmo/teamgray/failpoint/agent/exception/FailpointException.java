package vk.itmo.teamgray.failpoint.agent.exception;

public class FailpointException extends RuntimeException {
    public FailpointException(String message) {
        super(message);
    }

    public FailpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
