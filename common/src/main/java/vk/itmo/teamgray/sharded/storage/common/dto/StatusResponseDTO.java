package vk.itmo.teamgray.sharded.storage.common.dto;

import java.util.Objects;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;

public final class StatusResponseDTO {
    private boolean success;

    private String message;

    public StatusResponseDTO() {
        // No-op.
    }

    public StatusResponseDTO(
        boolean success,
        String message
    ) {
        this.success = success;
        this.message = message;
    }

    public StatusResponseDTO(StatusResponse grpc) {
        this(grpc.getSuccess(), grpc.getMessage());
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        var that = (StatusResponseDTO)obj;

        return this.success == that.success &&
            Objects.equals(this.message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, message);
    }

    @Override
    public String toString() {
        return "StatusResponseDTO[" +
            "success=" + success + ", " +
            "message=" + message + ']';
    }
}
