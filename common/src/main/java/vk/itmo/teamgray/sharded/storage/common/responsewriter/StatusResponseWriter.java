package vk.itmo.teamgray.sharded.storage.common.responsewriter;

import java.util.function.Consumer;
import vk.itmo.teamgray.sharded.storage.common.StatusResponse;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;

@FunctionalInterface
public interface StatusResponseWriter {
    void writeResponse(boolean success, String message);

    class Helper {
        private Helper() {
            // No-op.
        }

        public static StatusResponseWriter voidRw() {
            return (success, message) -> {
                // No-op.
            };
        }

        public static StatusResponseWriter fromGrpcBuilder(StatusResponse.Builder builder) {
            return (success, message) -> {
                builder.setMessage(message);
                builder.setSuccess(success);
            };
        }

        public static StatusResponseDTO toDto(Consumer<StatusResponseWriter> rwConsumer) {
            var dto = new StatusResponseDTO();

            rwConsumer.accept(
                (success, message) -> {
                    dto.setSuccess(success);
                    dto.setMessage(message);
                }
            );

            return dto;
        }
    }
}
