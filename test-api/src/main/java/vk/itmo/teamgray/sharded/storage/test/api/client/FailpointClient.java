package vk.itmo.teamgray.sharded.storage.test.api.client;

import java.time.Duration;
import vk.itmo.teamgray.sharded.storage.common.client.Client;
import vk.itmo.teamgray.sharded.storage.common.dto.StatusResponseDTO;

public interface FailpointClient extends Client {
    StatusResponseDTO addFailpoint(Class<?> methodClass, String methodName, Class<?> exceptionClass);

    StatusResponseDTO removeFailpoint(Class<?> methodClass, String methodName);

    boolean hasFailpoint(Class<?> methodClass, String methodName);

    StatusResponseDTO freezeMethod(Class<?> methodClass, String methodName);

    StatusResponseDTO unfreezeMethod(Class<?> methodClass, String methodName);

    StatusResponseDTO awaitUnfreeze(Class<?> methodClass, String methodName);

    StatusResponseDTO freezeFor(Class<?> methodClass, String methodName, Duration duration);

    boolean isFrozen(Class<?> methodClass, String methodName);

    StatusResponseDTO clear();
}
