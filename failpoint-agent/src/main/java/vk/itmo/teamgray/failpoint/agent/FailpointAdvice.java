package vk.itmo.teamgray.failpoint.agent;

import java.lang.reflect.Constructor;
import net.bytebuddy.asm.Advice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailpointAdvice {
    private static final Logger log = LoggerFactory.getLogger(FailpointRegistry.class);

    private FailpointAdvice() {
        // No-op.
    }

    @Advice.OnMethodEnter
    public static void enter(@Advice.Origin("#t") String methodClass, @Advice.Origin("#m") String method) throws Exception {
        var exClass = FailpointRegistry.getFailpoint(methodClass, method);

        if (exClass != null) {
            var exception = getExceptionToThrow(methodClass, method, exClass);

            log.info("Throwing exception: {}", exception.getClass().getSimpleName());

            throw exception;
        }

        FailpointRegistry.awaitUnfreezeForThreads(methodClass, method);
    }

    private static Exception getExceptionToThrow(String methodClass, String method, Class<? extends Exception> exClass) {
        // First we try to throw a String constructor, then empty constructor, and as a last resort we throw a Runtime Exception.
        try {
            try {
                Constructor<? extends Exception> ctor = exClass.getConstructor(String.class);
                return ctor.newInstance("Injected failure at: " + methodClass + "#" + method);
            } catch (NoSuchMethodException e) {
                return exClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception reflectionFailure) {
            return new RuntimeException("Failed to load injected exception for: " + method, reflectionFailure);
        }
    }
}
