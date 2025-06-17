package vk.itmo.teamgray.failpoint.agent;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.failpoint.agent.exception.FailpointException;
import vk.itmo.teamgray.sharded.storage.common.responsewriter.StatusResponseWriter;

public class FailpointRegistry {
    private record Method(Class<?> methodClass, String methodName) {
        public Method(String methodClass, String methodName) {
            this(tryLoadClass(methodClass), methodName);
        }
    }

    private record Freeze(CountDownLatch hitLatch, CountDownLatch unfreezeLatch) {
        // No-op.
    }

    private static final Logger log = LoggerFactory.getLogger(FailpointRegistry.class);

    private static final Map<Method, Class<? extends Exception>> failpoints = new ConcurrentHashMap<>();

    private static final Map<Method, Freeze> freezes = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private FailpointRegistry() {
        // No-op.
    }

    public static void addFailpoint(String methodClass, String methodName, String exceptionClass, StatusResponseWriter responseWriter) {
        try {
            log.info("Adding failpoint for method {}#{}", methodClass, methodName);

            failpoints.put(
                new Method(methodClass, methodName),
                tryLoadClass(exceptionClass)
            );

            responseWriter.writeResponse(true, "Failpoint added successfully");

            log.info("Added failpoint for method {}#{}", methodClass, methodName);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not add failpoint: " + e.getMessage());
        }
    }

    public static void removeFailpoint(String methodClass, String methodName, StatusResponseWriter responseWriter) {
        try {
            log.info("Removing failpoint for method {}#{}", methodClass, methodName);

            var removed = failpoints.remove(new Method(methodClass, methodName));

            if (removed == null) {
                responseWriter.writeResponse(false, "Failpoint does not exist");

                return;
            }

            responseWriter.writeResponse(true, "Failpoint removed successfully");

            log.info("Removed failpoint for method {}#{}", methodClass, methodName);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not remove failpoint: " + e.getMessage());
        }
    }

    public static Class<? extends Exception> getFailpoint(String methodClass, String methodName) {
        return failpoints.get(new Method(methodClass, methodName));
    }

    public static boolean hasFailpoint(String methodClass, String methodName) {
        log.info("Checking if method {}#{} has failpoint", methodClass, methodName);

        return failpoints.containsKey(new Method(methodClass, methodName));
    }

    public static void freeze(String methodClass, String methodName, StatusResponseWriter responseWriter) {
        try {
            log.info("Freezing method {}#{}", methodClass, methodName);

            freezes.computeIfAbsent(
                new Method(methodClass, methodName),
                m -> new Freeze(new CountDownLatch(1), new CountDownLatch(1))
            );

            responseWriter.writeResponse(true, "Method frozen successfully");

            log.info("Frozen method {}#{}", methodClass, methodName);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not freeze method: " + e.getMessage());
        }
    }

    public static void unfreeze(String methodClass, String methodName, StatusResponseWriter responseWriter) {
        try {
            log.info("Unfreezing method {}#{}", methodClass, methodName);

            Freeze freeze = freezes.remove(new Method(methodClass, methodName));

            if (freeze == null) {
                responseWriter.writeResponse(false, "Method is not frozen");

                return;
            }

            freeze.unfreezeLatch().countDown();

            responseWriter.writeResponse(true, "Method unfrozen successfully");

            log.info("Unfrozen method {}#{}", methodClass, methodName);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not unfreeze method: " + e.getMessage());
        }
    }

    public static void freezeFor(String methodClass, String methodName, Duration duration, StatusResponseWriter responseWriter) {
        try {
            log.info("Freezing method {}#{} for duration {}", methodClass, methodName, duration);

            CountDownLatch unfreezeLatch = new CountDownLatch(1);

            var failPoint = new Method(methodClass, methodName);

            freezes.put(failPoint, new Freeze(new CountDownLatch(1), unfreezeLatch));

            scheduler.schedule(
                () -> {
                    Freeze removed = freezes.remove(failPoint);

                    if (removed != null) {
                        removed.unfreezeLatch().countDown();
                    }
                },
                duration.toMillis(),
                TimeUnit.MILLISECONDS
            );

            responseWriter.writeResponse(true, "Method frozen for duration successfully");

            log.info("Frozen method {}#{} for duration {}", methodClass, methodName, duration);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not freeze method for duration: " + e.getMessage());
        }
    }

    public static void awaitFreezeHit(String methodClass, String methodName, StatusResponseWriter responseWriter) {
        try {
            log.info("Awaiting freeze to be hit for method {}#{}", methodClass, methodName);

            Freeze freeze = freezes.get(new Method(methodClass, methodName));

            if (freeze == null) {
                responseWriter.writeResponse(false, "Method is not frozen");

                return;
            }

            try {
                freeze.hitLatch().await();
            } catch (InterruptedException ignored) {
                // No-op.
            }

            responseWriter.writeResponse(true, "Freeze was hit successfully");

            log.info("Awaited freeze to be hit for method {}#{}", methodClass, methodName);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not await freeze to be hit: " + e.getMessage());
        }
    }

    public static void awaitUnfreeze(String methodClass, String methodName, StatusResponseWriter responseWriter) {
        try {
            log.info("Awaiting unfreeze for method {}#{}", methodClass, methodName);

            Freeze freeze = freezes.get(new Method(methodClass, methodName));

            if (freeze == null) {
                responseWriter.writeResponse(false, "Method is not frozen");

                return;
            }

            try {
                freeze.unfreezeLatch().await();
            } catch (InterruptedException ignored) {
                // No-op.
            }

            responseWriter.writeResponse(true, "Await unfreeze completed successfully");

            log.info("Awaited unfreeze for method {}#{}", methodClass, methodName);
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not await unfreeze: " + e.getMessage());
        }
    }

    public static void awaitUnfreezeForThreads(String methodClass, String methodName) {
        Method method = new Method(methodClass, methodName);

        if (!freezes.containsKey(method)) {
            return;
        }

        log.info("Frozen method {}#{} awaiting unfreeze", methodClass, methodName);

        Freeze freeze = freezes.get(method);

        if (freeze == null) {
            return;
        }

        // Signaling that any thread reached freeze.
        freeze.hitLatch().countDown();

        // Waiting for unfreeze.
        try {
            freeze.unfreezeLatch().await();
        } catch (InterruptedException ignored) {
            // No-op.
        }

        log.info("Frozen method {}#{} unfrozen", methodClass, methodName);
    }

    public static boolean isFrozen(String methodClass, String methodName) {
        log.info("Checking if method {}#{} is frozen", methodClass, methodName);

        return freezes.containsKey(new Method(methodClass, methodName));
    }

    public static void clear(StatusResponseWriter responseWriter) {
        try {
            freezes.clear();
            failpoints.clear();

            responseWriter.writeResponse(true, "FailpointRegistry cleared");
        } catch (Exception e) {
            responseWriter.writeResponse(false, "Could not clear FailpointRegistry:" + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> tryLoadClass(String canonicalName) {
        try {
            return (Class<T>)Class.forName(canonicalName, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            throw new FailpointException("Could not load class " + canonicalName + " for failpoint", e);
        }
    }
}
