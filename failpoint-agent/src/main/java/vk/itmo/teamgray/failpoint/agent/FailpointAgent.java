package vk.itmo.teamgray.failpoint.agent;

import io.grpc.BindableService;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.itmo.teamgray.failpoint.agent.proto.FailpointGrpcService;
import vk.itmo.teamgray.sharded.storage.common.proto.GrpcServerRunner;

public class FailpointAgent {
    private static final Logger log = LoggerFactory.getLogger(FailpointAgent.class);

    private FailpointAgent() {
        // No-op.
    }

    public static void premain(String agentArgs, Instrumentation inst) {
        new AgentBuilder.Default()
            .type(
                ElementMatchers.nameStartsWith("vk.itmo.teamgray.sharded.storage")
            )
            .transform(
                (builder, typeDescription, classLoader, javaModule, protectionDomain) ->
                    builder.visit(Advice.to(FailpointAdvice.class).on(ElementMatchers.isMethod()))
            )
            .installOn(inst);

        tryInjectFailpointService();
    }

    public static void tryInjectFailpointService() {
        try {
            Optional<Object> maybeRunner = findRunnerInstance();

            if (maybeRunner.isEmpty()) {
                throw new IllegalStateException("GrpcServerRunner instance not found.");
            }

            Object runner = maybeRunner.get();

            Method registerMethod = runner.getClass().getMethod("registerService", BindableService.class);

            BindableService failpointService = new FailpointGrpcService();

            registerMethod.invoke(runner, failpointService);

            log.info("Injected FailpointGrpcService into GrpcServerRunner.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject FailpointGrpcService", e);
        }
    }

    private static Optional<Object> findRunnerInstance() {
        try {
            Class<?> runnerClass = Class.forName(GrpcServerRunner.class.getCanonicalName());

            try {
                Field instanceField = runnerClass.getDeclaredField("INSTANCE");
                instanceField.setAccessible(true);
                Object instance = instanceField.get(null);
                return Optional.ofNullable(instance);
            } catch (NoSuchFieldException e) {
                return Optional.empty();
            }

        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
