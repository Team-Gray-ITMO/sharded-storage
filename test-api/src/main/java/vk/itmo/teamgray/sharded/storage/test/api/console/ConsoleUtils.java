package vk.itmo.teamgray.sharded.storage.test.api.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleUtils {
    private static final Logger log = LoggerFactory.getLogger(ConsoleUtils.class);

    private ConsoleUtils() {
        // No-op.
    }

    public static void runAtPathOrFail(List<String> commands, Path path, String failMessage) {
        var exitCode = runAtPath(commands, path);

        if (exitCode != 0) {
            throw new RuntimeException(failMessage);
        }
    }

    public static int runAtPath(List<String> command, Path path) {
        return runProcess(
            new ProcessBuilder(command)
                .directory(path.toFile())
                .redirectErrorStream(true)
        );
    }

    private static int runProcess(ProcessBuilder processBuilder) {
        try {
            var process = processBuilder.start();

            new BufferedReader(
                new InputStreamReader(
                    process.getInputStream(),
                    StandardCharsets.UTF_8
                )
            )
                .lines()
                .forEachOrdered(log::info);

            process.waitFor();

            return process.exitValue();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
