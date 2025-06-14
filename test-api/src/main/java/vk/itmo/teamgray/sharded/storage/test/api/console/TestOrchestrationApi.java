package vk.itmo.teamgray.sharded.storage.test.api.console;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static vk.itmo.teamgray.sharded.storage.test.api.console.ConsoleUtils.runAtPathOrFail;

public class TestOrchestrationApi {
    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");

    private static final Path PROJECT_ROOT = getProjectRoot();

    private static final Path SCRIPTS_PATH = getProjectRoot().resolve("scripts");

    private static final Logger log = LoggerFactory.getLogger(TestOrchestrationApi.class);

    public void buildProject() {
        log.info("Building project");

        ConsoleUtils.runAtPathOrFail(
            List.of("docker", "buildx", "bake"),
            PROJECT_ROOT,
            "Could not build project"
        );

        log.info("Built project");
    }

    public void runDiscovery() {
        log.info("Starting discovery");

        runDockerComposeUp("discovery");

        log.info("Started discovery");
    }

    public void runMaster() {
        log.info("Starting master");

        runDockerComposeUp("master");

        log.info("Started master");
    }

    public void runNode(int id) {
        log.info("Starting node: {}", id);

        runScriptAtPath(
            "run-node",
            List.of(String.valueOf(id)),
            "Could not run node"
        );

        log.info("Started node: {}", id);
    }

    public void stopDiscovery() {
        log.info("Stopping discovery");

        runDockerComposeDown("discovery");

        log.info("Stopped discovery");
    }

    public void stopMaster() {
        log.info("Stopping master");

        runDockerComposeDown("master");

        log.info("Stopped master");
    }

    public void stopNode(int id) {
        log.info("Stopping node: {}", id);

        runScriptAtPath(
            "stop-node",
            List.of(String.valueOf(id)),
            "Could not stop node"
        );

        log.info("Stopped node: {}", id);
    }

    public void purge() {
        log.info("Purging");

        runScriptAtPath(
            "stop-all-nodes",
            "Could not purge"
        );

        log.info("Purged");
    }

    private static void runDockerComposeUp() {
        runDockerComposeUp(null);
    }

    private static void runDockerComposeUp(String service) {
        List<String> command = Stream.of("docker-compose", "up", service, "-d")
            .filter(Objects::nonNull)
            .toList();

        ConsoleUtils.runAtPathOrFail(
            command,
            PROJECT_ROOT,
            "Could not start docker compose " + service
        );
    }

    private static void runDockerComposeDownV() {
        runDockerComposeDown(List.of("-v"));
    }

    private static void runDockerComposeDown(List<String> additionalArguments) {
        runDockerComposeDown(null, additionalArguments);
    }

    private static void runDockerComposeDown(String service) {
        runDockerComposeDown(service, List.of());
    }

    private static void runDockerComposeDown(String service, List<String> additionalArguments) {
        List<String> command = Stream.concat(
                Stream.of("docker-compose", "down", service),
                additionalArguments.stream()
            )
            .filter(Objects::nonNull)
            .toList();

        ConsoleUtils.runAtPathOrFail(
            command,
            PROJECT_ROOT,
            "Could not stop docker compose " + service
        );
    }

    private static void runScriptAtPath(String scriptName, String failMessage) {
        runScriptAtPath(scriptName, List.of(), failMessage);
    }

    private static void runScriptAtPath(String scriptName, List<String> arguments, String failMessage) {
        List<String> commands = new ArrayList<>();

        if (IS_WINDOWS) {
            commands.add("cmd.exe");
            commands.add("/c");
        } else {
            commands.add("bash");
        }

        commands.add(SCRIPTS_PATH.resolve(getScript(scriptName)).toString());

        commands.addAll(arguments);

        runAtPathOrFail(commands, PROJECT_ROOT, failMessage);
    }

    private static String getScript(String scriptName) {
        return scriptName + (IS_WINDOWS ? ".bat" : ".sh");
    }

    private static Path getProjectRoot() {
        return Paths.get("").toAbsolutePath().getParent();
    }
}
