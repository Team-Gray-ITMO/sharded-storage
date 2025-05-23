package vk.itmo.teamgray.sharded.storage.client;

import java.util.Map;
import java.util.Scanner;
import vk.itmo.teamgray.sharded.storage.client.service.ClientService;
import vk.itmo.teamgray.sharded.storage.common.discovery.dto.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.proto.CachedGrpcStubCreator;

public class CLI {
    private final ClientService clientService;

    private final Scanner scanner;

    public CLI(ClientService clientService) {
        this.clientService = clientService;
        this.scanner = new Scanner(System.in);
    }

    public void start() {
        printWelcomeMessage();
        boolean running = true;

        while (running) {
            print(System.lineSeparator() + "Enter command (help for list of commands): ");
            String command = scanner.nextLine().trim();

            switch (command.toLowerCase()) {
                case "help" -> printHelp();
                case "get" -> handleGet();
                case "set" -> handleSet();
                case "setfile" -> handleSetFromFile();
                case "addserver" -> handleAddServer();
                case "deleteserver" -> handleDeleteServer();
                case "changeshards" -> handleChangeShardCount();
                case "topology" -> handleGetTopology();
                case "heartbeat" -> handleHeartbeat();
                case "exit" -> running = false;
                default -> println("Unknown command. Type 'help' for available commands.");
            }
        }

        scanner.close();
        CachedGrpcStubCreator.getInstance().shutdownAll();
    }

    private void printWelcomeMessage() {
        println("Welcome to Sharded Storage CLI!");
        println("Connected to:");
        println("  Master: " + clientService.getMasterHost() + ":" + clientService.getMasterPort());
        printHelp();
    }

    private void printHelp() {
        println(System.lineSeparator() + "Available commands:");
        println("  help           - Show this help message");
        println("  get            - Get value by key");
        println("  set            - Set key-value pair");
        println("  setfile        - Set values from file");
        println("  addserver      - Add new server");
        println("  deleteserver   - Delete server");
        println("  changeshards   - Change number of shards");
        println("  topology       - Show current topology");
        println("  heartbeat      - Send heartbeat to both servers");
        println("  exit           - Exit the program");
    }

    private void handleGet() {
        print("Enter key: ");
        String key = scanner.nextLine().trim();
        try {
            String value = clientService.getValue(key);
            println("Value: " + value);
        } catch (Exception e) {
            errPrintln("Error getting value: " + e.getMessage());
        }
    }

    private void handleSet() {
        print("Enter key: ");
        String key = scanner.nextLine().trim();
        print("Enter value: ");
        String value = scanner.nextLine().trim();
        try {
            boolean success = clientService.setValue(key, value);
            println(success ? "Value set successfully" : "Failed to set value");
        } catch (Exception e) {
            errPrintln("Error setting value: " + e.getMessage());
        }
    }

    private void handleSetFromFile() {
        print("Enter file path: ");
        String filePath = scanner.nextLine().trim();
        try {
            var response = clientService.setFromFile(filePath);
            println(response.getMessage());
            println(response.isSuccess() ? "Success" : "Failed");
        } catch (Exception e) {
            errPrintln("Error setting from file: " + e.getMessage());
        }
    }

    private void handleAddServer() {
        print("Enter server ID: ");

        Integer id = parseIntSafely(scanner.nextLine().trim());

        if (id == null) {
            return;
        }

        print("Fork new instance? (y/n): ");
        boolean fork = scanner.nextLine().trim().equalsIgnoreCase("y");
        try {
            var response = clientService.addServer(id, fork);
            println(response.getMessage());
            println(response.isSuccess() ? "Success" : "Failed");
        } catch (Exception e) {
            errPrintln("Error adding server: " + e.getMessage());
        }
    }

    private void handleDeleteServer() {
        print("Enter server ID: ");
        Integer id = parseIntSafely(scanner.nextLine().trim());

        if (id == null) {
            return;
        }

        try {
            var response = clientService.deleteServer(id);
            println(response.getMessage());
            println(response.isSuccess() ? "Success" : "Failed");
        } catch (Exception e) {
            errPrintln("Error deleting server: " + e.getMessage());
        }
    }

    private void handleChangeShardCount() {
        print("Enter new shard count: ");
        Integer newCount = parseIntSafely(scanner.nextLine().trim());

        if (newCount == null) {
            return;
        }

        try {
            var response = clientService.changeShardCount(newCount);
            println(response.getMessage());
            println(response.isSuccess() ? "Success" : "Failed");
        } catch (Exception e) {
            errPrintln("Error changing shard count: " + e.getMessage());
        }
    }

    private void handleGetTopology() {
        try {
            Map<Integer, DiscoverableServiceDTO> shardToServer = clientService.getShardServerMapping();
            Map<Long, Integer> hashToShard = clientService.getHashToShardMapping();

            println(System.lineSeparator() + "Shard to Server mapping:");
            shardToServer.forEach((shard, server) ->
                println(
                    "  Shard " + shard + " -> ID: " + server.id() + " Host: " + server.host() + "/" + server.containerName()));

            println(System.lineSeparator() + "Hash to Shard mapping:");
            hashToShard.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(entry ->
                    println("  Hash " + entry.getKey() + " -> Shard " + entry.getValue()));
        } catch (Exception e) {
            errPrintln("Error getting topology: " + e.getMessage());
        }
    }

    private void handleHeartbeat() {
        try {
            var masterResponse = clientService.sendMasterHeartbeat();

            println(System.lineSeparator() + "Master Server Heartbeat:");
            println("  Healthy: " + masterResponse.healthy());
            println("  Status: " + masterResponse.statusMessage());
        } catch (Exception e) {
            errPrintln("Error sending heartbeat: " + e.getMessage());
        }
    }

    private Integer parseIntSafely(String line) {
        try {
            return Integer.parseInt(line);
        } catch (NumberFormatException e) {
            errPrintln("Not a valid number: '" + line + "'");

            return null;
        }
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private static void print(String string) {
        System.out.print(string);
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private static void println(String string) {
        System.out.println(string);
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private static void errPrintln(String string) {
        System.err.println(string);
    }
}
