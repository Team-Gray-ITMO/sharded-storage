package vk.itmo.teamgray.sharded.storage.client;

import java.util.Map;
import java.util.Scanner;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceDTO;
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
            System.out.print("\nEnter command (help for list of commands): ");
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
                default -> System.out.println("Unknown command. Type 'help' for available commands.");
            }
        }

        scanner.close();
        CachedGrpcStubCreator.getInstance().shutdownAll();
    }

    private void printWelcomeMessage() {
        System.out.println("Welcome to Sharded Storage CLI!");
        System.out.println("Connected to:");
        System.out.println("  Master: " + clientService.getMasterHost() + ":" + clientService.getMasterPort());
        printHelp();
    }

    private void printHelp() {
        System.out.println("\nAvailable commands:");
        System.out.println("  help           - Show this help message");
        System.out.println("  get            - Get value by key");
        System.out.println("  set            - Set key-value pair");
        System.out.println("  setfile        - Set values from file");
        System.out.println("  addserver      - Add new server");
        System.out.println("  deleteserver   - Delete server");
        System.out.println("  changeshards   - Change number of shards");
        System.out.println("  topology       - Show current topology");
        System.out.println("  heartbeat      - Send heartbeat to both servers");
        System.out.println("  exit           - Exit the program");
    }

    private void handleGet() {
        System.out.print("Enter key: ");
        String key = scanner.nextLine().trim();
        try {
            String value = clientService.getValue(key);
            System.out.println("Value: " + value);
        } catch (Exception e) {
            System.err.println("Error getting value: " + e.getMessage());
        }
    }

    private void handleSet() {
        System.out.print("Enter key: ");
        String key = scanner.nextLine().trim();
        System.out.print("Enter value: ");
        String value = scanner.nextLine().trim();
        try {
            boolean success = clientService.setValue(key, value);
            System.out.println(success ? "Value set successfully" : "Failed to set value");
        } catch (Exception e) {
            System.err.println("Error setting value: " + e.getMessage());
        }
    }

    private void handleSetFromFile() {
        System.out.print("Enter file path: ");
        String filePath = scanner.nextLine().trim();
        try {
            var response = clientService.setFromFile(filePath);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error setting from file: " + e.getMessage());
        }
    }

    private void handleAddServer() {
        System.out.print("Enter server ID: ");
        int id = Integer.parseInt(scanner.nextLine().trim());
        System.out.print("Fork new instance? (y/n): ");
        boolean fork = scanner.nextLine().trim().equalsIgnoreCase("y");
        try {
            var response = clientService.addServer(id, fork);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error adding server: " + e.getMessage());
        }
    }

    private void handleDeleteServer() {
        System.out.print("Enter server ID: ");
        int id = Integer.parseInt(scanner.nextLine().trim());
        try {
            var response = clientService.deleteServer(id);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error deleting server: " + e.getMessage());
        }
    }

    private void handleChangeShardCount() {
        System.out.print("Enter new shard count: ");
        int newCount = Integer.parseInt(scanner.nextLine().trim());
        try {
            var response = clientService.changeShardCount(newCount);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error changing shard count: " + e.getMessage());
        }
    }

    private void handleGetTopology() {
        try {
            Map<Integer, DiscoverableServiceDTO> shardToServer = clientService.getShardServerMapping();
            Map<Long, Integer> hashToShard = clientService.getHashToShardMapping();
            
            System.out.println("\nShard to Server mapping:");
            shardToServer.forEach((shard, server) -> 
                System.out.println("  Shard " + shard + " -> " + server));
            
            System.out.println("\nHash to Shard mapping:");
            hashToShard.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(entry ->
                System.out.println("  Hash " + entry.getKey() + " -> Shard " + entry.getValue()));
        } catch (Exception e) {
            System.err.println("Error getting topology: " + e.getMessage());
        }
    }

    private void handleHeartbeat() {
        try {
            var masterResponse = clientService.sendMasterHeartbeat();
            
            System.out.println("\nMaster Server Heartbeat:");
            System.out.println("  Healthy: " + masterResponse.healthy());
            System.out.println("  Status: " + masterResponse.statusMessage());
        } catch (Exception e) {
            System.err.println("Error sending heartbeat: " + e.getMessage());
        }
    }
}
