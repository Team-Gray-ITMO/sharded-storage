package vk.itmo.teamgray.sharded.storage.client;

import java.util.Map;
import java.util.Scanner;

public class CLI {
    private final NodeClient nodeClient;
    private final MasterClient masterClient;
    private final Scanner scanner;

    public CLI(NodeClient nodeClient, MasterClient masterClient) {
        this.nodeClient = nodeClient;
        this.masterClient = masterClient;
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
        try {
            nodeClient.shutdown();
            masterClient.shutdown();
        } catch (InterruptedException e) {
            System.err.println("Error while shutting down clients: " + e.getMessage());
        }
    }

    private void printWelcomeMessage() {
        System.out.println("Welcome to Sharded Storage CLI!");
        System.out.println("Connected to:");
        System.out.println("  Master: " + masterClient.getHost() + ":" + masterClient.getPort());
        System.out.println("  Node: " + nodeClient.getHost() + ":" + nodeClient.getPort());
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
            String value = nodeClient.getKey(key);
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
            boolean success = nodeClient.setKey(key, value);
            System.out.println(success ? "Value set successfully" : "Failed to set value");
        } catch (Exception e) {
            System.err.println("Error setting value: " + e.getMessage());
        }
    }

    private void handleSetFromFile() {
        System.out.print("Enter file path: ");
        String filePath = scanner.nextLine().trim();
        try {
            var response = nodeClient.setFromFile(filePath);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error setting from file: " + e.getMessage());
        }
    }

    private void handleAddServer() {
        System.out.print("Enter server IP: ");
        String ip = scanner.nextLine().trim();
        System.out.print("Enter server port: ");
        int port = Integer.parseInt(scanner.nextLine().trim());
        System.out.print("Fork new instance? (y/n): ");
        boolean fork = scanner.nextLine().trim().equalsIgnoreCase("y");
        try {
            var response = masterClient.addServer(ip, port, fork);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error adding server: " + e.getMessage());
        }
    }

    private void handleDeleteServer() {
        System.out.print("Enter server IP: ");
        String ip = scanner.nextLine().trim();
        System.out.print("Enter server port: ");
        int port = Integer.parseInt(scanner.nextLine().trim());
        try {
            var response = masterClient.deleteServer(ip, port);
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
            var response = masterClient.changeShardCount(newCount);
            System.out.println(response.message());
            System.out.println(response.success() ? "Success" : "Failed");
        } catch (Exception e) {
            System.err.println("Error changing shard count: " + e.getMessage());
        }
    }

    private void handleGetTopology() {
        try {
            Map<Integer, String> shardToServer = masterClient.getShardToServerMap();
            Map<Long, Integer> hashToShard = masterClient.getHashToShardMap();
            
            System.out.println("\nShard to Server mapping:");
            shardToServer.forEach((shard, server) -> 
                System.out.println("  Shard " + shard + " -> " + server));
            
            System.out.println("\nHash to Shard mapping:");
            hashToShard.forEach((hash, shard) -> 
                System.out.println("  Hash " + hash + " -> Shard " + shard));
        } catch (Exception e) {
            System.err.println("Error getting topology: " + e.getMessage());
        }
    }

    private void handleHeartbeat() {
        try {
            var masterResponse = masterClient.sendHeartbeat();
            var nodeResponse = nodeClient.sendHeartbeat();
            
            System.out.println("\nMaster Server Heartbeat:");
            System.out.println("  Healthy: " + masterResponse.healthy());
            System.out.println("  Status: " + masterResponse.statusMessage());
            
            System.out.println("\nNode Server Heartbeat:");
            System.out.println("  Healthy: " + nodeResponse.healthy());
            System.out.println("  Status: " + nodeResponse.statusMessage());
        } catch (Exception e) {
            System.err.println("Error sending heartbeat: " + e.getMessage());
        }
    }
} 