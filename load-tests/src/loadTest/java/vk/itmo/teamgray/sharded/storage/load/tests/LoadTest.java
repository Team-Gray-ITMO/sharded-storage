package vk.itmo.teamgray.sharded.storage.load.tests;

import org.junit.jupiter.api.Test;
import vk.itmo.teamgray.sharded.storage.test.api.BaseIntegrationTest;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoadTest extends BaseIntegrationTest {
    @Test
    public void test_ChangeShardCnt_10000() {
        orchestrationApi.runDiscovery();
        orchestrationApi.runMaster();

        orchestrationApi.runNode(1);
        orchestrationApi.runNode(2);
        orchestrationApi.runNode(3);

        // Change shard count multiple times
        Random random = new Random();
        for (int i = 0; i < 10_000; i++) {
            int newCount = random.nextInt(3, 100);
            clientService.changeShardCount(newCount);
            assertEquals(newCount, clientService.getTotalShardCount());
            if (i % 10 == 0) {
                System.out.println(MessageFormat.format("{0,number,#},{1,number,#}", i, System.nanoTime()));
            }
        }

        orchestrationApi.stopNode(3);
        orchestrationApi.stopNode(2);
        orchestrationApi.stopNode(1);

        orchestrationApi.stopMaster();
        orchestrationApi.stopDiscovery();
    }

    @Test
    public void test_Add_100000() throws InterruptedException {
        orchestrationApi.runDiscovery();
        orchestrationApi.runMaster();

        orchestrationApi.runNode(1);
        orchestrationApi.runNode(2);
        orchestrationApi.runNode(3);
        orchestrationApi.runNode(4);
        orchestrationApi.runNode(5);

        Thread.sleep(1000L);

        // Change shard count multiple times
        clientService.changeShardCount(24);
        clientService.addServer(1, false);
        clientService.addServer(2, false);
        clientService.addServer(3, false);
        clientService.addServer(4, false);
        clientService.addServer(5, false);
        for (int i = 0; i < 10_000; i++) {
            clientService.setValue(MessageFormat.format("key{0,number,#}", i), MessageFormat.format("value{0,number,#}", i));
            if (i % 10 == 0) {
                System.out.println(MessageFormat.format("{0,number,#},{1,number,#}", i, System.nanoTime()));
            }
        }

        orchestrationApi.stopNode(3);
        orchestrationApi.stopNode(2);
        orchestrationApi.stopNode(1);

        orchestrationApi.stopMaster();
        orchestrationApi.stopDiscovery();
    }

    @Test
    public void test_Add_Get_100000_Parallel() throws ExecutionException, InterruptedException {
        orchestrationApi.runDiscovery();
        orchestrationApi.runMaster();

        orchestrationApi.runNode(1);
        orchestrationApi.runNode(2);
        orchestrationApi.runNode(3);
        orchestrationApi.runNode(4);
        orchestrationApi.runNode(5);

        Thread.sleep(1000L);

        // Change shard count multiple times
        clientService.changeShardCount(24);
        clientService.addServer(1, false);
        clientService.addServer(2, false);
        clientService.addServer(3, false);
        clientService.addServer(4, false);
        clientService.addServer(5, false);
        List<String> read = new ArrayList<>();
        List<String> write = new ArrayList<>();
        var setFuture = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 10_000; i++) {
                final String key = MessageFormat.format("key{0,number,#}", i);
                final String value = MessageFormat.format("value{0,number,#}", i);
                clientService.setValue(key, value);
                write.add(MessageFormat.format("{0,number,#},{1,number,#}", i, System.nanoTime()));
            }
        });
        var getFuture = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 10_000; i++) {
                final String key = MessageFormat.format("key{0,number,#}", i);
                clientService.getValue(key);
                read.add(MessageFormat.format("{0,number,#},{1,number,#}", i, System.nanoTime()));
            }
        });

        setFuture.get();
        getFuture.get();

        for (int i = 0; i < 10_000; i++) {
            System.out.println(write.get(i));
        }

        for (int i = 0; i < 10_000; i++) {
            System.out.println(read.get(i));
        }

        orchestrationApi.stopNode(3);
        orchestrationApi.stopNode(2);
        orchestrationApi.stopNode(1);

        orchestrationApi.stopMaster();
        orchestrationApi.stopDiscovery();
    }
}
