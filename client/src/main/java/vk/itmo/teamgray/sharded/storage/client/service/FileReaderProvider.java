package vk.itmo.teamgray.sharded.storage.client.service;

import java.io.BufferedReader;
import java.io.IOException;

@FunctionalInterface
public interface FileReaderProvider {
    BufferedReader getReader(String fileName) throws IOException;
}
