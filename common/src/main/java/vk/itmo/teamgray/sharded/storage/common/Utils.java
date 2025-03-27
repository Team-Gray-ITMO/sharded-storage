package vk.itmo.teamgray.sharded.storage.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class Utils {
    private Utils() {
        // No-op.
    }

    public static Properties getProperties() {
        Properties properties = new Properties();

        try (InputStream input = Utils.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IOException("Properties file not found in classpath.");
            }

            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties", e);
        }

        return properties;
    }

    public static int getServerPort() {
        Properties properties = getProperties();

        return Optional.ofNullable(properties.getProperty("grpc.port"))
            .map(Integer::parseInt)
            .orElseThrow();
    }

    public static String getServerHost() {
        Properties properties = getProperties();

        return properties.getProperty("grpc.host");
    }
}
