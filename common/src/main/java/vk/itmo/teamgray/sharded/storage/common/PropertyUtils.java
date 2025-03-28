package vk.itmo.teamgray.sharded.storage.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class PropertyUtils {
    private PropertyUtils() {
        // No-op.
    }

    public static Properties getProperties() {
        Properties properties = new Properties();

        try (InputStream input = PropertyUtils.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IOException("Properties file not found in classpath.");
            }

            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties", e);
        }

        return properties;
    }

    //TODO Implement something more fancy later.
    public static int getServerPort(String serverType) {
        Properties properties = getProperties();

        return Optional.ofNullable(properties.getProperty(serverType + ".grpc.port"))
            .map(Integer::parseInt)
            .orElseThrow();
    }

    //TODO Implement something more fancy later.
    public static String getServerHost(String serverType) {
        Properties properties = getProperties();

        return properties.getProperty(serverType + ".grpc.host");
    }
}
