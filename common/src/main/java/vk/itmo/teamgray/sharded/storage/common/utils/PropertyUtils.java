package vk.itmo.teamgray.sharded.storage.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceDTO;
import vk.itmo.teamgray.sharded.storage.common.discovery.DiscoverableServiceType;

public class PropertyUtils {
    private static final Properties SERVICE_PROPS = loadProperties("service.properties", false);

    private static final Properties APP_PROPS = loadProperties("application.properties", true);

    private PropertyUtils() {
        // No-op.
    }

    private static Properties loadProperties(String filename, boolean failIfNotFound) {
        Properties props = new Properties();

        try (InputStream input = PropertyUtils.class.getClassLoader().getResourceAsStream(filename)) {
            if (input == null) {
                if (failIfNotFound) {
                    throw new IOException("File " + filename + " not found in classpath.");
                } else {
                    return null;
                }
            }

            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from " + filename, e);
        }

        return props;
    }

    private static String envKeyFor(String key) {
        return key.toUpperCase()
            .replace(".", "_")
            .replace("-", "_");
    }

    private static String getNullableString(String key, Properties propertyFile) {
        String envValue = System.getenv(envKeyFor(key));

        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        return propertyFile.getProperty(key);
    }

    public static DiscoverableServiceDTO getDiscoverableService() {
        return new DiscoverableServiceDTO(
            Integer.parseInt(getNullableString("service.id", SERVICE_PROPS)),
            DiscoverableServiceType.valueOf(getNullableString("service.type", SERVICE_PROPS)),
            getNullableString("service.host", SERVICE_PROPS),
            getNullableString("service.container-name", SERVICE_PROPS)
        );
    }

    public static int getServerPort(String serverType) {
        return Optional.ofNullable(getNullableString(serverType + ".grpc.port", APP_PROPS))
            .map(Integer::parseInt)
            .orElseThrow(() -> new IllegalStateException("Port not specified for: " + serverType));
    }

    public static String getServerHost(String serverType) {
        return getNullableString(serverType + ".grpc.host", APP_PROPS);
    }
}

