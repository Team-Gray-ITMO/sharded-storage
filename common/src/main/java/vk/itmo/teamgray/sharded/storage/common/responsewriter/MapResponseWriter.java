package vk.itmo.teamgray.sharded.storage.common.responsewriter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@FunctionalInterface
public interface MapResponseWriter<K, V> {
    void writeMapEntry(K key, V value);

    class Helper {
        private Helper() {
            // No-op.
        }

        public static <K, V> MapResponseWriter<K, V> voidRw() {
            return (success, message) -> {
                // No-op.
            };
        }

        public static <K, V> Map<K, V> toMap(Consumer<MapResponseWriter<K, V>> rwConsumer) {
            Map<K, V> map = new HashMap<>();

            rwConsumer.accept(map::put);

            return map;
        }
    }
}
