package vk.itmo.teamgray.sharded.storage.common.concurrency;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class AtomicEnum<E extends Enum<E>> {
    private final AtomicInteger atomicOrdinal;

    private final Map<Integer, E> ordinalMap;

    public AtomicEnum(E initialValue) {
        Objects.requireNonNull(initialValue, "Initial value cannot be null");

        this.atomicOrdinal = new AtomicInteger(initialValue.ordinal());

        this.ordinalMap = createOrdinalMap(initialValue.getDeclaringClass());
    }

    private static <E extends Enum<E>> Map<Integer, E> createOrdinalMap(Class<E> enumClass) {
        Map<Integer, E> map = new HashMap<>();

        for (E enumValue : EnumSet.allOf(enumClass)) {
            map.put(enumValue.ordinal(), enumValue);
        }

        return map;
    }

    public E get() {
        return ordinalMap.get(atomicOrdinal.get());
    }

    public void set(E newValue) {
        Objects.requireNonNull(newValue, "New value cannot be null");

        atomicOrdinal.set(newValue.ordinal());
    }

    public boolean compareAndSet(E expect, E update) {
        Objects.requireNonNull(update, "Update value cannot be null");

        int expectedOrdinal = expect == null
            ? -1
            : expect.ordinal();

        return atomicOrdinal.compareAndSet(expectedOrdinal, update.ordinal());
    }

    public E getAndSet(E newValue) {
        Objects.requireNonNull(newValue, "New value cannot be null");

        int oldOrdinal = atomicOrdinal.getAndSet(newValue.ordinal());

        return ordinalMap.get(oldOrdinal);
    }
}
