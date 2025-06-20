package vk.itmo.teamgray.sharded.storage.common.utils;

import java.util.Locale;

public class MemoryUtils {
    public static final int KIBIBYTE = 1 << 10;        // 1024

    public static final int MEBIBYTE = KIBIBYTE << 10;        // 1024^2

    public static final int GIBIBYTE = MEBIBYTE << 10;         // 1024^4

    public static Long parseMemSize(String sizeStr) {
        if (sizeStr == null || sizeStr.isBlank()) {
            return null;
        }

        String s = sizeStr.trim().toLowerCase(Locale.ROOT);
        long multiplier = 1;

        if (s.endsWith("k")) {
            multiplier = KIBIBYTE;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            multiplier = MEBIBYTE;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("g")) {
            multiplier = GIBIBYTE;
            s = s.substring(0, s.length() - 1);
        }

        try {
            return Long.parseLong(s) * multiplier;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static int utf8Size(String s) {
        int count = 0;

        for (int i = 0; i < s.length(); i++) {
            int c = s.charAt(i);

            if (c <= 0x7F) {
                count += 1;
            } else if (c <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate((char)c)) {
                count += 4;
                i++;
            } else {
                count += 3;
            }
        }

        return count;
    }
}
