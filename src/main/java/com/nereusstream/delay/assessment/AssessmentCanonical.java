package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

final class AssessmentCanonical {
    static final Comparator<String> UTF8_ORDER = AssessmentCanonical::compareUtf8;

    private AssessmentCanonical() {}

    static String text(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8 without NUL");
        }
        return value;
    }

    static List<String> sortedUniqueText(final List<String> values, final String name) {
        Objects.requireNonNull(values, name);
        final List<String> result = new ArrayList<>(values.size());
        for (String value : values) {
            result.add(text(value, name));
        }
        result.sort(UTF8_ORDER);
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).equals(result.get(index))) {
                throw new IllegalArgumentException("duplicate " + name + ": " + result.get(index));
            }
        }
        return List.copyOf(result);
    }

    static byte[] digest(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        final byte[] result = Bytes.copy(value);
        for (byte element : result) {
            if (element != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    static String hex(final byte[] value) {
        return Bytes.hex(value);
    }

    static String quote(final String value) {
        final String canonical = text(value, "JSON string");
        final StringBuilder result = new StringBuilder(canonical.length() + 2).append('"');
        for (int index = 0; index < canonical.length(); index++) {
            final char current = canonical.charAt(index);
            switch (current) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\b' -> result.append("\\b");
                case '\f' -> result.append("\\f");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (current < 0x20) {
                        result.append("\\u00");
                        final String hex = Integer.toHexString(current);
                        if (hex.length() == 1) {
                            result.append('0');
                        }
                        result.append(hex);
                    } else {
                        result.append(current);
                    }
                }
            }
        }
        return result.append('"').toString();
    }

    static String strings(final List<String> values) {
        final StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < values.size(); index++) {
            if (index != 0) {
                result.append(',');
            }
            result.append(quote(values.get(index)));
        }
        return result.append(']').toString();
    }

    private static int compareUtf8(final String left, final String right) {
        final byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        final byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        final int length = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < length; index++) {
            final int comparison =
                    Integer.compare(Byte.toUnsignedInt(leftBytes[index]), Byte.toUnsignedInt(rightBytes[index]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }
}
