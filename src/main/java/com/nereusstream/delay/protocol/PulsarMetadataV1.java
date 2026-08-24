package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Canonical Pulsar metadata branch used by native and managed projections. */
public final class PulsarMetadataV1 {
    public enum KeyEncoding {
        UTF8(1),
        BASE64_BYTES(2);

        private final int wireValue;

        KeyEncoding(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static KeyEncoding fromWire(final long value) {
            for (KeyEncoding encoding : values()) {
                if (encoding.wireValue == value) {
                    return encoding;
                }
            }
            throw new IllegalArgumentException("unknown Pulsar metadata key encoding: " + value);
        }
    }

    private final byte[] partitionKey;
    private final KeyEncoding keyEncoding;
    private final byte[] orderingKey;
    private final List<Property> properties;

    public PulsarMetadataV1(
            final byte[] partitionKey,
            final KeyEncoding keyEncoding,
            final byte[] orderingKey,
            final List<Property> properties) {
        if ((partitionKey == null) != (keyEncoding == null)) {
            throw new IllegalArgumentException("Pulsar partition key and encoding must be present together");
        }
        this.partitionKey = partitionKey == null ? null : Bytes.copy(partitionKey);
        this.keyEncoding = keyEncoding;
        this.orderingKey = orderingKey == null ? null : Bytes.copy(orderingKey);
        Objects.requireNonNull(properties, "properties");
        final List<Property> copy = new ArrayList<>(properties);
        for (int index = 1; index < copy.size(); index++) {
            final int comparison =
                    compareKey(copy.get(index - 1).keyUtf8(), copy.get(index).keyUtf8());
            if (comparison >= 0) {
                throw new IllegalArgumentException("Pulsar properties must be strictly key-sorted and unique");
            }
        }
        this.properties = Collections.unmodifiableList(copy);
    }

    public byte[] partitionKey() {
        return partitionKey == null ? null : Bytes.copy(partitionKey);
    }

    public KeyEncoding keyEncoding() {
        return keyEncoding;
    }

    public byte[] orderingKey() {
        return orderingKey == null ? null : Bytes.copy(orderingKey);
    }

    public List<Property> properties() {
        return properties;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            if (partitionKey != null) {
                CanonicalProtobuf.bytes(output, 1, partitionKey);
                CanonicalProtobuf.uint32(output, 2, keyEncoding.wireValue());
            }
            if (orderingKey != null) {
                CanonicalProtobuf.bytes(output, 3, orderingKey);
            }
            for (Property property : properties) {
                CanonicalProtobuf.bytes(output, 4, property.canonicalBytes());
            }
        });
    }

    public static PulsarMetadataV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded);
        byte[] partitionKey = null;
        KeyEncoding keyEncoding = null;
        byte[] orderingKey = null;
        final List<Property> properties = new ArrayList<>();
        int index = 0;
        if (index < fields.size() && fields.get(index).number() == 1) {
            partitionKey = QueryCodecSupport.bytes(fields.get(index), 1);
            index++;
            if (index >= fields.size() || fields.get(index).number() != 2) {
                throw new IllegalArgumentException("Pulsar partition key is missing key encoding");
            }
            keyEncoding = KeyEncoding.fromWire(QueryCodecSupport.uint(fields.get(index), 2));
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 2) {
            throw new IllegalArgumentException("Pulsar metadata key encoding has no partition key");
        }
        if (index < fields.size() && fields.get(index).number() == 3) {
            orderingKey = QueryCodecSupport.bytes(fields.get(index), 3);
            index++;
        }
        while (index < fields.size()) {
            if (fields.get(index).number() != 4) {
                throw new IllegalArgumentException(
                        "unexpected Pulsar metadata field " + fields.get(index).number());
            }
            properties.add(Property.decode(QueryCodecSupport.nested(fields.get(index), 4)));
            index++;
        }
        final PulsarMetadataV1 result = new PulsarMetadataV1(partitionKey, keyEncoding, orderingKey, properties);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarMetadataV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PulsarMetadataV1 that)) {
            return false;
        }
        return Arrays.equals(partitionKey, that.partitionKey)
                && keyEncoding == that.keyEncoding
                && Arrays.equals(orderingKey, that.orderingKey)
                && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(partitionKey), keyEncoding, Arrays.hashCode(orderingKey), properties);
    }

    public static final class Property {
        private final byte[] keyUtf8;
        private final byte[] valueUtf8;

        public Property(final String key, final String value) {
            this(utf8Nfc(key, "property key", true), utf8Nfc(value, "property value", false));
        }

        public Property(final byte[] keyUtf8, final byte[] valueUtf8) {
            this.keyUtf8 = validateCallerMetadataName(keyUtf8, "property key");
            this.valueUtf8 = validateUtf8Nfc(valueUtf8, "property value", false);
        }

        public String key() {
            return new String(keyUtf8, StandardCharsets.UTF_8);
        }

        public String value() {
            return new String(valueUtf8, StandardCharsets.UTF_8);
        }

        public byte[] keyUtf8() {
            return Bytes.copy(keyUtf8);
        }

        public byte[] valueUtf8() {
            return Bytes.copy(valueUtf8);
        }

        private byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, keyUtf8);
                CanonicalProtobuf.bytes(output, 2, valueUtf8);
            });
        }

        private static Property decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PulsarPropertyV1");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "PulsarPropertyV1");
            final Property result =
                    new Property(QueryCodecSupport.bytes(fields.get(0), 1), QueryCodecSupport.bytes(fields.get(1), 2));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarPropertyV1");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Property that
                    && Arrays.equals(keyUtf8, that.keyUtf8)
                    && Arrays.equals(valueUtf8, that.valueUtf8);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(keyUtf8), Arrays.hashCode(valueUtf8));
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static int compareKey(final byte[] left, final byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }

    private static byte[] utf8Nfc(final String value, final String name, final boolean nonBlank) {
        Objects.requireNonNull(value, name);
        return validateUtf8Nfc(value.getBytes(StandardCharsets.UTF_8), name, nonBlank);
    }

    private static byte[] validateUtf8Nfc(final byte[] value, final String name, final boolean nonBlank) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || decoded.indexOf('\0') >= 0
                || !decoded.equals(Normalizer.normalize(decoded, Normalizer.Form.NFC))
                || (nonBlank && decoded.isBlank())) {
            throw new IllegalArgumentException(name + " must be valid UTF-8 NFC");
        }
        return Bytes.copy(value);
    }

    private static byte[] validateCallerMetadataName(final byte[] value, final String name) {
        final byte[] canonical = validateUtf8Nfc(value, name, true);
        if (new String(canonical, StandardCharsets.UTF_8).startsWith("nereus.delay.")) {
            throw new IllegalArgumentException(name + " uses a reserved Nereus metadata prefix");
        }
        return canonical;
    }
}
