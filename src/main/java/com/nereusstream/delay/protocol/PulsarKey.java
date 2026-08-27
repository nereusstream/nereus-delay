package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed Pulsar key union with byte-preserving UTF-8 and binary branches. */
public final class PulsarKey {
    public enum Kind {
        NONE(1),
        UTF8(2),
        BINARY(3);

        private final int wireValue;

        Kind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static Kind fromWire(final long value) {
            for (Kind kind : values()) {
                if (kind.wireValue == value) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown PulsarKey kind: " + value);
        }
    }

    private final Kind kind;
    private final byte[] value;

    private PulsarKey(final Kind kind, final byte[] value) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (kind == Kind.NONE) {
            if (value != null) {
                throw new IllegalArgumentException("PulsarKey.NONE cannot carry a value");
            }
            this.value = null;
        } else {
            Objects.requireNonNull(value, "value");
            if (value.length == 0) {
                throw new IllegalArgumentException("Pulsar key value must not be empty");
            }
            this.value = Bytes.copy(value);
            if (kind == Kind.UTF8) {
                validateUtf8Nfc(this.value);
            }
        }
    }

    public static PulsarKey none() {
        return new PulsarKey(Kind.NONE, null);
    }

    public static PulsarKey utf8(final String value) {
        Objects.requireNonNull(value, "value");
        return new PulsarKey(Kind.UTF8, value.getBytes(StandardCharsets.UTF_8));
    }

    public static PulsarKey utf8(final byte[] value) {
        return new PulsarKey(Kind.UTF8, value);
    }

    public static PulsarKey binary(final byte[] value) {
        return new PulsarKey(Kind.BINARY, value);
    }

    public Kind kind() {
        return kind;
    }

    public boolean isPresent() {
        return kind != Kind.NONE;
    }

    public byte[] value() {
        if (value == null) {
            throw new IllegalStateException("PulsarKey.NONE has no value");
        }
        return Bytes.copy(value);
    }

    public String utf8Value() {
        if (kind != Kind.UTF8) {
            throw new IllegalStateException("Pulsar key is not UTF8");
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    /** Canonical form is a kind enum followed by exactly one value for non-NONE branches. */
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (value != null) {
                CanonicalProtobuf.bytes(output, 2, value);
            }
        });
    }

    public static PulsarKey decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PulsarKey");
        if (fields.size() != 1 && fields.size() != 2) {
            throw new IllegalArgumentException("PulsarKey has an unexpected field count");
        }
        final Kind kind = Kind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final PulsarKey result;
        if (kind == Kind.NONE) {
            if (fields.size() != 1) {
                throw new IllegalArgumentException("PulsarKey.NONE has a value");
            }
            result = none();
        } else {
            if (fields.size() != 2) {
                throw new IllegalArgumentException("PulsarKey value is missing");
            }
            final byte[] value = QueryCodecSupport.bytes(fields.get(1), 2);
            result = kind == Kind.UTF8 ? utf8(value) : binary(value);
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarKey");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarKey that && kind == that.kind && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(value));
    }

    private static void validateUtf8Nfc(final byte[] value) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || decoded.indexOf('\0') >= 0
                || !decoded.equals(Normalizer.normalize(decoded, Normalizer.Form.NFC))
                || decoded.isEmpty()) {
            throw new IllegalArgumentException("Pulsar UTF8 key must be nonempty valid NFC UTF-8");
        }
    }
}
