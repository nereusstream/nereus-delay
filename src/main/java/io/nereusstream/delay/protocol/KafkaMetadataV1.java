package io.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Canonical Kafka metadata branch used by managed publish intents. */
public final class KafkaMetadataV1 {
    private final byte[] key;
    private final List<Header> headers;

    /**
     * @param key nullable; a non-null empty key is an intentional wire-level
     *             presence value
     */
    public KafkaMetadataV1(final byte[] key, final List<Header> headers) {
        this.key = key == null ? null : Bytes.copy(key);
        Objects.requireNonNull(headers, "headers");
        this.headers = Collections.unmodifiableList(new ArrayList<>(headers));
    }

    public byte[] key() {
        return key == null ? null : Bytes.copy(key);
    }

    public List<Header> headers() {
        return headers;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            if (key != null) {
                CanonicalProtobuf.bytes(output, 1, key);
            }
            for (Header header : headers) {
                CanonicalProtobuf.bytes(output, 2, header.canonicalBytes());
            }
        });
    }

    public static KafkaMetadataV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        byte[] key = null;
        final List<Header> headers = new ArrayList<>();
        int index = 0;
        if (index < fields.size() && fields.get(index).number() == 1) {
            key = QueryCodecSupport.bytes(fields.get(index++), 1);
        }
        while (index < fields.size()) {
            if (fields.get(index).number() != 2) {
                throw new IllegalArgumentException("unexpected Kafka metadata field "
                        + fields.get(index).number());
            }
            headers.add(Header.decode(QueryCodecSupport.nested(fields.get(index++), 2)));
        }
        final KafkaMetadataV1 result = new KafkaMetadataV1(key, headers);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "KafkaMetadataV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof KafkaMetadataV1 that && Arrays.equals(key, that.key)
                && headers.equals(that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), headers);
    }

    public static final class Header {
        private final byte[] nameUtf8;
        private final byte[] value;

        public Header(final String name, final byte[] value) {
            this(utf8Nfc(name, "header name", true), value);
        }

        public Header(final byte[] nameUtf8, final byte[] value) {
            this.nameUtf8 = validateCallerMetadataName(nameUtf8, "header name");
            this.value = Bytes.copy(Objects.requireNonNull(value, "header value"));
        }

        public String name() {
            return new String(nameUtf8, StandardCharsets.UTF_8);
        }

        public byte[] nameUtf8() {
            return Bytes.copy(nameUtf8);
        }

        public byte[] value() {
            return Bytes.copy(value);
        }

        private byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, nameUtf8);
                CanonicalProtobuf.bytes(output, 2, value);
            });
        }

        private static Header decode(final byte[] encoded) {
            final var fields = QueryCodecSupport.read(encoded, "KafkaHeaderV1");
            QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "KafkaHeaderV1");
            final Header result = new Header(QueryCodecSupport.bytes(fields.get(0), 1),
                    QueryCodecSupport.bytes(fields.get(1), 2));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "KafkaHeaderV1");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Header that && Arrays.equals(nameUtf8, that.nameUtf8)
                    && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(nameUtf8), Arrays.hashCode(value));
        }
    }

    private static byte[] utf8Nfc(final String value, final String name, final boolean nonEmpty) {
        Objects.requireNonNull(value, name);
        return validateUtf8Nfc(value.getBytes(StandardCharsets.UTF_8), name, nonEmpty);
    }

    private static byte[] validateUtf8Nfc(final byte[] value, final String name, final boolean nonEmpty) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || decoded.indexOf('\0') >= 0
                || (nonEmpty && decoded.isEmpty())
                || !Normalizer.normalize(decoded, Normalizer.Form.NFC).equals(decoded)) {
            throw new IllegalArgumentException(name + " must be canonical UTF-8 NFC");
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
