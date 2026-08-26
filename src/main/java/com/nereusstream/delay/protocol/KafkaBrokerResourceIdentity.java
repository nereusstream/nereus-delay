package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Exact Kafka resource identity branch of BrokerResourceIdentity. */
public final class KafkaBrokerResourceIdentity {
    private final String authenticatedClusterId;
    private final UUID nativeTopicUuid;

    public KafkaBrokerResourceIdentity(final String authenticatedClusterId, final UUID nativeTopicUuid) {
        this.authenticatedClusterId = nfc(authenticatedClusterId, "authenticatedClusterId");
        this.nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
    }

    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public UUID nativeTopicUuid() {
        return nativeTopicUuid;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, authenticatedClusterId.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.bytes(fields, 2, uuidBytes(nativeTopicUuid));
                })));
    }

    public static KafkaBrokerResourceIdentity decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = QueryCodecSupport.read(encoded, "BrokerResourceIdentity");
        QueryCodecSupport.requireNumbers(outer, new int[] {1}, "BrokerResourceIdentity");
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(QueryCodecSupport.nested(outer.get(0), 1), "KafkaResourceIdentity");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "KafkaResourceIdentity");
        final KafkaBrokerResourceIdentity result = new KafkaBrokerResourceIdentity(
                utf8(QueryCodecSupport.bytes(fields.get(0), 1)), uuid(QueryCodecSupport.fixed(fields.get(1), 2, 16)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "BrokerResourceIdentity");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof KafkaBrokerResourceIdentity that
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && nativeTopicUuid.equals(that.nativeTopicUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticatedClusterId, nativeTopicUuid);
    }

    private static String utf8(final byte[] value) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException("authenticatedClusterId is not valid UTF-8");
        }
        return decoded;
    }

    private static String nfc(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static UUID uuid(final byte[] value) {
        return new UUID(Bytes.readU64be(value, 0), Bytes.readU64be(value, 8));
    }
}
