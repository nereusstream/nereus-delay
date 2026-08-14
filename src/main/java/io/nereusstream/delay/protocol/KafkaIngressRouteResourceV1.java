package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Signed Route resource branch for a Kafka command topic. */
public final class KafkaIngressRouteResourceV1 implements IngressRouteResourceV1 {
    private final String authenticatedClusterId;
    private final String canonicalPhysicalTopic;
    private final UUID nativeTopicUuid;
    private final int partitionCount;

    public KafkaIngressRouteResourceV1(final String authenticatedClusterId, final String canonicalPhysicalTopic,
                                       final UUID nativeTopicUuid, final int partitionCount) {
        this.authenticatedClusterId = nfc(authenticatedClusterId, "authenticatedClusterId");
        this.canonicalPhysicalTopic = nfc(canonicalPhysicalTopic, "canonicalPhysicalTopic");
        this.nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (nativeTopicUuid.equals(new UUID(0, 0))) {
            throw new IllegalArgumentException("nativeTopicUuid must be non-zero");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        this.partitionCount = partitionCount;
    }

    @Override
    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public String canonicalPhysicalTopic() {
        return canonicalPhysicalTopic;
    }

    public UUID nativeTopicUuid() {
        return nativeTopicUuid;
    }

    @Override
    public int partitionCount() {
        return partitionCount;
    }

    @Override
    public AdapterKindV1 adapterKind() {
        return AdapterKindV1.KAFKA;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, authenticatedClusterId.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.bytes(fields, 2, canonicalPhysicalTopic.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.bytes(fields, 3, uuidBytes(nativeTopicUuid));
                    CanonicalProtobuf.uint32(fields, 4, partitionCount);
                })));
    }

    public static KafkaIngressRouteResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = QueryCodecSupport.read(encoded,
                "IngressRouteResourceV1");
        QueryCodecSupport.requireNumbers(outer, new int[]{1}, "IngressRouteResourceV1");
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(
                QueryCodecSupport.nested(outer.get(0), 1), "KafkaIngressRouteResourceV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "KafkaIngressRouteResourceV1");
        final KafkaIngressRouteResourceV1 result = new KafkaIngressRouteResourceV1(
                utf8(QueryCodecSupport.bytes(fields.get(0), 1)),
                utf8(QueryCodecSupport.bytes(fields.get(1), 2)),
                uuid(QueryCodecSupport.fixed(fields.get(2), 3, 16)),
                QueryCodecSupport.uint32(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "IngressRouteResourceV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof KafkaIngressRouteResourceV1 that
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && canonicalPhysicalTopic.equals(that.canonicalPhysicalTopic)
                && nativeTopicUuid.equals(that.nativeTopicUuid)
                && partitionCount == that.partitionCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticatedClusterId, canonicalPhysicalTopic, nativeTopicUuid, partitionCount);
    }

    private static String utf8(final byte[] value) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException("Kafka Route text is not valid UTF-8");
        }
        return decoded;
    }

    private static String nfc(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static UUID uuid(final byte[] value) {
        return new UUID(Bytes.readU64be(value, 0), Bytes.readU64be(value, 8));
    }
}
