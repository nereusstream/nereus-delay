package io.nereusstream.delay.protocol;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact Pulsar Broker resource identity used by the native-delivery branch. */
public final class PulsarBrokerResourceIdentityV1 {
    private static final int RESOURCE_LENGTH = 32;

    private final String authenticatedClusterId;
    private final byte[] resourceIncarnation;
    private final String physicalTopic;
    private final long physicalTopicCreationTimestamp;

    public PulsarBrokerResourceIdentityV1(final String authenticatedClusterId, final byte[] resourceIncarnation,
                                          final String physicalTopic,
                                          final long physicalTopicCreationTimestamp) {
        this.authenticatedClusterId = nfc(authenticatedClusterId, "authenticatedClusterId");
        Bytes.requireLength(resourceIncarnation, RESOURCE_LENGTH, "resourceIncarnation");
        this.resourceIncarnation = Bytes.copy(resourceIncarnation);
        this.physicalTopic = nfc(physicalTopic, "physicalTopic");
        if (physicalTopicCreationTimestamp < 0) {
            throw new IllegalArgumentException("physical topic creation timestamp must be non-negative");
        }
        this.physicalTopicCreationTimestamp = physicalTopicCreationTimestamp;
    }

    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    public String physicalTopic() {
        return physicalTopic;
    }

    public long physicalTopicCreationTimestamp() {
        return physicalTopicCreationTimestamp;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 2, canonicalPulsarFields()));
    }

    private byte[] canonicalPulsarFields() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, authenticatedClusterId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            CanonicalProtobuf.bytes(output, 2, resourceIncarnation);
            CanonicalProtobuf.bytes(output, 3, physicalTopic.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            CanonicalProtobuf.uint64(output, 4, physicalTopicCreationTimestamp);
        });
    }

    public static PulsarBrokerResourceIdentityV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = QueryCodecSupport.read(encoded,
                "BrokerResourceIdentityV1");
        QueryCodecSupport.requireNumbers(outer, new int[]{2}, "BrokerResourceIdentityV1");
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(
                QueryCodecSupport.nested(outer.get(0), 2), "PulsarResourceIdentityV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "PulsarResourceIdentityV1");
        final PulsarBrokerResourceIdentityV1 result = new PulsarBrokerResourceIdentityV1(
                utf8(QueryCodecSupport.bytes(fields.get(0), 1), "authenticatedClusterId"),
                QueryCodecSupport.fixed(fields.get(1), 2, RESOURCE_LENGTH),
                utf8(QueryCodecSupport.bytes(fields.get(2), 3), "physicalTopic"),
                QueryCodecSupport.uint(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "BrokerResourceIdentityV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PulsarBrokerResourceIdentityV1 that)) {
            return false;
        }
        return physicalTopicCreationTimestamp == that.physicalTopicCreationTimestamp
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && Arrays.equals(resourceIncarnation, that.resourceIncarnation)
                && physicalTopic.equals(that.physicalTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticatedClusterId, Arrays.hashCode(resourceIncarnation), physicalTopic,
                physicalTopicCreationTimestamp);
    }

    private static String utf8(final byte[] value, final String name) {
        final String decoded = new String(value, java.nio.charset.StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(java.nio.charset.StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
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
}
