package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact physical Pulsar partition identity embedded in a signed Route snapshot. */
public final class PulsarPhysicalPartitionIdentityV1 {
    private static final int RESOURCE_LENGTH = 32;

    private final int partition;
    private final String physicalTopic;
    private final byte[] resourceIncarnation;
    private final long physicalTopicCreationTimestamp;

    public PulsarPhysicalPartitionIdentityV1(
            final int partition,
            final String physicalTopic,
            final byte[] resourceIncarnation,
            final long physicalTopicCreationTimestamp) {
        if (partition < 0) {
            throw new IllegalArgumentException("Pulsar partition must be non-negative");
        }
        this.partition = partition;
        this.physicalTopic = nfc(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, RESOURCE_LENGTH, "resourceIncarnation");
        if (allZero(resourceIncarnation)) {
            throw new IllegalArgumentException("resourceIncarnation must be non-zero");
        }
        this.resourceIncarnation = Bytes.copy(resourceIncarnation);
        this.physicalTopicCreationTimestamp = physicalTopicCreationTimestamp;
    }

    public int partition() {
        return partition;
    }

    public String physicalTopic() {
        return physicalTopic;
    }

    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    public long physicalTopicCreationTimestamp() {
        return physicalTopicCreationTimestamp;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32Bits(output, 1, partition);
            CanonicalProtobuf.bytes(output, 2, physicalTopic.getBytes(StandardCharsets.UTF_8));
            CanonicalProtobuf.bytes(output, 3, resourceIncarnation);
            CanonicalProtobuf.uint64Bits(output, 4, physicalTopicCreationTimestamp);
        });
    }

    public static PulsarPhysicalPartitionIdentityV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PulsarPhysicalPartitionIdentityV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "PulsarPhysicalPartitionIdentityV1");
        final PulsarPhysicalPartitionIdentityV1 result = new PulsarPhysicalPartitionIdentityV1(
                QueryCodecSupport.uint32Bits(fields.get(0), 1),
                utf8(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, RESOURCE_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarPhysicalPartitionIdentityV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarPhysicalPartitionIdentityV1 that
                && partition == that.partition
                && physicalTopic.equals(that.physicalTopic)
                && physicalTopicCreationTimestamp == that.physicalTopicCreationTimestamp
                && Arrays.equals(resourceIncarnation, that.resourceIncarnation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition, physicalTopic, Arrays.hashCode(resourceIncarnation), physicalTopicCreationTimestamp);
    }

    private static String utf8(final byte[] value) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException("physicalTopic is not valid UTF-8");
        }
        return decoded;
    }

    private static String nfc(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
        }
        return value;
    }

    private static boolean allZero(final byte[] value) {
        for (byte item : value) {
            if (item != 0) {
                return false;
            }
        }
        return true;
    }
}
