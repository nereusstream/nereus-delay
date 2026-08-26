package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourceActivationBarrier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Local projection of a source assignment accepted by the external broker
 * adapter. The assignment identity is deliberately separate from the
 * activation barrier; both are required before a shard can become active.
 */
public record SourceAssignment(
        ShardId shardId, byte[] assignmentId, long assignmentEpoch, SourceActivationBarrier activationBarrier) {
    public static final int ID_LENGTH = 32;

    public SourceAssignment {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(assignmentId, ID_LENGTH, "assignmentId");
        if (assignmentEpoch <= 0) {
            throw new IllegalArgumentException("assignmentEpoch must be positive");
        }
        Objects.requireNonNull(activationBarrier, "activationBarrier");
        if (!shardId.equals(activationBarrier.shardId())) {
            throw new IllegalArgumentException("source assignment barrier belongs to another shard");
        }
        boolean nonZero = false;
        for (byte value : assignmentId) {
            if (value != 0) {
                nonZero = true;
                break;
            }
        }
        if (!nonZero) {
            throw new IllegalArgumentException("assignmentId must be non-zero");
        }
        assignmentId = Bytes.copy(assignmentId);
    }

    @Override
    public byte[] assignmentId() {
        return Bytes.copy(assignmentId);
    }

    /** Exact assignment identity used when a broker rereads an accepted assignment. */
    public boolean sameIdentity(final SourceAssignment other) {
        return other != null
                && shardId.equals(other.shardId)
                && assignmentEpoch == other.assignmentEpoch
                && Arrays.equals(assignmentId, other.assignmentId)
                && Objects.equals(activationBarrier, other.activationBarrier);
    }

    /** Exact canonical bytes used by the Oxia assignment authority. */
    public byte[] canonicalBytes() {
        return encode(this);
    }

    /** Decodes and revalidates an exact canonical assignment projection. */
    public static SourceAssignment decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = fields(encoded, "SourceAssignment");
        requireNumbers(fields, 1, 2, 3, 4, 5);
        final RouteIncarnation route = new RouteIncarnation(fixed(fields.get(0), 1, RouteIncarnation.LENGTH));
        final int partition = uint32Bits(fields.get(1), 2);
        final byte[] assignmentId = fixed(fields.get(2), 3, ID_LENGTH);
        final long assignmentEpoch = uint64Bits(fields.get(3), 4);
        if (assignmentEpoch == 0) {
            throw new IllegalArgumentException("SourceAssignment assignmentEpoch must be non-zero");
        }
        final SourceActivationBarrier barrier = decodeBarrier(bytes(fields.get(4), 5));
        final SourceAssignment result =
                new SourceAssignment(new ShardId(route, partition), assignmentId, assignmentEpoch, barrier);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical SourceAssignment");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SourceAssignment that && sameIdentity(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, Arrays.hashCode(assignmentId), assignmentEpoch, activationBarrier);
    }

    private static byte[] encode(final SourceAssignment assignment) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output, 1, assignment.shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, assignment.shardId().partition());
            CanonicalProtobuf.bytes(output, 3, assignment.assignmentId());
            CanonicalProtobuf.uint64Bits(output, 4, assignment.assignmentEpoch());
            CanonicalProtobuf.bytes(output, 5, encodeBarrier(assignment.activationBarrier()));
        });
    }

    private static byte[] encodeBarrier(final SourceActivationBarrier barrier) {
        if (barrier instanceof KafkaActivationBarrier kafka) {
            return CanonicalProtobuf.message(
                    output -> CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(fields -> {
                        CanonicalProtobuf.bytes(
                                fields, 1, kafka.shardId().routeIncarnation().bytes());
                        CanonicalProtobuf.uint32Bits(fields, 2, kafka.shardId().partition());
                        CanonicalProtobuf.bytes(
                                fields, 3, utf8(kafka.authenticatedClusterId(), "authenticatedClusterId"));
                        CanonicalProtobuf.bytes(fields, 4, uuidBytes(kafka.nativeTopicUuid()));
                        CanonicalProtobuf.uint64Bits(fields, 5, kafka.exclusiveOffset());
                    })));
        }
        if (!(barrier instanceof PulsarActivationBarrier pulsar)) {
            throw new IllegalArgumentException("unsupported SourceActivationBarrier implementation");
        }
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(
                            fields, 1, pulsar.shardId().routeIncarnation().bytes());
                    CanonicalProtobuf.uint32Bits(fields, 2, pulsar.shardId().partition());
                    CanonicalProtobuf.bytes(fields, 3, pulsar.brokerResourceIncarnation());
                    CanonicalProtobuf.bytes(fields, 4, utf8(pulsar.physicalTopic(), "physicalTopic"));
                    CanonicalProtobuf.uint64Bits(fields, 5, pulsar.ledgerId());
                    CanonicalProtobuf.uint64Bits(fields, 6, pulsar.entryId());
                    CanonicalProtobuf.uint32Bits(fields, 7, pulsar.normalizedLastBatchIndex());
                    CanonicalProtobuf.uint32Bits(fields, 8, pulsar.batchSize());
                    CanonicalProtobuf.uint64Bits(fields, 9, pulsar.guardedSourceConnectionGeneration());
                    CanonicalProtobuf.bytes(fields, 10, pulsar.resourceGuardAttestationDigest());
                    CanonicalProtobuf.uint32(fields, 11, pulsar.empty() ? 1 : 0);
                })));
    }

    private static SourceActivationBarrier decodeBarrier(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = fields(encoded, "SourceActivationBarrier");
        if (outer.size() != 1) {
            throw new IllegalArgumentException("SourceActivationBarrier must select one branch");
        }
        final CanonicalProtobuf.Reader.Field branch = outer.get(0);
        final List<CanonicalProtobuf.Reader.Field> fields = fields(branch.rawValue(), "SourceActivationBarrier branch");
        if (branch.number() == 1) {
            requireNumbers(fields, 1, 2, 3, 4, 5);
            final RouteIncarnation route = new RouteIncarnation(fixed(fields.get(0), 1, RouteIncarnation.LENGTH));
            final int partition = uint32Bits(fields.get(1), 2);
            final String cluster = text(bytes(fields.get(2), 3), "authenticatedClusterId");
            final UUID topic = uuid(fixed(fields.get(3), 4, 16));
            return new KafkaActivationBarrier(
                    new ShardId(route, partition), cluster, topic, uint64Bits(fields.get(4), 5));
        }
        if (branch.number() == 2) {
            requireNumbers(fields, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
            final RouteIncarnation route = new RouteIncarnation(fixed(fields.get(0), 1, RouteIncarnation.LENGTH));
            final int partition = uint32Bits(fields.get(1), 2);
            final byte[] resource = fixed(fields.get(2), 3, 32);
            final String topic = text(bytes(fields.get(3), 4), "physicalTopic");
            final long generation = uint64Bits(fields.get(8), 9);
            final byte[] attestation = fixed(fields.get(9), 10, 32);
            final long empty = uint(fields.get(10), 11);
            if (empty > 1) {
                throw new IllegalArgumentException("invalid Pulsar empty barrier flag");
            }
            return new PulsarActivationBarrier(
                    new ShardId(route, partition),
                    resource,
                    topic,
                    uint64Bits(fields.get(4), 5),
                    uint64Bits(fields.get(5), 6),
                    uint32Bits(fields.get(6), 7),
                    uint32Bits(fields.get(7), 8),
                    generation,
                    attestation,
                    empty == 1);
        }
        throw new IllegalArgumentException("unknown SourceActivationBarrier branch");
    }

    private static List<CanonicalProtobuf.Reader.Field> fields(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> result = new ArrayList<>();
        while (reader.hasRemaining()) {
            result.add(reader.next());
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return result;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int... numbers) {
        if (fields.size() != numbers.length) {
            throw new IllegalArgumentException("unexpected canonical field count");
        }
        for (int index = 0; index < numbers.length; index++) {
            if (fields.get(index).number() != numbers[index]) {
                throw new IllegalArgumentException("unexpected canonical field order");
            }
        }
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid canonical bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "canonical field " + number);
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid canonical uint field " + number);
        }
        return field.unsignedValue();
    }

    private static int uint32Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("canonical uint32 field is out of range " + number);
        }
        return (int) value;
    }

    private static long uint64Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        return uint(field, number);
    }

    private static byte[] utf8(final String value, final String name) {
        final String canonical = canonicalText(value, name);
        return canonical.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(final byte[] value, final String name) {
        final String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
        }
        return canonicalText(result, name);
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
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
