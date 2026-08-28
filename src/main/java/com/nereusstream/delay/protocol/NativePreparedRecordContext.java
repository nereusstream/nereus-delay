package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact logical identity needed to project an AUTO_FAST envelope into the
 * final Pulsar record. It is separate from {@link NativePreparedDelivery}
 * because the latter is also used by the public native outcome union.
 */
public final class NativePreparedRecordContext {
    public static final int SCHEMA_GENERATION = 1;
    public static final int HASH_LENGTH = 32;

    private final RouteIncarnation routeIncarnation;
    private final long shardPartition;
    private final DelayMessageId messageId;
    private final long generation;
    private final byte[] publishAttemptId;
    private final byte[] artifactGenerationSetDigest;

    public NativePreparedRecordContext(
            final RouteIncarnation routeIncarnation,
            final long shardPartition,
            final DelayMessageId messageId,
            final long generation,
            final byte[] publishAttemptId,
            final byte[] artifactGenerationSetDigest) {
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.shardPartition = uint32(shardPartition, "shardPartition");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (!messageId.routingId().shardId().routeIncarnation().equals(routeIncarnation)
                || messageId.routingId().shardId().unsignedPartition() != this.shardPartition) {
            throw new IllegalArgumentException("native record context message identity disagrees with the shard");
        }
        this.generation = uint32(generation, "generation");
        if (this.generation == 0) {
            throw new IllegalArgumentException("generation must be positive");
        }
        this.publishAttemptId = nonZero(publishAttemptId, "publishAttemptId");
        this.artifactGenerationSetDigest = fixed(artifactGenerationSetDigest, "artifactGenerationSetDigest");
    }

    /** Creates the logical record context for a new AUTO_FAST Schedule. */
    public static NativePreparedRecordContext initialSchedule(
            final PreparedCommand managedCommand,
            final byte[] publishAttemptId,
            final byte[] artifactGenerationSetDigest) {
        final PreparedCommand command = Objects.requireNonNull(managedCommand, "managedCommand");
        if (command.type() != CommandType.SCHEDULE || !CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            throw new IllegalArgumentException("AUTO_FAST record context requires a current Schedule command");
        }
        final ScheduleCommandBody body = CommandBodies.decodeSchedule(command.canonicalBody());
        if (!body.delayMessageId().equals(command.delayMessageId())) {
            throw new IllegalArgumentException("AUTO_FAST Schedule body identity mismatch");
        }
        return new NativePreparedRecordContext(
                command.shardId().routeIncarnation(),
                command.shardId().unsignedPartition(),
                command.delayMessageId(),
                1,
                publishAttemptId,
                artifactGenerationSetDigest);
    }

    public RouteIncarnation routeIncarnation() {
        return routeIncarnation;
    }

    public long shardPartition() {
        return shardPartition;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public long generation() {
        return generation;
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public byte[] artifactGenerationSetDigest() {
        return Bytes.copy(artifactGenerationSetDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, routeIncarnation.bytes());
            CanonicalProtobuf.uint32(output, 3, shardPartition);
            CanonicalProtobuf.bytes(output, 4, messageId.bytes());
            CanonicalProtobuf.uint32(output, 5, generation);
            CanonicalProtobuf.bytes(output, 6, publishAttemptId);
            CanonicalProtobuf.bytes(output, 7, artifactGenerationSetDigest);
        });
    }

    public static NativePreparedRecordContext decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "NativePreparedRecordContext");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "NativePreparedRecordContext");
        final NativePreparedRecordContext result = new NativePreparedRecordContext(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(1), 2, RouteIncarnation.LENGTH)),
                QueryCodecSupport.uint(fields.get(2), 3),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(3), 4, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH));
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported NativePreparedRecordContext generation");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativePreparedRecordContext");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof NativePreparedRecordContext that
                && shardPartition == that.shardPartition
                && generation == that.generation
                && routeIncarnation.equals(that.routeIncarnation)
                && messageId.equals(that.messageId)
                && Arrays.equals(publishAttemptId, that.publishAttemptId)
                && Arrays.equals(artifactGenerationSetDigest, that.artifactGenerationSetDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routeIncarnation,
                shardPartition,
                messageId,
                generation,
                Arrays.hashCode(publishAttemptId),
                Arrays.hashCode(artifactGenerationSetDigest));
    }

    private static long uint32(final long value, final String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " is outside uint32 range");
        }
        return value;
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, name);
        for (byte item : result) {
            if (item != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
