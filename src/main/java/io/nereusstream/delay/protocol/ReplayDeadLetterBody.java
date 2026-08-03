package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Semantic parser for the bounded {@code ReplayDeadLetterV1} body. */
public final class ReplayDeadLetterBody {
    private static final int HASH_LENGTH = 32;
    private final ControlRef controlRef;
    private final DelayMessageId messageId;
    private final int expectedGeneration;
    private final long expectedStateVersion;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final byte[] retryPolicy;
    private final RetryPolicyRefV1 retryPolicyRef;
    private final boolean allowPossibleDuplicate;
    private final byte[] acknowledgementHash;

    private ReplayDeadLetterBody(final ControlRef controlRef, final DelayMessageId messageId,
                                 final int expectedGeneration, final long expectedStateVersion,
                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                 final byte[] retryPolicy, final boolean allowPossibleDuplicate,
                                 final byte[] acknowledgementHash) {
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (expectedGeneration < 0 || expectedStateVersion < 0 || deliverAtEpochMs < 0
                || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Dead Letter replay timing/precondition");
        }
        this.expectedGeneration = expectedGeneration;
        this.expectedStateVersion = expectedStateVersion;
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.retryPolicy = copyNested(retryPolicy, "retryPolicy");
        this.retryPolicyRef = RetryPolicyRefV1.decode(this.retryPolicy);
        this.allowPossibleDuplicate = allowPossibleDuplicate;
        this.acknowledgementHash = Bytes.copy(acknowledgementHash);
        if (allowPossibleDuplicate != (acknowledgementHash.length == ControlRef.HASH_LENGTH)) {
            throw new IllegalArgumentException("Dead Letter replay acknowledgement presence mismatch");
        }
    }

    /**
     * Encodes the canonical source-ordered replay mutation body. The common
     * subject/type/retry fields are included so the result can be passed
     * directly to {@link SystemMutation#signed}.
     */
    public static byte[] encode(final ShardId shardId, final long retryUntilEpochMs,
                                final ControlRef controlRef, final DelayMessageId messageId,
                                final int expectedGeneration, final long expectedStateVersion,
                                final long deliverAtEpochMs, final long expireAtEpochMs,
                                final RetryPolicyRefV1 retryPolicy, final boolean allowPossibleDuplicate,
                                final byte[] acknowledgementHash) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(controlRef, "controlRef");
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        if (!messageId.routingId().shardId().equals(shardId)) {
            throw new IllegalArgumentException("replay messageId does not belong to shard");
        }
        if (retryUntilEpochMs < 0 || expectedGeneration < 0 || expectedStateVersion < 0
                || deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid replay mutation timing/precondition");
        }
        final byte[] acknowledgement = acknowledgementHash == null
                ? new byte[0] : Bytes.copy(acknowledgementHash);
        if (allowPossibleDuplicate) {
            Bytes.requireLength(acknowledgement, HASH_LENGTH, "acknowledgementHash");
        } else if (acknowledgement.length != 0) {
            throw new IllegalArgumentException("acknowledgementHash requires possible-duplicate acknowledgement");
        }
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, expectedGeneration);
            CanonicalProtobuf.uint64(output, 13, expectedStateVersion);
            CanonicalProtobuf.int64(output, 14, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 15, expireAtEpochMs);
            CanonicalProtobuf.bytes(output, 16, retryPolicy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 17, allowPossibleDuplicate ? 1 : 0);
            if (allowPossibleDuplicate) {
                CanonicalProtobuf.bytes(output, 18, acknowledgement);
            }
        });
        decode(encoded);
        return encoded;
    }

    public static ReplayDeadLetterBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.REPLAY_DEAD_LETTER, canonicalBody);
        final ShardId subjectShard = ShardSubjectV1.decode(bytes(fields.get(0), 1)).shardId();
        final ControlRef controlRef = ControlRef.decode(nested(field(fields, 10), 10));
        final DelayMessageId messageId = new DelayMessageId(fixed(field(fields, 11), 11, DelayMessageId.LENGTH));
        if (!subjectShard.equals(messageId.routingId().shardId())) {
            throw new IllegalArgumentException("replay messageId does not belong to body shard");
        }
        final int generation = intValue(field(fields, 12), 12);
        final long stateVersion = unsigned(field(fields, 13), 13);
        final long deliverAt = unsigned(field(fields, 14), 14);
        final long expireAt = unsigned(field(fields, 15), 15);
        final byte[] retryPolicy = nested(field(fields, 16), 16);
        final boolean allowDuplicate = bool(field(fields, 17), 17);
        final byte[] acknowledgement = optionalFixed(fields, 18, ControlRef.HASH_LENGTH);
        return new ReplayDeadLetterBody(controlRef, messageId, generation, stateVersion, deliverAt, expireAt,
                retryPolicy, allowDuplicate, acknowledgement);
    }

    public ControlRef controlRef() {
        return controlRef;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int expectedGeneration() {
        return expectedGeneration;
    }

    public long expectedStateVersion() {
        return expectedStateVersion;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public long expireAtEpochMs() {
        return expireAtEpochMs;
    }

    public byte[] retryPolicy() {
        return Bytes.copy(retryPolicy);
    }

    /** Returns the typed immutable policy reference carried by field 16. */
    public RetryPolicyRefV1 retryPolicyRef() {
        return retryPolicyRef;
    }

    public boolean allowPossibleDuplicate() {
        return allowPossibleDuplicate;
    }

    public byte[] acknowledgementHash() {
        return Bytes.copy(acknowledgementHash);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fields.get(index);
            }
        }
        throw new IllegalArgumentException("missing ReplayDeadLetter field " + number);
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("ReplayDeadLetter field exceeds Java int range " + number);
        }
        return (int) value;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid ReplayDeadLetter scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static boolean bool(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > 1) {
            throw new IllegalArgumentException("invalid ReplayDeadLetter boolean field " + number);
        }
        return value == 1;
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException("ReplayDeadLetter nested field must not be empty: " + number);
        }
        readAll(new CanonicalProtobuf.Reader(value));
        return value;
    }

    private static byte[] copyNested(final byte[] value, final String name) {
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] optionalFixed(final List<CanonicalProtobuf.Reader.Field> fields, final int number,
                                        final int length) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fixed(fields.get(index), number, length);
            }
        }
        return new byte[0];
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "ReplayDeadLetter field " + number);
        return value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid ReplayDeadLetter bytes field " + number);
        }
        return field.rawValue();
    }
}
