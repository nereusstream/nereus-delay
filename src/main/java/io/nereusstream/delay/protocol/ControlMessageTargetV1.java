package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Message identity, generation/state preconditions and optional attempt identity. */
public final class ControlMessageTargetV1 {
    public static final int PUBLISH_ATTEMPT_ID_LENGTH = 32;

    private final DelayMessageId messageId;
    private final long expectedGeneration;
    private final long expectedStateVersion;
    private final byte[] publishAttemptId;

    public ControlMessageTargetV1(final DelayMessageId messageId, final long expectedGeneration,
                                  final long expectedStateVersion, final byte[] publishAttemptId) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (expectedGeneration < 0 || expectedGeneration > 0xffff_ffffL || expectedStateVersion < 0) {
            throw new IllegalArgumentException("invalid Message target generation/state version");
        }
        this.expectedGeneration = expectedGeneration;
        this.expectedStateVersion = expectedStateVersion;
        if (publishAttemptId != null) {
            Bytes.requireLength(publishAttemptId, PUBLISH_ATTEMPT_ID_LENGTH, "publishAttemptId");
        }
        this.publishAttemptId = publishAttemptId == null ? null : Bytes.copy(publishAttemptId);
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public long expectedGeneration() {
        return expectedGeneration;
    }

    public long expectedStateVersion() {
        return expectedStateVersion;
    }

    public byte[] publishAttemptId() {
        return publishAttemptId == null ? null : Bytes.copy(publishAttemptId);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, messageId.bytes());
            CanonicalProtobuf.uint32(output, 2, expectedGeneration);
            CanonicalProtobuf.uint64(output, 3, expectedStateVersion);
            if (publishAttemptId != null) {
                CanonicalProtobuf.bytes(output, 4, publishAttemptId);
            }
        });
    }

    public static ControlMessageTargetV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ControlMessageTargetV1");
        if (fields.size() != 3 && fields.size() != 4) {
            throw new IllegalArgumentException("invalid ControlMessageTargetV1 field count");
        }
        if (fields.get(0).number() != 1 || fields.get(1).number() != 2 || fields.get(2).number() != 3
                || (fields.size() == 4 && fields.get(3).number() != 4)) {
            throw new IllegalArgumentException("invalid ControlMessageTargetV1 field order");
        }
        final ControlMessageTargetV1 result = new ControlMessageTargetV1(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint(fields.get(1), 2), QueryCodecSupport.uint(fields.get(2), 3),
                fields.size() == 4 ? QueryCodecSupport.fixed(fields.get(3), 4, PUBLISH_ATTEMPT_ID_LENGTH) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlMessageTargetV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlMessageTargetV1 that && expectedGeneration == that.expectedGeneration
                && expectedStateVersion == that.expectedStateVersion && messageId.equals(that.messageId)
                && Arrays.equals(publishAttemptId, that.publishAttemptId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, expectedGeneration, expectedStateVersion, Arrays.hashCode(publishAttemptId));
    }
}
