package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Canonical progress marker retained while a Lane retirement intent is open. */
public final class LaneRetirementProgressV1 {
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-lane-retirement-progress-v1\0");
    private static final int HASH_LENGTH = 32;

    private final byte[] retireMutationId;
    private final long appliedShardMutationSequence;
    private final SourcePosition intentSourcePosition;
    private final byte[] progressDigest;

    public LaneRetirementProgressV1(final byte[] retireMutationId, final long appliedShardMutationSequence,
                                    final SourcePosition intentSourcePosition) {
        this.retireMutationId = nonZero(retireMutationId, "retireMutationId");
        if (appliedShardMutationSequence <= 0) {
            throw new IllegalArgumentException("appliedShardMutationSequence must be positive");
        }
        this.appliedShardMutationSequence = appliedShardMutationSequence;
        this.intentSourcePosition = Objects.requireNonNull(intentSourcePosition, "intentSourcePosition");
        this.progressDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToThree());
    }

    private LaneRetirementProgressV1(final byte[] retireMutationId, final long appliedShardMutationSequence,
                                     final SourcePosition intentSourcePosition, final byte[] progressDigest) {
        this.retireMutationId = nonZero(retireMutationId, "retireMutationId");
        if (appliedShardMutationSequence <= 0) {
            throw new IllegalArgumentException("appliedShardMutationSequence must be positive");
        }
        this.appliedShardMutationSequence = appliedShardMutationSequence;
        this.intentSourcePosition = Objects.requireNonNull(intentSourcePosition, "intentSourcePosition");
        Bytes.requireLength(progressDigest, HASH_LENGTH, "progressDigest");
        this.progressDigest = Bytes.copy(progressDigest);
    }

    public byte[] retireMutationId() {
        return Bytes.copy(retireMutationId);
    }

    public long appliedShardMutationSequence() {
        return appliedShardMutationSequence;
    }

    public SourcePosition intentSourcePosition() {
        return intentSourcePosition;
    }

    public byte[] progressDigest() {
        return Bytes.copy(progressDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToThree());
            CanonicalProtobuf.bytes(output, 4, progressDigest);
        });
    }

    public static LaneRetirementProgressV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "LaneRetirementProgressV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "LaneRetirementProgressV1");
        final LaneRetirementProgressV1 result = new LaneRetirementProgressV1(
                nonZero(QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH), "retireMutationId"),
                QueryCodecSupport.uint(fields.get(1), 2),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        if (!Bytes.constantTimeEquals(result.progressDigest,
                Bytes.sha256(DIGEST_DOMAIN, result.fieldsOneToThree()))) {
            throw new IllegalArgumentException("LaneRetirementProgressV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneRetirementProgressV1");
        return result;
    }

    private byte[] fieldsOneToThree() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, retireMutationId);
            CanonicalProtobuf.uint64(output, 2, appliedShardMutationSequence);
            CanonicalProtobuf.bytes(output, 3, QueryCodecSupport.encodeSourcePosition(intentSourcePosition));
        });
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneRetirementProgressV1 that
                && appliedShardMutationSequence == that.appliedShardMutationSequence
                && Arrays.equals(retireMutationId, that.retireMutationId)
                && Objects.equals(intentSourcePosition, that.intentSourcePosition)
                && Arrays.equals(progressDigest, that.progressDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(retireMutationId), appliedShardMutationSequence,
                intentSourcePosition, Arrays.hashCode(progressDigest));
    }
}
