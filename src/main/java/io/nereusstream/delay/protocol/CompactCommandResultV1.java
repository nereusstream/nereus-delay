package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Compact command result retained after the full view has expired. */
public final class CompactCommandResultV1 implements QueryResponseBranchV1 {
    private final CommandApplyStatusV1 status;
    private final StableCode stableCode;
    private final SourcePosition firstAppliedSourcePosition;
    private final long fullResultRetainUntilEpochMs;

    public CompactCommandResultV1(final CommandApplyStatusV1 status, final StableCode stableCode,
                                  final SourcePosition firstAppliedSourcePosition,
                                  final long fullResultRetainUntilEpochMs) {
        this.status = Objects.requireNonNull(status, "status");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.firstAppliedSourcePosition = Objects.requireNonNull(firstAppliedSourcePosition,
                "firstAppliedSourcePosition");
        if (fullResultRetainUntilEpochMs < 0) {
            throw new IllegalArgumentException("full result retention deadline must be non-negative");
        }
        this.fullResultRetainUntilEpochMs = fullResultRetainUntilEpochMs;
    }

    public CommandApplyStatusV1 status() {
        return status;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public SourcePosition firstAppliedSourcePosition() {
        return firstAppliedSourcePosition;
    }

    public long fullResultRetainUntilEpochMs() {
        return fullResultRetainUntilEpochMs;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, status.wireValue());
            CanonicalProtobuf.uint32(output, 2, stableCode.wireValue());
            CanonicalProtobuf.bytes(output, 3, QueryCodecSupport.encodeSourcePosition(firstAppliedSourcePosition));
            CanonicalProtobuf.int64(output, 4, fullResultRetainUntilEpochMs);
        });
    }

    public static CompactCommandResultV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CompactCommandResultV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "CompactCommandResultV1");
        final CompactCommandResultV1 result = new CompactCommandResultV1(
                CommandApplyStatusV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(1), 2)),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CompactCommandResultV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof CompactCommandResultV1 that)) {
            return false;
        }
        return fullResultRetainUntilEpochMs == that.fullResultRetainUntilEpochMs && status == that.status
                && stableCode == that.stableCode
                && firstAppliedSourcePosition.equals(that.firstAppliedSourcePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, stableCode, firstAppliedSourcePosition, fullResultRetainUntilEpochMs);
    }
}
