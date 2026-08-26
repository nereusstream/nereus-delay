package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Compact command result retained after the full view has expired. */
public final class CompactCommandResult implements QueryResponseBranch {
    private final CommandApplyStatus status;
    private final StableCode stableCode;
    private final SourcePosition firstAppliedSourcePosition;
    private final long fullResultRetainUntilEpochMs;

    public CompactCommandResult(
            final CommandApplyStatus status,
            final StableCode stableCode,
            final SourcePosition firstAppliedSourcePosition,
            final long fullResultRetainUntilEpochMs) {
        this.status = Objects.requireNonNull(status, "status");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.firstAppliedSourcePosition =
                Objects.requireNonNull(firstAppliedSourcePosition, "firstAppliedSourcePosition");
        if (fullResultRetainUntilEpochMs < firstAppliedSourcePosition.brokerPersistenceTimeEpochMs()) {
            throw new IllegalArgumentException("full result retention deadline precedes Broker persistence time");
        }
        this.fullResultRetainUntilEpochMs = fullResultRetainUntilEpochMs;
    }

    public CommandApplyStatus status() {
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

    public static CompactCommandResult decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CompactCommandResult");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "CompactCommandResult");
        final CompactCommandResult result = new CompactCommandResult(
                CommandApplyStatus.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(1), 2)),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CompactCommandResult");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof CompactCommandResult that)) {
            return false;
        }
        return fullResultRetainUntilEpochMs == that.fullResultRetainUntilEpochMs
                && status == that.status
                && stableCode == that.stableCode
                && firstAppliedSourcePosition.equals(that.firstAppliedSourcePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, stableCode, firstAppliedSourcePosition, fullResultRetainUntilEpochMs);
    }
}
