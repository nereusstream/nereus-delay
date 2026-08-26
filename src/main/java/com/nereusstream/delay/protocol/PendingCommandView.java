package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Public pending-command projection anchored to a source barrier. */
public final class PendingCommandView implements QueryResponseBranch {
    private final SourcePosition awaitedSourcePosition;
    private final SourcePosition currentSourcePosition;
    private final long retryAtEpochMs;

    public PendingCommandView(
            final SourcePosition awaitedSourcePosition,
            final SourcePosition currentSourcePosition,
            final long retryAtEpochMs) {
        this.awaitedSourcePosition = Objects.requireNonNull(awaitedSourcePosition, "awaitedSourcePosition");
        if (currentSourcePosition != null) {
            if (!awaitedSourcePosition.sameSourceIdentity(currentSourcePosition)
                    || currentSourcePosition.shardId() == null
                    || currentSourcePosition.compareTo(awaitedSourcePosition) >= 0) {
                throw new IllegalArgumentException("current source position must precede awaited position");
            }
        }
        if (retryAtEpochMs < 0) {
            throw new IllegalArgumentException("pending retryAt must be non-negative");
        }
        this.currentSourcePosition = currentSourcePosition;
        this.retryAtEpochMs = retryAtEpochMs;
    }

    public SourcePosition awaitedSourcePosition() {
        return awaitedSourcePosition;
    }

    public SourcePosition currentSourcePosition() {
        return currentSourcePosition;
    }

    public long retryAtEpochMs() {
        return retryAtEpochMs;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, QueryCodecSupport.encodeSourcePosition(awaitedSourcePosition));
            if (currentSourcePosition != null) {
                CanonicalProtobuf.bytes(output, 2, QueryCodecSupport.encodeSourcePosition(currentSourcePosition));
            }
            CanonicalProtobuf.int64(output, 3, retryAtEpochMs);
        });
    }

    public static PendingCommandView decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PendingCommandView");
        if (fields.size() != 2 && fields.size() != 3) {
            throw new IllegalArgumentException("invalid PendingCommandView field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 3
                || (fields.size() == 3 && fields.get(1).number() != 2)) {
            throw new IllegalArgumentException("invalid PendingCommandView fields");
        }
        final PendingCommandView result = new PendingCommandView(
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(0), 1)),
                fields.size() == 3
                        ? QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(1), 2))
                        : null,
                QueryCodecSupport.uint(fields.get(fields.size() - 1), 3));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PendingCommandView");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PendingCommandView that)) {
            return false;
        }
        return retryAtEpochMs == that.retryAtEpochMs
                && awaitedSourcePosition.equals(that.awaitedSourcePosition)
                && Objects.equals(currentSourcePosition, that.currentSourcePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(awaitedSourcePosition, currentSourcePosition, retryAtEpochMs);
    }
}
