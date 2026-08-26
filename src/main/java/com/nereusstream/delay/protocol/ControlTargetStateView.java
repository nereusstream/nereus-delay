package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Public-safe state projection for one immutable Control Operation target. */
public final class ControlTargetStateView {
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-control-target-state\0");
    private static final int HASH_LENGTH = 32;

    private final long targetIndex;
    private final TargetMarkerState markerState;
    private final StableCode stableCode;
    private final long targetRevision;
    private final SourcePosition appliedSourcePosition;
    private final byte[] targetResultDigest;

    public ControlTargetStateView(
            final long targetIndex,
            final TargetMarkerState markerState,
            final StableCode stableCode,
            final long targetRevision,
            final SourcePosition appliedSourcePosition) {
        if (targetIndex < 0 || targetIndex > 0xffff_ffffL) {
            throw new IllegalArgumentException("targetIndex must be an unsigned uint32");
        }
        this.targetIndex = targetIndex;
        this.markerState = Objects.requireNonNull(markerState, "markerState");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        if (targetRevision < 0) {
            throw new IllegalArgumentException("targetRevision must be non-negative");
        }
        this.targetRevision = targetRevision;
        this.appliedSourcePosition = appliedSourcePosition;
        this.targetResultDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToFive());
    }

    private ControlTargetStateView(
            final long targetIndex,
            final TargetMarkerState markerState,
            final StableCode stableCode,
            final long targetRevision,
            final SourcePosition appliedSourcePosition,
            final byte[] digest) {
        if (targetIndex < 0 || targetIndex > 0xffff_ffffL) {
            throw new IllegalArgumentException("targetIndex must be an unsigned uint32");
        }
        this.targetIndex = targetIndex;
        this.markerState = Objects.requireNonNull(markerState, "markerState");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        if (targetRevision < 0) {
            throw new IllegalArgumentException("targetRevision must be non-negative");
        }
        this.targetRevision = targetRevision;
        this.appliedSourcePosition = appliedSourcePosition;
        Bytes.requireLength(digest, HASH_LENGTH, "targetResultDigest");
        this.targetResultDigest = Bytes.copy(digest);
    }

    public long targetIndex() {
        return targetIndex;
    }

    public TargetMarkerState markerState() {
        return markerState;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public long targetRevision() {
        return targetRevision;
    }

    public SourcePosition appliedSourcePosition() {
        return appliedSourcePosition;
    }

    public byte[] targetResultDigest() {
        return Bytes.copy(targetResultDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFive());
            CanonicalProtobuf.bytes(output, 6, targetResultDigest);
        });
    }

    public static ControlTargetStateView decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ControlTargetStateView");
        if (fields.size() < 5 || fields.size() > 6) {
            throw new IllegalArgumentException("invalid ControlTargetStateView field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3
                || fields.get(3).number() != 4
                || (fields.size() == 6 && fields.get(4).number() != 5)
                || fields.get(fields.size() - 1).number() != 6) {
            throw new IllegalArgumentException("invalid ControlTargetStateView field order");
        }
        final int sourceIndex = fields.size() == 6 ? 4 : -1;
        final SourcePosition source = sourceIndex < 0
                ? null
                : QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(sourceIndex), 5));
        final ControlTargetStateView result = new ControlTargetStateView(
                QueryCodecSupport.uint(fields.get(0), 1),
                TargetMarkerState.fromWire(QueryCodecSupport.uint32(fields.get(1), 2)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4),
                source,
                QueryCodecSupport.fixed(fields.get(fields.size() - 1), 6, HASH_LENGTH));
        if (!Bytes.constantTimeEquals(
                result.targetResultDigest, Bytes.sha256(DIGEST_DOMAIN, result.fieldsOneToFive()))) {
            throw new IllegalArgumentException("ControlTargetStateView digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlTargetStateView");
        return result;
    }

    private byte[] fieldsOneToFive() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, targetIndex);
            CanonicalProtobuf.uint32(output, 2, markerState.wireValue());
            CanonicalProtobuf.uint32(output, 3, stableCode.wireValue());
            CanonicalProtobuf.uint64(output, 4, targetRevision);
            if (appliedSourcePosition != null) {
                CanonicalProtobuf.bytes(output, 5, QueryCodecSupport.encodeSourcePosition(appliedSourcePosition));
            }
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlTargetStateView that
                && targetIndex == that.targetIndex
                && targetRevision == that.targetRevision
                && markerState == that.markerState
                && stableCode == that.stableCode
                && Objects.equals(appliedSourcePosition, that.appliedSourcePosition)
                && Arrays.equals(targetResultDigest, that.targetResultDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                targetIndex,
                markerState,
                stableCode,
                targetRevision,
                appliedSourcePosition,
                Arrays.hashCode(targetResultDigest));
    }
}
