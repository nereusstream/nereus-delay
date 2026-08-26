package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed capacity envelope bound to one immutable shard placement decision.
 *
 * <p>This class validates the local byte/dimension invariants. It does not
 * perform Oxia placement, Owner Lease CAS, or artifact publication; those
 * remain authority operations outside the protocol codec.</p>
 */
public final class ShardCapacityEnvelope {
    public static final int SCHEMA_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-shard-capacity-envelope\0");

    private final byte[] envelopeId;
    private final long envelopeVersion;
    private final QuotaGrantRef logicalGrant;
    private final CapacityVector committed;
    private final CapacityGrant outcomeReserve;
    private final CapacityGrant nonOutcomeControl;
    private final CapacityGrant recoveryWorking;
    private final CapacityGrant emergencyHeadroom;
    private final byte[] releaseCapacityArtifactDigest;
    private final byte[] envelopeDigest;

    public ShardCapacityEnvelope(
            final byte[] envelopeId,
            final long envelopeVersion,
            final QuotaGrantRef logicalGrant,
            final CapacityVector committed,
            final CapacityGrant outcomeReserve,
            final CapacityGrant nonOutcomeControl,
            final CapacityGrant recoveryWorking,
            final CapacityGrant emergencyHeadroom,
            final byte[] releaseCapacityArtifactDigest) {
        this.envelopeId = fixedNonZero(envelopeId, "envelopeId");
        if (envelopeVersion == 0) {
            throw new IllegalArgumentException("envelopeVersion must be nonzero");
        }
        this.envelopeVersion = envelopeVersion;
        this.logicalGrant = Objects.requireNonNull(logicalGrant, "logicalGrant");
        this.committed = Objects.requireNonNull(committed, "committed");
        this.outcomeReserve = requireGrant(outcomeReserve, CapacityGrantKind.OUTCOME_RESERVE, "outcomeReserve");
        this.nonOutcomeControl =
                requireGrant(nonOutcomeControl, CapacityGrantKind.NON_OUTCOME_CONTROL, "nonOutcomeControl");
        this.recoveryWorking = requireGrant(recoveryWorking, CapacityGrantKind.RECOVERY_WORKING, "recoveryWorking");
        this.emergencyHeadroom =
                requireGrant(emergencyHeadroom, CapacityGrantKind.EMERGENCY_HEADROOM, "emergencyHeadroom");
        this.releaseCapacityArtifactDigest = fixed(releaseCapacityArtifactDigest, "releaseCapacityArtifactDigest");
        validateProjection();
        this.envelopeDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToTen());
    }

    private ShardCapacityEnvelope(
            final byte[] envelopeId,
            final long envelopeVersion,
            final QuotaGrantRef logicalGrant,
            final CapacityVector committed,
            final CapacityGrant outcomeReserve,
            final CapacityGrant nonOutcomeControl,
            final CapacityGrant recoveryWorking,
            final CapacityGrant emergencyHeadroom,
            final byte[] releaseCapacityArtifactDigest,
            final byte[] envelopeDigest) {
        this.envelopeId = Bytes.copy(envelopeId);
        this.envelopeVersion = envelopeVersion;
        this.logicalGrant = logicalGrant;
        this.committed = committed;
        this.outcomeReserve = outcomeReserve;
        this.nonOutcomeControl = nonOutcomeControl;
        this.recoveryWorking = recoveryWorking;
        this.emergencyHeadroom = emergencyHeadroom;
        this.releaseCapacityArtifactDigest = Bytes.copy(releaseCapacityArtifactDigest);
        this.envelopeDigest = Bytes.copy(envelopeDigest);
    }

    public byte[] envelopeId() {
        return Bytes.copy(envelopeId);
    }

    public long envelopeVersion() {
        return envelopeVersion;
    }

    public QuotaGrantRef logicalGrant() {
        return logicalGrant;
    }

    public CapacityVector committed() {
        return committed;
    }

    public CapacityGrant outcomeReserve() {
        return outcomeReserve;
    }

    public CapacityGrant nonOutcomeControl() {
        return nonOutcomeControl;
    }

    public CapacityGrant recoveryWorking() {
        return recoveryWorking;
    }

    public CapacityGrant emergencyHeadroom() {
        return emergencyHeadroom;
    }

    public byte[] releaseCapacityArtifactDigest() {
        return Bytes.copy(releaseCapacityArtifactDigest);
    }

    public byte[] envelopeDigest() {
        return Bytes.copy(envelopeDigest);
    }

    /** Returns the four component grants as an immutable list in registry field order. */
    public java.util.List<CapacityGrant> componentGrants() {
        return java.util.List.of(outcomeReserve, nonOutcomeControl, recoveryWorking, emergencyHeadroom);
    }

    /** Validates that the registered component projections fit in the committed envelope. */
    public void validateProjection() {
        final CapacityVector sum = outcomeReserve
                .vector()
                .add(nonOutcomeControl.vector())
                .add(recoveryWorking.vector())
                .add(emergencyHeadroom.vector());
        if (!committed.covers(sum)) {
            throw new IllegalArgumentException("capacity component grants exceed committed envelope");
        }
        if (!committed.covers(logicalGrant.limit().toCapacityVector())) {
            throw new IllegalArgumentException("logical quota grant exceeds committed envelope");
        }
        final long[] logical = logicalGrant.limit().toCapacityVector().amounts();
        final long[] full = committed.amounts();
        for (int index = 0; index < 17; index++) {
            if (full[index] != logical[index]) {
                throw new IllegalArgumentException("committed logical dimensions do not project the quota grant");
            }
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToTen());
            CanonicalProtobuf.bytes(output, 11, envelopeDigest);
        });
    }

    public static ShardCapacityEnvelope decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ShardCapacityEnvelope");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "ShardCapacityEnvelope");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported ShardCapacityEnvelope schema version");
        }
        final byte[] envelopeId = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        if (isZero(envelopeId)) {
            throw new IllegalArgumentException("envelopeId must be non-zero");
        }
        final long envelopeVersion = QueryCodecSupport.uint64Bits(fields.get(2), 3);
        if (envelopeVersion == 0) {
            throw new IllegalArgumentException("envelopeVersion must be nonzero");
        }
        final QuotaGrantRef logicalGrant = QuotaGrantRef.decode(QueryCodecSupport.nested(fields.get(3), 4));
        final CapacityVector committed = CapacityVector.decode(QueryCodecSupport.nested(fields.get(4), 5));
        final CapacityGrant outcome = CapacityGrant.decode(QueryCodecSupport.nested(fields.get(5), 6));
        final CapacityGrant nonOutcome = CapacityGrant.decode(QueryCodecSupport.nested(fields.get(6), 7));
        final CapacityGrant recovery = CapacityGrant.decode(QueryCodecSupport.nested(fields.get(7), 8));
        final CapacityGrant emergency = CapacityGrant.decode(QueryCodecSupport.nested(fields.get(8), 9));
        final byte[] artifact = QueryCodecSupport.fixed(fields.get(9), 10, HASH_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(10), 11, HASH_LENGTH);
        final ShardCapacityEnvelope result = new ShardCapacityEnvelope(
                envelopeId,
                envelopeVersion,
                logicalGrant,
                committed,
                outcome,
                nonOutcome,
                recovery,
                emergency,
                artifact,
                digest);
        result.validateProjection();
        if (!Arrays.equals(digest, Bytes.sha256(DIGEST_DOMAIN, result.fieldsOneToTen()))) {
            throw new IllegalArgumentException("ShardCapacityEnvelope digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ShardCapacityEnvelope");
        return result;
    }

    private byte[] fieldsOneToTen() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_VERSION);
            CanonicalProtobuf.bytes(output, 2, envelopeId);
            CanonicalProtobuf.uint64Bits(output, 3, envelopeVersion);
            CanonicalProtobuf.bytes(output, 4, logicalGrant.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, committed.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, outcomeReserve.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, nonOutcomeControl.canonicalBytes());
            CanonicalProtobuf.bytes(output, 8, recoveryWorking.canonicalBytes());
            CanonicalProtobuf.bytes(output, 9, emergencyHeadroom.canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, releaseCapacityArtifactDigest);
        });
    }

    private static CapacityGrant requireGrant(
            final CapacityGrant grant, final CapacityGrantKind expected, final String name) {
        Objects.requireNonNull(grant, name);
        if (grant.kind() != expected) {
            throw new IllegalArgumentException(name + " has wrong CapacityGrantKind");
        }
        return grant;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] fixedNonZero(final byte[] value, final String name) {
        final byte[] fixed = fixed(value, name);
        if (isZero(fixed)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return fixed;
    }

    private static boolean isZero(final byte[] value) {
        for (byte current : value) {
            if (current != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ShardCapacityEnvelope that
                && envelopeVersion == that.envelopeVersion
                && Arrays.equals(envelopeId, that.envelopeId)
                && logicalGrant.equals(that.logicalGrant)
                && committed.equals(that.committed)
                && outcomeReserve.equals(that.outcomeReserve)
                && nonOutcomeControl.equals(that.nonOutcomeControl)
                && recoveryWorking.equals(that.recoveryWorking)
                && emergencyHeadroom.equals(that.emergencyHeadroom)
                && Arrays.equals(releaseCapacityArtifactDigest, that.releaseCapacityArtifactDigest)
                && Arrays.equals(envelopeDigest, that.envelopeDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(envelopeId),
                envelopeVersion,
                logicalGrant,
                committed,
                outcomeReserve,
                nonOutcomeControl,
                recoveryWorking,
                emergencyHeadroom,
                Arrays.hashCode(releaseCapacityArtifactDigest),
                Arrays.hashCode(envelopeDigest));
    }
}
