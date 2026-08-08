package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed capacity envelope bound to one immutable shard placement decision.
 *
 * <p>This class validates the local byte/dimension invariants. It does not
 * perform Oxia placement, Owner Lease CAS, or artifact publication; those
 * remain authority operations outside the protocol codec.</p>
 */
public final class ShardCapacityEnvelopeV1 {
    public static final int SCHEMA_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-shard-capacity-envelope-v1\0");

    private final byte[] envelopeId;
    private final long envelopeVersion;
    private final QuotaGrantRefV1 logicalGrant;
    private final CapacityVectorV1 committed;
    private final CapacityGrantV1 outcomeReserve;
    private final CapacityGrantV1 nonOutcomeControl;
    private final CapacityGrantV1 recoveryWorking;
    private final CapacityGrantV1 emergencyHeadroom;
    private final byte[] releaseCapacityArtifactDigest;
    private final byte[] envelopeDigest;

    public ShardCapacityEnvelopeV1(final byte[] envelopeId, final long envelopeVersion,
                                   final QuotaGrantRefV1 logicalGrant, final CapacityVectorV1 committed,
                                   final CapacityGrantV1 outcomeReserve,
                                   final CapacityGrantV1 nonOutcomeControl,
                                   final CapacityGrantV1 recoveryWorking,
                                   final CapacityGrantV1 emergencyHeadroom,
                                   final byte[] releaseCapacityArtifactDigest) {
        this.envelopeId = fixedNonZero(envelopeId, "envelopeId");
        if (envelopeVersion == 0) {
            throw new IllegalArgumentException("envelopeVersion must be nonzero");
        }
        this.envelopeVersion = envelopeVersion;
        this.logicalGrant = Objects.requireNonNull(logicalGrant, "logicalGrant");
        this.committed = Objects.requireNonNull(committed, "committed");
        this.outcomeReserve = requireGrant(outcomeReserve, CapacityGrantKindV1.OUTCOME_RESERVE,
                "outcomeReserve");
        this.nonOutcomeControl = requireGrant(nonOutcomeControl, CapacityGrantKindV1.NON_OUTCOME_CONTROL,
                "nonOutcomeControl");
        this.recoveryWorking = requireGrant(recoveryWorking, CapacityGrantKindV1.RECOVERY_WORKING,
                "recoveryWorking");
        this.emergencyHeadroom = requireGrant(emergencyHeadroom, CapacityGrantKindV1.EMERGENCY_HEADROOM,
                "emergencyHeadroom");
        this.releaseCapacityArtifactDigest = fixed(releaseCapacityArtifactDigest,
                "releaseCapacityArtifactDigest");
        validateProjection();
        this.envelopeDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToTen());
    }

    private ShardCapacityEnvelopeV1(final byte[] envelopeId, final long envelopeVersion,
                                    final QuotaGrantRefV1 logicalGrant, final CapacityVectorV1 committed,
                                    final CapacityGrantV1 outcomeReserve,
                                    final CapacityGrantV1 nonOutcomeControl,
                                    final CapacityGrantV1 recoveryWorking,
                                    final CapacityGrantV1 emergencyHeadroom,
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

    public QuotaGrantRefV1 logicalGrant() {
        return logicalGrant;
    }

    public CapacityVectorV1 committed() {
        return committed;
    }

    public CapacityGrantV1 outcomeReserve() {
        return outcomeReserve;
    }

    public CapacityGrantV1 nonOutcomeControl() {
        return nonOutcomeControl;
    }

    public CapacityGrantV1 recoveryWorking() {
        return recoveryWorking;
    }

    public CapacityGrantV1 emergencyHeadroom() {
        return emergencyHeadroom;
    }

    public byte[] releaseCapacityArtifactDigest() {
        return Bytes.copy(releaseCapacityArtifactDigest);
    }

    public byte[] envelopeDigest() {
        return Bytes.copy(envelopeDigest);
    }

    /** Returns the four component grants as an immutable list in registry field order. */
    public java.util.List<CapacityGrantV1> componentGrants() {
        return java.util.List.of(outcomeReserve, nonOutcomeControl, recoveryWorking, emergencyHeadroom);
    }

    /** Validates that the registered component projections fit in the committed envelope. */
    public void validateProjection() {
        final CapacityVectorV1 sum = outcomeReserve.vector().add(nonOutcomeControl.vector())
                .add(recoveryWorking.vector()).add(emergencyHeadroom.vector());
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

    public static ShardCapacityEnvelopeV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ShardCapacityEnvelopeV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                "ShardCapacityEnvelopeV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported ShardCapacityEnvelopeV1 schema version");
        }
        final byte[] envelopeId = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        if (isZero(envelopeId)) {
            throw new IllegalArgumentException("envelopeId must be non-zero");
        }
        final long envelopeVersion = QueryCodecSupport.uint64Bits(fields.get(2), 3);
        if (envelopeVersion == 0) {
            throw new IllegalArgumentException("envelopeVersion must be nonzero");
        }
        final QuotaGrantRefV1 logicalGrant = QuotaGrantRefV1.decode(QueryCodecSupport.nested(fields.get(3), 4));
        final CapacityVectorV1 committed = CapacityVectorV1.decode(QueryCodecSupport.nested(fields.get(4), 5));
        final CapacityGrantV1 outcome = CapacityGrantV1.decode(QueryCodecSupport.nested(fields.get(5), 6));
        final CapacityGrantV1 nonOutcome = CapacityGrantV1.decode(QueryCodecSupport.nested(fields.get(6), 7));
        final CapacityGrantV1 recovery = CapacityGrantV1.decode(QueryCodecSupport.nested(fields.get(7), 8));
        final CapacityGrantV1 emergency = CapacityGrantV1.decode(QueryCodecSupport.nested(fields.get(8), 9));
        final byte[] artifact = QueryCodecSupport.fixed(fields.get(9), 10, HASH_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(10), 11, HASH_LENGTH);
        final ShardCapacityEnvelopeV1 result = new ShardCapacityEnvelopeV1(envelopeId, envelopeVersion,
                logicalGrant, committed, outcome, nonOutcome, recovery, emergency, artifact, digest);
        result.validateProjection();
        if (!Arrays.equals(digest, Bytes.sha256(DIGEST_DOMAIN, result.fieldsOneToTen()))) {
            throw new IllegalArgumentException("ShardCapacityEnvelopeV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ShardCapacityEnvelopeV1");
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

    private static CapacityGrantV1 requireGrant(final CapacityGrantV1 grant, final CapacityGrantKindV1 expected,
                                                final String name) {
        Objects.requireNonNull(grant, name);
        if (grant.kind() != expected) {
            throw new IllegalArgumentException(name + " has wrong CapacityGrantKindV1");
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
        return other instanceof ShardCapacityEnvelopeV1 that
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
        return Objects.hash(Arrays.hashCode(envelopeId), envelopeVersion, logicalGrant, committed,
                outcomeReserve, nonOutcomeControl, recoveryWorking, emergencyHeadroom,
                Arrays.hashCode(releaseCapacityArtifactDigest), Arrays.hashCode(envelopeDigest));
    }
}
