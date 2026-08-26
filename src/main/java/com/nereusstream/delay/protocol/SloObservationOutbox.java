package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Component-local durable SLO outbox projection.
 *
 * <p>The Start is immutable. A Final is optional until the semantic endpoint
 * is observed, and repeated Finals are merged with the Registry's conservative
 * direction before the containing WriteBatch is synced.</p>
 */
public final class SloObservationOutbox {
    public static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-slo-outbox\0");

    private final SloSampleStart start;
    private final SloSampleFinal finalObservation;
    private final byte[] recordDigest;

    private SloObservationOutbox(
            final SloSampleStart start, final SloSampleFinal finalObservation, final byte[] suppliedDigest) {
        this.start = Objects.requireNonNull(start, "start");
        this.finalObservation = finalObservation;
        if (finalObservation != null) {
            validateFinalAgainstStart(finalObservation, start, null);
        }
        final byte[] expectedDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToThree());
        if (suppliedDigest != null && !Arrays.equals(suppliedDigest, expectedDigest)) {
            throw new IllegalArgumentException("SLO outbox digest mismatch");
        }
        this.recordDigest = expectedDigest;
    }

    public static SloObservationOutbox open(final SloSampleStart start) {
        return new SloObservationOutbox(start, null, null);
    }

    public SloSampleStart start() {
        return start;
    }

    public SloSampleFinal finalObservation() {
        return finalObservation;
    }

    public byte[] sampleId() {
        return start.sampleId();
    }

    public byte[] recordDigest() {
        return Bytes.copy(recordDigest);
    }

    public SloObservationOutbox mergeFinal(final SloSampleFinal incoming, final SloThresholdDirection direction) {
        Objects.requireNonNull(direction, "direction");
        validateFinalAgainstStart(Objects.requireNonNull(incoming, "incoming"), start, direction);
        final SloSampleFinal merged =
                finalObservation == null ? incoming : SloSampleFinal.merge(finalObservation, incoming, direction);
        return new SloObservationOutbox(start, merged, null);
    }

    /**
     * Compatibility merge for non-excluded projections. An excluded due
     * projection is rejected because this overload does not carry the exact
     * ALL_ACCEPTED companion; use the pair-aware overload below.
     */
    public SloObservationOutbox mergeFinal(
            final SloSampleFinal incoming, final SloThresholdDirection direction, final SloObjective healthyObjective) {
        Objects.requireNonNull(healthyObjective, "healthyObjective");
        Objects.requireNonNull(incoming, "incoming");
        if (incoming.exclusionReason() != null
                || finalObservation != null && finalObservation.exclusionReason() != null) {
            throw new IllegalArgumentException("SLO due exclusions require the exact ALL_ACCEPTED companion objective");
        }
        return mergeFinal(incoming, direction);
    }

    /**
     * Merges a Final after proving the complete HEALTHY/ALL_ACCEPTED due
     * objective pair. This is the only pair-aware merge used by the durable
     * shard outbox; the legacy three-argument overload remains for source
     * compatibility but cannot authorize an exclusion on its own.
     */
    public SloObservationOutbox mergeFinal(
            final SloSampleFinal incoming,
            final SloThresholdDirection direction,
            final SloObjective healthyObjective,
            final SloObjective allAcceptedObjective) {
        Objects.requireNonNull(healthyObjective, "healthyObjective");
        Objects.requireNonNull(allAcceptedObjective, "allAcceptedObjective");
        Objects.requireNonNull(incoming, "incoming").validateAgainst(start, healthyObjective, allAcceptedObjective);
        if (finalObservation != null) {
            finalObservation.validateAgainst(start, healthyObjective, allAcceptedObjective);
        }
        return mergeFinal(incoming, direction);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToThree());
            CanonicalProtobuf.bytes(output, 4, recordDigest);
        });
    }

    public static SloObservationOutbox decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloObservationOutbox");
        if (fields.size() != 3 && fields.size() != 4) {
            throw new IllegalArgumentException("SloObservationOutbox has an unexpected field count");
        }
        final int[] expected = fields.size() == 3 ? new int[] {1, 2, 4} : new int[] {1, 2, 3, 4};
        QueryCodecSupport.requireNumbers(fields, expected, "SloObservationOutbox");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported SloObservationOutbox version");
        }
        final SloSampleStart start = SloSampleStart.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final SloSampleFinal finalObservation =
                fields.size() == 4 ? SloSampleFinal.decode(QueryCodecSupport.nested(fields.get(2), 3)) : null;
        final SloObservationOutbox result = new SloObservationOutbox(
                start, finalObservation, QueryCodecSupport.fixed(fields.get(fields.size() - 1), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloObservationOutbox");
        return result;
    }

    private byte[] fieldsOneToThree() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, start.canonicalBytes());
            if (finalObservation != null) {
                CanonicalProtobuf.bytes(output, 3, finalObservation.canonicalBytes());
            }
        });
    }

    private static void validateFinalAgainstStart(
            final SloSampleFinal finalObservation,
            final SloSampleStart start,
            final SloThresholdDirection mergeDirection) {
        finalObservation.validateAgainst(start);
        final SloObjectiveName objective = start.objective();
        if (finalObservation.unit() != objective.requiredUnit()) {
            throw new IllegalArgumentException("SLO final unit does not match its objective branch");
        }
        if (mergeDirection != null && mergeDirection != objective.requiredDirection()) {
            throw new IllegalArgumentException("SLO merge direction does not match its objective branch");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloObservationOutbox that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
