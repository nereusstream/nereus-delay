package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Canonical durable SLO Final projection with conservative merge support. */
public final class SloSampleFinalV1 {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-slo-final-v1\0");

    private final byte[] sampleId;
    private final byte[] startDigest;
    private final SloFinalOutcomeV1 outcome;
    private final SloThresholdUnitV1 unit;
    private final long measuredLower;
    private final long measuredUpper;
    private final DueExclusionReasonV1 exclusionReason;
    private final SloTimeEndpointV1 finalObservation;
    private final byte[] sourceEventEvidenceSha256;
    private final long observationRevision;
    private final byte[] finalDigest;

    public SloSampleFinalV1(final byte[] sampleId, final byte[] startDigest, final SloFinalOutcomeV1 outcome,
                            final SloThresholdUnitV1 unit, final long measuredLower, final long measuredUpper,
                            final DueExclusionReasonV1 exclusionReason,
                            final SloTimeEndpointV1 finalObservation,
                            final byte[] sourceEventEvidenceSha256, final long observationRevision) {
        this(sampleId, startDigest, outcome, unit, measuredLower, measuredUpper, exclusionReason,
                finalObservation, sourceEventEvidenceSha256, observationRevision, null);
    }

    private SloSampleFinalV1(final byte[] sampleId, final byte[] startDigest, final SloFinalOutcomeV1 outcome,
                             final SloThresholdUnitV1 unit, final long measuredLower, final long measuredUpper,
                             final DueExclusionReasonV1 exclusionReason,
                             final SloTimeEndpointV1 finalObservation,
                             final byte[] sourceEventEvidenceSha256, final long observationRevision,
                             final byte[] suppliedDigest) {
        this.sampleId = fixed(sampleId, "sampleId");
        this.startDigest = fixed(startDigest, "startDigest");
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.unit = Objects.requireNonNull(unit, "unit");
        if (Long.compareUnsigned(measuredUpper, measuredLower) < 0) {
            throw new IllegalArgumentException("SLO measured interval is invalid");
        }
        this.measuredLower = measuredLower;
        this.measuredUpper = measuredUpper;
        if (exclusionReason != null && outcome == SloFinalOutcomeV1.SUCCESS) {
            throw new IllegalArgumentException("a successful SLO final cannot carry an exclusion");
        }
        this.exclusionReason = exclusionReason;
        this.finalObservation = Objects.requireNonNull(finalObservation, "finalObservation");
        this.sourceEventEvidenceSha256 = fixed(sourceEventEvidenceSha256, "sourceEventEvidenceSha256");
        if (observationRevision == 0) {
            throw new IllegalArgumentException("SLO observation revision must be positive");
        }
        this.observationRevision = observationRevision;
        final byte[] expectedDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToEleven());
        if (suppliedDigest != null && !Arrays.equals(suppliedDigest, expectedDigest)) {
            throw new IllegalArgumentException("SLO final digest mismatch");
        }
        this.finalDigest = expectedDigest;
    }

    public byte[] sampleId() {
        return Bytes.copy(sampleId);
    }

    public byte[] startDigest() {
        return Bytes.copy(startDigest);
    }

    public SloFinalOutcomeV1 outcome() {
        return outcome;
    }

    public SloThresholdUnitV1 unit() {
        return unit;
    }

    public long measuredLower() {
        return measuredLower;
    }

    public long measuredUpper() {
        return measuredUpper;
    }

    public DueExclusionReasonV1 exclusionReason() {
        return exclusionReason;
    }

    public SloTimeEndpointV1 finalObservation() {
        return finalObservation;
    }

    public byte[] sourceEventEvidenceSha256() {
        return Bytes.copy(sourceEventEvidenceSha256);
    }

    public long observationRevision() {
        return observationRevision;
    }

    public byte[] finalDigest() {
        return Bytes.copy(finalDigest);
    }

    /** Checks that this final can only close the exact durable Start. */
    public void validateAgainst(final SloSampleStartV1 start) {
        Objects.requireNonNull(start, "start");
        if (!Arrays.equals(sampleId, start.sampleId()) || !Arrays.equals(startDigest, start.startDigest())) {
            throw new IllegalArgumentException("SLO final does not match its durable start");
        }
        if (start.population() == SloPopulationV1.HEALTHY && exclusionReason != null) {
            throw new IllegalArgumentException("HEALTHY SLO final cannot carry an exclusion");
        }
        if (exclusionReason != null && (start.objective() != SloObjectiveNameV1.DUE_ADMISSION_LAG
                || start.population() != SloPopulationV1.ALL_ACCEPTED)) {
            throw new IllegalArgumentException("exclusions are only valid for due ALL_ACCEPTED samples");
        }
    }

    /**
     * Validates the additional catalog binding for an excluded ALL_ACCEPTED
     * due sample. A reason is valid only when the paired HEALTHY objective
     * explicitly declares it.
     */
    public void validateAgainst(final SloSampleStartV1 start, final SloObjectiveV1 healthyObjective) {
        validateAgainst(start);
        Objects.requireNonNull(healthyObjective, "healthyObjective");
        if (exclusionReason == null) {
            return;
        }
        if (start.objective() != SloObjectiveNameV1.DUE_ADMISSION_LAG
                || start.population() != SloPopulationV1.ALL_ACCEPTED) {
            throw new IllegalArgumentException("exclusion requires an ALL_ACCEPTED due sample");
        }
        if (!healthyObjective.exclusions().contains(exclusionReason)) {
            throw new IllegalArgumentException("SLO exclusion is not in the paired HEALTHY objective set");
        }
    }

    /**
     * Merges repeated exporter/recovery observations without allowing a bad
     * result or a worse measurement to be erased.
     */
    public static SloSampleFinalV1 merge(final SloSampleFinalV1 current, final SloSampleFinalV1 incoming,
                                         final SloThresholdDirectionV1 direction) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(incoming, "incoming");
        Objects.requireNonNull(direction, "direction");
        if (!Arrays.equals(current.sampleId, incoming.sampleId)
                || !Arrays.equals(current.startDigest, incoming.startDigest)
                || current.unit != incoming.unit) {
            throw new IllegalArgumentException("SLO finals do not identify the same sample");
        }
        if (current.observationRevision == incoming.observationRevision
                && !Arrays.equals(current.canonicalBytes(), incoming.canonicalBytes())) {
            throw new IllegalArgumentException("same SLO observation revision has different bytes");
        }
        if (Long.compareUnsigned(incoming.observationRevision, current.observationRevision) < 0
                && !Arrays.equals(current.canonicalBytes(), incoming.canonicalBytes())) {
            throw new IllegalArgumentException("SLO observation revision regressed with different bytes");
        }
        if (current.exclusionReason != null && incoming.exclusionReason != null
                && current.exclusionReason != incoming.exclusionReason) {
            throw new IllegalArgumentException("SLO Finals carry conflicting exclusion reasons");
        }
        final SloSampleFinalV1 evidenceSource = compareEvidence(current, incoming) >= 0 ? current : incoming;
        final long lower = direction == SloThresholdDirectionV1.AT_MOST
                ? unsignedMax(current.measuredLower, incoming.measuredLower)
                : unsignedMin(current.measuredLower, incoming.measuredLower);
        final long upper = direction == SloThresholdDirectionV1.AT_MOST
                ? unsignedMax(current.measuredUpper, incoming.measuredUpper)
                : unsignedMin(current.measuredUpper, incoming.measuredUpper);
        final DueExclusionReasonV1 exclusion = current.exclusionReason != null
                ? current.exclusionReason : incoming.exclusionReason;
        return new SloSampleFinalV1(current.sampleId, current.startDigest, evidenceSource.outcome,
                current.unit, lower, upper, exclusion, evidenceSource.finalObservation,
                evidenceSource.sourceEventEvidenceSha256, unsignedMax(current.observationRevision,
                incoming.observationRevision));
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToEleven());
            CanonicalProtobuf.bytes(output, 12, finalDigest);
        });
    }

    public static SloSampleFinalV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloSampleFinalV1");
        if (fields.size() != 11 && fields.size() != 12) {
            throw new IllegalArgumentException("SloSampleFinalV1 has an unexpected field count");
        }
        final int[] expected = fields.size() == 11
                ? new int[]{1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12}
                : new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        QueryCodecSupport.requireNumbers(fields, expected, "SloSampleFinalV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported SloSampleFinalV1 version");
        }
        final SloSampleFinalV1 result = new SloSampleFinalV1(
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                SloFinalOutcomeV1.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                SloThresholdUnitV1.fromWire(QueryCodecSupport.uint(fields.get(4), 5)),
                QueryCodecSupport.uint64Bits(fields.get(5), 6), QueryCodecSupport.uint64Bits(fields.get(6), 7),
                fields.size() == 12
                        ? DueExclusionReasonV1.fromWire(QueryCodecSupport.uint(fields.get(7), 8)) : null,
                SloTimeEndpointV1.decode(QueryCodecSupport.nested(fields.get(fields.size() == 12 ? 8 : 7), 9)),
                QueryCodecSupport.fixed(fields.get(fields.size() == 12 ? 9 : 8), 10, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(fields.size() == 12 ? 10 : 9), 11),
                QueryCodecSupport.fixed(fields.get(fields.size() - 1), 12, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloSampleFinalV1");
        return result;
    }

    private byte[] fieldsOneToEleven() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, sampleId);
            CanonicalProtobuf.bytes(output, 3, startDigest);
            CanonicalProtobuf.uint32(output, 4, outcome.wireValue());
            CanonicalProtobuf.uint32(output, 5, unit.wireValue());
            CanonicalProtobuf.uint64Bits(output, 6, measuredLower);
            CanonicalProtobuf.uint64Bits(output, 7, measuredUpper);
            if (exclusionReason != null) {
                CanonicalProtobuf.uint32(output, 8, exclusionReason.wireValue());
            }
            CanonicalProtobuf.bytes(output, 9, finalObservation.canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, sourceEventEvidenceSha256);
            CanonicalProtobuf.uint64Bits(output, 11, observationRevision);
        });
    }

    private static int severity(final SloFinalOutcomeV1 value) {
        return switch (value) {
            case SUCCESS -> 0;
            case BAD_DEFINITIVE -> 1;
            case BAD_UNCERTAIN -> 2;
            case BAD_TIMEOUT -> 3;
            case BAD_UNQUALIFIED_TIME -> 4;
            case BAD_EVIDENCE_GAP -> 5;
        };
    }

    private static int compareEvidence(final SloSampleFinalV1 left, final SloSampleFinalV1 right) {
        final int severity = Integer.compare(severity(left.outcome), severity(right.outcome));
        return severity != 0 ? severity
                : Long.compareUnsigned(left.observationRevision, right.observationRevision);
    }

    private static long unsignedMax(final long left, final long right) {
        return Long.compareUnsigned(left, right) >= 0 ? left : right;
    }

    private static long unsignedMin(final long left, final long right) {
        return Long.compareUnsigned(left, right) <= 0 ? left : right;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloSampleFinalV1 that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
