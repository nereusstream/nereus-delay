package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable Registry objective used to bind a durable SLO sample Start. */
public final class SloObjectiveV1 {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-slo-objective-v1\0");

    private final SloObjectiveNameV1 name;
    private final SloPopulationV1 population;
    private final SloThresholdDirectionV1 direction;
    private final SloThresholdUnitV1 unit;
    private final long threshold;
    private final long objectiveNumerator;
    private final long objectiveDenominator;
    private final long rollingWindowMs;
    private final long minimumSamples;
    private final SloTimeoutTreatmentV1 timeoutTreatment;
    private final List<DueExclusionReasonV1> exclusions;
    private final long healthyLoadEnvelopeVersion;
    private final byte[] healthyLoadEnvelopeDigest;
    private final byte[] objectiveDigest;

    public SloObjectiveV1(
            final SloObjectiveNameV1 name,
            final SloPopulationV1 population,
            final SloThresholdDirectionV1 direction,
            final SloThresholdUnitV1 unit,
            final long threshold,
            final long objectiveNumerator,
            final long objectiveDenominator,
            final long rollingWindowMs,
            final long minimumSamples,
            final List<DueExclusionReasonV1> exclusions,
            final long healthyLoadEnvelopeVersion,
            final byte[] healthyLoadEnvelopeDigest) {
        this(
                name,
                population,
                direction,
                unit,
                threshold,
                objectiveNumerator,
                objectiveDenominator,
                rollingWindowMs,
                minimumSamples,
                SloTimeoutTreatmentV1.BAD,
                exclusions,
                healthyLoadEnvelopeVersion,
                healthyLoadEnvelopeDigest,
                null);
    }

    private SloObjectiveV1(
            final SloObjectiveNameV1 name,
            final SloPopulationV1 population,
            final SloThresholdDirectionV1 direction,
            final SloThresholdUnitV1 unit,
            final long threshold,
            final long objectiveNumerator,
            final long objectiveDenominator,
            final long rollingWindowMs,
            final long minimumSamples,
            final SloTimeoutTreatmentV1 timeoutTreatment,
            final List<DueExclusionReasonV1> exclusions,
            final long healthyLoadEnvelopeVersion,
            final byte[] healthyLoadEnvelopeDigest,
            final byte[] suppliedDigest) {
        this.name = Objects.requireNonNull(name, "name");
        this.population = Objects.requireNonNull(population, "population");
        this.direction = Objects.requireNonNull(direction, "direction");
        this.unit = Objects.requireNonNull(unit, "unit");
        if (objectiveNumerator == 0
                || objectiveDenominator == 0
                || Long.compareUnsigned(objectiveNumerator, objectiveDenominator) > 0) {
            throw new IllegalArgumentException("invalid SLO objective scalar");
        }
        this.threshold = threshold;
        this.objectiveNumerator = objectiveNumerator;
        this.objectiveDenominator = objectiveDenominator;
        this.rollingWindowMs = rollingWindowMs;
        this.minimumSamples = minimumSamples;
        this.timeoutTreatment = Objects.requireNonNull(timeoutTreatment, "timeoutTreatment");
        if (timeoutTreatment != SloTimeoutTreatmentV1.BAD) {
            throw new IllegalArgumentException("V1 only supports BAD timeout treatment");
        }
        this.exclusions = sortedExclusions(exclusions);
        if (healthyLoadEnvelopeVersion == 0) {
            throw new IllegalArgumentException("healthyLoadEnvelopeVersion must be positive");
        }
        this.healthyLoadEnvelopeVersion = healthyLoadEnvelopeVersion;
        Bytes.requireLength(healthyLoadEnvelopeDigest, HASH_LENGTH, "healthyLoadEnvelopeDigest");
        this.healthyLoadEnvelopeDigest = Bytes.copy(healthyLoadEnvelopeDigest);
        validateSemantics();
        final byte[] expectedDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToThirteen());
        if (suppliedDigest != null && !Arrays.equals(suppliedDigest, expectedDigest)) {
            throw new IllegalArgumentException("SLO objective digest mismatch");
        }
        this.objectiveDigest = expectedDigest;
    }

    public SloObjectiveNameV1 name() {
        return name;
    }

    public SloPopulationV1 population() {
        return population;
    }

    public SloThresholdDirectionV1 direction() {
        return direction;
    }

    public SloThresholdUnitV1 unit() {
        return unit;
    }

    public long threshold() {
        return threshold;
    }

    public long objectiveNumerator() {
        return objectiveNumerator;
    }

    public long objectiveDenominator() {
        return objectiveDenominator;
    }

    public long rollingWindowMs() {
        return rollingWindowMs;
    }

    public long minimumSamples() {
        return minimumSamples;
    }

    public SloTimeoutTreatmentV1 timeoutTreatment() {
        return timeoutTreatment;
    }

    public List<DueExclusionReasonV1> exclusions() {
        return exclusions;
    }

    public long healthyLoadEnvelopeVersion() {
        return healthyLoadEnvelopeVersion;
    }

    public byte[] healthyLoadEnvelopeDigest() {
        return Bytes.copy(healthyLoadEnvelopeDigest);
    }

    public byte[] objectiveDigest() {
        return Bytes.copy(objectiveDigest);
    }

    /** Validates the exact Start/time-out contract for this objective. */
    public void validateStart(final SloSampleStartV1 start) {
        Objects.requireNonNull(start, "start");
        start.validateAgainst(this);
    }

    /**
     * Validates the immutable same-event pair required for a due-admission
     * HEALTHY objective and its ALL_ACCEPTED companion.
     *
     * <p>The pair is catalog metadata rather than an additional wire message:
     * the two objective digests remain independent, while every measurement
     * policy field other than population and the HEALTHY exclusion set must
     * agree.</p>
     */
    public void validateDueCompanion(final SloObjectiveV1 allAccepted) {
        Objects.requireNonNull(allAccepted, "allAccepted");
        if (name != SloObjectiveNameV1.DUE_ADMISSION_LAG
                || population != SloPopulationV1.HEALTHY
                || exclusions.isEmpty()) {
            throw new IllegalArgumentException("the primary due objective must be HEALTHY with exclusions");
        }
        if (allAccepted.name != SloObjectiveNameV1.DUE_ADMISSION_LAG
                || allAccepted.population != SloPopulationV1.ALL_ACCEPTED
                || !allAccepted.exclusions.isEmpty()) {
            throw new IllegalArgumentException("the due companion must be ALL_ACCEPTED without exclusions");
        }
        if (direction != allAccepted.direction
                || unit != allAccepted.unit
                || threshold != allAccepted.threshold
                || objectiveNumerator != allAccepted.objectiveNumerator
                || objectiveDenominator != allAccepted.objectiveDenominator
                || rollingWindowMs != allAccepted.rollingWindowMs
                || minimumSamples != allAccepted.minimumSamples
                || timeoutTreatment != allAccepted.timeoutTreatment
                || healthyLoadEnvelopeVersion != allAccepted.healthyLoadEnvelopeVersion
                || !Arrays.equals(healthyLoadEnvelopeDigest, allAccepted.healthyLoadEnvelopeDigest)) {
            throw new IllegalArgumentException("due objective and companion policy fields differ");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToThirteen());
            CanonicalProtobuf.bytes(output, 14, objectiveDigest);
        });
    }

    public static SloObjectiveV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 13 || fields.size() > 22) {
            throw new IllegalArgumentException("SloObjectiveV1 has an unexpected field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3
                || fields.get(3).number() != 4
                || fields.get(4).number() != 5
                || fields.get(5).number() != 6
                || fields.get(6).number() != 7
                || fields.get(7).number() != 8
                || fields.get(8).number() != 9
                || fields.get(9).number() != 10) {
            throw new IllegalArgumentException("SloObjectiveV1 scalar fields are incomplete");
        }
        int index = 10;
        final List<DueExclusionReasonV1> exclusions = new ArrayList<>();
        while (index < fields.size() - 3 && fields.get(index).number() == 11) {
            exclusions.add(DueExclusionReasonV1.fromWire(QueryCodecSupport.uint(fields.get(index), 11)));
            index++;
        }
        if (index + 3 != fields.size()
                || fields.get(index).number() != 12
                || fields.get(index + 1).number() != 13
                || fields.get(index + 2).number() != 14) {
            throw new IllegalArgumentException("SloObjectiveV1 trailing fields are incomplete");
        }
        final SloObjectiveV1 result = new SloObjectiveV1(
                SloObjectiveNameV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                SloPopulationV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                SloThresholdDirectionV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                SloThresholdUnitV1.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                QueryCodecSupport.uint64Bits(fields.get(4), 5),
                QueryCodecSupport.uint64Bits(fields.get(5), 6),
                QueryCodecSupport.uint64Bits(fields.get(6), 7),
                QueryCodecSupport.uint64Bits(fields.get(7), 8),
                QueryCodecSupport.uint64Bits(fields.get(8), 9),
                SloTimeoutTreatmentV1.fromWire(QueryCodecSupport.uint(fields.get(9), 10)),
                exclusions,
                QueryCodecSupport.uint64Bits(fields.get(index), 12),
                QueryCodecSupport.fixed(fields.get(index + 1), 13, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(index + 2), 14, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloObjectiveV1");
        return result;
    }

    private byte[] fieldsOneToThirteen() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, name.wireValue());
            CanonicalProtobuf.uint32(output, 2, population.wireValue());
            CanonicalProtobuf.uint32(output, 3, direction.wireValue());
            CanonicalProtobuf.uint32(output, 4, unit.wireValue());
            CanonicalProtobuf.uint64Bits(output, 5, threshold);
            CanonicalProtobuf.uint64Bits(output, 6, objectiveNumerator);
            CanonicalProtobuf.uint64Bits(output, 7, objectiveDenominator);
            CanonicalProtobuf.uint64Bits(output, 8, rollingWindowMs);
            CanonicalProtobuf.uint64Bits(output, 9, minimumSamples);
            CanonicalProtobuf.uint32(output, 10, timeoutTreatment.wireValue());
            for (DueExclusionReasonV1 exclusion : exclusions) {
                CanonicalProtobuf.uint32(output, 11, exclusion.wireValue());
            }
            CanonicalProtobuf.uint64Bits(output, 12, healthyLoadEnvelopeVersion);
            CanonicalProtobuf.bytes(output, 13, healthyLoadEnvelopeDigest);
        });
    }

    private void validateSemantics() {
        final boolean sourceMargin = name == SloObjectiveNameV1.SOURCE_RETENTION_TIME_MARGIN
                || name == SloObjectiveNameV1.SOURCE_RETENTION_BYTE_MARGIN;
        final SloThresholdUnitV1 expectedUnit = name == SloObjectiveNameV1.SOURCE_RETENTION_BYTE_MARGIN
                ? SloThresholdUnitV1.BYTES
                : SloThresholdUnitV1.MILLISECONDS;
        if (sourceMargin) {
            if (direction != SloThresholdDirectionV1.AT_LEAST || unit != expectedUnit) {
                throw new IllegalArgumentException("source margin objective has invalid direction/unit");
            }
        } else if (direction != SloThresholdDirectionV1.AT_MOST || unit != SloThresholdUnitV1.MILLISECONDS) {
            throw new IllegalArgumentException("SLO latency/RTO objective has invalid direction/unit");
        }
        final boolean due = name == SloObjectiveNameV1.DUE_ADMISSION_LAG;
        final boolean healthyLane = name == SloObjectiveNameV1.HEALTHY_LANE_DISCOVERY_AGE
                || name == SloObjectiveNameV1.HEALTHY_LANE_SERVICE_GAP;
        if (healthyLane) {
            if (population != SloPopulationV1.HEALTHY || !exclusions.isEmpty()) {
                throw new IllegalArgumentException("healthy Lane objectives require HEALTHY with no exclusions");
            }
        } else if (due) {
            if (population == SloPopulationV1.HEALTHY && exclusions.isEmpty()) {
                throw new IllegalArgumentException("healthy due objective requires exclusion reasons");
            }
            if (population == SloPopulationV1.ALL_ACCEPTED && !exclusions.isEmpty()) {
                throw new IllegalArgumentException("ALL_ACCEPTED due objective cannot carry exclusions");
            }
        } else if (population != SloPopulationV1.ALL_ACCEPTED || !exclusions.isEmpty()) {
            throw new IllegalArgumentException("this objective requires ALL_ACCEPTED with no exclusions");
        }
    }

    private static List<DueExclusionReasonV1> sortedExclusions(final List<DueExclusionReasonV1> values) {
        Objects.requireNonNull(values, "exclusions");
        final List<DueExclusionReasonV1> copy = new ArrayList<>(values);
        copy.sort(java.util.Comparator.comparingInt(DueExclusionReasonV1::wireValue));
        for (int index = 1; index < copy.size(); index++) {
            if (copy.get(index - 1).wireValue() >= copy.get(index).wireValue()) {
                throw new IllegalArgumentException("SLO exclusions must be numeric-sorted and unique");
            }
        }
        return List.copyOf(copy);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloObjectiveV1 that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
