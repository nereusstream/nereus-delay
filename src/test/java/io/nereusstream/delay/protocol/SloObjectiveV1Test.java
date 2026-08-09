package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SloObjectiveV1Test {
    @Test
    void objectiveAndBoundStartRoundTrip() {
        final SloObjectiveV1 objective = new SloObjectiveV1(SloObjectiveNameV1.QUERY_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10, List.of(), 7,
                bytes(32, 1));
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.QUERY_LATENCY, identityPayload());
        final SloSampleStartV1 start = new SloSampleStartV1(objective, SloPathV1.NOT_APPLICABLE, identity,
                endpoint(1_000), 1_100L);

        assertArrayEquals(objective.canonicalBytes(), SloObjectiveV1.decode(objective.canonicalBytes()).canonicalBytes());
        assertArrayEquals(start.canonicalBytes(), SloSampleStartV1.decode(start.canonicalBytes()).canonicalBytes());
    }

    @Test
    void objectiveRejectsWrongPopulationExclusionsAndTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new SloObjectiveV1(
                SloObjectiveNameV1.QUERY_LATENCY, SloPopulationV1.HEALTHY,
                SloThresholdDirectionV1.AT_MOST, SloThresholdUnitV1.MILLISECONDS, 1, 1, 1, 1, 1,
                List.of(), 1, bytes(32, 1)));
        final SloObjectiveV1 objective = new SloObjectiveV1(SloObjectiveNameV1.QUERY_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 1, 1, 1, 1, List.of(), 1, bytes(32, 1));
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.QUERY_LATENCY, identityPayload());
        final SloSampleStartV1 wrongTimeout = new SloSampleStartV1(objective.objectiveDigest(),
                objective.name(), objective.population(), SloPathV1.NOT_APPLICABLE, identity, endpoint(1_000),
                1_101L);
        assertThrows(IllegalArgumentException.class, () -> objective.validateStart(wrongTimeout));
    }

    @Test
    void objectiveRoundTripsCompleteUnsigned64BitFields() {
        final SloObjectiveV1 objective = new SloObjectiveV1(SloObjectiveNameV1.QUERY_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, Long.MIN_VALUE, Long.MIN_VALUE, -1L,
                Long.MIN_VALUE, Long.MIN_VALUE, List.of(), Long.MIN_VALUE, bytes(32, 2));

        final SloObjectiveV1 decoded = SloObjectiveV1.decode(objective.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.threshold());
        assertEquals(Long.MIN_VALUE, decoded.objectiveNumerator());
        assertEquals(-1L, decoded.objectiveDenominator());
        assertEquals(Long.MIN_VALUE, decoded.rollingWindowMs());
        assertEquals(Long.MIN_VALUE, decoded.minimumSamples());
        assertEquals(Long.MIN_VALUE, decoded.healthyLoadEnvelopeVersion());
        assertArrayEquals(objective.canonicalBytes(), decoded.canonicalBytes());
    }

    @Test
    void dueHealthyObjectiveMustMatchItsAllAcceptedCompanion() {
        final SloObjectiveV1 healthy = dueObjective(SloPopulationV1.HEALTHY,
                List.of(DueExclusionReasonV1.CAPACITY_GATED, DueExclusionReasonV1.ADMIN_PAUSED));
        final SloObjectiveV1 allAccepted = dueObjective(SloPopulationV1.ALL_ACCEPTED, List.of());

        healthy.validateDueCompanion(allAccepted);
        assertThrows(IllegalArgumentException.class,
                () -> healthy.validateDueCompanion(dueObjective(SloPopulationV1.ALL_ACCEPTED,
                List.of(DueExclusionReasonV1.CAPACITY_GATED))));
    }

    @Test
    void dueStartMustMatchIdentityPathAndSemanticStartEpoch() {
        final SloObjectiveV1 objective = dueObjective(SloPopulationV1.ALL_ACCEPTED, List.of());
        final SloSampleEventIdentityV1 identity = dueIdentity(100, SloPathV1.ORDINARY_MANAGED);

        assertThrows(IllegalArgumentException.class,
                () -> new SloSampleStartV1(objective, SloPathV1.MANAGED_PULSAR_HANDOFF,
                        identity, endpoint(100), 200L));
        assertThrows(IllegalArgumentException.class,
                () -> new SloSampleStartV1(objective, SloPathV1.ORDINARY_MANAGED,
                        identity, endpoint(101), 201L));
    }

    @Test
    void dueIdentityRejectsNegativePathStartEpoch() {
        assertThrows(IllegalArgumentException.class, () -> new SloSampleEventIdentityV1(
                SloObjectiveNameV1.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 9));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.int64(output, 3, -1);
                    CanonicalProtobuf.uint32(output, 4, SloPathV1.ORDINARY_MANAGED.wireValue());
                })));
    }

    private static SloObjectiveV1 dueObjective(final SloPopulationV1 population,
                                               final List<DueExclusionReasonV1> exclusions) {
        return new SloObjectiveV1(SloObjectiveNameV1.DUE_ADMISSION_LAG, population,
                SloThresholdDirectionV1.AT_MOST, SloThresholdUnitV1.MILLISECONDS, 100, 99, 100,
                60_000, 10, exclusions, 7, bytes(32, 3));
    }

    private static byte[] identityPayload() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, bytes(16, 7)));
    }

    private static SloSampleEventIdentityV1 dueIdentity(final long startEpoch, final SloPathV1 path) {
        return new SloSampleEventIdentityV1(SloObjectiveNameV1.DUE_ADMISSION_LAG,
                CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 8));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.int64(output, 3, startEpoch);
                    CanonicalProtobuf.uint32(output, 4, path.wireValue());
                }));
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }
}
