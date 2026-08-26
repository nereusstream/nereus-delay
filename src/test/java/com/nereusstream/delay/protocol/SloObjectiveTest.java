package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class SloObjectiveTest {
    @Test
    void objectiveAndBoundStartRoundTrip() {
        final SloObjective objective = new SloObjective(
                SloObjectiveName.QUERY_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                List.of(),
                7,
                bytes(32, 1));
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.QUERY_LATENCY, identityPayload());
        final SloSampleStart start =
                new SloSampleStart(objective, SloPath.NOT_APPLICABLE, identity, endpoint(1_000), 1_100L);

        assertArrayEquals(
                objective.canonicalBytes(),
                SloObjective.decode(objective.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                start.canonicalBytes(),
                SloSampleStart.decode(start.canonicalBytes()).canonicalBytes());
    }

    @Test
    void objectiveRejectsWrongPopulationExclusionsAndTimeout() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SloObjective(
                        SloObjectiveName.QUERY_LATENCY,
                        SloPopulation.HEALTHY,
                        SloThresholdDirection.AT_MOST,
                        SloThresholdUnit.MILLISECONDS,
                        1,
                        1,
                        1,
                        1,
                        1,
                        List.of(),
                        1,
                        bytes(32, 1)));
        final SloObjective objective = new SloObjective(
                SloObjectiveName.QUERY_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                1,
                1,
                1,
                1,
                List.of(),
                1,
                bytes(32, 1));
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.QUERY_LATENCY, identityPayload());
        final SloSampleStart wrongTimeout = new SloSampleStart(
                objective.objectiveDigest(),
                objective.name(),
                objective.population(),
                SloPath.NOT_APPLICABLE,
                identity,
                endpoint(1_000),
                1_101L);
        assertThrows(IllegalArgumentException.class, () -> objective.validateStart(wrongTimeout));
    }

    @Test
    void objectiveRoundTripsCompleteUnsigned64BitFields() {
        final SloObjective objective = new SloObjective(
                SloObjectiveName.QUERY_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                -1L,
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                List.of(),
                Long.MIN_VALUE,
                bytes(32, 2));

        final SloObjective decoded = SloObjective.decode(objective.canonicalBytes());
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
        final SloObjective healthy = dueObjective(
                SloPopulation.HEALTHY, List.of(DueExclusionReason.CAPACITY_GATED, DueExclusionReason.ADMIN_PAUSED));
        final SloObjective allAccepted = dueObjective(SloPopulation.ALL_ACCEPTED, List.of());

        healthy.validateDueCompanion(allAccepted);
        assertThrows(
                IllegalArgumentException.class,
                () -> healthy.validateDueCompanion(
                        dueObjective(SloPopulation.ALL_ACCEPTED, List.of(DueExclusionReason.CAPACITY_GATED))));
    }

    @Test
    void dueStartMustMatchIdentityPathAndSemanticStartEpoch() {
        final SloObjective objective = dueObjective(SloPopulation.ALL_ACCEPTED, List.of());
        final SloSampleEventIdentity identity = dueIdentity(100, SloPath.ORDINARY_MANAGED);

        assertThrows(
                IllegalArgumentException.class,
                () -> new SloSampleStart(objective, SloPath.MANAGED_PULSAR_HANDOFF, identity, endpoint(100), 200L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SloSampleStart(objective, SloPath.ORDINARY_MANAGED, identity, endpoint(101), 201L));
    }

    @Test
    void dueIdentityRejectsNegativePathStartEpoch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SloSampleEventIdentity(
                        SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                            CanonicalProtobuf.bytes(output, 1, bytes(41, 9));
                            CanonicalProtobuf.uint32(output, 2, 1);
                            CanonicalProtobuf.int64(output, 3, -1);
                            CanonicalProtobuf.uint32(output, 4, SloPath.ORDINARY_MANAGED.wireValue());
                        })));
    }

    private static SloObjective dueObjective(
            final SloPopulation population, final List<DueExclusionReason> exclusions) {
        return new SloObjective(
                SloObjectiveName.DUE_ADMISSION_LAG,
                population,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                exclusions,
                7,
                bytes(32, 3));
    }

    private static byte[] identityPayload() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, bytes(16, 7)));
    }

    private static SloSampleEventIdentity dueIdentity(final long startEpoch, final SloPath path) {
        return new SloSampleEventIdentity(SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, 8));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.int64(output, 3, startEpoch);
            CanonicalProtobuf.uint32(output, 4, path.wireValue());
        }));
    }

    private static SloTimeEndpoint endpoint(final long epochMs) {
        return new SloTimeEndpoint(
                SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }
}
