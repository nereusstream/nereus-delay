package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SloObservationOutboxV1Test {
    @Test
    void startFinalAndOutboxRoundTrip() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.MILLISECONDS,
                4,
                8,
                null,
                brokerEndpoint(200),
                bytes(32, 3),
                1);
        final SloObservationOutboxV1 outbox =
                SloObservationOutboxV1.open(start).mergeFinal(finalObservation, SloThresholdDirectionV1.AT_MOST);

        assertArrayEquals(
                start.canonicalBytes(),
                SloSampleStartV1.decode(start.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                finalObservation.canonicalBytes(),
                SloSampleFinalV1.decode(finalObservation.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                outbox.canonicalBytes(),
                SloObservationOutboxV1.decode(outbox.canonicalBytes()).canonicalBytes());
        assertEquals(finalObservation, outbox.finalObservation());
    }

    @Test
    void mergeNeverImprovesBadObservationAndKeepsWorstAtMostMeasurement() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 success = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.MILLISECONDS,
                4,
                8,
                null,
                brokerEndpoint(200),
                bytes(32, 3),
                1);
        final SloSampleFinalV1 timeout = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                20,
                25,
                null,
                endpoint(300),
                bytes(32, 4),
                2);

        final SloSampleFinalV1 merged = SloSampleFinalV1.merge(success, timeout, SloThresholdDirectionV1.AT_MOST);
        assertEquals(SloFinalOutcomeV1.BAD_TIMEOUT, merged.outcome());
        assertEquals(20, merged.measuredLower());
        assertEquals(25, merged.measuredUpper());
        assertEquals(2, merged.observationRevision());
    }

    @Test
    void mergeUsesNewestEvidenceWhenOutcomeSeverityIsEqual() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 first = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                20,
                25,
                null,
                endpoint(300),
                bytes(32, 10),
                1);
        final SloSampleFinalV1 second = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                25,
                30,
                null,
                endpoint(400),
                bytes(32, 11),
                2);

        final SloSampleFinalV1 merged = SloSampleFinalV1.merge(first, second, SloThresholdDirectionV1.AT_MOST);
        assertEquals(second.finalObservation(), merged.finalObservation());
        assertArrayEquals(second.sourceEventEvidenceSha256(), merged.sourceEventEvidenceSha256());
        assertEquals(2, merged.observationRevision());
        assertEquals(25, merged.measuredLower());
        assertEquals(30, merged.measuredUpper());
    }

    @Test
    void mergeRejectsDifferentBytesFromAnOlderObservationRevision() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 newer = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                20,
                25,
                null,
                endpoint(300),
                bytes(32, 24),
                2);
        final SloSampleFinalV1 older = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                20,
                25,
                null,
                endpoint(301),
                bytes(32, 25),
                1);

        assertThrows(
                IllegalArgumentException.class,
                () -> SloSampleFinalV1.merge(newer, older, SloThresholdDirectionV1.AT_MOST));
    }

    @Test
    void finalRoundTripsAndMergesCompleteUnsigned64BitFields() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 first = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                Long.MIN_VALUE,
                -1L,
                null,
                endpoint(300),
                bytes(32, 17),
                Long.MIN_VALUE);
        final SloSampleFinalV1 decoded = SloSampleFinalV1.decode(first.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.measuredLower());
        assertEquals(-1L, decoded.measuredUpper());
        assertEquals(Long.MIN_VALUE, decoded.observationRevision());
        assertArrayEquals(first.canonicalBytes(), decoded.canonicalBytes());

        final SloSampleFinalV1 wider = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT,
                SloThresholdUnitV1.MILLISECONDS,
                -1L,
                -1L,
                null,
                endpoint(301),
                bytes(32, 18),
                -1L);
        final SloSampleFinalV1 merged = SloSampleFinalV1.merge(first, wider, SloThresholdDirectionV1.AT_MOST);
        assertEquals(-1L, merged.measuredLower());
        assertEquals(-1L, merged.measuredUpper());
        assertEquals(-1L, merged.observationRevision());
    }

    @Test
    void dueIdentityPreservesCompleteUnsignedGenerationBits() {
        final SloSampleEventIdentityV1 identity =
                new SloSampleEventIdentityV1(SloObjectiveNameV1.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 19));
                    CanonicalProtobuf.uint32Bits(output, 2, Integer.MIN_VALUE);
                    CanonicalProtobuf.int64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPathV1.ORDINARY_MANAGED.wireValue());
                }));

        assertArrayEquals(
                identity.canonicalBytes(),
                SloSampleEventIdentityV1.decode(identity.canonicalBytes()).canonicalBytes());
    }

    @Test
    void excludedFinalMustBelongToPairedHealthyObjective() {
        final SloObjectiveV1 healthy =
                dueObjective(SloPopulationV1.HEALTHY, java.util.List.of(DueExclusionReasonV1.CAPACITY_GATED));
        final SloObjectiveV1 allAccepted = dueObjective(SloPopulationV1.ALL_ACCEPTED, java.util.List.of());
        healthy.validateDueCompanion(allAccepted);
        final SloSampleEventIdentityV1 identity =
                new SloSampleEventIdentityV1(SloObjectiveNameV1.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 20));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.int64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPathV1.ORDINARY_MANAGED.wireValue());
                }));
        final SloSampleStartV1 start =
                new SloSampleStartV1(allAccepted, SloPathV1.ORDINARY_MANAGED, identity, endpoint(100), 200L);
        final SloSampleFinalV1 excluded = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                2,
                DueExclusionReasonV1.CAPACITY_GATED,
                endpoint(200),
                bytes(32, 21),
                1);
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.open(start)
                .mergeFinal(excluded, SloThresholdDirectionV1.AT_MOST, healthy, allAccepted);
        assertEquals(excluded, outbox.finalObservation());

        final SloSampleFinalV1 wrongReason = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                2,
                DueExclusionReasonV1.ADMIN_PAUSED,
                endpoint(201),
                bytes(32, 22),
                2);
        assertThrows(IllegalArgumentException.class, () -> wrongReason.validateAgainst(start, healthy, allAccepted));
    }

    @Test
    void excludedFinalRejectsAHealthyObjectiveFromAnotherCatalogPair() {
        final SloObjectiveV1 healthy =
                dueObjective(SloPopulationV1.HEALTHY, java.util.List.of(DueExclusionReasonV1.CAPACITY_GATED));
        final SloObjectiveV1 allAccepted = dueObjective(SloPopulationV1.ALL_ACCEPTED, java.util.List.of());
        final SloObjectiveV1 wrongHealthy = new SloObjectiveV1(
                SloObjectiveNameV1.DUE_ADMISSION_LAG,
                SloPopulationV1.HEALTHY,
                SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS,
                101,
                99,
                100,
                60_000,
                10,
                java.util.List.of(DueExclusionReasonV1.CAPACITY_GATED),
                7,
                bytes(32, 23));
        final SloSampleEventIdentityV1 identity =
                new SloSampleEventIdentityV1(SloObjectiveNameV1.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 26));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.int64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPathV1.ORDINARY_MANAGED.wireValue());
                }));
        final SloSampleStartV1 start =
                new SloSampleStartV1(allAccepted, SloPathV1.ORDINARY_MANAGED, identity, endpoint(100), 200L);
        final SloSampleFinalV1 excluded = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                2,
                DueExclusionReasonV1.CAPACITY_GATED,
                endpoint(200),
                bytes(32, 27),
                1);

        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(excluded, SloThresholdDirectionV1.AT_MOST, wrongHealthy, allAccepted));
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(
                        excluded,
                        SloThresholdDirectionV1.AT_MOST,
                        healthy,
                        new SloObjectiveV1(
                                SloObjectiveNameV1.DUE_ADMISSION_LAG,
                                SloPopulationV1.ALL_ACCEPTED,
                                SloThresholdDirectionV1.AT_MOST,
                                SloThresholdUnitV1.MILLISECONDS,
                                101,
                                99,
                                100,
                                60_000,
                                10,
                                java.util.List.of(),
                                7,
                                bytes(32, 23))));
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(excluded, SloThresholdDirectionV1.AT_MOST, healthy));
    }

    private static SloObjectiveV1 dueObjective(
            final SloPopulationV1 population, final java.util.List<DueExclusionReasonV1> exclusions) {
        return new SloObjectiveV1(
                SloObjectiveNameV1.DUE_ADMISSION_LAG,
                population,
                SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                exclusions,
                7,
                bytes(32, 23));
    }

    @Test
    void mergeRejectsConflictingDueExclusionReasons() {
        final SloSampleStartV1 start = dueAcceptedStart();
        final SloSampleFinalV1 capacity = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                2,
                DueExclusionReasonV1.CAPACITY_GATED,
                endpoint(200),
                bytes(32, 12),
                1);
        final SloSampleFinalV1 paused = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                2,
                DueExclusionReasonV1.ADMIN_PAUSED,
                endpoint(201),
                bytes(32, 13),
                2);

        final SloObservationOutboxV1 outbox =
                SloObservationOutboxV1.open(start).mergeFinal(capacity, SloThresholdDirectionV1.AT_MOST);
        assertThrows(IllegalArgumentException.class, () -> outbox.mergeFinal(paused, SloThresholdDirectionV1.AT_MOST));
        assertThrows(
                IllegalArgumentException.class,
                () -> SloSampleFinalV1.merge(capacity, paused, SloThresholdDirectionV1.AT_MOST));
    }

    @Test
    void rejectsIdentityDriftAndTampering() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                1,
                null,
                brokerEndpoint(200),
                bytes(32, 5),
                1);
        final byte[] tampered = finalObservation.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SloSampleFinalV1.decode(tampered));

        final byte[] otherStartId = start.sampleId();
        otherStartId[0] ^= 1;
        final SloSampleFinalV1 other = new SloSampleFinalV1(
                otherStartId,
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                1,
                DueExclusionReasonV1.CAPACITY_GATED,
                endpoint(300),
                bytes(32, 6),
                2);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(other, SloThresholdDirectionV1.AT_MOST));

        final SloSampleFinalV1 excluded = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                1,
                DueExclusionReasonV1.CAPACITY_GATED,
                endpoint(300),
                bytes(32, 7),
                2);
        assertThrows(IllegalArgumentException.class, () -> excluded.validateAgainst(start));
    }

    @Test
    void rejectsFinalUnitAndMergeDirectionThatDisagreeWithObjective() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 semanticSuccess = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                1,
                null,
                endpoint(200),
                bytes(32, 7),
                1);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(semanticSuccess, SloThresholdDirectionV1.AT_MOST));

        final SloSampleFinalV1 wrongUnit = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.BYTES,
                1,
                1,
                null,
                endpoint(200),
                bytes(32, 8),
                1);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(wrongUnit, SloThresholdDirectionV1.AT_MOST));

        final SloSampleFinalV1 valid = new SloSampleFinalV1(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.MILLISECONDS,
                1,
                1,
                null,
                brokerEndpoint(200),
                bytes(32, 9),
                1);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutboxV1.open(start)
                .mergeFinal(valid, SloThresholdDirectionV1.AT_LEAST));
    }

    private static SloSampleStartV1 start() {
        final byte[] commandHash = bytes(32, 2);
        final byte[] physicalAttemptId = bytes(16, 3);
        final byte[] completeBranchPayload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, 1));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentityV1 identity =
                new SloSampleEventIdentityV1(SloObjectiveNameV1.COMMAND_QUEUED_LATENCY, completeBranchPayload);
        return new SloSampleStartV1(
                bytes(32, 1),
                SloObjectiveNameV1.COMMAND_QUEUED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED,
                SloPathV1.NOT_APPLICABLE,
                identity,
                endpoint(100),
                200L);
    }

    private static SloSampleStartV1 dueAcceptedStart() {
        final SloSampleEventIdentityV1 identity =
                new SloSampleEventIdentityV1(SloObjectiveNameV1.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 14));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.uint64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPathV1.ORDINARY_MANAGED.wireValue());
                }));
        return new SloSampleStartV1(
                bytes(32, 15),
                SloObjectiveNameV1.DUE_ADMISSION_LAG,
                SloPopulationV1.ALL_ACCEPTED,
                SloPathV1.ORDINARY_MANAGED,
                identity,
                endpoint(100),
                200L);
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(
                SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static SloTimeEndpointV1 brokerEndpoint(final long epochMs) {
        return new SloTimeEndpointV1(
                SloTimeEndpointKindV1.BROKER_PERSISTENCE, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
