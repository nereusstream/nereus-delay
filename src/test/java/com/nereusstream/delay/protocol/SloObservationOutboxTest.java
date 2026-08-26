package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SloObservationOutboxTest {
    @Test
    void startFinalAndOutboxRoundTrip() {
        final SloSampleStart start = start();
        final SloSampleFinal finalObservation = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.SUCCESS,
                SloThresholdUnit.MILLISECONDS,
                4,
                8,
                null,
                brokerEndpoint(200),
                bytes(32, 3),
                1);
        final SloObservationOutbox outbox =
                SloObservationOutbox.open(start).mergeFinal(finalObservation, SloThresholdDirection.AT_MOST);

        assertArrayEquals(
                start.canonicalBytes(),
                SloSampleStart.decode(start.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                finalObservation.canonicalBytes(),
                SloSampleFinal.decode(finalObservation.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                outbox.canonicalBytes(),
                SloObservationOutbox.decode(outbox.canonicalBytes()).canonicalBytes());
        assertEquals(finalObservation, outbox.finalObservation());
    }

    @Test
    void mergeNeverImprovesBadObservationAndKeepsWorstAtMostMeasurement() {
        final SloSampleStart start = start();
        final SloSampleFinal success = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.SUCCESS,
                SloThresholdUnit.MILLISECONDS,
                4,
                8,
                null,
                brokerEndpoint(200),
                bytes(32, 3),
                1);
        final SloSampleFinal timeout = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                20,
                25,
                null,
                endpoint(300),
                bytes(32, 4),
                2);

        final SloSampleFinal merged = SloSampleFinal.merge(success, timeout, SloThresholdDirection.AT_MOST);
        assertEquals(SloFinalOutcome.BAD_TIMEOUT, merged.outcome());
        assertEquals(20, merged.measuredLower());
        assertEquals(25, merged.measuredUpper());
        assertEquals(2, merged.observationRevision());
    }

    @Test
    void mergeUsesNewestEvidenceWhenOutcomeSeverityIsEqual() {
        final SloSampleStart start = start();
        final SloSampleFinal first = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                20,
                25,
                null,
                endpoint(300),
                bytes(32, 10),
                1);
        final SloSampleFinal second = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                25,
                30,
                null,
                endpoint(400),
                bytes(32, 11),
                2);

        final SloSampleFinal merged = SloSampleFinal.merge(first, second, SloThresholdDirection.AT_MOST);
        assertEquals(second.finalObservation(), merged.finalObservation());
        assertArrayEquals(second.sourceEventEvidenceSha256(), merged.sourceEventEvidenceSha256());
        assertEquals(2, merged.observationRevision());
        assertEquals(25, merged.measuredLower());
        assertEquals(30, merged.measuredUpper());
    }

    @Test
    void mergeRejectsDifferentBytesFromAnOlderObservationRevision() {
        final SloSampleStart start = start();
        final SloSampleFinal newer = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                20,
                25,
                null,
                endpoint(300),
                bytes(32, 24),
                2);
        final SloSampleFinal older = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                20,
                25,
                null,
                endpoint(301),
                bytes(32, 25),
                1);

        assertThrows(
                IllegalArgumentException.class,
                () -> SloSampleFinal.merge(newer, older, SloThresholdDirection.AT_MOST));
    }

    @Test
    void finalRoundTripsAndMergesCompleteUnsigned64BitFields() {
        final SloSampleStart start = start();
        final SloSampleFinal first = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                Long.MIN_VALUE,
                -1L,
                null,
                endpoint(300),
                bytes(32, 17),
                Long.MIN_VALUE);
        final SloSampleFinal decoded = SloSampleFinal.decode(first.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.measuredLower());
        assertEquals(-1L, decoded.measuredUpper());
        assertEquals(Long.MIN_VALUE, decoded.observationRevision());
        assertArrayEquals(first.canonicalBytes(), decoded.canonicalBytes());

        final SloSampleFinal wider = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_TIMEOUT,
                SloThresholdUnit.MILLISECONDS,
                -1L,
                -1L,
                null,
                endpoint(301),
                bytes(32, 18),
                -1L);
        final SloSampleFinal merged = SloSampleFinal.merge(first, wider, SloThresholdDirection.AT_MOST);
        assertEquals(-1L, merged.measuredLower());
        assertEquals(-1L, merged.measuredUpper());
        assertEquals(-1L, merged.observationRevision());
    }

    @Test
    void dueIdentityPreservesCompleteUnsignedGenerationBits() {
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 19));
                    CanonicalProtobuf.uint32Bits(output, 2, Integer.MIN_VALUE);
                    CanonicalProtobuf.int64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPath.ORDINARY_MANAGED.wireValue());
                }));

        assertArrayEquals(
                identity.canonicalBytes(),
                SloSampleEventIdentity.decode(identity.canonicalBytes()).canonicalBytes());
    }

    @Test
    void excludedFinalMustBelongToPairedHealthyObjective() {
        final SloObjective healthy =
                dueObjective(SloPopulation.HEALTHY, java.util.List.of(DueExclusionReason.CAPACITY_GATED));
        final SloObjective allAccepted = dueObjective(SloPopulation.ALL_ACCEPTED, java.util.List.of());
        healthy.validateDueCompanion(allAccepted);
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 20));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.int64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPath.ORDINARY_MANAGED.wireValue());
                }));
        final SloSampleStart start =
                new SloSampleStart(allAccepted, SloPath.ORDINARY_MANAGED, identity, endpoint(100), 200L);
        final SloSampleFinal excluded = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                2,
                DueExclusionReason.CAPACITY_GATED,
                endpoint(200),
                bytes(32, 21),
                1);
        final SloObservationOutbox outbox = SloObservationOutbox.open(start)
                .mergeFinal(excluded, SloThresholdDirection.AT_MOST, healthy, allAccepted);
        assertEquals(excluded, outbox.finalObservation());

        final SloSampleFinal wrongReason = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                2,
                DueExclusionReason.ADMIN_PAUSED,
                endpoint(201),
                bytes(32, 22),
                2);
        assertThrows(IllegalArgumentException.class, () -> wrongReason.validateAgainst(start, healthy, allAccepted));
    }

    @Test
    void excludedFinalRejectsAHealthyObjectiveFromAnotherCatalogPair() {
        final SloObjective healthy =
                dueObjective(SloPopulation.HEALTHY, java.util.List.of(DueExclusionReason.CAPACITY_GATED));
        final SloObjective allAccepted = dueObjective(SloPopulation.ALL_ACCEPTED, java.util.List.of());
        final SloObjective wrongHealthy = new SloObjective(
                SloObjectiveName.DUE_ADMISSION_LAG,
                SloPopulation.HEALTHY,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                101,
                99,
                100,
                60_000,
                10,
                java.util.List.of(DueExclusionReason.CAPACITY_GATED),
                7,
                bytes(32, 23));
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 26));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.int64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPath.ORDINARY_MANAGED.wireValue());
                }));
        final SloSampleStart start =
                new SloSampleStart(allAccepted, SloPath.ORDINARY_MANAGED, identity, endpoint(100), 200L);
        final SloSampleFinal excluded = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                2,
                DueExclusionReason.CAPACITY_GATED,
                endpoint(200),
                bytes(32, 27),
                1);

        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(excluded, SloThresholdDirection.AT_MOST, wrongHealthy, allAccepted));
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(
                        excluded,
                        SloThresholdDirection.AT_MOST,
                        healthy,
                        new SloObjective(
                                SloObjectiveName.DUE_ADMISSION_LAG,
                                SloPopulation.ALL_ACCEPTED,
                                SloThresholdDirection.AT_MOST,
                                SloThresholdUnit.MILLISECONDS,
                                101,
                                99,
                                100,
                                60_000,
                                10,
                                java.util.List.of(),
                                7,
                                bytes(32, 23))));
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(excluded, SloThresholdDirection.AT_MOST, healthy));
    }

    private static SloObjective dueObjective(
            final SloPopulation population, final java.util.List<DueExclusionReason> exclusions) {
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
                bytes(32, 23));
    }

    @Test
    void mergeRejectsConflictingDueExclusionReasons() {
        final SloSampleStart start = dueAcceptedStart();
        final SloSampleFinal capacity = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                2,
                DueExclusionReason.CAPACITY_GATED,
                endpoint(200),
                bytes(32, 12),
                1);
        final SloSampleFinal paused = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                2,
                DueExclusionReason.ADMIN_PAUSED,
                endpoint(201),
                bytes(32, 13),
                2);

        final SloObservationOutbox outbox =
                SloObservationOutbox.open(start).mergeFinal(capacity, SloThresholdDirection.AT_MOST);
        assertThrows(IllegalArgumentException.class, () -> outbox.mergeFinal(paused, SloThresholdDirection.AT_MOST));
        assertThrows(
                IllegalArgumentException.class,
                () -> SloSampleFinal.merge(capacity, paused, SloThresholdDirection.AT_MOST));
    }

    @Test
    void rejectsIdentityDriftAndTampering() {
        final SloSampleStart start = start();
        final SloSampleFinal finalObservation = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.SUCCESS,
                SloThresholdUnit.MILLISECONDS,
                1,
                1,
                null,
                brokerEndpoint(200),
                bytes(32, 5),
                1);
        final byte[] tampered = finalObservation.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SloSampleFinal.decode(tampered));

        final byte[] otherStartId = start.sampleId();
        otherStartId[0] ^= 1;
        final SloSampleFinal other = new SloSampleFinal(
                otherStartId,
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                1,
                DueExclusionReason.CAPACITY_GATED,
                endpoint(300),
                bytes(32, 6),
                2);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(other, SloThresholdDirection.AT_MOST));

        final SloSampleFinal excluded = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                1,
                DueExclusionReason.CAPACITY_GATED,
                endpoint(300),
                bytes(32, 7),
                2);
        assertThrows(IllegalArgumentException.class, () -> excluded.validateAgainst(start));
    }

    @Test
    void rejectsFinalUnitAndMergeDirectionThatDisagreeWithObjective() {
        final SloSampleStart start = start();
        final SloSampleFinal semanticSuccess = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.SUCCESS,
                SloThresholdUnit.MILLISECONDS,
                1,
                1,
                null,
                endpoint(200),
                bytes(32, 7),
                1);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(semanticSuccess, SloThresholdDirection.AT_MOST));

        final SloSampleFinal wrongUnit = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.SUCCESS,
                SloThresholdUnit.BYTES,
                1,
                1,
                null,
                endpoint(200),
                bytes(32, 8),
                1);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(wrongUnit, SloThresholdDirection.AT_MOST));

        final SloSampleFinal valid = new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                SloFinalOutcome.SUCCESS,
                SloThresholdUnit.MILLISECONDS,
                1,
                1,
                null,
                brokerEndpoint(200),
                bytes(32, 9),
                1);
        assertThrows(IllegalArgumentException.class, () -> SloObservationOutbox.open(start)
                .mergeFinal(valid, SloThresholdDirection.AT_LEAST));
    }

    private static SloSampleStart start() {
        final byte[] commandHash = bytes(32, 2);
        final byte[] physicalAttemptId = bytes(16, 3);
        final byte[] completeBranchPayload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, 1));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.COMMAND_QUEUED_LATENCY, completeBranchPayload);
        return new SloSampleStart(
                bytes(32, 1),
                SloObjectiveName.COMMAND_QUEUED_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloPath.NOT_APPLICABLE,
                identity,
                endpoint(100),
                200L);
    }

    private static SloSampleStart dueAcceptedStart() {
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 14));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.uint64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPath.ORDINARY_MANAGED.wireValue());
                }));
        return new SloSampleStart(
                bytes(32, 15),
                SloObjectiveName.DUE_ADMISSION_LAG,
                SloPopulation.ALL_ACCEPTED,
                SloPath.ORDINARY_MANAGED,
                identity,
                endpoint(100),
                200L);
    }

    private static SloTimeEndpoint endpoint(final long epochMs) {
        return new SloTimeEndpoint(
                SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static SloTimeEndpoint brokerEndpoint(final long epochMs) {
        return new SloTimeEndpoint(SloTimeEndpointKind.BROKER_PERSISTENCE, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
