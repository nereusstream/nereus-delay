package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.SloFinalOutcome;
import com.nereusstream.delay.protocol.SloObjectiveName;
import com.nereusstream.delay.protocol.SloObservationOutbox;
import com.nereusstream.delay.protocol.SloPath;
import com.nereusstream.delay.protocol.SloPopulation;
import com.nereusstream.delay.protocol.SloSampleEventIdentity;
import com.nereusstream.delay.protocol.SloSampleFinal;
import com.nereusstream.delay.protocol.SloSampleStart;
import com.nereusstream.delay.protocol.SloThresholdDirection;
import com.nereusstream.delay.protocol.SloThresholdUnit;
import com.nereusstream.delay.protocol.SloTimeEndpoint;
import com.nereusstream.delay.protocol.SloTimeEndpointKind;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SloObservationCollectorTest {
    @Test
    void mergesAtMostFinalsConservativelyAndKeepsBadEvidence() {
        final SloSampleStart start = start(1, 100);
        final SloObservationCollector collector = new SloObservationCollector();
        collector.merge(SloObservationOutbox.open(start), SloThresholdDirection.AT_MOST);
        collector.merge(
                SloObservationOutbox.open(start)
                        .mergeFinal(
                                finalObservation(start, SloFinalOutcome.SUCCESS, 1, 2, 1),
                                SloThresholdDirection.AT_MOST),
                SloThresholdDirection.AT_MOST);

        final SloObservationOutbox merged = collector.merge(
                SloObservationOutbox.open(start)
                        .mergeFinal(
                                finalObservation(start, SloFinalOutcome.BAD_EVIDENCE_GAP, 7, 9, 2),
                                SloThresholdDirection.AT_MOST),
                SloThresholdDirection.AT_MOST);

        assertEquals(SloFinalOutcome.BAD_EVIDENCE_GAP, merged.finalObservation().outcome());
        assertEquals(7, merged.finalObservation().measuredLower());
        assertEquals(9, merged.finalObservation().measuredUpper());
    }

    @Test
    void rejectsDifferentStartBytesForOneSampleId() {
        final SloSampleStart first = start(2, 100);
        final SloSampleStart differentStart = start(2, 101);
        assertArrayEquals(first.sampleId(), differentStart.sampleId());
        final SloObservationCollector collector = new SloObservationCollector();
        collector.merge(SloObservationOutbox.open(first), SloThresholdDirection.AT_MOST);

        assertThrows(
                IllegalStateException.class,
                () -> collector.merge(SloObservationOutbox.open(differentStart), SloThresholdDirection.AT_MOST));
    }

    @Test
    void snapshotIsSortedByCanonicalSampleId() {
        final SloObservationCollector collector = new SloObservationCollector();
        collector.merge(SloObservationOutbox.open(start(3, 100)), SloThresholdDirection.AT_MOST);
        collector.merge(SloObservationOutbox.open(start(1, 100)), SloThresholdDirection.AT_MOST);

        final var snapshot = collector.snapshot();
        assertEquals(2, snapshot.size());
        assertTrue(Bytes.hex(snapshot.get(0).sampleId())
                        .compareTo(Bytes.hex(snapshot.get(1).sampleId()))
                <= 0);
    }

    @Test
    void configuredLimitsRejectNewSamplesAndLargerReplacementsWithoutDroppingProjection() {
        final SloSampleStart first = start(4, 100);
        final SloObservationOutbox open = SloObservationOutbox.open(first);
        final long openBytes = open.canonicalBytes().length;

        final SloObservationCollector sampleBounded =
                new SloObservationCollector(new SloObservationCollectorLimits(1, openBytes * 2));
        sampleBounded.merge(open, SloThresholdDirection.AT_MOST);
        assertThrows(
                IllegalStateException.class,
                () -> sampleBounded.merge(SloObservationOutbox.open(start(5, 100)), SloThresholdDirection.AT_MOST));
        assertEquals(new SloObservationCollector.Usage(1, openBytes), sampleBounded.usage());

        final SloObservationCollector byteBounded =
                new SloObservationCollector(new SloObservationCollectorLimits(2, openBytes));
        byteBounded.merge(open, SloThresholdDirection.AT_MOST);
        final SloObservationOutbox withFinal = open.mergeFinal(
                finalObservation(first, SloFinalOutcome.SUCCESS, 1, 2, 1), SloThresholdDirection.AT_MOST);
        assertThrows(IllegalStateException.class, () -> byteBounded.merge(withFinal, SloThresholdDirection.AT_MOST));
        assertEquals(new SloObservationCollector.Usage(1, openBytes), byteBounded.usage());
        assertNull(byteBounded.get(first.sampleId()).finalObservation());
    }

    private static SloSampleStart start(final int seed, final long startEpoch) {
        final byte[] commandHash = bytes(32, seed + 1);
        final byte[] physicalAttemptId = bytes(16, seed + 2);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, seed));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.COMMAND_QUEUED_LATENCY, branch);
        return new SloSampleStart(
                bytes(32, seed),
                SloObjectiveName.COMMAND_QUEUED_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloPath.NOT_APPLICABLE,
                identity,
                endpoint(startEpoch),
                200L);
    }

    private static SloSampleFinal finalObservation(
            final SloSampleStart start,
            final SloFinalOutcome outcome,
            final long lower,
            final long upper,
            final long revision) {
        return new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                outcome,
                SloThresholdUnit.MILLISECONDS,
                lower,
                upper,
                null,
                finalEndpoint(outcome, 300),
                bytes(32, 9),
                revision);
    }

    private static SloTimeEndpoint endpoint(final long epochMs) {
        return new SloTimeEndpoint(
                SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static SloTimeEndpoint finalEndpoint(final SloFinalOutcome outcome, final long epochMs) {
        return new SloTimeEndpoint(
                outcome == SloFinalOutcome.SUCCESS
                        ? SloTimeEndpointKind.BROKER_PERSISTENCE
                        : SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH,
                epochMs,
                epochMs,
                bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
