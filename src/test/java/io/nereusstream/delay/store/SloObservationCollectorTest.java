package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.SloFinalOutcomeV1;
import io.nereusstream.delay.protocol.SloObjectiveNameV1;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloPathV1;
import io.nereusstream.delay.protocol.SloPopulationV1;
import io.nereusstream.delay.protocol.SloSampleEventIdentityV1;
import io.nereusstream.delay.protocol.SloSampleFinalV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;
import io.nereusstream.delay.protocol.SloThresholdUnitV1;
import io.nereusstream.delay.protocol.SloTimeEndpointKindV1;
import io.nereusstream.delay.protocol.SloTimeEndpointV1;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SloObservationCollectorTest {
    @Test
    void mergesAtMostFinalsConservativelyAndKeepsBadEvidence() {
        final SloSampleStartV1 start = start(1, 100);
        final SloObservationCollector collector = new SloObservationCollector();
        collector.merge(SloObservationOutboxV1.open(start), SloThresholdDirectionV1.AT_MOST);
        collector.merge(SloObservationOutboxV1.open(start).mergeFinal(
                finalObservation(start, SloFinalOutcomeV1.SUCCESS, 1, 2, 1),
                SloThresholdDirectionV1.AT_MOST), SloThresholdDirectionV1.AT_MOST);

        final SloObservationOutboxV1 merged = collector.merge(SloObservationOutboxV1.open(start).mergeFinal(
                finalObservation(start, SloFinalOutcomeV1.BAD_EVIDENCE_GAP, 7, 9, 2),
                SloThresholdDirectionV1.AT_MOST), SloThresholdDirectionV1.AT_MOST);

        assertEquals(SloFinalOutcomeV1.BAD_EVIDENCE_GAP, merged.finalObservation().outcome());
        assertEquals(7, merged.finalObservation().measuredLower());
        assertEquals(9, merged.finalObservation().measuredUpper());
    }

    @Test
    void rejectsDifferentStartBytesForOneSampleId() {
        final SloSampleStartV1 first = start(2, 100);
        final SloSampleStartV1 differentStart = start(2, 101);
        assertArrayEquals(first.sampleId(), differentStart.sampleId());
        final SloObservationCollector collector = new SloObservationCollector();
        collector.merge(SloObservationOutboxV1.open(first), SloThresholdDirectionV1.AT_MOST);

        assertThrows(IllegalStateException.class,
                () -> collector.merge(SloObservationOutboxV1.open(differentStart),
                        SloThresholdDirectionV1.AT_MOST));
    }

    @Test
    void snapshotIsSortedByCanonicalSampleId() {
        final SloObservationCollector collector = new SloObservationCollector();
        collector.merge(SloObservationOutboxV1.open(start(3, 100)), SloThresholdDirectionV1.AT_MOST);
        collector.merge(SloObservationOutboxV1.open(start(1, 100)), SloThresholdDirectionV1.AT_MOST);

        final var snapshot = collector.snapshot();
        assertEquals(2, snapshot.size());
        assertTrue(Bytes.hex(snapshot.get(0).sampleId()).compareTo(Bytes.hex(snapshot.get(1).sampleId())) <= 0);
    }

    private static SloSampleStartV1 start(final int seed, final long startEpoch) {
        final byte[] commandHash = bytes(32, seed + 1);
        final byte[] physicalAttemptId = bytes(16, seed + 2);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, seed));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.COMMAND_QUEUED_LATENCY, branch);
        return new SloSampleStartV1(bytes(32, seed), SloObjectiveNameV1.COMMAND_QUEUED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloPathV1.NOT_APPLICABLE, identity,
                endpoint(startEpoch), 200L);
    }

    private static SloSampleFinalV1 finalObservation(final SloSampleStartV1 start,
                                                     final SloFinalOutcomeV1 outcome,
                                                     final long lower, final long upper,
                                                     final long revision) {
        return new SloSampleFinalV1(start.sampleId(), start.startDigest(), outcome,
                SloThresholdUnitV1.MILLISECONDS, lower, upper, null, endpoint(300), bytes(32, 9), revision);
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
