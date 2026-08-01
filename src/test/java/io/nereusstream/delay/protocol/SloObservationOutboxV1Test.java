package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SloObservationOutboxV1Test {
    @Test
    void startFinalAndOutboxRoundTrip() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(start.sampleId(), start.startDigest(),
                SloFinalOutcomeV1.SUCCESS, SloThresholdUnitV1.MILLISECONDS, 4, 8, null,
                endpoint(200), bytes(32, 3), 1);
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.open(start)
                .mergeFinal(finalObservation, SloThresholdDirectionV1.AT_MOST);

        assertArrayEquals(start.canonicalBytes(), SloSampleStartV1.decode(start.canonicalBytes()).canonicalBytes());
        assertArrayEquals(finalObservation.canonicalBytes(),
                SloSampleFinalV1.decode(finalObservation.canonicalBytes()).canonicalBytes());
        assertArrayEquals(outbox.canonicalBytes(),
                SloObservationOutboxV1.decode(outbox.canonicalBytes()).canonicalBytes());
        assertEquals(finalObservation, outbox.finalObservation());
    }

    @Test
    void mergeNeverImprovesBadObservationAndKeepsWorstAtMostMeasurement() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 success = new SloSampleFinalV1(start.sampleId(), start.startDigest(),
                SloFinalOutcomeV1.SUCCESS, SloThresholdUnitV1.MILLISECONDS, 4, 8, null,
                endpoint(200), bytes(32, 3), 1);
        final SloSampleFinalV1 timeout = new SloSampleFinalV1(start.sampleId(), start.startDigest(),
                SloFinalOutcomeV1.BAD_TIMEOUT, SloThresholdUnitV1.MILLISECONDS, 20, 25,
                null, endpoint(300), bytes(32, 4), 2);

        final SloSampleFinalV1 merged = SloSampleFinalV1.merge(success, timeout,
                SloThresholdDirectionV1.AT_MOST);
        assertEquals(SloFinalOutcomeV1.BAD_TIMEOUT, merged.outcome());
        assertEquals(20, merged.measuredLower());
        assertEquals(25, merged.measuredUpper());
        assertEquals(2, merged.observationRevision());
    }

    @Test
    void rejectsIdentityDriftAndTampering() {
        final SloSampleStartV1 start = start();
        final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(start.sampleId(), start.startDigest(),
                SloFinalOutcomeV1.SUCCESS, SloThresholdUnitV1.MILLISECONDS, 1, 1, null,
                endpoint(200), bytes(32, 5), 1);
        final byte[] tampered = finalObservation.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SloSampleFinalV1.decode(tampered));

        final byte[] otherStartId = start.sampleId();
        otherStartId[0] ^= 1;
        final SloSampleFinalV1 other = new SloSampleFinalV1(otherStartId, start.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP, SloThresholdUnitV1.MILLISECONDS, 1, 1,
                DueExclusionReasonV1.CAPACITY_GATED, endpoint(300), bytes(32, 6), 2);
        assertThrows(IllegalArgumentException.class,
                () -> SloObservationOutboxV1.open(start).mergeFinal(other, SloThresholdDirectionV1.AT_MOST));
    }

    private static SloSampleStartV1 start() {
        final byte[] branchPayload = CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, 1, Bytes.utf8("command-identity")));
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.COMMAND_QUEUED_LATENCY, branchPayload);
        return new SloSampleStartV1(bytes(32, 1), SloObjectiveNameV1.COMMAND_QUEUED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloPathV1.NOT_APPLICABLE, identity, endpoint(100), 200L);
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
