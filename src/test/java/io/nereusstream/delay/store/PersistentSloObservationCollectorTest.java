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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistentSloObservationCollectorTest {
    @TempDir
    Path tempDirectory;

    @Test
    void openAndConservativeFinalSurviveReopen() {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStartV1 start = start(1, 100);
        final PersistentSloObservationCollector collector = new PersistentSloObservationCollector(stateFile,
                new SloObservationCollectorLimits(4, 10_000));
        collector.merge(SloObservationOutboxV1.open(start), SloThresholdDirectionV1.AT_MOST);
        final SloObservationOutboxV1 success = SloObservationOutboxV1.open(start).mergeFinal(
                finalObservation(start, SloFinalOutcomeV1.SUCCESS, 1, 2, 1),
                SloThresholdDirectionV1.AT_MOST);
        collector.merge(success, SloThresholdDirectionV1.AT_MOST);
        final SloObservationOutboxV1 bad = SloObservationOutboxV1.open(start).mergeFinal(
                finalObservation(start, SloFinalOutcomeV1.BAD_EVIDENCE_GAP, 7, 9, 2),
                SloThresholdDirectionV1.AT_MOST);
        collector.merge(bad, SloThresholdDirectionV1.AT_MOST);

        final PersistentSloObservationCollector reopened = new PersistentSloObservationCollector(stateFile,
                new SloObservationCollectorLimits(4, 10_000));
        final SloObservationOutboxV1 value = reopened.get(start.sampleId());
        assertEquals(SloFinalOutcomeV1.BAD_EVIDENCE_GAP, value.finalObservation().outcome());
        assertEquals(1, reopened.size());
        assertEquals(1, reopened.snapshot().size());
        assertTrue(reopened.usage().canonicalBytes() > 0);
    }

    @Test
    void separateInstancesRereadTheLatestMerge() {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStartV1 start = start(2, 100);
        final PersistentSloObservationCollector first = new PersistentSloObservationCollector(stateFile);
        final PersistentSloObservationCollector second = new PersistentSloObservationCollector(stateFile);
        first.merge(SloObservationOutboxV1.open(start), SloThresholdDirectionV1.AT_MOST);
        second.merge(SloObservationOutboxV1.open(start).mergeFinal(
                finalObservation(start, SloFinalOutcomeV1.SUCCESS, 1, 2, 1),
                SloThresholdDirectionV1.AT_MOST), SloThresholdDirectionV1.AT_MOST);

        assertEquals(SloFinalOutcomeV1.SUCCESS, first.get(start.sampleId()).finalObservation().outcome());
    }

    @Test
    void checksumCorruptionFailsClosedAndIdentityFailureLeavesStateUntouched() throws Exception {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStartV1 start = start(3, 100);
        final SloSampleStartV1 differentStart = start(3, 101);
        final PersistentSloObservationCollector collector = new PersistentSloObservationCollector(stateFile);
        collector.merge(SloObservationOutboxV1.open(start), SloThresholdDirectionV1.AT_MOST);
        assertThrows(IllegalStateException.class,
                () -> collector.merge(SloObservationOutboxV1.open(differentStart),
                        SloThresholdDirectionV1.AT_MOST));
        assertEquals(null, collector.get(start.sampleId()).finalObservation());

        final byte[] corrupted = Files.readAllBytes(stateFile);
        corrupted[corrupted.length - 1] ^= 0x01;
        Files.write(stateFile, corrupted);
        assertThrows(IllegalStateException.class, () -> new PersistentSloObservationCollector(stateFile));
    }

    @Test
    void rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary() throws Exception {
        final Path parentRoot = tempDirectory.resolve("collector-parent");
        final Path outside = tempDirectory.resolve("collector-outside");
        Files.createDirectories(parentRoot);
        Files.createDirectories(outside);
        Files.createSymbolicLink(parentRoot.resolve("nested"), outside);

        final Path stateFile = parentRoot.resolve("nested/state.bin");
        assertThrows(IllegalStateException.class, () -> new PersistentSloObservationCollector(stateFile));
        assertFalse(Files.exists(outside.resolve("state.bin"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.exists(outside.resolve("state.bin.lock"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void configuredEnvelopeRejectsReplacementWithoutDroppingOpenSample() {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStartV1 start = start(4, 100);
        final SloObservationOutboxV1 open = SloObservationOutboxV1.open(start);
        final PersistentSloObservationCollector collector = new PersistentSloObservationCollector(stateFile,
                new SloObservationCollectorLimits(1, open.canonicalBytes().length));
        collector.merge(open, SloThresholdDirectionV1.AT_MOST);

        assertThrows(IllegalStateException.class, () -> collector.merge(open.mergeFinal(
                finalObservation(start, SloFinalOutcomeV1.SUCCESS, 1, 2, 1),
                SloThresholdDirectionV1.AT_MOST), SloThresholdDirectionV1.AT_MOST));
        assertEquals(null, collector.get(start.sampleId()).finalObservation());
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
                SloThresholdUnitV1.MILLISECONDS, lower, upper, null, finalEndpoint(outcome, 300),
                bytes(32, 9), revision);
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static SloTimeEndpointV1 finalEndpoint(final SloFinalOutcomeV1 outcome, final long epochMs) {
        return new SloTimeEndpointV1(outcome == SloFinalOutcomeV1.SUCCESS
                        ? SloTimeEndpointKindV1.BROKER_PERSISTENCE : SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH,
                epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
