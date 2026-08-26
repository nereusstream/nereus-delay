package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PersistentSloObservationCollectorTest {
    @TempDir
    Path tempDirectory;

    @Test
    void openAndConservativeFinalSurviveReopen() {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStart start = start(1, 100);
        final PersistentSloObservationCollector collector =
                new PersistentSloObservationCollector(stateFile, new SloObservationCollectorLimits(4, 10_000));
        collector.merge(SloObservationOutbox.open(start), SloThresholdDirection.AT_MOST);
        final SloObservationOutbox success = SloObservationOutbox.open(start)
                .mergeFinal(finalObservation(start, SloFinalOutcome.SUCCESS, 1, 2, 1), SloThresholdDirection.AT_MOST);
        collector.merge(success, SloThresholdDirection.AT_MOST);
        final SloObservationOutbox bad = SloObservationOutbox.open(start)
                .mergeFinal(
                        finalObservation(start, SloFinalOutcome.BAD_EVIDENCE_GAP, 7, 9, 2),
                        SloThresholdDirection.AT_MOST);
        collector.merge(bad, SloThresholdDirection.AT_MOST);

        final PersistentSloObservationCollector reopened =
                new PersistentSloObservationCollector(stateFile, new SloObservationCollectorLimits(4, 10_000));
        final SloObservationOutbox value = reopened.get(start.sampleId());
        assertEquals(SloFinalOutcome.BAD_EVIDENCE_GAP, value.finalObservation().outcome());
        assertEquals(1, reopened.size());
        assertEquals(1, reopened.snapshot().size());
        assertTrue(reopened.usage().canonicalBytes() > 0);
    }

    @Test
    void separateInstancesRereadTheLatestMerge() {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStart start = start(2, 100);
        final PersistentSloObservationCollector first = new PersistentSloObservationCollector(stateFile);
        final PersistentSloObservationCollector second = new PersistentSloObservationCollector(stateFile);
        first.merge(SloObservationOutbox.open(start), SloThresholdDirection.AT_MOST);
        second.merge(
                SloObservationOutbox.open(start)
                        .mergeFinal(
                                finalObservation(start, SloFinalOutcome.SUCCESS, 1, 2, 1),
                                SloThresholdDirection.AT_MOST),
                SloThresholdDirection.AT_MOST);

        assertEquals(
                SloFinalOutcome.SUCCESS,
                first.get(start.sampleId()).finalObservation().outcome());
    }

    @Test
    void checksumCorruptionFailsClosedAndIdentityFailureLeavesStateUntouched() throws Exception {
        final Path stateFile = tempDirectory.resolve("collector.state");
        final SloSampleStart start = start(3, 100);
        final SloSampleStart differentStart = start(3, 101);
        final PersistentSloObservationCollector collector = new PersistentSloObservationCollector(stateFile);
        collector.merge(SloObservationOutbox.open(start), SloThresholdDirection.AT_MOST);
        assertThrows(
                IllegalStateException.class,
                () -> collector.merge(SloObservationOutbox.open(differentStart), SloThresholdDirection.AT_MOST));
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
        final SloSampleStart start = start(4, 100);
        final SloObservationOutbox open = SloObservationOutbox.open(start);
        final PersistentSloObservationCollector collector = new PersistentSloObservationCollector(
                stateFile, new SloObservationCollectorLimits(1, open.canonicalBytes().length));
        collector.merge(open, SloThresholdDirection.AT_MOST);

        assertThrows(
                IllegalStateException.class,
                () -> collector.merge(
                        open.mergeFinal(
                                finalObservation(start, SloFinalOutcome.SUCCESS, 1, 2, 1),
                                SloThresholdDirection.AT_MOST),
                        SloThresholdDirection.AT_MOST));
        assertEquals(null, collector.get(start.sampleId()).finalObservation());
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
