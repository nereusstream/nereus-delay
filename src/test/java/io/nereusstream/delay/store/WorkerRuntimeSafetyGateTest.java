package io.nereusstream.delay.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerRuntimeSafetyGateTest {
    @TempDir
    Path tempDir;

    @Test
    void runtimeShrinkIsStickyUntilAnExplicitEmptyDrainActivation() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("gate"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation healthy = observation(128L * 1024 * 1024);
        final WorkerRuntimeSafetyGate gate = new WorkerRuntimeSafetyGate(config, envelope, healthy);

        assertEquals(WorkerRuntimeSafetyGate.State.ACTIVE, gate.state());
        assertThrows(IllegalStateException.class,
                () -> gate.observe(observation(800L * 1024 * 1024)));
        assertEquals(WorkerRuntimeSafetyGate.State.DRAIN_OR_MIGRATE, gate.state());
        assertThrows(IllegalStateException.class, () -> gate.requireActive("Claim/Admission"));
        assertThrows(IllegalStateException.class,
                () -> gate.activateAfterDrain(1, 0, false, healthy));

        gate.activateAfterDrain(0, 0, false, healthy);
        assertEquals(WorkerRuntimeSafetyGate.State.ACTIVE, gate.state());
    }

    @Test
    void stagedEnvelopeFencesOwnershipBeforeActivation() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("staged"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation healthy = observation(128L * 1024 * 1024);
        final WorkerRuntimeSafetyGate gate = new WorkerRuntimeSafetyGate(config, envelope, healthy);
        final WorkerResourceEnvelope staged = envelope(640L * 1024 * 1024);

        gate.stage(staged, healthy);
        assertEquals(WorkerRuntimeSafetyGate.State.STAGED, gate.state());
        assertThrows(IllegalStateException.class, () -> gate.requireActive("ownership/restore"));
        assertThrows(IllegalStateException.class,
                () -> gate.activateAfterDrain(0, 0, true, healthy));

        gate.activateAfterDrain(0, 0, false, healthy);
        assertEquals(WorkerRuntimeSafetyGate.State.ACTIVE, gate.state());
    }

    @Test
    void sharedResourcesFenceNewOwnershipAfterRuntimeShrink() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resources"));
        final WorkerResourceEnvelope envelope = envelope(700L * 1024 * 1024);
        final WorkerRuntimeResourceObservation healthy = observation(128L * 1024 * 1024);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config, envelope, healthy)) {
            assertThrows(IllegalStateException.class,
                    () -> resources.revalidateRuntime(observation(800L * 1024 * 1024)));
            assertEquals(WorkerRuntimeSafetyGate.State.DRAIN_OR_MIGRATE, resources.runtimeSafetyState());
            assertThrows(IllegalStateException.class, resources::acquireShardAcquireSlot);

            resources.activateRuntimeAfterDrain(0, 0, false, healthy);
            resources.acquireShardAcquireSlot();
            resources.releaseShardAcquireSlot();
        }
    }

    private static WorkerResourceEnvelope envelope(final long maxProcessRssBytes) {
        return new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, maxProcessRssBytes, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);
    }

    private static WorkerRuntimeResourceObservation observation(final long rssBytes) {
        return new WorkerRuntimeResourceObservation(
                128L * 1024 * 1024, 64L * 1024 * 1024, rssBytes,
                1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                8L * 1024 * 1024 * 1024);
    }
}
