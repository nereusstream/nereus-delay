package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.store.CheckpointManifest;

/** Original post-A checkpoint publication and metadata adoption. Source activation is never undone here. */
public interface TargetMigrationAdoption extends AutoCloseable, CheckpointInputFuture.Cleanup {
    /** Drives the existing finite checkpoint/object publication; no new log or independently active Worker. */
    void begin(TargetMigrationStore.Activated applied, TargetMigrationProcessDriver.Opening original);
    /** Empty means original publication/CAS is still pending; it does not permit an old writer to resume. */
    java.util.Optional<Adopted> advance();
    boolean ioExited();
    void requestStop();
    Throwable pendingIoFailure();
    @Override void close();

    /** A durable exact CAS read, independently checked again before the original new Source can open. */
    final class Adopted {
        private final TargetMigrationCheckpointAdoption original;
        private Adopted(TargetMigrationCheckpointAdoption original) {
            this.original = java.util.Objects.requireNonNull(original); original.requireAdopted();
        }
        static Adopted afterOriginalCas(TargetMigrationCheckpointAdoption original) { return new Adopted(original); }
        public TargetMigrationPlan plan() { return original.adoptedPlan(); }
        public CheckpointManifest checkpoint() { return original.adoptedCheckpoint(); }
        public WorkerAssignmentAuthority.Publication publication() { return original.adoptedPublication(); }
        public void requireApplied(TargetMigrationStore.Activated exact) { original.requireAdoptedApplied(exact); }
        public void requireCurrent() { original.requireAdoptedCurrent(); }
    }
}
