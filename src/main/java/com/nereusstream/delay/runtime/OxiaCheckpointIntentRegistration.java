package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/** Existing Oxia CAS on a caller-owned finite executor; no client, session, queue or resource ownership transfer. */
public final class OxiaCheckpointIntentRegistration
        implements TargetCheckpointCandidateWorkClassExecutor.IntentRegistration {
    private final OxiaSyncCheckpointPublicationBackend backend;
    private final Duration timeout;
    private final Runnable protection;
    private final ThreadPoolExecutor executor;

    /**
     * Caller must configure finite SDK requestTimeout and hold all permissions/resources through actual I/O exit.
     * Cancellation or exceptional completion never authorizes deletion or means an uncertain CAS did not commit.
     */
    public OxiaCheckpointIntentRegistration(
            OxiaSyncCheckpointPublicationBackend backend,
            Duration timeout,
            Runnable protection,
            ThreadPoolExecutor executor) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public CompletableFuture<CheckpointUploadIntent> register(CheckpointUploadIntent pending) {
        return begin(pending).receipt();
    }

    @Override
    public TargetCheckpointCandidateWorkClassExecutor.RegistrationAttempt begin(CheckpointUploadIntent pending) {
        Objects.requireNonNull(pending, "pending");
        final var owned = CheckpointRegistryLookup.submitOwned(
                guard -> backend.create(pending, guard), timeout, protection, executor, System::nanoTime);
        return new TargetCheckpointCandidateWorkClassExecutor.RegistrationAttempt(
                owned.receipt(), owned::ioExited, owned::requestStop);
    }
}
