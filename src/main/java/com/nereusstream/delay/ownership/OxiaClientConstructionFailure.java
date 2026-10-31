package com.nereusstream.delay.ownership;

import io.oxia.client.OxiaClientCleanup;
import io.oxia.client.OxiaClientLifecycle;
import io.oxia.client.api.SyncOxiaClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Keeps all original SDK allocations after an application constructor failed. No half-client is returned. */
public final class OxiaClientConstructionFailure extends IllegalStateException
        implements CheckpointInputFuture.Cleanup {
    private static final long serialVersionUID = 1L;
    private final transient List<OxiaClientCleanup> cleanups;

    private OxiaClientConstructionFailure(Throwable cause, List<OxiaClientCleanup> cleanups) {
        super("Oxia construction failed; original native resources remain owned", cause);
        this.cleanups = List.copyOf(cleanups);
    }

    /** Bridges legacy synchronous factories to the SDK's original partial-opening capability. */
    public static SyncOxiaClient openClient(io.oxia.client.api.OxiaClientBuilder builder)
            throws io.oxia.client.api.exceptions.OxiaException {
        try {
            return builder.syncClient();
        } catch (RuntimeException failure) {
            for (Throwable current = failure; current != null; current = current.getCause()) {
                if (current instanceof OxiaClientLifecycle.OpeningFailure) {
                    throw retain(failure);
                }
            }
            throw failure;
        }
    }

    public static OxiaClientConstructionFailure retain(Throwable cause, SyncOxiaClient... clients) {
        Objects.requireNonNull(cause, "original construction failure");
        var cleanups = new ArrayList<OxiaClientCleanup>();
        for (var client : clients) {
            if (client != null) {
                add(cleanups, OxiaClientLifecycle.cleanupOf(client));
            }
        }
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof OxiaClientLifecycle.OpeningFailure failed) {
                add(cleanups, failed.cleanup());
            }
        }
        if (cleanups.isEmpty()) {
            throw new IllegalArgumentException("failure has no original SDK allocation", cause);
        }
        var retained = new OxiaClientConstructionFailure(cause, cleanups);
        retained.continueOriginalCleanup();
        return retained;
    }

    private static void add(List<OxiaClientCleanup> held, OxiaClientCleanup actual) {
        for (var prior : held) {
            if (prior == actual) {
                return;
            }
        }
        held.add(actual);
    }

    public List<OxiaClientCleanup.Status> nativeStatus() {
        return cleanups.stream().map(OxiaClientCleanup::status).toList();
    }

    @Override
    public boolean cleanupComplete() {
        return cleanups.stream().allMatch(c -> c.status().terminated());
    }

    /** Called by the owning bounded I/O executor after its prior task exited. */
    @Override
    public CompletableFuture<Void> retryCleanup() {
        continueOriginalCleanup();
        return cleanupComplete() ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(this);
    }

    private void continueOriginalCleanup() {
        for (var cleanup : cleanups) {
            var status = cleanup.close(Duration.ofNanos(1));
            var failure = status.firstFailure();
            if (failure != null && failure != getCause() && failure != this) {
                boolean held = false;
                for (var prior : getSuppressed()) {
                    if (prior == failure) {
                        held = true;
                    }
                }
                if (!held) {
                    addSuppressed(failure);
                }
            }
        }
    }
}
