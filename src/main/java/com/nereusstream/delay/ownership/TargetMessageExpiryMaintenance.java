package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TargetExpiryDiscoveryStore;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One bounded message-expiry discovery or handoff-settlement step for an active Target Worker. */
final class TargetMessageExpiryMaintenance {
    private final TargetWorkerShardRuntime worker;
    private final TargetMessageExpiryWorkClassExecutor handoff;
    private final Supplier<TargetWorkerShardRuntime.MessageExpiryRequest> requests;
    private final LongSupplier ownerClock;
    private TargetExpiryDiscoveryStore.Cursor cursor;
    private Long sweepCutoff;

    TargetMessageExpiryMaintenance(
            final TargetWorkerShardRuntime worker,
            final TargetMessageExpiryWorkClassExecutor handoff,
            final Supplier<TargetWorkerShardRuntime.MessageExpiryRequest> requests,
            final LongSupplier ownerClock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.handoff = Objects.requireNonNull(handoff, "handoff");
        this.requests = Objects.requireNonNull(requests, "requests");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    }

    /** Returns the exact queued append while it remains unsettled; an empty turn submits no action. */
    synchronized Optional<WorkClassTask> runTurn() {
        final var pending = handoff.pendingSubmission();
        if (pending != null) {
            handoff.settlePending(ownerClock);
            if (handoff.pendingSubmission() != null) {
                return Optional.of(pending.task());
            }
            restartSweep();
            return Optional.empty();
        }

        final var request = Objects.requireNonNull(requests.get(), "Target expiry request");
        final TrustedUtcIntervalEvidence evidence = request.evidence();
        if (sweepCutoff != null && evidence.earliestEpochMs() < sweepCutoff) {
            restartSweep();
        }
        if (cursor == null) {
            sweepCutoff = evidence.earliestEpochMs();
        }
        final var discovery = worker.discoverMessageExpiry(
                request.discoveryBudget(), cursor, evidence, ownerClock);
        if (discovery.sweepComplete()) {
            restartSweep();
            return Optional.empty();
        }
        cursor = discovery.nextCursor();
        try {
            final var candidate = discovery.candidate().orElseThrow();
            final var submission = handoff.submit(
                    candidate,
                    evidence,
                    request.retryUntilEpochMs(),
                    request.owner(),
                    request.signingKeyVersion(),
                    request.signingKey(),
                    ownerClock);
            return Optional.of(submission.task());
        } catch (RuntimeException | Error failure) {
            restartSweep();
            throw failure;
        }
    }

    private void restartSweep() {
        cursor = null;
        sweepCutoff = null;
    }
}
