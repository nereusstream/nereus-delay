package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Original recovery material and the concrete, Root-owned Source Fence transport in one retained input. */
public final class TargetSloSourceRecoveryInputs implements TargetSourceRecoveryInputs {
    /** Independent protected history/time/key/resource authority. Preparing a plan performs no Source append. */
    public interface Authority extends AutoCloseable {
        FencePlan prepareFence(
                TargetStoreBootstrap.SloRecoveryAudit original, long maximumRetainedBytes, Runnable guard);

        void requireComplete(
                TargetStoreBootstrap.SloRecoveryAudit original,
                TargetStoreBootstrap.SloRecoveryAudit replayed,
                TargetTimeFenceMutationFactory.Prepared exactFence,
                Runnable guard);
        /** Actual retained material must fit the reservation before it is allocated, including partial reads. */
        long retainedBytes();
        /** Already-held local versions/pins only; remains valid through the original input's lifetime. */
        void requireCurrent();
        /** Local read-hold release only; never releases an unresolved grant, UNKNOWN or retention commitment. */
        @Override
        void close();
    }

    @FunctionalInterface
    public interface AuthorityProvider {
        /** Retain partial failures with CheckpointInputFuture.Cleanup; never infer a range from observed Native D. */
        Authority load(
                TargetWorkerSourceStartup.RecoveryInputsOpening opening,
                TargetSourceRecoveryInputs original,
                long maximumRetainedBytes,
                Runnable guard);
    }

    @FunctionalInterface
    public interface WriterFactory {
        /** Called on the original preparation I/O; register the actual backend/writer before native allocation. */
        TargetRecoveryFenceWriter open(TargetWorkerSourceStartup.RecoveryInputsOpening opening, Runnable guard);
    }

    /** Routing only. An existing-frame choice still requires the Root's actual pre-cut APPLIED first/POS proof. */
    public record FencePlan(TargetTimeFenceMutationFactory.Prepared exactFence, boolean previouslyApplied) {
        public FencePlan {
            if (previouslyApplied && exactFence == null) {
                throw new IllegalArgumentException("existing Fence needs its exact frame");
            }
        }
    }

    private final TargetWorkerSourceStartup.RecoveryInputsOpening opening;
    private final WriterFactory writers;
    private final TargetWorkerProcessRuntime.SourceExitCustody oldLocalExit;
    private final long oldLocalExitBytes;
    private final long maximumAuthorityBytes;
    private final long maximumFrameBytes;
    private final long maximumInputBytes;
    private TargetSourceRecoveryInputs original;
    private Authority authority;
    private long originalBytes;
    private long reservedBytes;
    private TargetStoreBootstrap.SloRecoveryAudit preparedAt;
    private TargetTimeFenceMutationFactory.Prepared prepared;
    private boolean preparing, preparedOnce, closing, authorityClosed, originalClosed;

    private TargetSloSourceRecoveryInputs(
            TargetWorkerSourceStartup.RecoveryInputsOpening opening,
            WriterFactory writers,
            long maximumAuthorityBytes,
            long maximumFrameBytes,
            long maximumInputBytes,
            TargetWorkerProcessRuntime.SourceExitCustody oldLocalExit) {
        this.opening = Objects.requireNonNull(opening);
        this.writers = Objects.requireNonNull(writers);
        this.oldLocalExit = oldLocalExit;
        oldLocalExitBytes = oldLocalExit == null ? 0 : oldLocalExit.retainedBytes();
        if (maximumAuthorityBytes < 1
                || maximumAuthorityBytes == Long.MAX_VALUE
                || maximumFrameBytes < 1
                || maximumFrameBytes == Long.MAX_VALUE
                || maximumInputBytes < 1
                || maximumInputBytes == Long.MAX_VALUE
                || oldLocalExitBytes < 0
                || oldLocalExitBytes >= maximumAuthorityBytes
                || Math.addExact(maximumAuthorityBytes, maximumFrameBytes) >= maximumInputBytes) {
            throw new IllegalArgumentException(
                    "SLO recovery inputs need a finite combined base/authority/frame reservation");
        }
        this.maximumAuthorityBytes = maximumAuthorityBytes;
        this.maximumFrameBytes = maximumFrameBytes;
        this.maximumInputBytes = maximumInputBytes;
    }

    /**
     * Decorates the existing deployment provider without choosing an epoch, deadline, signer or native backend.
     * Both authority material and one maximum exact frame are reserved before either provider acquires material.
     */
    public static Provider provider(
            Provider original, AuthorityProvider authority, WriterFactory writers, long maximumAuthorityBytes) {
        return providerWithLocalExit(original, authority, writers, maximumAuthorityBytes, null);
    }

    /**
     * Adds the actual exited old local Process's bounded receipt. It proves only that Process's exit;
     * the independent Authority must still bind its original Owner to the complete protected old domain.
     * Cross-process recovery without an actual local handle continues to require independent exit material.
     */
    public static Provider provider(
            Provider original,
            AuthorityProvider authority,
            WriterFactory writers,
            long maximumAuthorityBytes,
            TargetWorkerProcessRuntime.SourceExitCustody oldLocalExit) {
        return providerWithLocalExit(
                original, authority, writers, maximumAuthorityBytes, Objects.requireNonNull(oldLocalExit));
    }

    private static Provider providerWithLocalExit(
            Provider original,
            AuthorityProvider authority,
            WriterFactory writers,
            long maximumAuthorityBytes,
            TargetWorkerProcessRuntime.SourceExitCustody oldLocalExit) {
        Objects.requireNonNull(original);
        Objects.requireNonNull(authority);
        Objects.requireNonNull(writers);
        if (maximumAuthorityBytes < 1 || maximumAuthorityBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("finite original SLO authority reservation required");
        }
        return (opening, owner, assignment, maximumRetainedBytes, guard) -> {
            final long frame = opening.startup().graph().config().maximumSourceFrameBytes();
            final var retained = new TargetSloSourceRecoveryInputs(
                    opening, writers, maximumAuthorityBytes, frame, maximumRetainedBytes, oldLocalExit);
            try {
                guard.run();
                if (oldLocalExit != null) {
                    oldLocalExit.requireOpeningBinding(opening);
                }
                final long baseBound =
                        Math.subtractExact(maximumRetainedBytes, Math.addExact(maximumAuthorityBytes, frame));
                retained.original = Objects.requireNonNull(original.load(opening, owner, assignment, baseBound, guard));
                retained.originalBytes = retained.original.retainedBytes();
                if (retained.originalBytes < 0 || retained.originalBytes > baseBound) {
                    throw new IllegalArgumentException("original recovery input exceeds its reserved base bytes");
                }
                retained.reservedBytes =
                        Math.addExact(retained.originalBytes, Math.addExact(maximumAuthorityBytes, frame));
                retained.original.requireSourceCurrent();
                guard.run();
                retained.authority = Objects.requireNonNull(authority.load(
                        opening,
                        retained.original,
                        Math.subtractExact(maximumAuthorityBytes, retained.oldLocalExitBytes),
                        guard));
                retained.requireSourceCurrent();
                guard.run();
                return retained;
            } catch (RuntimeException | Error failure) {
                // The outer original OwnedCheckpointIo retains this cleanup even when no complete input was returned.
                throw new OpeningFailure(failure, retained);
            }
        };
    }

    @Override
    public OwnerIdentity owner() {
        return original.owner();
    }

    @Override
    public LocalBrokerMtlsResolver.SourceReceipt credential() {
        return original.credential();
    }

    @Override
    public int maximumFrameBytes() {
        return original.maximumFrameBytes();
    }

    @Override
    public TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames() {
        requireSourceCurrent();
        final var actual = original.sourceFrames();
        requireSourceCurrent();
        return actual;
    }

    @Override
    public PulsarAttemptJournal journal() {
        return original.journal();
    }

    @Override
    public TargetOwnedBackendGroup journalBackend() {
        return original.journalBackend();
    }

    @Override
    public JournalWriter journalWriter() {
        return original.journalWriter();
    }

    @Override
    public TargetRecoveryFenceWriter fenceWriter() {
        return opening.fenceWriter();
    }

    @Override
    public long retainedBytes() {
        return reservedBytes;
    }

    @Override
    public boolean requiresSloRecoveryQualification() {
        return true;
    }

    @Override
    public TargetTimeFenceMutationFactory.Prepared prepareSloRecoveryFence(
            TargetStoreBootstrap.SloRecoveryAudit originalAudit, Runnable guard) {
        Objects.requireNonNull(originalAudit);
        Objects.requireNonNull(guard);
        synchronized (this) {
            if (preparing || preparedOnce) {
                throw new IllegalStateException(
                        "original SLO Fence preparation cannot be replaced or retried with new evidence");
            }
            preparing = true;
            preparedAt = originalAudit;
        }
        try {
            requireSourceCurrent();
            guard.run();
            requireOldLocalExit(originalAudit);
            final var plan = Objects.requireNonNull(authority.prepareFence(
                    originalAudit, Math.subtractExact(maximumAuthorityBytes, oldLocalExitBytes), guard));
            requireSourceCurrent();
            guard.run();
            requireOldLocalExit(originalAudit);
            final var exact = plan.exactFence();
            if (exact != null
                    && exact.mutation().encodeFrame().length > Math.min(maximumFrameBytes, maximumFrameBytes())) {
                throw new IllegalArgumentException("qualified original Fence exceeds its reserved Source frame");
            }
            synchronized (this) {
                prepared = exact;
            }
            if (exact != null) {
                if (plan.previouslyApplied()) {
                    opening.retainPreviouslyAppliedFence(exact);
                } else {
                    final var actual = Objects.requireNonNull(writers.open(opening, guard));
                    if (actual != opening.fenceWriter()) {
                        throw new IllegalStateException("Source Fence factory did not retain its original writer");
                    }
                    requireSourceCurrent();
                    guard.run();
                    // Root retains UNKNOWN before native entry; PERSISTED is still not an APPLIED Source result.
                    opening.appendFence(actual, exact);
                }
            }
            requireSourceCurrent();
            guard.run();
            synchronized (this) {
                preparedOnce = true;
            }
            return exact;
        } finally {
            synchronized (this) {
                preparing = false;
            }
        }
    }

    @Override
    public void requireSloRecoveryComplete(
            TargetStoreBootstrap.SloRecoveryAudit originalAudit,
            TargetStoreBootstrap.SloRecoveryAudit replayed,
            TargetTimeFenceMutationFactory.Prepared exactFence,
            Runnable guard) {
        synchronized (this) {
            if (!preparedOnce || preparedAt != originalAudit || !sameFrame(prepared, exactFence)) {
                throw new IllegalStateException("SLO completion changed its original preparation or exact Fence");
            }
        }
        requireSourceCurrent();
        guard.run();
        requireOldLocalExit(originalAudit);
        requireOldLocalExit(replayed);
        authority.requireComplete(originalAudit, replayed, exactFence, guard);
        requireSourceCurrent();
        guard.run();
        requireOldLocalExit(originalAudit);
        requireOldLocalExit(replayed);
    }

    private void requireOldLocalExit(TargetStoreBootstrap.SloRecoveryAudit audit) {
        if (oldLocalExit != null) {
            oldLocalExit.requireExited(opening, audit);
        }
    }

    private static boolean sameFrame(
            TargetTimeFenceMutationFactory.Prepared left, TargetTimeFenceMutationFactory.Prepared right) {
        return left == null
                ? right == null
                : right != null
                        && java.util.Arrays.equals(
                                left.mutation().canonicalEnvelope(),
                                right.mutation().canonicalEnvelope());
    }

    @Override
    public void requireSourceCurrent() {
        synchronized (this) {
            if (closing || originalClosed || authorityClosed) {
                throw new IllegalStateException("original SLO input is closing");
            }
        }
        if (oldLocalExit != null) {
            oldLocalExit.requireOpeningBinding(opening);
        }
        original.requireSourceCurrent();
        if (original.retainedBytes() != originalBytes
                || reservedBytes > maximumInputBytes
                || authority == null
                || authority.retainedBytes() < 0
                || authority.retainedBytes() > maximumAuthorityBytes - oldLocalExitBytes) {
            throw new IllegalStateException("original SLO material exceeded or changed its fixed input reservation");
        }
        authority.requireCurrent();
        original.requireSourceCurrent();
    }

    @Override
    public void requireJournalCurrent() {
        original.requireJournalCurrent();
    }

    @Override
    public synchronized void close() {
        closing = true;
        if (preparing) {
            throw new IllegalStateException("original SLO preparation I/O is still running");
        }
        if (!authorityClosed) {
            if (authority != null) {
                authority.close();
            }
            authorityClosed = true;
        }
        if (!originalClosed) {
            if (original != null) {
                original.close();
            }
            originalClosed = true;
        }
    }

    private synchronized boolean holdsClosed() {
        return authorityClosed && originalClosed;
    }

    /** Partial provider acquisition is retried on the same original executor after actual SDK owners terminate. */
    private static final class OpeningFailure extends RuntimeException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetSloSourceRecoveryInputs retained;
        private final transient CheckpointInputFuture.Cleanup partial;
        private transient CompletableFuture<Void> partialClose;

        private OpeningFailure(Throwable cause, TargetSloSourceRecoveryInputs retained) {
            super("original SLO recovery input acquisition retained for cleanup", cause);
            this.retained = retained;
            Throwable actual = cause;
            while (actual instanceof CompletionException && actual.getCause() != null) {
                actual = actual.getCause();
            }
            partial = actual instanceof CheckpointInputFuture.Cleanup cleanup ? cleanup : null;
        }

        @Override
        public synchronized boolean cleanupComplete() {
            return retained.holdsClosed() && (partial == null || partial.cleanupComplete());
        }

        @Override
        public synchronized CompletableFuture<Void> retryCleanup() {
            try {
                if (partial != null && !partial.cleanupComplete()) {
                    if (partialClose == null || partialClose.isDone()) {
                        partialClose = Objects.requireNonNull(partial.retryCleanup());
                    }
                    return partialClose;
                }
                final var base = retained.original;
                if (base != null
                        && base.journalBackend() != null
                        && !base.journalBackend().resourcesTerminated()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("original recovery SDK must exit before input hold release"));
                }
                retained.close();
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException | Error failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }
    }
}
