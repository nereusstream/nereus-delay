package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded preemptive handoff for an Owner Lease loss or expiry event.
 *
 * <p>The task carries the complete lease identity that caused the event.  It
 * never fences a replacement local Owner, and it never treats an uncertain
 * authority read as proof that the old Owner is safe to continue.  A
 * successful fence invokes the external stop callback exactly once for this
 * handoff; source/scheduler quiescence itself remains owned by that callback.
 */
public final class LeaseFenceWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-lease-fence-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public LeaseFenceWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers one exact lease-loss handoff in the preemptive class. */
    public Submission submit(
            final OwnerLease expectedLease, final LongSupplier ownerClock, final FenceCallbacks callbacks) {
        final OwnerLease expected = Objects.requireNonNull(expectedLease, "expectedLease");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        final FenceCallbacks stop = Objects.requireNonNull(callbacks, "callbacks");
        ownedShard.requireLeaseFenceSubmission(expected);
        final byte[] requestBytes = canonicalLease(expected);
        final WorkClassTask task = new WorkClassTask(
                WorkClass.LEASE_FENCE,
                "lease-fence/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, requestBytes)),
                requestBytes.length);
        final Submission submission = new Submission(task, expected);
        workClasses.submit(task, () -> execute(submission, clock, stop));
        return submission;
    }

    private void execute(final Submission submission, final LongSupplier clock, final FenceCallbacks callbacks) {
        final OwnerLease expected = submission.expectedLease();
        final long now;
        try {
            now = readNow(clock);
        } catch (RuntimeException | Error failure) {
            fenceAndStop(submission, callbacks, failure);
            throw failure;
        }

        // A local replacement wins over a delayed old-owner notification. Do
        // not read or fence through the old task after the local identity has
        // already changed.
        try {
            ownedShard.requireLeaseFenceSubmission(expected);
        } catch (RuntimeException staleLocalOwner) {
            submission.complete(FenceResult.stale(expected, staleLocalOwner));
            return;
        }

        final Optional<OwnerLease> observed;
        try {
            observed = authority.current(expected.shardId());
        } catch (RuntimeException | Error authorityFailure) {
            fenceAndStop(submission, callbacks, authorityFailure);
            if (authorityFailure instanceof Error fatal) {
                throw fatal;
            }
            return;
        }

        if (observed.isPresent()) {
            final OwnerLease current = observed.orElseThrow();
            if (expected.sameIdentity(current) && current.validAt(now)) {
                submission.complete(FenceResult.ownerStillValid(expected));
                return;
            }
        }

        final boolean fenced = ownedShard.fenceIfLeaseMatches(expected);
        if (!fenced) {
            submission.complete(FenceResult.stale(expected, null));
            return;
        }
        try {
            invokeStopOnce(submission, callbacks);
            submission.complete(FenceResult.fenced(expected, observed.orElse(null)));
        } catch (RuntimeException | Error stopFailure) {
            submission.complete(FenceResult.unknown(expected, observed.orElse(null), stopFailure));
            throw stopFailure;
        }
    }

    private void fenceAndStop(final Submission submission, final FenceCallbacks callbacks, final Throwable failure) {
        if (!ownedShard.fenceIfLeaseMatches(submission.expectedLease())) {
            submission.complete(FenceResult.stale(submission.expectedLease(), failure));
            return;
        }
        try {
            invokeStopOnce(submission, callbacks);
            submission.complete(FenceResult.unknown(submission.expectedLease(), null, failure));
        } catch (RuntimeException | Error stopFailure) {
            failure.addSuppressed(stopFailure);
            submission.complete(FenceResult.unknown(submission.expectedLease(), null, failure));
            if (stopFailure instanceof Error fatal) {
                throw fatal;
            }
        }
    }

    private static void invokeStopOnce(final Submission submission, final FenceCallbacks callbacks) {
        if (submission.stopCompleted()) {
            return;
        }
        callbacks.stopSourceAndScheduling();
        submission.markStopCompleted();
    }

    private static long readNow(final LongSupplier clock) {
        final long now = clock.getAsLong();
        if (now < 0) {
            throw new IllegalArgumentException("lease-fence clock returned a negative time");
        }
        return now;
    }

    private static byte[] canonicalLease(final OwnerLease lease) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output, 1, lease.shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, lease.shardId().partition());
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8(lease.ownerId()));
            CanonicalProtobuf.uint64Bits(output, 4, lease.ownerEpoch());
            CanonicalProtobuf.bytes(output, 5, lease.leaseToken());
            CanonicalProtobuf.uint32(output, 6, lease.state().wireValue());
            CanonicalProtobuf.int64(output, 7, lease.expiresAtEpochMs());
            if (lease.context() != null) {
                CanonicalProtobuf.bytes(output, 8, CanonicalProtobuf.message(context -> {
                    CanonicalProtobuf.bytes(context, 1, lease.context().sourceAssignmentId());
                    CanonicalProtobuf.uint64Bits(context, 2, lease.context().assignmentEpoch());
                    CanonicalProtobuf.bytes(context, 3, lease.context().sessionIdentity());
                }));
            }
        });
    }

    @FunctionalInterface
    public interface FenceCallbacks {
        /** Stops source fetch, scheduling and new owner-side admissions. */
        void stopSourceAndScheduling();
    }

    public enum ResultKind {
        OWNER_STILL_VALID,
        FENCED,
        STALE_TASK,
        UNKNOWN
    }

    public record FenceResult(ResultKind kind, OwnerLease expectedLease, OwnerLease observedLease, Throwable failure) {
        public FenceResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(expectedLease, "expectedLease");
            if (kind == ResultKind.OWNER_STILL_VALID && failure != null) {
                throw new IllegalArgumentException("a valid owner cannot carry a failure");
            }
            if (kind != ResultKind.UNKNOWN && kind != ResultKind.STALE_TASK && failure != null) {
                throw new IllegalArgumentException("only UNKNOWN or STALE_TASK carries failure evidence");
            }
        }

        private static FenceResult ownerStillValid(final OwnerLease expected) {
            return new FenceResult(ResultKind.OWNER_STILL_VALID, expected, expected, null);
        }

        private static FenceResult fenced(final OwnerLease expected, final OwnerLease observed) {
            return new FenceResult(ResultKind.FENCED, expected, observed, null);
        }

        private static FenceResult stale(final OwnerLease expected, final Throwable failure) {
            return new FenceResult(ResultKind.STALE_TASK, expected, null, failure);
        }

        private static FenceResult unknown(
                final OwnerLease expected, final OwnerLease observed, final Throwable failure) {
            return new FenceResult(ResultKind.UNKNOWN, expected, observed, Objects.requireNonNull(failure));
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final OwnerLease expectedLease;
        private volatile FenceResult result;
        private boolean stopCompleted;

        private Submission(final WorkClassTask task, final OwnerLease expectedLease) {
            this.task = Objects.requireNonNull(task, "task");
            this.expectedLease = Objects.requireNonNull(expectedLease, "expectedLease");
        }

        public WorkClassTask task() {
            return task;
        }

        public OwnerLease expectedLease() {
            return expectedLease;
        }

        public Optional<FenceResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized boolean stopCompleted() {
            return stopCompleted;
        }

        private synchronized void markStopCompleted() {
            stopCompleted = true;
        }

        private synchronized void complete(final FenceResult completed) {
            if (result != null) {
                throw new IllegalStateException("lease-fence handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }
}
