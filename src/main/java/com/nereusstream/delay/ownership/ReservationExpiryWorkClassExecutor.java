package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded GC-class materialization of a source-ordered reservation expiry.
 *
 * <p>The TIME_FENCE mutation decides that a reservation is expired. This
 * executor only materializes that already-decided projection and releases the
 * reservation quota in the same local batch. It never creates a System
 * Mutation or a Source Position.</p>
 */
public final class ReservationExpiryWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-reservation-expiry-materialization-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public ReservationExpiryWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Queues one exact durable reservation-expiry candidate without local I/O. */
    public Submission submit(final DelayShard.ReservationExpiryWork candidate, final LongSupplier ownerClock) {
        final DelayShard.ReservationExpiryWork submitted = Objects.requireNonNull(candidate, "candidate");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        ownedShard.requireReservationExpirySubmission(authority, submitted);
        final byte[] identity = Bytes.concat(
                TASK_ID_DOMAIN,
                submitted.reservationId(),
                submitted.messageId().bytes(),
                Bytes.u64be(submitted.reservationExpiryEpochMs()),
                Bytes.u64be(submitted.stateVersion()));
        final WorkClassTask task = new WorkClassTask(
                WorkClass.GC, "reservation-expiry/" + Bytes.hex(Bytes.sha256(identity)), identity.length);
        final Submission submission = new Submission(task);
        workClasses.submit(task, () -> execute(submitted, clock, submission));
        return submission;
    }

    private void execute(
            final DelayShard.ReservationExpiryWork candidate, final LongSupplier clock, final Submission submission) {
        try {
            submission.complete(Outcome.succeeded(
                    ownedShard.materializeReservationExpiryAuthoritativelyStrict(authority, candidate, clock)));
        } catch (RuntimeException failure) {
            ownedShard.fence();
            submission.complete(Outcome.failed(failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(Outcome.failed(failure));
            throw failure;
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private volatile Outcome outcome;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<Outcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        private synchronized void complete(final Outcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("reservation expiry outcome is already complete");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    public record Outcome(DelayShard.ReservationExpiryMaterializationResult result, Throwable failure) {
        public Outcome {
            if ((result == null) == (failure == null)) {
                throw new IllegalArgumentException("reservation expiry outcome must contain one branch");
            }
        }

        private static Outcome succeeded(final DelayShard.ReservationExpiryMaterializationResult result) {
            return new Outcome(Objects.requireNonNull(result, "result"), null);
        }

        private static Outcome failed(final Throwable failure) {
            return new Outcome(null, Objects.requireNonNull(failure, "failure"));
        }
    }
}
