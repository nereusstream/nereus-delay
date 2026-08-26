package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded CHECKPOINT handoff for the optional final checkpoint of an owner
 * drain.
 *
 * <p>The drain coordinator owns the lifecycle and teardown state machine. This
 * class owns only the physical checkpoint action: admission binds the exact
 * target, checkpoint identity and draining Owner Lease without touching the
 * Store, while the selected action rereads the exact lease before and after
 * the RocksDB snapshot. A queued action therefore cannot bypass the shared
 * CHECKPOINT fairness/resource boundary.</p>
 */
public final class CheckpointDrainWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-checkpoint-drain-handoff\0");

    private final WorkClassExecutionRegistry workClasses;
    private final ShardStore store;

    public CheckpointDrainWorkClassExecutor(final WorkClassExecutionRegistry workClasses, final ShardStore store) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.store = Objects.requireNonNull(store, "store");
        this.store.sharedResources().bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers one exact final-checkpoint action without Store or filesystem I/O. */
    public Submission submit(final Request request) {
        final Request submitted = Objects.requireNonNull(request, "request");
        requireSubmission(submitted);
        final byte[] identity = canonicalIdentity(submitted);
        final WorkClassTask task = new WorkClassTask(
                WorkClass.CHECKPOINT,
                "checkpoint-drain/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, identity)),
                identity.length);
        final Submission result = new Submission(task, submitted);
        workClasses.submit(task, () -> execute(submitted, result));
        return result;
    }

    private void execute(final Request request, final Submission submission) {
        try {
            requireAuthoritativeDrain(request);
            final Path checkpoint = store.createCheckpoint(request.checkpointPath(), request.checkpointId());
            requireAuthoritativeDrain(request);
            submission.complete(Outcome.succeeded(checkpoint));
        } catch (RuntimeException failure) {
            submission.complete(Outcome.failed(failure));
        } catch (Error failure) {
            submission.complete(Outcome.failed(failure));
            throw failure;
        }
    }

    private void requireSubmission(final Request request) {
        if (!store.shardId().equals(request.expectedLease().shardId())) {
            throw new IllegalArgumentException("drain checkpoint lease belongs to another shard");
        }
        requireStoreOwnerEpoch(request.expectedLease());
        if (request.expectedLease().state() != ShardLifecycleState.DRAINING) {
            throw new IllegalArgumentException("drain checkpoint requires a DRAINING lease");
        }
        if (request.checkpointPath().toString().isBlank()) {
            throw new IllegalArgumentException("checkpoint path must not be blank");
        }
        if (request.deadlineEpochMs() < 0) {
            throw new IllegalArgumentException("drain deadline must be non-negative");
        }
    }

    private void requireAuthoritativeDrain(final Request request) {
        requireStoreOwnerEpoch(request.expectedLease());
        final long now = request.ownerClock().getAsLong();
        if (now < 0) {
            throw new IllegalArgumentException("drain checkpoint clock returned a negative time");
        }
        if (now >= request.deadlineEpochMs()) {
            throw new IllegalStateException("drain checkpoint deadline expired");
        }
        final Optional<OwnerLease> current =
                request.authority().current(request.expectedLease().shardId());
        if (current.isEmpty()) {
            throw new IllegalStateException("drain checkpoint Owner Lease is absent");
        }
        final OwnerLease observed = current.orElseThrow();
        if (!request.expectedLease().sameIdentity(observed)
                || observed.state() != ShardLifecycleState.DRAINING
                || !observed.validAt(now)) {
            throw new IllegalStateException("drain checkpoint Owner Lease changed or expired");
        }
    }

    private void requireStoreOwnerEpoch(final OwnerLease expectedLease) {
        if (store.runtimeMetadata().lastOpenedOwnerEpoch() != expectedLease.ownerEpoch()) {
            throw new IllegalStateException("drain checkpoint Store was not opened by the expected Owner epoch");
        }
    }

    private static byte[] canonicalIdentity(final Request request) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output,
                    1,
                    request.expectedLease().shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32(
                    output, 2, request.expectedLease().shardId().partition());
            CanonicalProtobuf.bytes(
                    output,
                    3,
                    Bytes.utf8(request.checkpointPath()
                            .toAbsolutePath()
                            .normalize()
                            .toString()));
            CanonicalProtobuf.bytes(output, 4, request.checkpointId());
            CanonicalProtobuf.bytes(
                    output, 5, Bytes.utf8(request.expectedLease().ownerId()));
            CanonicalProtobuf.uint64Bits(output, 6, request.expectedLease().ownerEpoch());
            CanonicalProtobuf.bytes(output, 7, request.expectedLease().leaseToken());
            CanonicalProtobuf.uint32(output, 8, request.expectedLease().state().wireValue());
            CanonicalProtobuf.int64(output, 9, request.expectedLease().expiresAtEpochMs());
            CanonicalProtobuf.int64(output, 11, request.deadlineEpochMs());
            if (request.expectedLease().context() != null) {
                CanonicalProtobuf.bytes(output, 10, CanonicalProtobuf.message(context -> {
                    CanonicalProtobuf.bytes(
                            context, 1, request.expectedLease().context().sourceAssignmentId());
                    CanonicalProtobuf.uint64Bits(
                            context, 2, request.expectedLease().context().assignmentEpoch());
                    CanonicalProtobuf.bytes(
                            context, 3, request.expectedLease().context().sessionIdentity());
                }));
            }
        });
    }

    /** Exact immutable input captured before the checkpoint action is queued. */
    public record Request(
            Path checkpointPath,
            byte[] checkpointId,
            OwnerLease expectedLease,
            OxiaOwnerLeaseStore authority,
            LongSupplier ownerClock,
            long deadlineEpochMs) {
        public Request {
            Objects.requireNonNull(checkpointPath, "checkpointPath");
            checkpointPath = checkpointPath.toAbsolutePath().normalize();
            Bytes.requireLength(checkpointId, 16, "checkpointId");
            boolean nonZero = false;
            for (byte value : checkpointId) {
                nonZero |= value != 0;
            }
            if (!nonZero) {
                throw new IllegalArgumentException("checkpointId must be non-zero");
            }
            checkpointId = Bytes.copy(checkpointId);
            Objects.requireNonNull(expectedLease, "expectedLease");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(ownerClock, "ownerClock");
        }

        @Override
        public byte[] checkpointId() {
            return Bytes.copy(checkpointId);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final Request request;
        private volatile Outcome outcome;

        private Submission(final WorkClassTask task, final Request request) {
            this.task = Objects.requireNonNull(task, "task");
            this.request = Objects.requireNonNull(request, "request");
        }

        public WorkClassTask task() {
            return task;
        }

        public Request request() {
            return request;
        }

        public Optional<Outcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        private synchronized void complete(final Outcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("drain checkpoint action already completed");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    /** Exactly one physical checkpoint path or failure evidence is present. */
    public record Outcome(Path checkpointPath, Throwable failure) {
        public Outcome {
            if ((checkpointPath == null) == (failure == null)) {
                throw new IllegalArgumentException("drain checkpoint outcome must contain one branch");
            }
        }

        private static Outcome succeeded(final Path checkpointPath) {
            return new Outcome(Objects.requireNonNull(checkpointPath, "checkpointPath"), null);
        }

        private static Outcome failed(final Throwable failure) {
            return new Outcome(null, Objects.requireNonNull(failure, "failure"));
        }
    }
}
