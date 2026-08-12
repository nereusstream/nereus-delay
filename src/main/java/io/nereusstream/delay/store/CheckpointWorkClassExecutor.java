package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Routes an exact scheduled checkpoint attempt through the bounded V1
 * {@link WorkClass#CHECKPOINT} execution class.
 *
 * <p>Submission validates the exact scheduler claim and immutable
 * Store/intent identity before queue admission, but performs no filesystem or
 * provider I/O.  Queue rejection therefore leaves the claim current and the
 * caller can submit the same request again.  The action repeats those checks
 * in {@link CheckpointExecutionCoordinator} after the queue wait.</p>
 *
 * <p>An ordinary checkpoint attempt failure is captured in the returned
 * {@link Submission}: the checkpoint coordinator has already either
 * rescheduled the exact claim or retained it because completion could not be
 * proved, so the generic work registry must not independently retry that
 * action.  A fatal {@link Error} is recorded and rethrown so the work-class
 * fatal-stop and process fencing rules still apply.</p>
 */
public final class CheckpointWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-checkpoint-handoff-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final CheckpointExecutionCoordinator checkpointExecutor;

    public CheckpointWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                       final CheckpointExecutionCoordinator checkpointExecutor) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.checkpointExecutor = Objects.requireNonNull(checkpointExecutor, "checkpointExecutor");
    }

    /** Registers one exact action; physical checkpoint work starts only when its bounded turn runs. */
    public Submission submit(final ExecutionRequest request) {
        final ExecutionRequest submitted = Objects.requireNonNull(request, "request");
        checkpointExecutor.requireCurrentExecution(submitted.claim(), submitted.pending());
        final byte[] identity = canonicalIdentity(submitted);
        final WorkClassTask task = new WorkClassTask(WorkClass.CHECKPOINT,
                "checkpoint/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, identity)), identity.length);
        final Submission result = new Submission(task);
        workClasses.submit(task, () -> execute(submitted, result));
        return result;
    }

    private void execute(final ExecutionRequest request, final Submission submission) {
        if (submission.outcome().isPresent()) {
            throw new IllegalStateException("checkpoint work-class action already completed");
        }
        try {
            submission.complete(AttemptOutcome.succeeded(checkpointExecutor.execute(
                    request.claim(), request.checkpointDirectory(), request.pending(),
                    request.manifestFactory(), request.uploadNowEpochMs(), request.completionClock(),
                    request.adapter())));
        } catch (RuntimeException failure) {
            // The checkpoint coordinator owns exact-claim rescheduling.  A
            // completed failure outcome is the handler result, not a second
            // generic retry authority.
            submission.complete(AttemptOutcome.failed(failure));
        } catch (Error failure) {
            submission.complete(AttemptOutcome.failed(failure));
            throw failure;
        }
    }

    private static byte[] canonicalIdentity(final ExecutionRequest request) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1,
                    request.claim().shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, request.claim().shardId().partition());
            CanonicalProtobuf.int64(output, 3, request.claim().dueAtEpochMs());
            CanonicalProtobuf.bytes(output, 4,
                    Bytes.utf8(request.checkpointDirectory().toString()));
            CanonicalProtobuf.bytes(output, 5, request.pending().canonicalBytes());
            CanonicalProtobuf.int64(output, 6, request.uploadNowEpochMs());
        });
    }

    /** Complete immutable inputs for one queued checkpoint attempt. */
    public record ExecutionRequest(
            CheckpointScheduler.ScheduledCheckpoint claim,
            Path checkpointDirectory,
            CheckpointUploadIntentV1 pending,
            CheckpointExecutionCoordinator.CheckpointManifestFactory manifestFactory,
            long uploadNowEpochMs,
            LongSupplier completionClock,
            CheckpointUploadAdapter adapter) {
        public ExecutionRequest {
            Objects.requireNonNull(claim, "claim");
            Objects.requireNonNull(checkpointDirectory, "checkpointDirectory");
            checkpointDirectory = checkpointDirectory.toAbsolutePath().normalize();
            Objects.requireNonNull(pending, "pending");
            Objects.requireNonNull(manifestFactory, "manifestFactory");
            Objects.requireNonNull(completionClock, "completionClock");
            Objects.requireNonNull(adapter, "adapter");
            if (uploadNowEpochMs < 0) {
                throw new IllegalArgumentException("uploadNowEpochMs must be non-negative");
            }
        }
    }

    /** Read-only process-local handle for the queued action and its completed attempt outcome. */
    public static final class Submission {
        private final WorkClassTask task;
        private volatile AttemptOutcome outcome;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<AttemptOutcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        private synchronized void complete(final AttemptOutcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("checkpoint work-class outcome is already complete");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    /** Exactly one of result/failure is present. */
    public record AttemptOutcome(CheckpointExecutionCoordinator.ExecutionResult result, Throwable failure) {
        public AttemptOutcome {
            if ((result == null) == (failure == null)) {
                throw new IllegalArgumentException("checkpoint attempt outcome must contain one branch");
            }
        }

        private static AttemptOutcome succeeded(final CheckpointExecutionCoordinator.ExecutionResult result) {
            return new AttemptOutcome(Objects.requireNonNull(result, "result"), null);
        }

        private static AttemptOutcome failed(final Throwable failure) {
            return new AttemptOutcome(null, Objects.requireNonNull(failure, "failure"));
        }
    }
}
