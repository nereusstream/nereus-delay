package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.util.Objects;
import java.util.Optional;

/**
 * Bounded CHECKPOINT handoff for one exact checkpoint restore.
 *
 * <p>Admission binds the complete manifest/resource/pin identity without
 * catalog, filesystem or provider I/O. The queued action then performs the
 * entire download-to-install interval through {@link CheckpointRestoreCoordinator};
 * a queue rejection therefore leaves the restore request and its recovery
 * authority untouched. A normal failure is returned as restore-owned evidence,
 * while a fatal {@link Error} remains visible to the event-loop stop path.</p>
 */
public final class CheckpointRestoreWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-checkpoint-restore-handoff-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final CheckpointRestoreCoordinator restoreCoordinator;

    public CheckpointRestoreWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                              final CheckpointRestoreCoordinator restoreCoordinator) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.restoreCoordinator = Objects.requireNonNull(restoreCoordinator, "restoreCoordinator");
        this.restoreCoordinator.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers an exact restore action; physical download starts only when selected. */
    public Submission submit(final RestoreRequest request) {
        final RestoreRequest submitted = Objects.requireNonNull(request, "request");
        restoreCoordinator.requireRestoreSubmission(submitted.request(), submitted.pin());
        final byte[] identity = canonicalIdentity(submitted);
        final WorkClassTask task = new WorkClassTask(WorkClass.CHECKPOINT,
                "checkpoint-restore/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, identity)),
                identity.length);
        final Submission result = new Submission(task, submitted);
        workClasses.submit(task, () -> execute(submitted, result));
        return result;
    }

    private void execute(final RestoreRequest request, final Submission submission) {
        try {
            restoreCoordinator.requireRestoreSubmission(request.request(), request.pin());
            submission.complete(RestoreOutcome.succeeded(
                    restoreCoordinator.restore(request.request(), request.pin())));
        } catch (RuntimeException failure) {
            submission.complete(RestoreOutcome.failed(failure));
        } catch (Error failure) {
            submission.complete(RestoreOutcome.failed(failure));
            throw failure;
        }
    }

    private static byte[] canonicalIdentity(final RestoreRequest request) {
        return io.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 1,
                    request.request().manifest().canonicalJsonBytes());
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 2,
                    request.request().resource().canonicalBytes());
            if (request.pin() != null) {
                io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 3,
                        request.pin().canonicalBytes());
            }
        });
    }

    public record RestoreRequest(CheckpointDownloadRequest request, RecoveryPinV1 pin) {
        public RestoreRequest {
            Objects.requireNonNull(request, "request");
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final RestoreRequest request;
        private volatile RestoreOutcome outcome;

        private Submission(final WorkClassTask task, final RestoreRequest request) {
            this.task = Objects.requireNonNull(task, "task");
            this.request = Objects.requireNonNull(request, "request");
        }

        public WorkClassTask task() {
            return task;
        }

        public RestoreRequest request() {
            return request;
        }

        public Optional<RestoreOutcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        private synchronized void complete(final RestoreOutcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("checkpoint restore action already completed");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    /** Exactly one of installed Store or failure evidence is present. */
    public record RestoreOutcome(ShardStore restored, Throwable failure) {
        public RestoreOutcome {
            if ((restored == null) == (failure == null)) {
                throw new IllegalArgumentException("checkpoint restore outcome must contain one branch");
            }
        }

        private static RestoreOutcome succeeded(final ShardStore restored) {
            return new RestoreOutcome(Objects.requireNonNull(restored, "restored"), null);
        }

        private static RestoreOutcome failed(final Throwable failure) {
            return new RestoreOutcome(null, Objects.requireNonNull(failure, "failure"));
        }
    }
}
