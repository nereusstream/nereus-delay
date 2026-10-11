package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** One existing CHECKPOINT action covers the authenticated Target provider upload and Catalog publication. */
public final class TargetCheckpointPublicationWorkClassExecutor {
    private final WorkClassExecutionRegistry workClasses;
    private final CheckpointPublicationCoordinator publication;

    public TargetCheckpointPublicationWorkClassExecutor(WorkClassExecutionRegistry workClasses,
            CheckpointPublicationCoordinator publication) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.publication = Objects.requireNonNull(publication, "publication");
        publication.resources().bindWorkClassExecutionRegistry(workClasses);
    }

    /** Pure queue identity/admission; authentication and provider/Catalog I/O run only in the selected action. */
    public Submission submit(CheckpointUploadIntent pending, TargetCheckpointRootVerifier.AuthenticatedImage image,
            long expectedCatalogGeneration, long nowEpochMs, CheckpointUploadAdapter adapter) {
        Objects.requireNonNull(pending, "pending");
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(adapter, "adapter");
        if (nowEpochMs < 0 || pending.baseCatalogGeneration() != expectedCatalogGeneration
                || !pending.shard().shardId().equals(image.manifest().shardId())
                || !Arrays.equals(pending.checkpointId(), image.manifest().checkpointId())) {
            throw new IllegalArgumentException("Target publication request does not match its exact image/intent");
        }
        final byte[] identity = Bytes.concat(pending.canonicalBytes(), image.manifest().manifestSha256());
        final var task = new WorkClassTask(WorkClass.CHECKPOINT,
                "target-checkpoint-publication/" + Bytes.hex(Bytes.sha256(identity)), identity.length);
        final var result = new Submission(task);
        workClasses.submit(task, () -> {
            try {
                result.complete(new Outcome(publication.publishTarget(pending, image, expectedCatalogGeneration,
                        nowEpochMs, adapter), null));
            } catch (RuntimeException failure) {
                result.complete(new Outcome(null, failure));
            } catch (Error failure) {
                result.complete(new Outcome(null, failure));
                throw failure;
            }
        });
        return result;
    }

    public static final class Submission {
        private final WorkClassTask task;
        private volatile Outcome outcome;

        private Submission(WorkClassTask task) { this.task = task; }
        public WorkClassTask task() { return task; }
        public Optional<Outcome> outcome() { return Optional.ofNullable(outcome); }

        private synchronized void complete(Outcome result) {
            if (outcome != null) { throw new IllegalStateException("Target publication already completed"); }
            outcome = result;
        }
    }

    public record Outcome(CheckpointPublicationCoordinator.CheckpointPublication published, Throwable failure) {
        public Outcome {
            if ((published == null) == (failure == null)) {
                throw new IllegalArgumentException("Target publication outcome must contain exactly one branch");
            }
        }
    }
}
