package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

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

    /** Queues candidate inspection, Manifest construction and full authentication before upload/Catalog I/O. */
    public Submission submitCandidate(TargetCheckpointRootVerifier.CandidateImage candidate,
            LongSupplier clock, CheckpointUploadAdapter adapter) {
        return submitCandidate(candidate, clock, adapter, null);
    }

    /** Explicit first-publication path; namespace initialization needs separate trusted permission. */
    public Submission submitInitialCandidate(TargetCheckpointRootVerifier.CandidateImage candidate,
            LongSupplier clock, CheckpointUploadAdapter adapter, CatalogInitializationAuthority initialization) {
        return submitCandidate(candidate, clock, adapter, Objects.requireNonNull(initialization, "initialization"));
    }

    private Submission submitCandidate(TargetCheckpointRootVerifier.CandidateImage candidate,
            LongSupplier clock, CheckpointUploadAdapter adapter, CatalogInitializationAuthority initialization) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(adapter, "adapter");
        Objects.requireNonNull(clock, "clock");
        final var identity = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, candidate.pending().canonicalBytes());
            CanonicalProtobuf.bytes(out, 2, Bytes.utf8(candidate.image().toString()));
            CanonicalProtobuf.uint64Bits(out, 3, candidate.lineageGeneration());
            for (var file : candidate.fileObjects()) {
                CanonicalProtobuf.bytes(out, 4, CanonicalProtobuf.message(entry -> {
                    CanonicalProtobuf.bytes(entry, 1, Bytes.utf8(file.name()));
                    CanonicalProtobuf.int64(entry, 2, file.length());
                    CanonicalProtobuf.bytes(entry, 3, file.checksum());
                    CanonicalProtobuf.bytes(entry, 4, file.objectKey());
                    CanonicalProtobuf.bytes(entry, 5, file.objectVersion());
                    if (file.etag() != null) { CanonicalProtobuf.bytes(entry, 6, file.etag()); }
                }));
            }
            CanonicalProtobuf.bytes(out, 5, CanonicalProtobuf.message(limits -> {
                CanonicalProtobuf.uint32(limits, 1, candidate.physical().maxFiles());
                CanonicalProtobuf.int64(limits, 2, candidate.physical().maxTotalFileBytes());
                CanonicalProtobuf.int64(limits, 3, candidate.physical().maxIndividualFileBytes());
                CanonicalProtobuf.uint32(limits, 4, candidate.physical().maxPathBytes());
                CanonicalProtobuf.uint32(limits, 5, candidate.physical().maxManifestBytes());
                CanonicalProtobuf.uint32(limits, 6, candidate.physical().maxEvidenceCursors());
                CanonicalProtobuf.uint32(limits, 7, candidate.physical().maxObjectIdentityBytes());
                CanonicalProtobuf.uint32(limits, 8, candidate.quota().maxRecords());
                CanonicalProtobuf.int64(limits, 9, candidate.quota().maxKeyValueBytes());
                CanonicalProtobuf.uint32(limits, 10, candidate.ledger().maxRecords());
                CanonicalProtobuf.int64(limits, 11, candidate.ledger().maxKeyValueBytes());
                CanonicalProtobuf.uint32(limits, 12, candidate.ledger().maxPointReads());
                CanonicalProtobuf.int64(limits, 13, candidate.ledger().maxPointReadBytes());
                CanonicalProtobuf.uint32(limits, 14, candidate.semantic().maximumRecords());
                CanonicalProtobuf.uint32(limits, 15, candidate.semantic().maximumBytes());
                CanonicalProtobuf.int64(limits, 16, candidate.semantic().maximumAdmissionFrameBytes());
            }));
        });
        final var task = new WorkClassTask(WorkClass.CHECKPOINT,
                (initialization == null ? "target-candidate-publication/" : "target-initial-publication/")
                        + Bytes.hex(Bytes.sha256(identity)), identity.length);
        final var result = new Submission(task);
        workClasses.submit(task, () -> {
            try {
                final var image = TargetCheckpointRootVerifier.authenticateCandidate(candidate);
                if (initialization != null) {
                    publication.initializeTarget(candidate.pending(), image, initialization);
                }
                final var published = publication.publishTarget(candidate.pending(), image,
                        candidate.pending().baseCatalogGeneration(), clock.getAsLong(), adapter);
                result.authenticatedImage = image;
                result.complete(new Outcome(published, null));
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
        private TargetCheckpointRootVerifier.AuthenticatedImage authenticatedImage;

        private Submission(WorkClassTask task) { this.task = task; }
        public WorkClassTask task() { return task; }
        public Optional<Outcome> outcome() { return Optional.ofNullable(outcome); }

        /** Available only after successful candidate publication; the caller retains the image protections. */
        public Optional<TargetCheckpointRootVerifier.AuthenticatedImage> authenticatedImage() {
            final var settled = outcome;
            if (settled == null || settled.failure() != null || authenticatedImage == null) { return Optional.empty(); }
            authenticatedImage.requireCurrent();
            return Optional.of(authenticatedImage);
        }

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
