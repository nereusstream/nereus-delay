package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Bounded CHECKPOINT handoff for a local, unpublished format-2 candidate. */
public final class TargetCheckpointCandidateWorkClassExecutor {
    private static final byte[] TASK_DOMAIN = Bytes.utf8("nereus-delay-target-candidate-handoff\0");

    private final WorkClassExecutionRegistry workClasses;
    private final ShardStore store;
    private final OxiaOwnerLeaseStore leases;
    private final CheckpointUploadIntentAuthority intents;
    private final LongSupplier ownerClock;
    private final Runnable localOwnerGuard;

    /** Local/test seam; an active Worker must supply its own fencing guard through the overload below. */
    public TargetCheckpointCandidateWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final OxiaOwnerLeaseStore leases,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock) {
        this(workClasses, store, leases, intents, ownerClock, () -> {});
    }

    /** The Worker-bound form also rechecks the exact local Owner runtime on every authority read. */
    public TargetCheckpointCandidateWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final OxiaOwnerLeaseStore leases,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Runnable localOwnerGuard) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.store = Objects.requireNonNull(store, "store");
        this.leases = Objects.requireNonNull(leases, "leases");
        this.intents = Objects.requireNonNull(intents, "intents");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.localOwnerGuard = Objects.requireNonNull(localOwnerGuard, "localOwnerGuard");
        if (store.metadata().storeFormatVersion() != 2) {
            throw new IllegalArgumentException("Target candidate requires a format-2 Store");
        }
        store.sharedResources().bindWorkClassExecutionRegistry(workClasses);
    }

    /** Queue admission performs no Store or filesystem mutation. */
    public Submission submit(final Request request) {
        final Request submitted = Objects.requireNonNull(request, "request");
        requireCurrent(submitted);
        final byte[] identity = canonicalIdentity(submitted);
        final WorkClassTask task = new WorkClassTask(
                WorkClass.CHECKPOINT,
                "target-candidate/" + Bytes.hex(Bytes.sha256(TASK_DOMAIN, identity)),
                identity.length);
        final Submission submission = new Submission(task);
        workClasses.submit(task, () -> execute(submitted, submission));
        return submission;
    }

    /** Queues the exact immutable semantic bytes and limits in the existing CHECKPOINT graph. */
    public Submission submitSemantic(final SemanticRequest request) {
        return submitSemantic(request, null);
    }

    /** The same retained task covers candidate creation through authenticated upload/Catalog completion. */
    public Submission submitSemanticPublication(SemanticRequest request, PublicationPlan publication) {
        Objects.requireNonNull(publication, "publication");
        if (publication.coordinator().resources() != store.sharedResources()) {
            throw new IllegalArgumentException("Target publication uses another Worker resource graph");
        }
        return submitSemantic(request, publication);
    }

    private Submission submitSemantic(SemanticRequest request, PublicationPlan publication) {
        final var semantic = Objects.requireNonNull(request, "request");
        requireCurrentSemantic(semantic, publication != null);
        final byte[] identity = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, canonicalIdentity(semantic.candidate()));
            CanonicalProtobuf.bytes(out, 2, semantic.snapshot().canonicalBytes());
            CanonicalProtobuf.uint32(out, 3, semantic.limits().maximumRecords());
            CanonicalProtobuf.uint32(out, 4, semantic.limits().maximumBytes());
            CanonicalProtobuf.int64(out, 5, semantic.limits().maximumAdmissionFrameBytes());
            if (publication != null) {
                CanonicalProtobuf.uint64Bits(out, 6, publication.lineageGeneration());
                CanonicalProtobuf.uint32(out, 7, publication.initialization() == null ? 1 : 2);
            }
        });
        final var task = new WorkClassTask(WorkClass.CHECKPOINT,
                (publication == null ? "target-semantic-candidate/" : "target-semantic-publication/")
                        + Bytes.hex(Bytes.sha256(TASK_DOMAIN, identity)), identity.length);
        final var submission = new Submission(task);
        workClasses.submit(task, () -> executeSemantic(semantic, submission, publication));
        return submission;
    }

    private void executeSemantic(SemanticRequest semantic, Submission submission, PublicationPlan publication) {
        try {
            requireCurrentSemantic(semantic, publication != null);
            final var request = semantic.candidate();
            final Path path = Files.exists(request.checkpointPath(), LinkOption.NOFOLLOW_LINKS)
                    ? store.reuseTargetSemanticCheckpointCandidate(
                            request.checkpointPath(), request.pending().checkpointId(),
                            request.pending().recoveryLineageId(), request.physicalLimits(), request.quotaLimits(),
                            request.ledgerLimits(), semantic.snapshot(), semantic.limits())
                    : store.createTargetSemanticCheckpointCandidate(
                            request.checkpointPath(), request.pending().checkpointId(),
                            request.pending().recoveryLineageId(), request.physicalLimits(), request.quotaLimits(),
                            request.ledgerLimits(), semantic.snapshot(), semantic.limits());
            requireCurrentSemantic(semantic, publication != null);
            if (publication != null) {
                final var objects = new java.util.ArrayList<CheckpointManifest.FileEntry>();
                for (var file : CheckpointFileInventory.collect(path, request.physicalLimits())) {
                    requireCurrentSemantic(semantic, publication != null);
                    objects.add(Objects.requireNonNull(publication.fileObjects().apply(file), "file object"));
                }
                final var image = TargetCheckpointRootVerifier.authenticateCandidate(
                        new TargetCheckpointRootVerifier.CandidateImage(path, request.pending(),
                                publication.lineageGeneration(), objects, request.physicalLimits(),
                                request.quotaLimits(),
                                request.ledgerLimits(), semantic.limits(), publication.authority(),
                                () -> requireCurrentSemantic(semantic, publication != null)));
                if (publication.initialization() != null) {
                    publication.coordinator().initializeTarget(request.pending(), image, publication.initialization());
                }
                final var published = publication.coordinator().publishTarget(request.pending(), image,
                        request.pending().baseCatalogGeneration(), ownerClock.getAsLong(), publication.adapter());
                requireCurrentSemantic(semantic, publication != null);
                submission.published = published;
                submission.authenticatedImage = image;
            }
            submission.complete(new Outcome(path, null));
        } catch (RuntimeException failure) {
            submission.complete(new Outcome(null, failure));
        } catch (Error failure) {
            submission.complete(new Outcome(null, failure));
            throw failure;
        }
    }

    private void requireCurrentSemantic(SemanticRequest request, boolean allowPublished) {
        requireCurrent(request.candidate(), allowPublished);
        request.snapshot().requireCut(store.metadata(), store.appliedShardLogPosition(), store.shardMutationSequence(),
                request.candidate().pending().recoveryLineageId());
        request.protectionGuard().run();
    }

    public record SemanticRequest(Request candidate, TargetCheckpointSemanticSnapshot snapshot,
            TargetCheckpointSemanticSnapshot.Limits limits, Runnable protectionGuard) {
        public SemanticRequest {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(snapshot, "snapshot");
            Objects.requireNonNull(limits, "limits");
            Objects.requireNonNull(protectionGuard, "protectionGuard");
            TargetCheckpointSemanticSnapshot.decode(snapshot.canonicalBytes(), limits);
        }
    }

    /** Exact object identities and full image/namespace rights remain caller-owned through this task. */
    public record PublicationPlan(CheckpointPublicationCoordinator coordinator, long lineageGeneration,
            java.util.function.Function<CheckpointFileInventory, CheckpointManifest.FileEntry> fileObjects,
            TargetCheckpointRootVerifier.ImageAuthority authority, CheckpointUploadAdapter adapter,
            CatalogInitializationAuthority initialization) {
        public PublicationPlan {
            Objects.requireNonNull(coordinator, "coordinator");
            Objects.requireNonNull(fileObjects, "fileObjects");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(adapter, "adapter");
        }
    }

    private void execute(final Request request, final Submission submission) {
        try {
            requireCurrent(request);
            final Path path = Files.exists(request.checkpointPath(), LinkOption.NOFOLLOW_LINKS)
                    ? store.reuseTargetCheckpointCandidate(
                            request.checkpointPath(),
                            request.pending().checkpointId(),
                            request.pending().recoveryLineageId(),
                            request.physicalLimits(),
                            request.quotaLimits(),
                            request.ledgerLimits())
                    : store.createTargetCheckpointCandidate(
                            request.checkpointPath(),
                            request.pending().checkpointId(),
                            request.pending().recoveryLineageId(),
                            request.physicalLimits(),
                            request.quotaLimits(),
                            request.ledgerLimits());
            requireCurrent(request);
            submission.complete(new Outcome(path, null));
        } catch (RuntimeException failure) {
            submission.complete(new Outcome(null, failure));
        } catch (Error failure) {
            submission.complete(new Outcome(null, failure));
            throw failure;
        }
    }

    private void requireCurrent(final Request request) {
        requireCurrent(request, false);
    }

    private void requireCurrent(Request request, boolean allowPublished) {
        localOwnerGuard.run();
        final CheckpointUploadIntent pending = request.pending();
        final OwnerLease expected = request.expectedLease();
        if (!store.shardId().equals(pending.shard().shardId())
                || !store.shardId().equals(expected.shardId())
                || !Bytes.constantTimeEquals(store.metadata().storeIncarnation(), pending.sourceStoreIncarnation())
                || !Bytes.constantTimeEquals(pending.owner().leaseFencingDigest(), expected.leaseToken())
                || pending.owner().ownerEpoch() != expected.ownerEpoch()
                || store.runtimeMetadata().lastOpenedOwnerEpoch() != expected.ownerEpoch()) {
            throw new IllegalStateException("Target candidate Store, intent and Owner identities differ");
        }
        if (pending.state() != CheckpointUploadState.PENDING_UPLOAD
                || expected.context() == null
                || expected.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException(
                    "Target candidate requires an active session-bound Owner and pending intent");
        }
        final long now = ownerClock.getAsLong();
        if (now < 0 || now >= pending.uploadDeadlineEpochMs()) {
            throw new IllegalStateException("Target candidate upload deadline expired");
        }
        final OwnerLease current = leases.current(expected.shardId())
                .orElseThrow(() -> new IllegalStateException("Target candidate Owner Lease is absent"));
        if (!expected.sameIdentity(current)
                || current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || !current.validAt(now)) {
            throw new IllegalStateException("Target candidate Owner Lease changed or expired");
        }
        if (intents.current(pending).isEmpty()
                && !(allowPublished && intents.currentPublishedFor(pending).isPresent())) {
            throw new IllegalStateException("Target candidate pending intent changed or is absent");
        }
        localOwnerGuard.run();
    }

    private static byte[] canonicalIdentity(final Request request) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, request.pending().canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, Bytes.utf8(request.checkpointPath().toString()));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8(request.expectedLease().ownerId()));
            CanonicalProtobuf.bytes(output, 4, request.expectedLease().leaseToken());
            CanonicalProtobuf.uint64Bits(output, 5, request.expectedLease().ownerEpoch());
            CanonicalProtobuf.int64(output, 6, request.expectedLease().expiresAtEpochMs());
            CanonicalProtobuf.bytes(output, 7, request.expectedLease().context().sourceAssignmentId());
            CanonicalProtobuf.uint64Bits(output, 8, request.expectedLease().context().assignmentEpoch());
            CanonicalProtobuf.bytes(output, 9, request.expectedLease().context().sessionIdentity());
            CanonicalProtobuf.bytes(output, 10, CanonicalProtobuf.message(limits -> {
                CanonicalProtobuf.uint32(limits, 1, request.physicalLimits().maxFiles());
                CanonicalProtobuf.int64(limits, 2, request.physicalLimits().maxTotalFileBytes());
                CanonicalProtobuf.int64(limits, 3, request.physicalLimits().maxIndividualFileBytes());
                CanonicalProtobuf.uint32(limits, 4, request.physicalLimits().maxPathBytes());
                CanonicalProtobuf.uint32(limits, 5, request.physicalLimits().maxManifestBytes());
                CanonicalProtobuf.uint32(limits, 6, request.physicalLimits().maxEvidenceCursors());
                CanonicalProtobuf.uint32(limits, 7, request.physicalLimits().maxObjectIdentityBytes());
            }));
            CanonicalProtobuf.bytes(output, 11, CanonicalProtobuf.message(limits -> {
                CanonicalProtobuf.uint32(limits, 1, request.quotaLimits().maxRecords());
                CanonicalProtobuf.int64(limits, 2, request.quotaLimits().maxKeyValueBytes());
            }));
            CanonicalProtobuf.bytes(output, 12, CanonicalProtobuf.message(limits -> {
                CanonicalProtobuf.uint32(limits, 1, request.ledgerLimits().maxRecords());
                CanonicalProtobuf.int64(limits, 2, request.ledgerLimits().maxKeyValueBytes());
                CanonicalProtobuf.uint32(limits, 3, request.ledgerLimits().maxPointReads());
                CanonicalProtobuf.int64(limits, 4, request.ledgerLimits().maxPointReadBytes());
            }));
        });
    }

    public record Request(
            Path checkpointPath,
            CheckpointUploadIntent pending,
            OwnerLease expectedLease,
            CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        public Request {
            checkpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath").toAbsolutePath().normalize();
            Objects.requireNonNull(pending, "pending");
            Objects.requireNonNull(expectedLease, "expectedLease");
            TargetCheckpointRootVerifier.requireFinitePhysicalLimits(physicalLimits);
            Objects.requireNonNull(quotaLimits, "quotaLimits");
            Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private volatile Outcome outcome;
        private CheckpointPublicationCoordinator.CheckpointPublication published;
        private TargetCheckpointRootVerifier.AuthenticatedImage authenticatedImage;

        private Submission(final WorkClassTask task) {
            this.task = task;
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<Outcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        /** Empty until the complete protected creation/auth/upload/Catalog action has succeeded. */
        public Optional<CheckpointPublicationCoordinator.CheckpointPublication> publication() {
            final var completed = outcome;
            return completed == null || completed.failure() != null ? Optional.empty() : Optional.ofNullable(published);
        }

        public Optional<TargetCheckpointRootVerifier.AuthenticatedImage> authenticatedImage() {
            final var completed = outcome;
            if (completed == null || completed.failure() != null || authenticatedImage == null) {
                return Optional.empty();
            }
            authenticatedImage.requireCurrent();
            return Optional.of(authenticatedImage);
        }

        private synchronized void complete(final Outcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("Target candidate action already completed");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    /** A failed postcheck leaves only a local candidate, never publication authority. */
    public record Outcome(Path checkpointPath, Throwable failure) {
        public Outcome {
            if ((checkpointPath == null) == (failure == null)) {
                throw new IllegalArgumentException("Target candidate outcome requires exactly one branch");
            }
        }
    }
}
