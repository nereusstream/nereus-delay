package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Bounded CHECKPOINT handoff for a local, unpublished format-2 candidate. */
public final class TargetCheckpointCandidateWorkClassExecutor {
    /** Local observation of all provider I/O/cleanup retained by one semantic preparation. */
    public interface PreparationIo {
        boolean ioExited();
        Throwable pendingIoFailure();
    }
    @FunctionalInterface
    public interface IntentRegistration {
        /** Prompt return; provider owns finite metadata I/O, namespace/Owner protection and uncertain/late cleanup. */
        java.util.concurrent.CompletableFuture<CheckpointUploadIntent> register(CheckpointUploadIntent pending);

        /** Legacy providers must complete their owned future only after actual attempt I/O exits. */
        default RegistrationAttempt begin(CheckpointUploadIntent pending) {
            final var owned = Objects.requireNonNull(register(pending), "intent registration future");
            return new RegistrationAttempt(owned, owned::isDone);
        }
    }

    /** Receipt views are detached; I/O exit is a separate provider-owned fact, never caller future cancellation. */
    public static final class RegistrationAttempt {
        private final java.util.concurrent.CompletableFuture<CheckpointUploadIntent> receipt;
        private final java.util.function.BooleanSupplier ioExited;
        private final Runnable requestStop;

        public RegistrationAttempt(java.util.concurrent.CompletableFuture<CheckpointUploadIntent> receipt,
                java.util.function.BooleanSupplier ioExited) {
            this(receipt, ioExited, () -> {});
        }

        public RegistrationAttempt(java.util.concurrent.CompletableFuture<CheckpointUploadIntent> receipt,
                java.util.function.BooleanSupplier ioExited, Runnable requestStop) {
            this.receipt = Objects.requireNonNull(receipt, "receipt");
            this.ioExited = Objects.requireNonNull(ioExited, "ioExited");
            this.requestStop = Objects.requireNonNull(requestStop, "requestStop");
        }

        public java.util.concurrent.CompletableFuture<CheckpointUploadIntent> receipt() { return receipt.copy(); }

        public boolean ioExited() { return ioExited.getAsBoolean(); }

        /** Prompt local request; an already-started RPC remains owned until its actual exit. */
        public void requestStop() { requestStop.run(); }
    }
    private static final byte[] TASK_DOMAIN = Bytes.utf8("nereus-delay-target-candidate-handoff\0");

    private final WorkClassExecutionRegistry workClasses;
    private final ShardStore store;
    private final OxiaOwnerLeaseStore leases;
    private final CheckpointUploadIntentAuthority intents;
    private final LongSupplier ownerClock;
    private final Runnable localOwnerGuard;
    private final SchedulerBudget settlementBudget;

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
        this(workClasses, store, leases, intents, ownerClock, localOwnerGuard, null);
    }

    /** The periodic Host supplies its exact fixed settlement budget before any task is queued. */
    public TargetCheckpointCandidateWorkClassExecutor(WorkClassExecutionRegistry workClasses, ShardStore store,
            OxiaOwnerLeaseStore leases, CheckpointUploadIntentAuthority intents, LongSupplier ownerClock,
            Runnable localOwnerGuard, SchedulerBudget settlementBudget) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.store = Objects.requireNonNull(store, "store");
        this.leases = Objects.requireNonNull(leases, "leases");
        this.intents = Objects.requireNonNull(intents, "intents");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.localOwnerGuard = Objects.requireNonNull(localOwnerGuard, "localOwnerGuard");
        this.settlementBudget = settlementBudget;
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
        requireServiceable(task);
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
        requireServiceable(task);
        workClasses.submit(task, () -> executeSemantic(semantic, submission, publication));
        return submission;
    }

    /** One bounded history step per CHECKPOINT invocation; an unfinished step keeps the same logical task. */
    public Submission submitCollectedSemantic(Request request,
            Supplier<Optional<TargetCheckpointSemanticSnapshot>> inputs,
            TargetCheckpointSemanticSnapshot.Limits limits, Runnable protection, PublicationPlan publication) {
        return submitCollectedSemantic(request, inputs, limits, protection, publication, null);
    }

    public Submission submitCollectedSemantic(Request request,
            Supplier<Optional<TargetCheckpointSemanticSnapshot>> inputs,
            TargetCheckpointSemanticSnapshot.Limits limits, Runnable protection, PublicationPlan publication,
            IntentRegistration registration) {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(protection, "protection");
        if (publication != null && publication.coordinator().resources() != store.sharedResources()) {
            throw new IllegalArgumentException("Target publication uses another Worker resource graph");
        }
        final var registered = new java.util.concurrent.atomic.AtomicBoolean(registration == null);
        final Runnable guard = () -> {
            requireCurrent(request, false, registered.get());
            protection.run();
        };
        guard.run();
        final byte[] identity = canonicalIdentity(request);
        final var task = new WorkClassTask(WorkClass.CHECKPOINT,
                (registration == null ? "target-collected-semantic/" : "target-register-collected-semantic/")
                        + Bytes.hex(Bytes.sha256(TASK_DOMAIN, identity)),
                Math.addExact(identity.length, limits.maximumBytes()));
        final var submission = new Submission(task);
        requireServiceable(task);
        submission.preparationGuard = guard;
        submission.preparationRegistry = workClasses;
        submission.preparationIo = inputs instanceof PreparationIo io ? io : null;
        submission.nextStep = () -> workClasses.submit(task, () -> {
            try {
                guard.run();
                if (!registered.get()) {
                    if (submission.registration == null) {
                        submission.registration = Objects.requireNonNull(registration.begin(request.pending()),
                                "intent registration attempt");
                        submission.registrationReceipt = submission.registration.receipt();
                        guard.run();
                        return;
                    }
                    if (!submission.registrationReceipt.isDone()) { return; }
                    final var receipt = Objects.requireNonNull(
                            submission.registrationReceipt.join(), "registered intent");
                    if (!submission.registration.ioExited()) { return; }
                    if (!request.pending().equals(receipt)) {
                        throw new IllegalStateException("checkpoint registration returned another pending value");
                    }
                    registered.set(true);
                    guard.run(); // Includes the mandatory exact durable reread, not just the completed future.
                    submission.registrationAccepted = true;
                    return;
                }
                final var snapshot = Objects.requireNonNull(inputs.get(), "semantic inputs");
                guard.run();
                if (snapshot.isEmpty()) { return; }
                if (snapshot.orElseThrow().schemaVersion() != 3) {
                    throw new IllegalArgumentException("collected Target checkpoint requires original Outcome schema");
                }
                final var semantic = new SemanticRequest(request, snapshot.orElseThrow(), limits, protection);
                submission.nextStep = null;
                executeSemantic(semantic, submission, publication);
            } catch (ReadIncompleteException incomplete) {
                // A fresh bounded read may continue on the next exact task invocation.
            } catch (RuntimeException failure) {
                if (submission.outcome().isEmpty()) { submission.complete(new Outcome(null, failure)); }
            } catch (Error failure) {
                if (submission.outcome().isEmpty()) { submission.complete(new Outcome(null, failure)); }
                throw failure;
            }
        });
        submission.nextStep.run();
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

    private void requireServiceable(WorkClassTask task) {
        if (settlementBudget == null) { workClasses.requireTaskServiceable(task); }
        else { workClasses.requireTaskServiceable(task, settlementBudget); }
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
        requireCurrent(request, allowPublished, true);
    }

    private void requireCurrent(Request request, boolean allowPublished, boolean requireRegistered) {
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
        if (requireRegistered && intents.current(pending).filter(pending::equals).isEmpty()
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
        private volatile Runnable nextStep;
        private RegistrationAttempt registration;
        private java.util.concurrent.CompletableFuture<CheckpointUploadIntent> registrationReceipt;
        private Outcome retainedOutcome;
        private boolean registrationAccepted;
        private PreparationIo preparationIo;
        private Runnable preparationGuard;
        private WorkClassExecutionRegistry preparationRegistry;

        private Submission(final WorkClassTask task) {
            this.task = task;
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<Outcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        public boolean preparing() { return outcome == null && nextStep != null; }

        /** Stops only an existing registration between bounded actions; completed history preparation is unchanged. */
        public synchronized void requestRegistrationStop() {
            if (registration == null || registrationAccepted || outcome != null || retainedOutcome != null
                    || preparationRegistry.state(task).isPresent()) { return; }
            complete(new Outcome(null, new IllegalStateException("Target checkpoint registration stopped")));
        }

        /** Only the owning Worker uses this to admit history reads while its ordinary turns are paused. */
        public void requirePreparationCurrent() {
            if (!preparing()) { throw new IllegalStateException("Target checkpoint history is no longer preparing"); }
            preparationGuard.run();
        }

        /** Reoffers an unfinished preparation only after its previous bounded invocation left the registry. */
        public synchronized void enqueueNextPreparation() {
            if (settleFailedRegistrationIo()) { return; }
            if (!preparing() || preparationRegistry.state(task).isPresent()) { return; }
            try { requirePreparationCurrent(); nextStep.run(); }
            catch (RuntimeException failure) { complete(new Outcome(null, failure)); }
            catch (Error failure) { complete(new Outcome(null, failure)); throw failure; }
        }

        /** Local failure settlement only; no Owner, Store, history or new business admission is performed. */
        public synchronized boolean settleFailedRegistrationIo() {
            if (retainedOutcome != null) {
                if (preparationRegistry.state(task).isEmpty() && ioExited()) {
                    outcome = retainedOutcome;
                    retainedOutcome = null;
                }
                return true;
            }
            return false;
        }

        /** Nonterminal diagnostic preserves native cleanup handles without granting completion/publication. */
        public synchronized Optional<Throwable> pendingIoFailure() {
            if (retainedOutcome == null) { return Optional.empty(); }
            final var cleanup = preparationIo == null ? null : preparationIo.pendingIoFailure();
            return Optional.ofNullable(cleanup == null ? retainedOutcome.failure() : cleanup);
        }

        private boolean ioExited() {
            return (registration == null || registration.ioExited())
                    && (preparationIo == null || preparationIo.ioExited());
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
            if (outcome != null || retainedOutcome != null) {
                throw new IllegalStateException("Target candidate action already completed");
            }
            Objects.requireNonNull(completed, "completed");
            if (registration != null && completed.failure() != null) { registration.requestStop(); }
            if (!ioExited()) {
                retainedOutcome = completed;
                nextStep = null;
                return;
            }
            outcome = completed;
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
