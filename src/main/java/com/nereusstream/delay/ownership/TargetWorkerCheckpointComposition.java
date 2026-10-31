package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialProfileAuthority;
import com.nereusstream.delay.runtime.OxiaCheckpointIntentRegistration;
import com.nereusstream.delay.store.CatalogInitializationAuthority;
import com.nereusstream.delay.store.CheckpointDownloadAdapter;
import com.nereusstream.delay.store.CheckpointDownloadRequest;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.CheckpointPublicationCoordinator;
import com.nereusstream.delay.store.OxiaObjectStoreCredentialLeaseActivator;
import com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend;
import com.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapter;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** One Source's actual checkpoint SDK, atomic Catalog and original protected preparation loans. */
public final class TargetWorkerCheckpointComposition
        implements AutoCloseable, TargetCheckpointCandidateWorkClassExecutor.PreparationIo {
    /** The existing three SDK readers, each authorized independently for signed Source HISTORY. */
    public record Histories(
            TargetCheckpointControlInputs.History controls,
            TargetCheckpointOutcomeInputs.History outcomes,
            TargetCheckpointAdmissionInputs.History admissions,
            TargetCheckpointCandidateWorkClassExecutor.PreparationIo io) {
        public Histories {
            Objects.requireNonNull(controls);
            Objects.requireNonNull(outcomes);
            Objects.requireNonNull(admissions);
            Objects.requireNonNull(io);
        }
    }

    /** Original deployment acquisition, invoked only on this process's already bounded I/O executor. */
    @FunctionalInterface
    public interface TrustedDeployment {
        HeldInputs acquire(TargetWorkerCheckpointConfiguration configuration, Runnable current);
    }

    /** Actual transport/protection allocations; their original close and late-failure handles remain retained. */
    public interface HeldInputs extends AutoCloseable, CheckpointInputFuture.Cleanup {
        boolean ioExited();

        Throwable pendingIoFailure();

        void requestStop();

        CredentialProfileAuthority credentials();

        OxiaObjectStoreCredentialLeaseActivator.CredentialMaterialResolver materialResolver();
        /** The original dedicated owned transport instance; never a shared caller/global client. */
        HttpClient httpClient();
        /** Fresh original signed evidence already held locally; this method must not start a provider or Registry RPC. */
        TrustedUtcIntervalEvidence trustedTime();
        /** Local binding of original deployment/run identity to the actual acquired lease; configuration grants no identity. */
        OwnerIdentity bindOwner(OwnerLease original);
        /** Local checks of already-held original permissions, epochs, transport and credential allocation. */
        void requireCurrent();
        /** Transport HISTORY permission alone grants no physical Source retention or reader admission. */
        default void requireHistoryReadable(SourcePosition exact) {
            Objects.requireNonNull(exact, "exact original Source history position");
            throw unavailable("original physical Source history retention/admission authority is unavailable");
        }

        CutInputs acquireCut(CutRequest request, Runnable current);

        RestoreInputs acquireRestore(
                RecoveryCandidateRef candidate,
                RecoveryPin originalPin,
                CheckpointDownloadRequest request,
                Runnable current);
        /** Closes the original dedicated HTTP/native allocation; cleanupComplete alone cannot replace real exit. */
        @Override
        void close();
    }

    public record CutRequest(
            TargetWorkerShardRuntime worker,
            SourceRecordConsumer.CheckpointCut cut,
            CheckpointUploadIntent pending,
            TargetCheckpointControlInputs.Result controls,
            TargetCheckpointOutcomeInputs.Result outcomes,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,
            TargetCheckpointDependencies dependencies) {
        public CutRequest {
            Objects.requireNonNull(worker);
            Objects.requireNonNull(cut);
            Objects.requireNonNull(pending);
            Objects.requireNonNull(controls);
            Objects.requireNonNull(outcomes);
            admissions = List.copyOf(admissions);
            Objects.requireNonNull(dependencies);
        }
    }

    /** A distinct original cut/Pin loan, never shared with another candidate's stop or cleanup. */
    public interface CutInputs extends AutoCloseable, CheckpointInputFuture.Cleanup {
        boolean ioExited();

        Throwable pendingIoFailure();

        void requestStop();

        TargetCheckpointSemanticInputs.OriginalAuthorityFactory authorities();

        TargetCheckpointRootVerifier.ImageAuthority images();

        CatalogInitializationAuthority initialization();
        /** Independently retains this exact publication's live reuse purpose, including original Owner/Pin epochs.
         * A partial failed acquisition retains its original native cleanup through the original exception handle. */
        default TargetCheckpointCandidateSchedule.PublishedValidationHold acquirePublishedValidation(
                TargetCheckpointCandidateSchedule.PublishedValidationRequest request, Runnable current) {
            throw unavailable("original checkpoint publication reuse authority/Pin loan is unavailable");
        }

        void requireCurrent();

        @Override
        void close();
    }

    public interface RestoreInputs extends AutoCloseable, CheckpointInputFuture.Cleanup {
        boolean ioExited();

        Throwable pendingIoFailure();

        void requestStop();

        TargetCheckpointRootVerifier.ImageAuthority images();

        void requireCurrent();

        @Override
        void close();
    }

    public record RestoreMaterial(
            CheckpointDownloadRequest request,
            CheckpointDownloadAdapter downloader,
            TargetCheckpointRootVerifier.ImageAuthority images,
            Runnable protection) {
        public RestoreMaterial {
            Objects.requireNonNull(request);
            Objects.requireNonNull(downloader);
            Objects.requireNonNull(images);
            Objects.requireNonNull(protection);
        }
    }

    private final TargetWorkerProcessRuntime.Assembly graph;
    private final TargetWorkerCheckpointConfiguration configuration;
    private final Histories histories;
    private final TrustedDeployment deployment;
    private final Clock clock;
    private final LongSupplier nanos;
    private final OxiaSyncCheckpointPublicationBackend catalog;
    private final CheckpointPublicationCoordinator publication;
    private final List<CutLoan> cuts = new ArrayList<>();
    private final List<DeferredCut> preparations = new ArrayList<>();
    private OwnedCheckpointIo<Boolean> opening;
    private HeldInputs held;
    private HttpClient httpClient;
    private RenewableS3CompatibleCheckpointObjectStoreAdapter adapter;
    private TargetWorkerSourceStartup startup;
    private TargetWorkerShardRuntime worker;
    private LongSupplier ownerClock;
    private OwnerIdentity checkpointOwner;
    private boolean stopping;
    private Throwable failure;
    private OwnedCheckpointIo<Boolean> closing;
    private RecoveryCandidateRef restoreCandidate;
    private RecoveryPin restorePin;
    private OwnedCheckpointIo<RestoreMaterial> restoreOpening;
    private RestoreInputs restoreInputs;
    private boolean restoreCloseRequested;
    private boolean heldCloseRequested;
    private CompletableFuture<Void> restoreCleanup;
    private CompletableFuture<Void> heldCleanup;
    private OwnedCheckpointIo<Boolean> startupRestoreClosing;
    private volatile int startupOwnerRebinds;
    private com.nereusstream.delay.store.WorkerNativeResourceLedger.Reservation nativeReservation;

    static TargetWorkerCheckpointComposition create(
            TargetWorkerProcessRuntime.Assembly graph,
            TargetWorkerCheckpointConfiguration configuration,
            TrustedDeployment deployment,
            Histories histories,
            Clock clock,
            LongSupplier nanos) {
        return new TargetWorkerCheckpointComposition(graph, configuration, deployment, histories, clock, nanos);
    }

    private TargetWorkerCheckpointComposition(
            TargetWorkerProcessRuntime.Assembly graph,
            TargetWorkerCheckpointConfiguration configuration,
            TrustedDeployment deployment,
            Histories histories,
            Clock clock,
            LongSupplier nanos) {
        this.graph = Objects.requireNonNull(graph);
        this.configuration = Objects.requireNonNull(configuration);
        this.histories = Objects.requireNonNull(histories);
        this.clock = Objects.requireNonNull(clock);
        this.nanos = Objects.requireNonNull(nanos);
        this.deployment = Objects.requireNonNull(deployment);
        final var storeRoot = graph.config().store().rootPath().toAbsolutePath().normalize();
        if (configuration.checkpointDirectory().startsWith(storeRoot)
                || storeRoot.startsWith(configuration.checkpointDirectory())) {
            throw new IllegalArgumentException(
                    "checkpoint candidates overlap the actual Worker Store/ACTIVE/STAGED root");
        }
        graph.config().checkpoint().requireWorkClasses(graph.config().workClasses());
        catalog = graph.oxia()
                .providers()
                .checkpointPublication(
                        configuration.shardPrefix(), graph.config().checkpoint().physical());
        publication = new CheckpointPublicationCoordinator(
                graph.resources(), catalog, graph.config().checkpoint().physical(), catalog);
    }

    /** The Process registers this original owner before the first accepted external allocation attempt. */
    synchronized void startPreparation() {
        requireOpenLocal();
        if (opening != null) {
            throw new IllegalStateException("original checkpoint preparation was already started");
        }
        if (nativeReservation == null) {
            nativeReservation = graph.resources()
                    .reserveNativeResource(
                            "checkpoint-http/" + Bytes.hex(configuration.scope().digest()),
                            com.nereusstream.delay.store.NativeResourceUsage.zero(),
                            configuration.sdkNativeBytes());
        }
        opening = OwnedCheckpointIo.submit(
                guard -> {
                    final var original = Objects.requireNonNull(
                            deployment.acquire(configuration, guard), "original checkpoint deployment inputs");
                    synchronized (this) {
                        held = original;
                    }
                    final var client =
                            Objects.requireNonNull(original.httpClient(), "original dedicated checkpoint HTTP client");
                    synchronized (this) {
                        httpClient = client;
                    }
                    guard.run();
                    original.requireCurrent();
                    final var time = Objects.requireNonNull(original.trustedTime(), "original trusted checkpoint time");
                    final var activation = new OxiaObjectStoreCredentialLeaseActivator(
                            Objects.requireNonNull(original.credentials()),
                                    Objects.requireNonNull(original.materialResolver()),
                            configuration.credentialLeaseTtlMillis(), configuration.maximumAttestationAgeMillis());
                    final var request = new OxiaObjectStoreCredentialLeaseActivator.ActivationRequest(
                            configuration.objectStoreProfile(),
                            configuration.endpoint(),
                            configuration.region(),
                            configuration.bucket(),
                            configuration.scope().digest(),
                            time,
                            Math.addExact(time.latestEpochMs(), configuration.credentialLeaseTtlMillis()),
                            graph.config().checkpoint().physical(),
                            client,
                            clock,
                            configuration.requestTimeout());
                    final var actual = activation.activateRenewableS3Compatible(
                            request, configuration.renewBeforeMillis(), original::trustedTime);
                    synchronized (this) {
                        adapter = actual;
                    }
                    guard.run();
                    return true;
                },
                this::requireOpen,
                graph.io());
    }

    /** Prompt observation only. Actual SDK acquisition remains on the original accepted I/O operation. */
    public boolean advancePreparation() {
        if (opening == null) {
            return false;
        }
        opening.retryCleanup();
        if (!opening.operationExited()) {
            return false;
        }
        if (!opening.receipt().isDone()) {
            return false;
        }
        try {
            opening.receipt().join();
        } catch (RuntimeException | Error rejected) {
            retain(rejected);
            throw rejected;
        }
        requireOpen();
        return true;
    }

    public CheckpointPublicationCoordinator evidencePublication() {
        if (!advancePreparation()) {
            throw unavailable("checkpoint deployment preparation is pending");
        }
        return publication;
    }

    /** Binds the original Source only after its Content already retained this same atomic publication. */
    public void bindStartup(TargetWorkerSourceStartup original, LongSupplier originalOwnerClock) {
        requireOpen();
        Objects.requireNonNull(original);
        final HeldInputs originalHeld;
        final OwnerIdentity previousOwner;
        synchronized (this) {
            requireOpenLocal();
            originalHeld = Objects.requireNonNull(held, "original checkpoint inputs");
            previousOwner = checkpointOwner;
            if (startup != null && startup != original) {
                throw new IllegalArgumentException("checkpoint composition already binds another original Source");
            }
        }
        final var sourceGraph = original.graph();
        if (sourceGraph.config() != graph.config()
                || sourceGraph.classes() != graph.classes()
                || sourceGraph.resources() != graph.resources()
                || sourceGraph.io() != graph.io()
                || sourceGraph.oxia() != graph.oxia()
                || !configuration.scope().equals(original.sourceScopeForExitBinding())) {
            throw new IllegalArgumentException("checkpoint composition belongs to another original Source graph");
        }
        final var lease = original.ownerLease();
        if (previousOwner != null
                && (previousOwner.ownerEpoch() != lease.ownerEpoch()
                        || !Bytes.constantTimeEquals(previousOwner.leaseFencingDigest(), lease.leaseToken()))) {
            throw new IllegalStateException("changed Source Owner requires its original startup cleanup/rebind gate");
        }
        final var owner = originalHeld.bindOwner(lease);
        if (owner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalArgumentException("checkpoint authorities changed the actual acquired Source Owner");
        }
        if (previousOwner != null && !java.util.Arrays.equals(previousOwner.canonicalBytes(), owner.canonicalBytes())) {
            throw new IllegalStateException(
                    "initial checkpoint binding cannot replace its original deployment/Owner identity");
        }
        original.retainCheckpointComposition(this, configuration.recoveryLineageId());
        synchronized (this) {
            requireOpenLocal();
            if (held != originalHeld || checkpointOwner != previousOwner || startup != null && startup != original) {
                throw unavailable("original checkpoint Source binding changed while its alias was retained");
            }
            startup = original;
            ownerClock = Objects.requireNonNull(originalOwnerClock);
            checkpointOwner = owner;
        }
    }

    /** Final SDK startup handoff only; pending original restore cleanup retains the same admitted Source handle. */
    public boolean rebindStartupOwner(TargetWorkerSourceStartup original, LongSupplier originalOwnerClock) {
        Objects.requireNonNull(original);
        Objects.requireNonNull(originalOwnerClock);
        final HeldInputs originalHeld;
        synchronized (this) {
            requireOpenLocal();
            if (startup != original || held == null || worker != null || !cuts.isEmpty() || !preparations.isEmpty()) {
                throw new IllegalStateException(
                        "checkpoint Owner rebind is restricted to its original pre-checkpoint startup");
            }
            originalHeld = held;
            startupOwnerRebinds++;
        }
        try {
            final var process = original.sourceProcessForExitBinding();
            if (process.host().isPresent()) {
                throw new IllegalStateException(
                        "checkpoint startup Owner cannot change after original Host preparation");
            }
            final var actual = original.workerForHost(process);
            original.requireAdmittedWorker(actual);
            final var source = original.sourceExitBinding();
            final var lease = original.ownerLease();
            if (!configuration.scope().equals(source.scope)
                    || source.source == null
                    || source.lineage == null
                    || !Bytes.constantTimeEquals(source.lineage, configuration.recoveryLineageId())
                    || source.verifiedOwner == null
                    || source.verifiedOwner.ownerEpoch() != lease.ownerEpoch()
                    || !Bytes.constantTimeEquals(source.verifiedOwner.leaseFencingDigest(), lease.leaseToken())) {
                throw unavailable("final Source SDK startup lacks its actual Native Source/lineage/Owner binding");
            }
            final OwnedCheckpointIo<RestoreMaterial> originalRestore;
            final RestoreInputs previousRestore;
            synchronized (this) {
                requireOpenLocal();
                originalRestore = restoreOpening;
                previousRestore = restoreInputs;
            }
            if (originalRestore != null) {
                originalRestore.retryCleanup();
                if (!originalRestore.receipt().isDone()) {
                    return false;
                }
                originalRestore.receipt().join();
                if (!originalRestore.ioExited()) {
                    return false;
                }
            }
            if (previousRestore != null) {
                final OwnedCheckpointIo<Boolean> cleanup;
                synchronized (this) {
                    requireOpenLocal();
                    if (startupRestoreClosing == null || startupRestoreClosing.ioExited() && !restoreInputsClosed()) {
                        try {
                            startupRestoreClosing = OwnedCheckpointIo.submit(
                                    ignored -> {
                                        previousRestore.requestStop();
                                        closeOriginalRestoreInputs();
                                        return restoreInputsClosed();
                                    },
                                    () -> {},
                                    graph.io());
                        } catch (java.util.concurrent.RejectedExecutionException full) {
                            if (graph.io().isShutdown()) {
                                throw full;
                            }
                            return false;
                        }
                    }
                    cleanup = startupRestoreClosing;
                }
                cleanup.retryCleanup();
                if (!cleanup.receipt().isDone()) {
                    return false;
                }
                cleanup.receipt().join();
                if (!cleanup.ioExited() || !restoreInputsClosed()) {
                    return false;
                }
            }
            final var owner = Objects.requireNonNull(originalHeld.bindOwner(lease), "final original checkpoint Owner");
            if (!java.util.Arrays.equals(owner.canonicalBytes(), source.verifiedOwner.canonicalBytes())) {
                throw unavailable("final checkpoint Owner differs from the actual admitted Source SDK Owner");
            }
            originalHeld.requireCurrent();
            synchronized (this) {
                requireOpenLocal();
                if (startup != original || held != originalHeld || worker != null || !preparations.isEmpty()) {
                    throw unavailable("original checkpoint startup changed during final Owner handoff");
                }
                checkpointOwner = owner;
                ownerClock = originalOwnerClock;
            }
            return true;
        } finally {
            synchronized (this) {
                startupOwnerRebinds--;
            }
        }
    }

    /** Local original-retention admission, independent of today's activation barrier or transport HISTORY right. */
    public void requireHistoryReadable(SourcePosition exact) {
        Objects.requireNonNull(exact, "exact original Source history position");
        final TargetWorkerSourceStartup originalStartup;
        final HeldInputs originalHeld;
        final OwnerIdentity originalOwner;
        synchronized (this) {
            requireOpenLocal();
            originalStartup = startup;
            originalHeld = held;
            originalOwner = checkpointOwner;
            if (originalStartup == null || originalHeld == null || originalOwner == null) {
                throw unavailable("original Source history owner is not bound");
            }
        }
        originalHeld.requireCurrent();
        final var original = originalStartup.sourceExitBinding();
        if (!configuration.scope().equals(original.scope)
                || !configuration.scope().shard().equals(exact.shardId())
                || original.lineage == null
                || !Bytes.constantTimeEquals(original.lineage, configuration.recoveryLineageId())
                || original.source == null
                || !original.source.sameSourceIdentity(exact)
                || original.verifiedOwner == null
                || !java.util.Arrays.equals(original.verifiedOwner.canonicalBytes(), originalOwner.canonicalBytes())) {
            throw unavailable("history read changed its original physical Source/scope/lineage/Owner");
        }
        originalHeld.requireHistoryReadable(exact);
        synchronized (this) {
            requireOpenLocal();
            if (startup != originalStartup || held != originalHeld || checkpointOwner != originalOwner) {
                throw unavailable("original Source history binding changed while admission was checked");
            }
        }
    }

    public TargetCheckpointCandidateSchedule.DeferredCollectedSemanticRequestFactory deferredRequestFactory() {
        return this::createDeferred;
    }

    private TargetCheckpointCandidateSchedule.DeferredPreparation createDeferred(
            TargetWorkerShardRuntime selected,
            com.nereusstream.delay.store.CheckpointScheduler.ScheduledCheckpoint claim,
            SourceRecordConsumer.CheckpointCut cut) {
        requireOpen();
        final TargetWorkerSourceStartup originalStartup;
        final HeldInputs originalHeld;
        final OwnerIdentity originalOwner;
        synchronized (this) {
            requireOpenLocal();
            originalStartup = startup;
            originalHeld = held;
            originalOwner = checkpointOwner;
            if (originalStartup == null || originalHeld == null || adapter == null) {
                throw unavailable("original checkpoint Source binding is not prepared");
            }
        }
        originalStartup.requireAdmittedWorker(selected);
        final var originalBinding = originalStartup.sourceExitBinding();
        if (originalBinding.lineage == null
                || !Bytes.constantTimeEquals(originalBinding.lineage, configuration.recoveryLineageId())
                || originalBinding.verifiedOwner == null
                || !java.util.Arrays.equals(
                        originalBinding.verifiedOwner.canonicalBytes(), originalOwner.canonicalBytes())) {
            throw new IllegalStateException("checkpoint configuration changed the actual Native Source lineage/Owner");
        }
        if (!selected.shardId().equals(configuration.scope().shard())
                || !claim.shardId().equals(selected.shardId())) {
            throw unavailable("checkpoint candidate changes its original Source");
        }
        cut.requireCurrent();
        originalHeld.requireCurrent();
        final var preparation = new DeferredCut(selected, cut);
        synchronized (this) {
            requireOpenLocal();
            if (startup != originalStartup
                    || held != originalHeld
                    || checkpointOwner != originalOwner
                    || worker != null && worker != selected) {
                throw unavailable("original checkpoint Source binding changed during cut admission");
            }
            worker = selected;
            preparations.removeIf(prior -> !prior.taskDirectoryCreated
                    && prior.loan == null
                    && prior.ioExited()
                    && (prior.stopped && prior.submission == null
                            || prior.submission != null
                                    && prior.submission.outcome().isPresent()
                                    && prior.submission.outcome().orElseThrow().failure() != null));
            for (var prior : preparations) {
                if (prior.submission == null || prior.submission.outcome().isEmpty()) {
                    throw new IllegalStateException(
                            "original Source already owns an unfinished checkpoint preparation");
                }
            }
            preparations.add(preparation);
            return preparation;
        }
    }

    private TargetCheckpointCandidateSchedule.CollectedSemanticRequest createPreparedRequest(
            DeferredCut preparation, OxiaSyncCheckpointPublicationBackend.CheckpointHead head) {
        requireOpen();
        requireHead(head);
        preparation.requireCurrent();
        final TargetWorkerSourceStartup originalStartup;
        final HeldInputs originalHeld;
        final OwnerIdentity originalOwner;
        final LongSupplier originalOwnerClock;
        synchronized (this) {
            requireOpenLocal();
            originalStartup = startup;
            originalHeld = held;
            originalOwner = checkpointOwner;
            originalOwnerClock = ownerClock;
        }
        final var time = Objects.requireNonNull(originalHeld.trustedTime());
        final var storeIncarnation = originalStartup.store().metadata().storeIncarnation();
        final var parent = head.latest();
        final var checkpointId = preparation.checkpointId;
        final long generation = head.catalogGeneration() == 0 ? 1 : head.catalogGeneration();
        final var pending = new CheckpointUploadIntent(
                new ShardSubject(configuration.scope().shard()),
                configuration.recoveryLineageId(),
                checkpointId,
                originalOwner,
                storeIncarnation,
                preparation.token,
                generation,
                parent == null ? null : parent.checkpointId(),
                parent == null ? null : parent.manifestSha256(),
                configuration.objectStoreProfile(),
                time,
                Math.addExact(time.latestEpochMs(), configuration.uploadTtlMillis()),
                CheckpointUploadState.PENDING_UPLOAD,
                1,
                null,
                null);
        final var loan = new CutLoan(preparation.selected, preparation.cut, pending);
        synchronized (this) {
            requireOpenLocal();
            if (startup != originalStartup || held != originalHeld || checkpointOwner != originalOwner) {
                throw unavailable("original Source binding changed while checkpoint request was prepared");
            }
            preparation.loan = loan;
            cuts.add(loan);
        }
        final var runtime = graph.config().checkpoint();
        final var providers = runtime.newRegistryProviders(
                graph.oxia().providers().sourceProfiles(),
                graph.oxia().providers().retries(),
                graph.oxia().providers().payloadTrusts(),
                loan::requireCurrent,
                graph.io());
        final var inputs = runtime.newInputs(
                preparation.selected,
                histories.controls(),
                histories.outcomes(),
                histories.admissions(),
                providers,
                loan,
                originalOwnerClock,
                nanos,
                loan::requireCurrent);
        loan.inputs = inputs;
        final var request = new TargetCheckpointCandidateSchedule.Request(
                catalog,
                originalOwnerClock,
                preparation.taskDirectory.resolve("image"),
                pending,
                runtime.physical(),
                runtime.quota(),
                runtime.ledger());
        final var plan = new TargetCheckpointCandidateWorkClassExecutor.PublicationPlan(
                publication,
                parent == null ? 0 : Math.addExact(parent.lineageGeneration(), 1),
                file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8(
                                configuration.objectNamePrefix() + "/" + Bytes.hex(checkpointId) + "/" + file.name()),
                        file.checksum(),
                        null),
                new CutImageAuthority(loan),
                adapter,
                head.catalogGeneration() == 0
                        ? (initialization, image) -> loan.initialization().acquire(initialization, image)
                        : null);
        return new TargetCheckpointCandidateSchedule.CollectedSemanticRequest(
                request,
                inputs,
                runtime.semantics(),
                loan::requireCurrent,
                plan,
                new OxiaCheckpointIntentRegistration(
                        catalog, configuration.requestTimeout(), loan::requireCurrent, graph.io()));
    }

    public void prepareRestore(RecoveryCandidateRef candidate, RecoveryPin originalPin) {
        requireOpen();
        Objects.requireNonNull(candidate);
        Objects.requireNonNull(originalPin);
        synchronized (this) {
            requireOpenLocal();
            if (adapter == null || held == null) {
                throw unavailable("checkpoint adapter preparation is pending");
            }
            if (restoreCandidate != null) {
                if (!java.util.Arrays.equals(restoreCandidate.canonicalBytes(), candidate.canonicalBytes())
                        || !restorePin.equals(originalPin)) {
                    throw new IllegalStateException("original checkpoint restore cannot be replaced");
                }
                return;
            }
            if (!originalPin.candidate().equals(candidate)
                    || !originalPin
                            .shard()
                            .shardId()
                            .equals(configuration.scope().shard())
                    || checkpointOwner == null
                    || !java.util.Arrays.equals(
                            originalPin.owner().canonicalBytes(), checkpointOwner.canonicalBytes())) {
                throw new IllegalArgumentException(
                        "checkpoint restore changed its original Source Owner/Pin candidate");
            }
            restoreCandidate = candidate;
            restorePin = originalPin;
            restoreOpening = OwnedCheckpointIo.submit(
                    guard -> {
                        if (!catalog.activeRecoveryPin().orElseThrow().equals(originalPin)) {
                            throw new IllegalStateException("original checkpoint restore Pin is no longer active");
                        }
                        final var request = catalog.publishedDownload(candidate);
                        if (!request.manifest()
                                        .shardId()
                                        .equals(configuration.scope().shard())
                                || !Bytes.constantTimeEquals(
                                        request.manifest().recoveryLineageId(), configuration.recoveryLineageId())) {
                            throw new IllegalArgumentException(
                                    "checkpoint restore changed its configured Source/lineage");
                        }
                        final var original =
                                Objects.requireNonNull(held.acquireRestore(candidate, originalPin, request, guard));
                        synchronized (this) {
                            restoreInputs = original;
                        }
                        guard.run();
                        original.requireCurrent();
                        return new RestoreMaterial(request, adapter, Objects.requireNonNull(original.images()), () -> {
                            requireOpen();
                            original.requireCurrent();
                        });
                    },
                    this::requireOpen,
                    graph.io());
        }
    }

    public synchronized Optional<RestoreMaterial> restoreMaterial() {
        if (restoreOpening == null || !restoreOpening.operationExited()) {
            return Optional.empty();
        }
        restoreOpening.retryCleanup();
        try {
            return Optional.of(restoreOpening.receipt().join());
        } catch (RuntimeException | Error rejected) {
            retain(rejected);
            throw rejected;
        }
    }

    private void requireOpen() {
        final HeldInputs current;
        synchronized (this) {
            requireOpenLocal();
            current = held;
        }
        if (current != null) {
            current.requireCurrent();
        }
        synchronized (this) {
            requireOpenLocal();
            if (held != current) {
                throw unavailable("original checkpoint held inputs changed during admission");
            }
        }
    }

    private void requireOpenLocal() {
        if (stopping || failure != null) {
            throw unavailable("original checkpoint owner is stopped or failed");
        }
    }

    private void requireHead(OxiaSyncCheckpointPublicationBackend.CheckpointHead value) {
        final var latest = value.latest();
        if (latest != null
                && (!latest.shardId().equals(configuration.scope().shard())
                        || !Bytes.constantTimeEquals(latest.recoveryLineageId(), configuration.recoveryLineageId()))) {
            throw new IllegalArgumentException("actual checkpoint Catalog belongs to another Source/lineage");
        }
        if (value.initialization() != null
                && !Bytes.constantTimeEquals(
                        value.initialization().pending().recoveryLineageId(), configuration.recoveryLineageId())) {
            throw new IllegalArgumentException("actual checkpoint genesis belongs to another lineage");
        }
    }

    private synchronized void retain(Throwable rejected) {
        if (failure == null) {
            failure = rejected;
        } else if (failure != rejected) {
            failure.addSuppressed(rejected);
        }
    }

    @Override
    public void requestStop() {
        final OwnedCheckpointIo<Boolean> originalOpening;
        final OwnedCheckpointIo<RestoreMaterial> originalRestore;
        final HeldInputs originalHeld;
        final RestoreInputs originalRestoreInputs;
        final List<CutLoan> originalCuts;
        final List<DeferredCut> originalPreparations;
        synchronized (this) {
            stopping = true;
            originalOpening = opening;
            originalRestore = restoreOpening;
            originalHeld = held;
            originalRestoreInputs = restoreInputs;
            originalCuts = List.copyOf(cuts);
            originalPreparations = List.copyOf(preparations);
        }
        if (originalOpening != null) {
            originalOpening.requestStop();
        }
        if (originalRestore != null) {
            originalRestore.requestStop();
        }
        if (originalHeld != null) {
            originalHeld.requestStop();
        }
        if (originalRestoreInputs != null) {
            originalRestoreInputs.requestStop();
        }
        histories.io().requestStop();
        for (var cut : originalCuts) {
            cut.stopInputs();
        }
        for (var preparation : originalPreparations) {
            preparation.requestStop();
            if (preparation.submission != null) {
                preparation.submission.requestPreparationStop();
            }
        }
    }

    @Override
    public void close() {
        requestStop();
        retryCleanup();
    }

    @Override
    public void retryCleanup() {
        final OwnedCheckpointIo<Boolean> originalOpening;
        final OwnedCheckpointIo<RestoreMaterial> originalRestore;
        final List<CutLoan> originalCuts;
        final List<DeferredCut> originalPreparations;
        final OwnedCheckpointIo<Boolean> originalStartupRestoreClosing;
        synchronized (this) {
            originalOpening = opening;
            originalRestore = restoreOpening;
            originalStartupRestoreClosing = startupRestoreClosing;
            originalCuts = List.copyOf(cuts);
            originalPreparations = List.copyOf(preparations);
        }
        if (originalOpening != null) {
            originalOpening.retryCleanup();
        }
        if (originalRestore != null) {
            originalRestore.retryCleanup();
        }
        if (originalStartupRestoreClosing != null) {
            originalStartupRestoreClosing.retryCleanup();
        }
        for (var cut : originalCuts) {
            cut.retryCleanup();
        }
        for (var preparation : originalPreparations) {
            preparation.retryCleanup();
        }
        histories.io().retryCleanup();
        final OwnedCheckpointIo<Boolean> cleanup;
        synchronized (this) {
            if (!stopping || !operationsExited()) {
                return;
            }
            if (closing == null || closing.ioExited() && !closedInputs()) {
                try {
                    closing = OwnedCheckpointIo.submit(
                            guard -> {
                                if (adapter != null) {
                                    adapter.beginProviderQuiescence();
                                    adapter.requireProviderQuiescence();
                                }
                                for (var preparation : preparations) {
                                    if (!preparation.closeValidationLoans()) {
                                        return false;
                                    }
                                }
                                for (var cut : cuts) {
                                    cut.closeOriginal();
                                }
                                closeOriginalRestoreInputs();
                                if (held != null) {
                                    if (!heldCloseRequested) {
                                        heldCloseRequested = true;
                                        held.close();
                                    }
                                    if (!held.cleanupComplete() && CheckpointInputFuture.ioExited(heldCleanup)) {
                                        heldCleanup = Objects.requireNonNull(held.retryCleanup());
                                    }
                                }
                                return closedInputs();
                            },
                            () -> {},
                            graph.io());
                } catch (java.util.concurrent.RejectedExecutionException full) {
                    if (graph.io().isShutdown()) {
                        retain(full);
                    }
                }
            }
            cleanup = closing;
        }
        if (cleanup != null) {
            cleanup.retryCleanup();
            synchronized (this) {
                if (cleanup.ioExited()
                        && closedInputs()
                        && nativeReservation != null
                        && !nativeReservation.isReleased()) {
                    nativeReservation.close();
                }
            }
        }
    }

    private synchronized boolean operationsExited() {
        if (startupOwnerRebinds != 0
                || startupRestoreClosing != null && !startupRestoreClosing.ioExited()
                || opening != null && (!opening.operationExited() || !opening.ioExited())
                || restoreOpening != null && !restoreOpening.ioExited()
                || restoreInputs != null && !restoreInputs.ioExited()
                || !histories.io().ioExited()) {
            return false;
        }
        for (var cut : cuts) {
            if (!cut.collectionIoExited()) {
                return false;
            }
        }
        for (var preparation : preparations) {
            if (!preparation.ioExited()
                    || preparation.submission != null && !preparation.submission.ownedExecutionIoExited()) {
                return false;
            }
        }
        return held == null || held.ioExited();
    }

    private synchronized boolean closedInputs() {
        for (var preparation : preparations) {
            if (!preparation.validationCleanupComplete()) {
                return false;
            }
        }
        for (var cut : cuts) {
            if (!cut.cleanupComplete()) {
                return false;
            }
        }
        return restoreInputsClosed()
                && (held == null || held.cleanupComplete())
                && CheckpointInputFuture.ioExited(restoreCleanup)
                && CheckpointInputFuture.ioExited(heldCleanup)
                && (httpClient == null || httpClient.isTerminated());
    }

    private void closeOriginalRestoreInputs() {
        if (restoreInputs == null) {
            return;
        }
        if (!restoreCloseRequested) {
            restoreCloseRequested = true;
            restoreInputs.close();
        }
        if (!restoreInputs.cleanupComplete() && CheckpointInputFuture.ioExited(restoreCleanup)) {
            restoreCleanup = Objects.requireNonNull(restoreInputs.retryCleanup());
        }
    }

    private boolean restoreInputsClosed() {
        return (restoreInputs == null
                        || restoreCloseRequested && restoreInputs.cleanupComplete() && restoreInputs.ioExited())
                && CheckpointInputFuture.ioExited(restoreCleanup);
    }

    @Override
    public synchronized boolean ioExited() {
        return operationsExited()
                && (!stopping
                        || closing != null
                                && closing.ioExited()
                                && closedInputs()
                                && (nativeReservation == null || nativeReservation.isReleased()));
    }

    @Override
    public synchronized Throwable pendingIoFailure() {
        if (failure != null) {
            return failure;
        }
        if (opening != null && CheckpointInputFuture.ownedFailure(opening.receipt()) != null) {
            return CheckpointInputFuture.ownedFailure(opening.receipt());
        }
        if (closing != null && CheckpointInputFuture.ownedFailure(closing.receipt()) != null) {
            return CheckpointInputFuture.ownedFailure(closing.receipt());
        }
        if (restoreOpening != null && CheckpointInputFuture.ownedFailure(restoreOpening.receipt()) != null) {
            return CheckpointInputFuture.ownedFailure(restoreOpening.receipt());
        }
        if (startupRestoreClosing != null
                && CheckpointInputFuture.ownedFailure(startupRestoreClosing.receipt()) != null) {
            return CheckpointInputFuture.ownedFailure(startupRestoreClosing.receipt());
        }
        if (restoreInputs != null && restoreInputs.pendingIoFailure() != null) {
            return restoreInputs.pendingIoFailure();
        }
        if (histories.io().pendingIoFailure() != null) {
            return histories.io().pendingIoFailure();
        }
        if (held != null && held.pendingIoFailure() != null) {
            return held.pendingIoFailure();
        }
        for (var cut : cuts) {
            if (cut.pendingIoFailure() != null) {
                return cut.pendingIoFailure();
            }
        }
        for (var preparation : preparations) {
            if (preparation.pendingIoFailure() != null) {
                return preparation.pendingIoFailure();
            }
        }
        return null;
    }

    private static TargetSourceAuthorityPreparation.InputUnavailable unavailable(String message) {
        return new TargetSourceAuthorityPreparation.InputUnavailable(message);
    }

    private static byte[] uuid(java.util.UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    /** Only an older actual publication may release its task-owned local image after replacement succeeds. */
    private synchronized void retireReplacedCandidates(DeferredCut current) {
        DeferredCut latest = null;
        for (var preparation : preparations) {
            if (preparation.submission != null
                    && preparation.submission.publication().isPresent()) {
                latest = preparation;
            }
        }
        final var retained = latest;
        for (var prior : List.copyOf(preparations)) {
            if (prior == current
                    || prior == retained
                    || prior.submission == null
                    || prior.submission.publication().isEmpty()) {
                continue;
            }
            if (prior.retirement != null) {
                prior.retirement.retryCleanup();
                if (prior.retirement.ioExited()
                        && prior.retirement.completedValue().orElse(false)) {
                    preparations.remove(prior);
                    cuts.remove(prior.loan);
                    continue;
                }
                if (!prior.retirement.ioExited()) {
                    continue;
                }
            }
            if (!prior.submission.ownedIoExited() || !prior.ioExited() || prior.loan == null) {
                continue;
            }
            try {
                prior.retirement = OwnedCheckpointIo.submit(
                        guard -> {
                            prior.validationRetiring = true;
                            if (!prior.closeValidationLoans()) {
                                return false;
                            }
                            prior.loan.stopInputs();
                            if (!prior.loan.collectionIoExited()) {
                                return false;
                            }
                            prior.loan.closeOriginal();
                            if (!prior.loan.cleanupComplete()) {
                                return false;
                            }
                            prior.deletePublishedTaskDirectory();
                            return true;
                        },
                        () -> {},
                        graph.io());
            } catch (java.util.concurrent.RejectedExecutionException full) {
                if (graph.io().isShutdown()) {
                    throw full;
                }
            }
        }
    }

    private synchronized int retainedCandidateDirectories() {
        int retained = 0;
        for (var preparation : preparations) {
            if (preparation.taskDirectoryCreated) {
                retained++;
            }
        }
        return retained;
    }

    private final class DeferredCut implements TargetCheckpointCandidateSchedule.DeferredPreparation {
        private final TargetWorkerShardRuntime selected;
        private final SourceRecordConsumer.CheckpointCut cut;
        private final byte[] checkpointId = uuid(java.util.UUID.randomUUID());
        private final byte[] token = new byte[32];
        private volatile TargetCheckpointCandidateWorkClassExecutor.Submission submission;
        private volatile OwnedCheckpointIo<OxiaSyncCheckpointPublicationBackend.CheckpointHead> metadata;
        private volatile TargetCheckpointCandidateSchedule.CollectedSemanticRequest request;
        private volatile CutLoan loan;
        private volatile boolean stopped;
        private final Path taskDirectory;
        private volatile boolean taskDirectoryCreated;
        private volatile Object taskDirectoryKey;
        private volatile OwnedCheckpointIo<Boolean> retirement;
        private boolean cleanupInventoryAuthenticated;
        private long firstTurnNanos = Long.MIN_VALUE;
        private volatile List<PublishedValidationLoan> validationLoans = List.of();
        private volatile int validationAcquisitions;
        private volatile long latestValidationGeneration;
        private volatile boolean validationRetiring;

        DeferredCut(TargetWorkerShardRuntime selected, SourceRecordConsumer.CheckpointCut cut) {
            this.selected = selected;
            this.cut = cut;
            taskDirectory = configuration.checkpointDirectory().resolve(Bytes.hex(checkpointId));
            new java.security.SecureRandom().nextBytes(token);
        }

        @Override
        public String localTaskIdentity() {
            return "configured-checkpoint/" + Bytes.hex(configuration.scope().digest()) + "/" + Bytes.hex(checkpointId);
        }

        @Override
        public long maximumTaskBytes() {
            return graph.config().checkpoint().settlementTurn().maxBytes();
        }

        @Override
        public java.util.concurrent.ThreadPoolExecutor checkpointExecutor() {
            return graph.io();
        }

        @Override
        public void accepted(TargetCheckpointCandidateWorkClassExecutor.Submission original) {
            if (submission != null && submission != original) {
                throw new IllegalStateException("original deferred checkpoint submission cannot be replaced");
            }
            submission = Objects.requireNonNull(original);
        }

        @Override
        public TargetCheckpointCandidateSchedule.PublishedValidationHold acquirePublishedValidation(
                TargetCheckpointCandidateSchedule.PublishedValidationRequest validation, Runnable current) {
            requireCurrent();
            Objects.requireNonNull(current);
            final var previousLoans = validationLoans;
            final var retainedPrevious = previousLoans.stream()
                    .filter(value -> !value.cleanupComplete())
                    .toList();
            final CheckpointPublicationCoordinator.CheckpointPublication publication;
            final long nativeSequence;
            synchronized (TargetWorkerCheckpointComposition.this) {
                requireOpenLocal();
                if (validationLoans == previousLoans) {
                    validationLoans = retainedPrevious;
                }
                if (validationRetiring
                        || validation.original() != submission
                        || loan == null
                        || validation.validationGeneration() == 0
                        || latestValidationGeneration != 0
                                && Long.compareUnsigned(validation.validationGeneration(), latestValidationGeneration)
                                        <= 0) {
                    throw unavailable("checkpoint reuse changed its original publication, generation or retained cut");
                }
                publication = submission.publication().orElseThrow();
                nativeSequence = submission.publicationStoreSequence();
                final var manifest = publication.catalogPublication().manifest();
                if (!Bytes.constantTimeEquals(publication.uploadIntent().checkpointId(), checkpointId)
                        || !Bytes.constantTimeEquals(manifest.recoveryLineageId(), configuration.recoveryLineageId())
                        || !manifest.shardId().equals(configuration.scope().shard())
                        || !Bytes.constantTimeEquals(
                                loan.pending.sourceStoreIncarnation(), uuid(manifest.sourceStoreIncarnation()))
                        || !java.util.Arrays.equals(
                                loan.pending.owner().canonicalBytes(), checkpointOwner.canonicalBytes())) {
                    throw unavailable("checkpoint reuse changed its actual Manifest/Store/Source Owner binding");
                }
                latestValidationGeneration = validation.validationGeneration();
                validationAcquisitions++;
            }
            try {
                current.run();
                final var original = Objects.requireNonNull(
                        loan.value().acquirePublishedValidation(validation, current),
                        "original live checkpoint publication validation loan");
                final var retained =
                        new PublishedValidationLoan(this, validation, publication, nativeSequence, original);
                synchronized (TargetWorkerCheckpointComposition.this) {
                    final var loans = new ArrayList<>(validationLoans);
                    loans.add(retained);
                    validationLoans = List.copyOf(loans);
                }
                if (original.request() != validation) {
                    throw unavailable("original checkpoint validation loan changed its exact request nonce");
                }
                current.run();
                retained.requireLocalCurrent();
                return retained;
            } finally {
                synchronized (TargetWorkerCheckpointComposition.this) {
                    validationAcquisitions--;
                }
            }
        }

        void requireCurrent() {
            requireOpen();
            if (stopped) {
                throw unavailable("original deferred checkpoint preparation is stopped");
            }
            cut.requireCurrent();
        }

        @Override
        public Optional<TargetCheckpointCandidateSchedule.CollectedSemanticRequest> preparedRequest() {
            requireCurrent();
            if (request != null) {
                return Optional.of(request);
            }
            final long now = nanos.getAsLong();
            if (firstTurnNanos == Long.MIN_VALUE) {
                firstTurnNanos = now;
            }
            if (now < firstTurnNanos
                    || now - firstTurnNanos >= configuration.requestTimeout().toNanos()) {
                requestStop();
                throw unavailable("original checkpoint metadata preparation exceeded its cumulative time bound");
            }
            synchronized (TargetWorkerCheckpointComposition.this) {
                retireReplacedCandidates(this);
                if (metadata == null && retainedCandidateDirectories() >= configuration.maximumCandidates()) {
                    return Optional.empty();
                }
            }
            if (metadata == null) {
                metadata = OwnedCheckpointIo.submit(
                        guard -> {
                            final var value = catalog.checkpointHead();
                            requireHead(value);
                            guard.run();
                            createTaskDirectory();
                            guard.run();
                            return value;
                        },
                        this::requireCurrent,
                        graph.io());
                return Optional.empty();
            }
            if (!metadata.operationExited() || !metadata.receipt().isDone()) {
                return Optional.empty();
            }
            final var actual = metadata.receipt().join();
            if (!metadata.ioExited()) {
                return Optional.empty();
            }
            requireCurrent();
            final var prepared = createPreparedRequest(this, actual);
            request = prepared;
            return Optional.of(prepared);
        }

        @Override
        public boolean ioExited() {
            if (validationAcquisitions != 0) {
                return false;
            }
            for (var validation : validationLoans) {
                if (!validation.ioExited()) {
                    return false;
                }
            }
            return (metadata == null || metadata.ioExited())
                    && (loan == null || loan.collectionIoExited())
                    && (retirement == null || retirement.ioExited());
        }

        @Override
        public Throwable pendingIoFailure() {
            if (metadata != null && CheckpointInputFuture.ownedFailure(metadata.receipt()) != null) {
                return CheckpointInputFuture.ownedFailure(metadata.receipt());
            }
            if (retirement != null && CheckpointInputFuture.ownedFailure(retirement.receipt()) != null) {
                return CheckpointInputFuture.ownedFailure(retirement.receipt());
            }
            for (var validation : validationLoans) {
                if (validation.pendingIoFailure() != null) {
                    return validation.pendingIoFailure();
                }
            }
            return loan == null ? null : loan.pendingIoFailure();
        }

        @Override
        public void requestStop() {
            stopped = true;
            if (metadata != null) {
                metadata.requestStop();
            }
            if (loan != null) {
                loan.stopInputs();
            }
            for (var validation : validationSnapshot()) {
                validation.requestStop();
            }
        }

        @Override
        public void retryCleanup() {
            if (metadata != null) {
                metadata.retryCleanup();
            }
            if (loan != null) {
                loan.retryCleanup();
            }
            if (retirement != null) {
                retirement.retryCleanup();
            }
            for (var validation : validationSnapshot()) {
                validation.retryCleanup();
            }
        }

        private List<PublishedValidationLoan> validationSnapshot() {
            return validationLoans;
        }

        boolean closeValidationLoans() {
            validationRetiring = true;
            for (var validation : validationSnapshot()) {
                validation.close();
                validation.retryCleanup();
            }
            return validationCleanupComplete();
        }

        boolean validationCleanupComplete() {
            if (validationAcquisitions != 0) {
                return false;
            }
            for (var validation : validationSnapshot()) {
                if (!validation.cleanupComplete()) {
                    return false;
                }
            }
            return true;
        }

        private void createTaskDirectory() {
            try {
                Path current = configuration.checkpointDirectory().getRoot();
                for (var element : configuration.checkpointDirectory()) {
                    current = current.resolve(element);
                    if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                        Files.createDirectory(current);
                    }
                    if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(current)) {
                        throw new IOException("checkpoint candidate parent is not an ordinary owned directory");
                    }
                }
                Files.createDirectory(taskDirectory);
                taskDirectoryCreated = true;
                taskDirectoryKey = Files.readAttributes(
                                taskDirectory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
                        .fileKey();
                if (taskDirectoryKey == null) {
                    throw new IOException("checkpoint task directory has no stable local identity");
                }
            } catch (IOException rejected) {
                throw new IllegalStateException(
                        "cannot allocate the original checkpoint candidate directory", rejected);
            }
        }

        private void deletePublishedTaskDirectory() {
            final var published = submission.publication().orElseThrow();
            final var outcome = submission.outcome().orElseThrow();
            final var image = taskDirectory.resolve("image");
            if (!taskDirectoryCreated
                    || taskDirectoryKey == null
                    || !taskDirectory.getParent().equals(configuration.checkpointDirectory())
                    || !taskDirectory.getFileName().toString().equals(Bytes.hex(checkpointId))
                    || !image.equals(outcome.checkpointPath())
                    || outcome.failure() != null
                    || !Bytes.constantTimeEquals(published.uploadIntent().checkpointId(), checkpointId)
                    || !submission.ownedIoExited()
                    || !loan.collectionIoExited()
                    || !loan.cleanupComplete()) {
                throw new IllegalStateException(
                        "checkpoint candidate lacks its original published local ownership/reader exit");
            }
            try {
                final var attributes =
                        Files.readAttributes(taskDirectory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!attributes.isDirectory() || !taskDirectoryKey.equals(attributes.fileKey())) {
                    throw new IOException("checkpoint candidate directory changed its original identity");
                }
                try (var entries = Files.list(taskDirectory)) {
                    final var children = entries.toList();
                    if (!children.equals(List.of(image)) && !(cleanupInventoryAuthenticated && children.isEmpty())) {
                        throw new IOException("checkpoint task directory contains an unowned path");
                    }
                }
                final var expected = published.catalogPublication().manifest().files();
                final var byName = expected.stream()
                        .collect(java.util.stream.Collectors.toMap(
                                CheckpointManifest.FileEntry::name, java.util.function.Function.identity()));
                final var directories = new java.util.HashSet<Path>();
                directories.add(taskDirectory);
                directories.add(image);
                for (var file : expected) {
                    Path directory = image.resolve(file.name()).getParent();
                    while (directory != null && directory.startsWith(image)) {
                        directories.add(directory);
                        directory = directory.getParent();
                    }
                }
                final var actual = Files.exists(image, LinkOption.NOFOLLOW_LINKS)
                        ? com.nereusstream.delay.store.CheckpointFileInventory.collect(
                                image, graph.config().checkpoint().physical())
                        : List.<com.nereusstream.delay.store.CheckpointFileInventory>of();
                if (!cleanupInventoryAuthenticated && actual.size() != expected.size()) {
                    throw new IOException("checkpoint local inventory differs from publication");
                }
                for (var local : actual) {
                    final var remote = byName.get(local.name());
                    if (remote == null
                            || local.length() != remote.length()
                            || !Bytes.constantTimeEquals(local.checksum(), remote.checksum())) {
                        throw new IOException("checkpoint local file differs from its original actual publication");
                    }
                }
                cleanupInventoryAuthenticated = true;
                // Same no-follow file-tree deletion used by native checkpoint staging; the exact root is checked above.
                Files.walkFileTree(taskDirectory, new java.nio.file.SimpleFileVisitor<>() {
                    @Override
                    public java.nio.file.FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs)
                            throws IOException {
                        if (!directories.contains(directory) || !attrs.isDirectory() || attrs.isSymbolicLink()) {
                            throw new IOException("checkpoint cleanup encountered an unowned directory");
                        }
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }

                    @Override
                    public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        final var original = file.startsWith(image)
                                ? byName.get(image.relativize(file)
                                        .toString()
                                        .replace(file.getFileSystem().getSeparator(), "/"))
                                : null;
                        if (original == null
                                || !attrs.isRegularFile()
                                || attrs.isSymbolicLink()
                                || attrs.size() != original.length()) {
                            throw new IOException("checkpoint cleanup encountered an unowned non-regular file");
                        }
                        Files.delete(file);
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }

                    @Override
                    public java.nio.file.FileVisitResult postVisitDirectory(Path directory, IOException rejected)
                            throws IOException {
                        if (rejected != null) {
                            throw rejected;
                        }
                        Files.delete(directory);
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }
                });
                taskDirectoryCreated = false;
            } catch (IOException rejected) {
                throw new IllegalStateException("original published checkpoint local cleanup is incomplete", rejected);
            }
        }
    }

    /** A real independent purpose loan; local invalidation schedules its original release on the same bounded pool. */
    private final class PublishedValidationLoan implements TargetCheckpointCandidateSchedule.PublishedValidationHold {
        private final DeferredCut preparation;
        private final TargetCheckpointCandidateSchedule.PublishedValidationRequest request;
        private final CheckpointPublicationCoordinator.CheckpointPublication publication;
        private final long nativeSequence;
        private final TargetCheckpointCandidateSchedule.PublishedValidationHold original;
        private volatile boolean stopping;
        private volatile boolean closeRequested;
        private volatile OwnedCheckpointIo<Boolean> closing;
        private volatile Throwable failure;

        PublishedValidationLoan(
                DeferredCut preparation,
                TargetCheckpointCandidateSchedule.PublishedValidationRequest request,
                CheckpointPublicationCoordinator.CheckpointPublication publication,
                long nativeSequence,
                TargetCheckpointCandidateSchedule.PublishedValidationHold original) {
            this.preparation = preparation;
            this.request = request;
            this.publication = publication;
            this.nativeSequence = nativeSequence;
            this.original = original;
        }

        @Override
        public TargetCheckpointCandidateSchedule.PublishedValidationRequest request() {
            return request;
        }

        @Override
        public void requireLocalCurrent() {
            requireOpen();
            if (stopping
                    || preparation.stopped
                    || preparation.validationRetiring
                    || preparation.submission != request.original()
                    || preparation.latestValidationGeneration != request.validationGeneration()
                    || request.original().publication().orElse(null) != publication
                    || request.original().publicationStoreSequence() != nativeSequence
                    || original.request() != request) {
                throw unavailable("original held publication validation changed its nonce, epoch or exact image cut");
            }
            original.requireLocalCurrent();
            synchronized (TargetWorkerCheckpointComposition.this) {
                requireOpenLocal();
                if (stopping
                        || preparation.validationRetiring
                        || preparation.latestValidationGeneration != request.validationGeneration()) {
                    throw unavailable("original publication validation loan is closing");
                }
            }
        }

        @Override
        public void requestStop() {
            stopping = true;
        }

        @Override
        public void close() {
            requestStop();
            retryCleanup();
        }

        @Override
        public void retryCleanup() {
            final var prior = closing;
            if (prior != null) {
                prior.retryCleanup();
                final var rejected = CheckpointInputFuture.ownedFailure(prior.receipt());
                if (rejected != null && failure == null) {
                    failure = rejected;
                }
            }
            if (!stopping || prior != null && !prior.ioExited() || cleanupComplete()) {
                return;
            }
            synchronized (this) {
                if (closing != prior) {
                    return;
                }
                try {
                    closing = OwnedCheckpointIo.submit(
                            ignored -> {
                                original.requestStop();
                                if (!closeRequested) {
                                    closeRequested = true;
                                    original.close();
                                }
                                original.retryCleanup();
                                return original.cleanupComplete() && original.ioExited();
                            },
                            () -> {},
                            graph.io());
                } catch (java.util.concurrent.RejectedExecutionException rejected) {
                    if (graph.io().isShutdown() && failure == null) {
                        failure = rejected;
                    }
                }
            }
        }

        @Override
        public boolean ioExited() {
            final var pending = closing;
            return original.ioExited() && (pending == null || pending.ioExited());
        }

        @Override
        public boolean cleanupComplete() {
            return stopping && closeRequested && original.cleanupComplete() && ioExited();
        }

        @Override
        public Throwable pendingIoFailure() {
            return failure == null ? original.pendingIoFailure() : failure;
        }
    }

    private final class CutLoan implements TargetCheckpointSemanticInputs.OriginalAuthorityFactory {
        private final TargetWorkerShardRuntime selected;
        private final SourceRecordConsumer.CheckpointCut cut;
        private final CheckpointUploadIntent pending;
        private volatile OwnedCheckpointIo<CutInputs> acquisition;
        private volatile CutInputs original;
        private volatile TargetCheckpointSemanticInputs inputs;
        private volatile TargetCheckpointSemanticInputs.OriginalAuthorities authenticatedAuthorities;
        private volatile boolean stopped;
        private boolean closeRequested;
        private volatile CompletableFuture<Void> cleanup;

        CutLoan(
                TargetWorkerShardRuntime selected,
                SourceRecordConsumer.CheckpointCut cut,
                CheckpointUploadIntent pending) {
            this.selected = selected;
            this.cut = cut;
            this.pending = pending;
        }

        void requireCurrent() {
            requireOpen();
            if (stopped) {
                throw unavailable("original checkpoint cut loan is stopped");
            }
            cut.requireCurrent();
            if (original != null) {
                original.requireCurrent();
            }
        }

        @Override
        public Optional<TargetCheckpointSemanticInputs.OriginalAuthorities> prepare(
                TargetCheckpointControlInputs.Result controls,
                TargetCheckpointOutcomeInputs.Result outcomes,
                List<TargetCheckpointDependencies.AdmissionImage> admissions,
                TargetCheckpointDependencies dependencies) {
            requireCurrent();
            synchronized (this) {
                if (acquisition == null) {
                    final var request =
                            new CutRequest(selected, cut, pending, controls, outcomes, admissions, dependencies);
                    acquisition = OwnedCheckpointIo.submit(
                            guard -> {
                                final var value = Objects.requireNonNull(held.acquireCut(request, guard));
                                original = value;
                                guard.run();
                                value.requireCurrent();
                                return value;
                            },
                            this::requireCurrent,
                            graph.io());
                    return Optional.empty();
                }
            }
            final var attempt = acquisition;
            if (!attempt.operationExited() || !attempt.receipt().isDone()) {
                return Optional.empty();
            }
            final var actual = attempt.receipt().join();
            final var prepared =
                    Objects.requireNonNull(actual.authorities()).prepare(controls, outcomes, admissions, dependencies);
            prepared.ifPresent(value -> authenticatedAuthorities = value);
            return prepared;
        }

        @Override
        public TargetCheckpointSemanticInputs.OriginalAuthorities create(
                TargetCheckpointControlInputs.Result controls,
                TargetCheckpointOutcomeInputs.Result outcomes,
                List<TargetCheckpointDependencies.AdmissionImage> admissions,
                TargetCheckpointDependencies dependencies) {
            return prepare(controls, outcomes, admissions, dependencies)
                    .orElseThrow(() -> unavailable("original cut authority acquisition is pending"));
        }

        CutInputs value() {
            requireCurrent();
            if (original == null || acquisition == null || !acquisition.operationExited()) {
                throw unavailable("original checkpoint image authority is pending");
            }
            return original;
        }

        void authenticateMigrationImage(
                TargetCheckpointRootVerifier.LedgerProof proof, TargetCheckpointSemanticSnapshot snapshot) {
            if (proof.dependencies().migrationRoot() == null) {
                return;
            }
            requireCurrent();
            final var actual =
                    Objects.requireNonNull(authenticatedAuthorities, "original authenticated Control/material owner");
            proof.dependencies()
                    .requireControlImages(
                            snapshot.controls(),
                            graph.config().checkpoint().semantics().maximumBytes(),
                            graph.config().checkpoint().newReadBudget(nanos),
                            actual.controls(),
                            this::requireCurrent);
            actual.controls().migrationPayloadMaterials(proof.dependencies()).requireComplete(proof.dependencies());
            requireCurrent();
        }

        CatalogInitializationAuthority initialization() {
            final var authority = value().initialization();
            if (authority == null) {
                throw unavailable("original checkpoint Catalog initialization authority is unavailable");
            }
            return authority;
        }

        @Override
        public boolean ioExited() {
            return (acquisition == null || acquisition.ioExited())
                    && (original == null
                            || original.ioExited() && original.authorities().ioExited());
        }

        @Override
        public Throwable pendingIoFailure() {
            if (acquisition != null && CheckpointInputFuture.ownedFailure(acquisition.receipt()) != null) {
                return CheckpointInputFuture.ownedFailure(acquisition.receipt());
            }
            if (original != null && original.pendingIoFailure() != null) {
                return original.pendingIoFailure();
            }
            return original == null ? null : original.authorities().pendingIoFailure();
        }

        @Override
        public void requestStop() {
            stopped = true;
            if (acquisition != null) {
                acquisition.requestStop();
            }
            if (original != null) {
                original.requestStop();
                original.authorities().requestStop();
            }
        }

        void stopInputs() {
            final var current = inputs;
            if (current == null) {
                requestStop();
            } else {
                current.requestStop();
            }
        }

        @Override
        public void retryCleanup() {
            if (acquisition != null) {
                acquisition.retryCleanup();
            }
            if (original != null) {
                original.authorities().retryCleanup();
            }
            if (cleanup != null) {
                CheckpointInputFuture.retryCleanup(cleanup);
            }
        }

        void closeOriginal() {
            if (original != null) {
                if (!closeRequested) {
                    closeRequested = true;
                    original.close();
                }
                if (!original.cleanupComplete() && CheckpointInputFuture.ioExited(cleanup)) {
                    cleanup = Objects.requireNonNull(original.retryCleanup());
                }
            }
        }

        boolean collectionIoExited() {
            return inputs == null ? ioExited() : inputs.ioExited();
        }

        boolean cleanupComplete() {
            return (original == null || original.cleanupComplete()) && CheckpointInputFuture.ioExited(cleanup);
        }
    }

    /** Full typed delegation preserves every lifecycle and terminal-policy gate on the exact acquired cut loan. */
    private static final class CutImageAuthority implements TargetCheckpointRootVerifier.ImageAuthority {
        private final CutLoan loan;

        CutImageAuthority(CutLoan loan) {
            this.loan = loan;
        }

        private TargetCheckpointRootVerifier.ImageAuthority original() {
            final var original = loan.value().images();
            if (original == null) {
                throw unavailable("original complete checkpoint image authority is unavailable");
            }
            return original;
        }

        @Override
        public void requireAuthenticated(
                CheckpointManifest m, TargetCheckpointRootVerifier.LedgerProof p, TargetCheckpointSemanticSnapshot s) {
            original().requireAuthenticated(m, p, s);
            loan.authenticateMigrationImage(p, s);
        }

        @Override
        public void requirePayloadSummaryInitializationAuthenticated(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest request,
                com.nereusstream.delay.protocol.TargetQuotaMutation at) {
            original().requirePayloadSummaryInitializationAuthenticated(m, p, s, request, at);
        }

        @Override
        public void requireLifecycleAuthenticated(
                CheckpointManifest m, TargetCheckpointRootVerifier.LedgerProof p, TargetCheckpointSemanticSnapshot s) {
            original().requireLifecycleAuthenticated(m, p, s);
        }

        @Override
        public void requireTerminalPoliciesAuthenticated(
                CheckpointManifest m, TargetCheckpointRootVerifier.LedgerProof p, TargetCheckpointSemanticSnapshot s) {
            original().requireTerminalPoliciesAuthenticated(m, p, s);
        }

        @Override
        public com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts inlinePayloadSourceFacts(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                long maximum) {
            return original().inlinePayloadSourceFacts(m, p, s, b, maximum);
        }

        @Override
        public void requireInlinePayloadIndependent(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                com.nereusstream.delay.protocol.RecoveryFloorRef f,
                com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts c) {
            original().requireInlinePayloadIndependent(m, p, s, b, f, c);
        }

        @Override
        public com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts objectPayloadSourceFacts(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                long maximum) {
            return original().objectPayloadSourceFacts(m, p, s, b, maximum);
        }

        @Override
        public void requireObjectPayloadIndependent(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                com.nereusstream.delay.protocol.RecoveryFloorRef f) {
            original().requireObjectPayloadIndependent(m, p, s, b, f);
        }

        @Override
        public void requireObjectPayloadAcceptanceAuthenticated(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b) {
            original().requireObjectPayloadAcceptanceAuthenticated(m, p, s, b);
        }

        @Override
        public com.nereusstream.delay.store.TargetMigrationPayloadMaterials migrationPayloadMaterials(
                CheckpointManifest m, TargetCheckpointRootVerifier.LedgerProof p, TargetCheckpointSemanticSnapshot s) {
            loan.requireCurrent();
            final var authorities = Objects.requireNonNull(
                    loan.authenticatedAuthorities, "original complete checkpoint Control authorities");
            return authorities.controls().migrationPayloadMaterials(p.dependencies());
        }

        @Override
        public com.nereusstream.delay.protocol.TargetUncommittedPayloadFacts uncommittedPayloadFacts(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                long maximum) {
            return original().uncommittedPayloadFacts(m, p, s, b, maximum);
        }

        @Override
        public void requireUncommittedObjectFactsAuthenticated(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b) {
            original().requireUncommittedObjectFactsAuthenticated(m, p, s, b);
        }

        @Override
        public void requireUncommittedObjectIndependent(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                com.nereusstream.delay.protocol.RecoveryFloorRef f) {
            original().requireUncommittedObjectIndependent(m, p, s, b, f);
        }

        @Override
        public com.nereusstream.delay.protocol.TargetNativeReclaimFacts nativeRetainedFacts(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                long maximum) {
            return original().nativeRetainedFacts(m, p, s, b, maximum);
        }

        @Override
        public void requireNativeRetainedFactsAuthenticated(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b) {
            original().requireNativeRetainedFactsAuthenticated(m, p, s, b);
        }

        @Override
        public void requireIncarnationIndependent(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                com.nereusstream.delay.protocol.RecoveryFloorRef f) {
            original().requireIncarnationIndependent(m, p, s, b, f);
        }

        @Override
        public void requireNativeRetainedIndependent(
                CheckpointManifest m,
                TargetCheckpointRootVerifier.LedgerProof p,
                TargetCheckpointSemanticSnapshot s,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody b,
                com.nereusstream.delay.protocol.RecoveryFloorRef f) {
            original().requireNativeRetainedIndependent(m, p, s, b, f);
        }
    }
}
