package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.CredentialProfileAuthority;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.store.*;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The original maintenance CHECKPOINT/owned-I/O turn performs one finite post-A step per advance. */
public final class TargetMigrationCheckpointAdoption implements TargetMigrationAdoption {
    /** Original acquired authorities and raw materials; no callback can return a completed adoption. */
    public interface Materials extends AutoCloseable, CheckpointInputFuture.Cleanup {
        CredentialProfileAuthority credentials();
        OxiaObjectStoreCredentialLeaseActivator.CredentialMaterialResolver materialResolver();
        HttpClient httpClient();
        TrustedUtcIntervalEvidence trustedTime();
        Optional<TargetCheckpointSemanticSnapshot> semantic(TargetMigrationStore.Activated applied,
                CheckpointUploadIntent pending);
        TargetCheckpointRootVerifier.ImageAuthority images();
        CatalogInitializationAuthority initialization();
        void requireCurrent();
        boolean ioExited();
        void requestStop();
        Throwable pendingIoFailure();
        @Override void close();
    }

    private enum Phase { PREPARE, SEMANTIC, IMAGE, PUBLISH, CATALOG, ASSIGN, COMPLETE }
    private final TargetWorkerOxiaProviders providers;
    private final TargetWorkerCheckpointConfiguration configuration;
    private final Materials materials;
    private final Clock clock;
    private TargetMigrationProcessDriver.Opening opening;
    private TargetMigrationStore.Activated applied;
    private OxiaSyncCheckpointPublicationBackend catalog;
    private CheckpointPublicationCoordinator coordinator;
    private RenewableS3CompatibleCheckpointObjectStoreAdapter adapter;
    private HttpClient httpClient;
    private WorkerNativeResourceLedger.Reservation nativeReservation;
    private CheckpointUploadIntent pending;
    private WorkerAssignmentAuthority.Publication predecessor, publication;
    private TargetMigrationAssignmentReceipt receipt;
    private TargetCheckpointSemanticSnapshot snapshot;
    private TargetCheckpointRootVerifier.AuthenticatedImage image;
    private CheckpointManifest alreadyPublished;
    private Path imagePath;
    private Phase phase = Phase.PREPARE;
    private boolean stopping, closed, providerQuiescent;
    private int calls;
    private Throwable failure;
    private CompletableFuture<Void> materialCleanup;
    private Adopted adopted;
    private TargetMigrationCheckpointArtifacts artifacts;

    TargetMigrationCheckpointAdoption(TargetWorkerOxiaProviders providers,
            TargetWorkerCheckpointConfiguration configuration, Materials materials, Clock clock) {
        this.providers = Objects.requireNonNull(providers); this.configuration = Objects.requireNonNull(configuration);
        this.materials = Objects.requireNonNull(materials); this.clock = Objects.requireNonNull(clock);
    }

    @Override public synchronized void begin(TargetMigrationStore.Activated exact, TargetMigrationProcessDriver.Opening original) {
        if (applied != null) {
            if (applied != exact || opening != original) throw new IllegalStateException("migration adoption changed its original A/opening");
            return;
        }
        if (opening != null && opening != original) throw new IllegalStateException("post-A adoption changed its original artifact owner");
        original.requireCurrent(); exact.requireCurrent();
        if (original.graph().oxia().providers() != providers
                || exact.candidate().store().sharedResources() != original.graph().resources()
                || !configuration.scope().equals(exact.migration().plan().scope())
                || !Arrays.equals(configuration.recoveryLineageId(), exact.migration().plan().newLineage())
                || !original.assignment().shardId().equals(configuration.scope().shard())
                || configuration.retainedHeapBytes() > original.configuration().maximumRetainedBytes())
            throw new IllegalArgumentException("post-A adoption changed the original session, Source or finite resource graph");
        final var storeRoot = original.graph().config().store().rootPath().toAbsolutePath().normalize();
        if (configuration.checkpointDirectory().startsWith(storeRoot) || storeRoot.startsWith(configuration.checkpointDirectory())
                || configuration.checkpointDirectory().startsWith(original.configuration().candidateDirectory())
                || original.configuration().candidateDirectory().startsWith(configuration.checkpointDirectory()))
            throw new IllegalArgumentException("formal post-A checkpoint must be separate from live Store and sealed C candidate");
        catalog = providers.checkpointPublication(configuration.shardPrefix(), original.graph().config().checkpoint().physical());
        coordinator = new CheckpointPublicationCoordinator(original.graph().resources(), catalog,
                original.graph().config().checkpoint().physical(), catalog);
        opening = original; applied = exact;
    }

    /** Invoke on the already retained original maintenance owned-I/O turn, before sealing the C bundle. */
    public TargetMigrationCheckpointArtifacts artifacts(TargetMigrationProcessDriver.Opening original,
            CheckpointManifest legacy, LegacyMigrationBundle.Limits limits,
            TargetMigrationCheckpointArtifacts.Protection protection) {
        synchronized (this) {
            if (stopping || opening != null && opening != original)
                throw new IllegalStateException("migration artifacts changed their original maintenance owner");
            if (artifacts != null) { artifacts.requireBinding(legacy, limits, protection); return artifacts; }
            if (original.graph().oxia().providers() != providers
                    || !configuration.scope().equals(original.configuration().scope())
                    || configuration.retainedHeapBytes() > original.configuration().maximumRetainedBytes())
                throw new IllegalArgumentException("artifact materials changed the original session/resource admission");
            opening = original; calls++;
        }
        try {
            // Retain before credential/HTTP/native allocation; a pre-A failure has no driver adoption field yet.
            original.retainCleanup(this);
            requireCurrent(); prepareAdapter();
            final var actual = new TargetMigrationCheckpointArtifacts(original, adapter, legacy, limits, protection,
                    this::requireCurrent, this::enterArtifact, this::exitArtifact);
            synchronized (this) { artifacts = actual; }
            requireCurrent(); return actual;
        } catch (RuntimeException | Error rejected) {
            synchronized (this) { if (failure == null) failure = rejected; }
            throw rejected;
        } finally { synchronized (this) { calls--; } }
    }

    private synchronized void enterArtifact() {
        if (stopping) throw new IllegalStateException("original migration artifact I/O stopped");
        calls++;
    }
    private synchronized void exitArtifact() { calls--; }

    @Override public Optional<Adopted> advance() {
        synchronized (this) {
            if (opening == null || applied == null || stopping) throw new IllegalStateException("original migration adoption is unavailable");
            calls++;
        }
        try {
            requireCurrent();
            switch (phase) {
                case PREPARE -> prepare();
                case SEMANTIC -> {
                    final var raw = Objects.requireNonNull(materials.semantic(applied, pending));
                    if (raw.isEmpty()) return Optional.empty();
                    snapshot = raw.orElseThrow(); phase = Phase.IMAGE;
                }
                case IMAGE -> createImage();
                case PUBLISH -> {
                    coordinator.publishMigration(pending, image, clock.millis(), adapter,
                            materials.initialization(), applied); phase = Phase.CATALOG;
                }
                case CATALOG -> {
                    if (receipt == null) receipt = TargetMigrationAssignmentReceipt.from(predecessor, applied,
                            image.manifest(), catalog.migrationNamespace());
                    catalog.adoptMigration(applied, image, receipt, this::requireCurrent); phase = Phase.ASSIGN;
                }
                case ASSIGN -> {
                    publication = predecessor.migration() == null
                            ? providers.assignments().adoptMigration(predecessor, applied, image, catalog, receipt, this::requireCurrent)
                            : predecessor;
                    final var current = providers.assignments().current(configuration.scope().shard()).orElseThrow();
                    if (!publication.sameIdentity(current)) throw new IllegalStateException("adopted assignment is no longer current");
                    publication.requireMigration(applied.candidate().store().metadata(), applied.migration());
                    phase = Phase.COMPLETE; adopted = Adopted.afterOriginalCas(this);
                }
                case COMPLETE -> { }
            }
            requireCurrent(); return Optional.ofNullable(adopted);
        } catch (RuntimeException | Error rejected) {
            synchronized (this) { if (failure == null) failure = rejected; }
            throw rejected;
        } finally { synchronized (this) { calls--; } }
    }

    private void prepare() {
        prepareAdapter();
        final var time = Objects.requireNonNull(materials.trustedTime());
        predecessor = providers.assignments().current(configuration.scope().shard()).orElseThrow();
        if (!predecessor.assignment().sourceAssignment().sameIdentity(opening.assignment()))
            throw new IllegalStateException("migration adoption lost the accepted original Source assignment");
        final byte[] identity = Bytes.sha256(Bytes.utf8("nereus-delay-migration-formal-checkpoint\0"),
                applied.migration().plan().migrationId(), applied.migration().activatedAt().canonicalBytes());
        final byte[] checkpointId = Arrays.copyOf(identity, 16);
        imagePath = configuration.checkpointDirectory().resolve("migration-" + Bytes.hex(identity)).resolve("image");
        final var proposed = new CheckpointUploadIntent(new ShardSubject(configuration.scope().shard()),
                configuration.recoveryLineageId(), checkpointId, opening.ownerIdentity(), applied.candidate().store().metadata().storeIncarnation(),
                identity, 1, null, null, configuration.objectStoreProfile(), time,
                Math.addExact(time.latestEpochMs(), configuration.uploadTtlMillis()), CheckpointUploadState.PENDING_UPLOAD, 1, null, null);
        final var existing = catalog.current(proposed);
        pending = existing.isEmpty() ? proposed : asPending(existing.orElseThrow());
        if (!Arrays.equals(pending.sourceStoreIncarnation(), applied.migration().plan().newStore())
                || !Arrays.equals(pending.recoveryLineageId(), applied.migration().plan().newLineage()))
            throw new IllegalStateException("formal migration intent names another new Store");
        final var head = catalog.checkpointHead();
        if (head.initialization() != null && !head.initialization().pending().equals(pending)
                || head.initialization() == null && (head.catalogGeneration() != 0 || head.latest() != null))
            throw new IllegalStateException("migration requires its original independently identified Target Catalog; legacy Catalog remains held");
        if (existing.isEmpty()) catalog.create(pending, this::requireCurrent);
        if (predecessor.migration() != null) {
            receipt = predecessor.migration(); receipt.requireApplied(applied);
            if (!Arrays.equals(receipt.catalogNamespace(), catalog.migrationNamespace()))
                throw new IllegalStateException("recovered migration assignment changed original Catalog");
        }
        if (existing.isPresent() && existing.orElseThrow().state() == CheckpointUploadState.PUBLISHED) {
            alreadyPublished = catalog.manifest(checkpointId).orElseThrow();
            if (!Files.exists(imagePath, LinkOption.NOFOLLOW_LINKS)) {
                adapter.download(catalog.publishedDownload(new RecoveryCandidateRef(RecoveryCandidateKind.CATALOG_CHECKPOINT,
                        configuration.recoveryLineageId(), checkpointId, alreadyPublished.manifestSha256(), null)), imagePath);
            }
            phase = Phase.IMAGE;
        } else phase = Files.exists(imagePath, LinkOption.NOFOLLOW_LINKS) ? Phase.IMAGE : Phase.SEMANTIC;
    }

    private void prepareAdapter() {
        if (adapter != null) return;
        if (nativeReservation == null) nativeReservation = opening.graph().resources().reserveNativeResource(
                "migration-checkpoint-http/" + Bytes.hex(opening.configuration().migrationId()),
                NativeResourceUsage.zero(), configuration.sdkNativeBytes());
        final var time = Objects.requireNonNull(materials.trustedTime());
        httpClient = Objects.requireNonNull(materials.httpClient(), "original dedicated migration checkpoint HTTP client");
        final var activator = new OxiaObjectStoreCredentialLeaseActivator(materials.credentials(), materials.materialResolver(),
                configuration.credentialLeaseTtlMillis(), configuration.maximumAttestationAgeMillis());
        adapter = activator.activateRenewableS3Compatible(new OxiaObjectStoreCredentialLeaseActivator.ActivationRequest(
                configuration.objectStoreProfile(), configuration.endpoint(), configuration.region(), configuration.bucket(),
                configuration.scope().digest(), time, Math.addExact(time.latestEpochMs(), configuration.credentialLeaseTtlMillis()),
                opening.graph().config().checkpoint().physical(), httpClient, clock, configuration.requestTimeout()),
                configuration.renewBeforeMillis(), materials::trustedTime);
    }

    private void createImage() {
        final var runtime = opening.graph().config().checkpoint();
        if (alreadyPublished != null) {
            image = TargetCheckpointRootVerifier.authenticateImage(imagePath, alreadyPublished, runtime.physical(),
                    runtime.quota(), runtime.ledger(), runtime.semantics(), materials.images(), this::requireCurrent);
        } else {
            if (!Files.exists(imagePath, LinkOption.NOFOLLOW_LINKS)) {
                if (!Arrays.equals(pending.owner().canonicalBytes(), opening.ownerIdentity().canonicalBytes()))
                    throw new IllegalStateException("original pending image must be recovered; a new Owner cannot recreate its immutable bytes");
                requireCurrent(); applied.candidate().store().recordOpenedOwnerEpoch(opening.ownerLease().ownerEpoch()); requireCurrent();
                applied.candidate().store().createTargetSemanticCheckpointCandidate(imagePath, pending.checkpointId(),
                        pending.recoveryLineageId(), runtime.physical(), runtime.quota(), runtime.ledger(),
                        Objects.requireNonNull(snapshot), runtime.semantics());
            }
            final var files = new ArrayList<CheckpointManifest.FileEntry>();
            for (var file : CheckpointFileInventory.collect(imagePath, runtime.physical()))
                files.add(new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8(configuration.objectNamePrefix() + "/" + Bytes.hex(pending.checkpointId()) + "/" + file.name()),
                        file.checksum(), null));
            image = TargetCheckpointRootVerifier.authenticateCandidate(new TargetCheckpointRootVerifier.CandidateImage(
                    imagePath, pending, 0, files, runtime.physical(), runtime.quota(), runtime.ledger(),
                    runtime.semantics(), materials.images(), this::requireCurrent));
        }
        phase = Phase.PUBLISH;
    }

    private static CheckpointUploadIntent asPending(CheckpointUploadIntent value) {
        if (value.state() == CheckpointUploadState.PENDING_UPLOAD) return value;
        if (value.state() != CheckpointUploadState.PUBLISHED || value.stateRevision() != 2)
            throw new IllegalStateException("original migration upload intent is not retryable");
        return new CheckpointUploadIntent(value.shard(), value.recoveryLineageId(), value.checkpointId(), value.owner(),
                value.sourceStoreIncarnation(), value.uploadToken(), value.baseCatalogGeneration(), value.parentCheckpointId(),
                value.parentManifestSha256(), value.objectStoreProfile(), value.checkpointCreatedAt(), value.uploadDeadlineEpochMs(),
                CheckpointUploadState.PENDING_UPLOAD, 1, null, null);
    }

    private void requireCurrent() {
        synchronized (this) { if (stopping) throw new IllegalStateException("migration adoption stopped"); }
        opening.requireCurrent(); materials.requireCurrent();
        if (applied != null) applied.requireCurrent();
    }

    @Override public synchronized void requestStop() { stopping = true; materials.requestStop(); }
    @Override public synchronized boolean ioExited() {
        return calls == 0 && materials.ioExited() && (!stopping || adapter == null || providerQuiescent);
    }
    @Override public synchronized Throwable pendingIoFailure() { return failure == null ? materials.pendingIoFailure() : failure; }
    @Override public void close() {
        requestStop();
        synchronized (this) {
            if (calls != 0) return;
            if (adapter != null) {
                adapter.beginProviderQuiescence(); adapter.requireProviderQuiescence(); providerQuiescent = true;
            }
            if (!closed) { closed = true; materials.close(); }
            if (!materials.cleanupComplete() && CheckpointInputFuture.ioExited(materialCleanup))
                materialCleanup = Objects.requireNonNull(materials.retryCleanup());
            if (materials.ioExited() && materials.cleanupComplete() && CheckpointInputFuture.ioExited(materialCleanup)
                    && (httpClient == null || httpClient.isTerminated()) && nativeReservation != null && !nativeReservation.isReleased())
                nativeReservation.close();
        }
    }
    @Override public CompletableFuture<Void> retryCleanup() {
        close();
        return cleanupComplete() ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(new IllegalStateException("original migration checkpoint inputs remain retained"));
    }
    @Override public synchronized boolean cleanupComplete() {
        return closed && calls == 0 && materials.ioExited() && materials.cleanupComplete()
                && CheckpointInputFuture.ioExited(materialCleanup) && (httpClient == null || httpClient.isTerminated())
                && (adapter == null || providerQuiescent)
                && (nativeReservation == null || nativeReservation.isReleased());
    }

    void requireAdopted() {
        if (phase != Phase.COMPLETE || publication == null || receipt == null || image == null)
            throw new IllegalStateException("original post-A checkpoint and both exact metadata CAS steps are incomplete");
    }
    TargetMigrationPlan adoptedPlan() { requireAdopted(); return receipt.plan(); }
    CheckpointManifest adoptedCheckpoint() { requireAdopted(); return image.manifest(); }
    WorkerAssignmentAuthority.Publication adoptedPublication() { requireAdopted(); return publication; }
    void requireAdoptedApplied(TargetMigrationStore.Activated exact) { requireAdopted(); receipt.requireApplied(exact); }
    void requireAdoptedCurrent() {
        requireAdopted(); receipt.requireApplied(applied); catalog.requireMigration(receipt, image.manifest());
        if (!publication.sameIdentity(providers.assignments().current(configuration.scope().shard()).orElseThrow()))
            throw new IllegalStateException("migration adoption is no longer the exact current assignment");
    }
}
