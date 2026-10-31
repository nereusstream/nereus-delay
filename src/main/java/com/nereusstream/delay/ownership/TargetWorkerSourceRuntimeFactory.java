package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Constructs actual configured Source runtimes from native committed/recovered Store roots, not caller-built runtimes. */
public final class TargetWorkerSourceRuntimeFactory {
    private TargetWorkerSourceRuntimeFactory() {}

    /** A local subloan of the original Source owner's already acquired SDK/read/codec capacity. */
    public interface FrameReadLoan {
        void requireCurrent();

        void requestRelease();

        boolean cleanupComplete();
    }

    /** Complete retained-prefix and future-writer constraint, never minted from a configured number. */
    public static final class SourceFrameContract implements AutoCloseable {
        private final TargetWorkerSourceStartup.FrameOpening opening;
        private final TargetMigrationProcessDriver.FrameOpening migrationOpening;
        private final TargetScopeDeploymentAllocation.SourceHandle original;
        private final TargetScopeDeploymentAllocation.SourceSnapshot snapshot;
        private final com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds maximumDecode;
        private final Runnable held;
        private volatile boolean closed;

        private SourceFrameContract(
                TargetWorkerSourceStartup.FrameOpening opening,
                TargetScopeDeploymentAllocation.SourceHandle original,
                TargetScopeDeploymentAllocation.SourceSnapshot snapshot,
                Runnable held) {
            this.opening = opening;
            this.migrationOpening = null;
            this.original = original;
            this.snapshot = snapshot;
            this.held = held;
            maximumDecode = com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.frameDecode(
                    snapshot.maximumSourceFrameBytes());
        }

        private SourceFrameContract(
                TargetMigrationProcessDriver.FrameOpening opening,
                TargetScopeDeploymentAllocation.SourceHandle original,
                TargetScopeDeploymentAllocation.SourceSnapshot snapshot,
                Runnable held) {
            this.opening = null;
            this.migrationOpening = opening;
            this.original = original;
            this.snapshot = snapshot;
            this.held = held;
            maximumDecode = com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.frameDecode(
                    snapshot.maximumSourceFrameBytes());
        }

        static SourceFrameContract capture(
                TargetMigrationProcessDriver.FrameOpening opening,
                TargetScopeDeploymentAllocation.SourceHandle original,
                Runnable held) {
            Objects.requireNonNull(opening).requireCurrent();
            Objects.requireNonNull(held).run();
            final var snapshot =
                    Objects.requireNonNull(Objects.requireNonNull(original).readAuthenticated());
            opening.requireSource(snapshot.source());
            original.requireAllRetainedAndFutureFrames(opening, snapshot);
            original.requireLocalCurrent();
            held.run();
            opening.requireCurrent();
            final var captured = new SourceFrameContract(opening, original, snapshot, held);
            captured.requireLocalCurrent();
            return captured;
        }
        /** Called on the original bounded acquisition flight; the same original handle remains owned. */
        public static SourceFrameContract capture(
                TargetWorkerSourceStartup.FrameOpening opening,
                TargetScopeDeploymentAllocation.SourceHandle original,
                Runnable held) {
            Objects.requireNonNull(opening).requireCurrent();
            Objects.requireNonNull(held).run();
            final var snapshot =
                    Objects.requireNonNull(Objects.requireNonNull(original).readAuthenticated());
            opening.requireSource(snapshot.source());
            original.requireAllRetainedAndFutureFrames(opening, snapshot);
            original.requireLocalCurrent();
            held.run();
            opening.requireCurrent();
            final var captured = new SourceFrameContract(opening, original, snapshot, held);
            captured.requireLocalCurrent();
            return captured;
        }

        public long maximumFrameBytes() {
            requireLocalCurrent();
            return snapshot.maximumSourceFrameBytes();
        }

        public void requireLocalCurrent() {
            if (closed) {
                throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                        "original complete Source frame qualification was released");
            }
            if (opening != null) {
                opening.requireLocalCurrent();
            } else {
                migrationOpening.requireLocalCurrent();
            }
            held.run();
            original.requireLocalCurrent();
            if (opening != null) {
                original.requireAllRetainedAndFutureFrames(opening, snapshot);
            } else {
                original.requireAllRetainedAndFutureFrames(migrationOpening, snapshot);
            }
            if (opening != null) {
                original.requireFrameReadAndCodecWindow(opening, snapshot, maximumDecode);
            } else {
                original.requireFrameReadAndCodecWindow(migrationOpening, snapshot, maximumDecode);
            }
            if (opening != null) {
                opening.requireLocalCurrent();
            } else {
                migrationOpening.requireLocalCurrent();
            }
        }

        public void requireCurrent(TargetStoreBackend.Reader actual) {
            requireLocalCurrent();
            if (opening == null) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable(
                        "old-domain reader frame qualification grants no Target Native service");
            }
            opening.requireReader(actual);
            requireLocalCurrent();
        }

        public void requireService(
                TargetStoreBackend.Reader actual,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.Bounds computed) {
            requireCurrent(actual);
            if (Objects.requireNonNull(computed).requiresSourceFrameQualification()) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable(
                        "Native service calculation lacks its original Source qualification");
            }
            original.requireNativeSourceService(opening, snapshot, computed);
            requireCurrent(actual);
        }

        public void requireCheckpoint(com.nereusstream.delay.store.TargetCheckpointRootVerifier.RootProof actual) {
            requireLocalCurrent();
            if (opening == null) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable(
                        "old-domain reader frame hold grants no Target checkpoint qualification");
            }
            opening.requireCheckpoint(Objects.requireNonNull(actual));
            requireLocalCurrent();
        }
        /** Retained authenticated archive material has its historical guard proof in the same original complete prefix hold. */
        public void requireRetainedFrame(com.nereusstream.delay.protocol.SourcePosition actual, long bytes) {
            requireLocalCurrent();
            if (opening != null) {
                opening.requireSource(actual);
            } else {
                migrationOpening.requireSource(actual);
            }
            if (bytes < 1 || bytes > snapshot.maximumSourceFrameBytes()) {
                throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                        "retained archive Source frame exceeds its original complete frame hold");
            }
        }

        void requireOpening(TargetWorkerSourceStartup.FrameOpening actual) {
            requireLocalCurrent();
            if (opening == null) {
                throw new IllegalArgumentException(
                        "migration reader frame qualification cannot become a Target Root hold");
            }
            opening.requireSame(actual);
        }

        public void requireFrame(
                com.nereusstream.delay.protocol.SourcePosition actual,
                long bytes,
                Long generation,
                byte[] attestation) {
            requireLocalCurrent();
            if (opening != null) {
                opening.requireFrame(actual, generation, attestation);
            } else {
                migrationOpening.requireFrame(actual, generation, attestation);
            }
            if (bytes < 1 || bytes > snapshot.maximumSourceFrameBytes()) {
                throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                        "retained actual Source frame is outside the authenticated complete frame freeze; preserve its raw responsibility");
            }
        }

        FrameReadLoan borrow(
                com.nereusstream.delay.protocol.SourcePosition actual,
                long bytes,
                Long generation,
                byte[] attestation,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds decode) {
            requireFrame(actual, bytes, generation, attestation);
            final var loan = Objects.requireNonNull(
                    opening != null
                            ? original.borrowFrameReadAndCodec(opening, actual, bytes, generation, attestation, decode)
                            : original.borrowFrameReadAndCodec(
                                    migrationOpening, actual, bytes, generation, attestation, decode),
                    "original Source read/codec subloan");
            return loan;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    public record Content(
            TargetSourceAuthorityLoader.Provider provider,
            TargetStoreBackend.CommitAuthority physical,
            TargetStoreBackend.ReadAuthority reads,
            Duration lookupTimeout,
            long maximumBytes,
            com.nereusstream.delay.store.CheckpointPublicationCoordinator evidencePublication,
            TargetScopeResourceManager.Inputs scopeResources,
            TargetWorkerProcessRuntime.SourcePlanInputs sourcePlanInputs) {
        public Content(
                TargetSourceAuthorityLoader.Provider provider,
                TargetStoreBackend.CommitAuthority physical,
                TargetStoreBackend.ReadAuthority reads,
                Duration lookupTimeout,
                long maximumBytes,
                com.nereusstream.delay.store.CheckpointPublicationCoordinator evidencePublication,
                TargetScopeResourceManager.Inputs scopeResources) {
            this(provider, physical, reads, lookupTimeout, maximumBytes, evidencePublication, scopeResources, null);
        }

        public Content(
                TargetSourceAuthorityLoader.Provider provider,
                TargetStoreBackend.CommitAuthority physical,
                TargetStoreBackend.ReadAuthority reads,
                Duration lookupTimeout,
                long maximumBytes) {
            this(provider, physical, reads, lookupTimeout, maximumBytes, null);
        }

        public Content(
                TargetSourceAuthorityLoader.Provider provider,
                TargetStoreBackend.CommitAuthority physical,
                TargetStoreBackend.ReadAuthority reads,
                Duration lookupTimeout,
                long maximumBytes,
                com.nereusstream.delay.store.CheckpointPublicationCoordinator evidencePublication) {
            this(provider, physical, reads, lookupTimeout, maximumBytes, evidencePublication, null);
        }

        public Content {
            Objects.requireNonNull(provider);
            Objects.requireNonNull(physical);
            Objects.requireNonNull(reads);
            if (lookupTimeout == null
                    || lookupTimeout.isZero()
                    || lookupTimeout.isNegative()
                    || lookupTimeout.toNanos() == Long.MAX_VALUE
                    || maximumBytes < 1
                    || maximumBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Source content requires finite original provider bounds");
            }
        }

        Content withSourcePlanInputs(TargetWorkerProcessRuntime.SourcePlanInputs actual) {
            if (sourcePlanInputs != null && sourcePlanInputs != actual) {
                throw new IllegalStateException("Source content changed its original plan reservation");
            }
            return new Content(
                    provider,
                    physical,
                    reads,
                    lookupTimeout,
                    maximumBytes,
                    evidencePublication,
                    scopeResources,
                    Objects.requireNonNull(actual));
        }
    }

    private static com.nereusstream.delay.store.RecoveryCatalogAuthority evidenceCatalog(
            Content content,
            com.nereusstream.delay.store.RecoveryCatalogAuthority restored,
            SharedRocksDbResources resources,
            TargetWorkerOxiaRuntime oxia) {
        final var publication = content.evidencePublication();
        if (publication == null) {
            return restored;
        }
        publication.requireOriginalEvidenceResources(resources);
        final var configured = publication
                .managedEvidenceRecoveryCatalog()
                .orElseThrow(() -> new IllegalArgumentException(
                        "configured Source evidence publication requires its original managed atomic Catalog"));
        oxia.providers().requireOriginalEvidenceCatalog(configured);
        if (restored != null) {
            if (!(restored instanceof com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend original)) {
                throw new IllegalArgumentException(
                        "configured Source publication differs from its restored Catalog authority");
            }
            original.requireSameOriginalEvidenceAuthority(configured);
            return original;
        }
        return configured;
    }

    /** Only this factory can bind an activated runtime to the accepted original process/resource graph. */
    public static final class Activated {
        private final TargetWorkerOxiaRuntime oxia;
        private final TargetWorkerRuntimeConfig config;
        private final WorkClassExecutionRegistry classes;
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final SourceAssignment assignment;
        private final TargetSourceApplyRuntime runtime;
        private boolean sourceConstructionFailed;

        private Activated(
                TargetWorkerOxiaRuntime oxia,
                TargetWorkerRuntimeConfig config,
                WorkClassExecutionRegistry classes,
                SharedRocksDbResources resources,
                ShardStore store,
                SourceAssignment assignment,
                TargetSourceApplyRuntime runtime) {
            this.oxia = oxia;
            this.config = config;
            this.classes = classes;
            this.resources = resources;
            this.store = store;
            this.assignment = assignment;
            this.runtime = runtime;
        }

        public TargetSourceApplyRuntime runtime() {
            return runtime;
        }

        public SourceAssignment assignment() {
            return assignment;
        }

        public synchronized void requireGraph(
                WorkClassExecutionRegistry expected, SharedRocksDbResources shared, ShardStore actual) {
            if (sourceConstructionFailed || classes != expected || resources != shared || store != actual) {
                throw new IllegalArgumentException("Source activation belongs to another actual Worker/Store graph");
            }
            config.requireRuntime(classes, resources);
            if (!runtime.operationEnvelopeReady()) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable(
                        "actual Source graph has not completed its Native operation service gate");
            }
            runtime.requireWorkerStore(store, resources);
            if (runtime.drainAuthority() != oxia.providers().leases() || !runtime.hasAuthorityPreparation()) {
                throw new IllegalStateException("Source activation lost its original Oxia/content graph");
            }
            runtime.requireAcceptedAssignment(assignment);
        }
    }

    public static final class SourceConstructionFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient Activated activation;

        private SourceConstructionFailure(Throwable failure, Activated activation) {
            super("original Source activation retained after SDK/Worker construction failure", failure);
            this.activation = activation;
        }

        public Activated activation() {
            return activation;
        }

        public void retryContentCleanup() {
            activation.runtime.retryAuthorityCleanup();
        }

        public boolean contentIoExited() {
            return activation.runtime.authorityIoExited();
        }
    }

    public static SourceConstructionFailure sourceConstructionFailed(Activated activation, Throwable failure) {
        synchronized (activation) {
            activation.sourceConstructionFailed = true;
            activation.runtime.fence();
        }
        return new SourceConstructionFailure(failure, activation);
    }

    /** Uses the actual accepted assignment and this root's original Oxia session; an uncertain acquire is never guessed on reread. */
    public static OwnerLease acquire(
            TargetWorkerOxiaRuntime oxia,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            String ownerId,
            long leaseDurationMs,
            LongSupplier ownerClock) {
        if (leaseDurationMs < 1 || leaseDurationMs == Long.MAX_VALUE) {
            throw new IllegalArgumentException("finite lease duration required");
        }
        var assignment = accept(oxia, tenant, publication, workerId);
        var session = oxia.providers().sessionIdentity();
        long now = ownerClock.getAsLong();
        if (now < 0) {
            throw new IllegalArgumentException("Owner clock must be non-negative");
        }
        try {
            return oxia.providers()
                    .leases()
                    .acquire(assignment, ownerId, session, now, leaseDurationMs)
                    .orElseThrow(() -> new IllegalStateException("actual Source assignment Owner is unavailable"));
        } catch (RuntimeException | Error failure) {
            throw new AcquisitionFailure(failure, assignment, ownerId, session, now);
        }
    }

    public static final class AcquisitionFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient SourceAssignment assignment;
        private final String ownerId;
        private final byte[] session;
        private final long requestedAt;

        private AcquisitionFailure(
                Throwable cause, SourceAssignment assignment, String ownerId, byte[] session, long requestedAt) {
            super("original Owner acquisition request retained; no successful lease inferred", cause);
            this.assignment = assignment;
            this.ownerId = ownerId;
            this.session = Bytes.copy(session);
            this.requestedAt = requestedAt;
        }

        public SourceAssignment assignment() {
            return assignment;
        }

        public String ownerId() {
            return ownerId;
        }

        public byte[] sessionIdentity() {
            return Bytes.copy(session);
        }

        public long requestedAtEpochMs() {
            return requestedAt;
        }
    }

    /** Fresh bootstrap has already committed the genuine initial Source/root batch; this performs actual Owner activation. */
    public static Activated initialize(
            TargetWorkerOxiaRuntime oxia,
            TargetWorkerRuntimeConfig config,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            TargetStoreBootstrap.Initialized initialized,
            ShardStore store,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            OwnerLease expectedLease,
            SourceReplaySuccessor actualSourceSuccessor,
            Content content,
            LongSupplier ownerClock,
            LongSupplier nanos) {
        requireResolvedScopeResources(content);
        Objects.requireNonNull(config).requireRuntime(classes, resources);
        var assignment = accept(oxia, tenant, publication, workerId);
        requireLeaseContext(oxia, assignment, expectedLease);
        requireScope(oxia, tenant, assignment, initialized.root().scope());
        var preparation = oxia.newSourcePreparation(
                content.provider(), content.physical(), content.lookupTimeout(), content.maximumBytes());
        var authorities = preparation.authorities(
                oxia.providers().leases(), actualSourceSuccessor, content.physical(), content.reads());
        TargetSourceApplyRuntime runtime = null;
        OwnerLease active = null;
        try {
            bindOperationEnvelope(initialized.backend(), store, config, classes, resources, content);
            preparation.bindSourcePlanInputs(content.sourcePlanInputs());
            active = TargetWorkerOwnerActivation.activate(
                    initialized, store, assignment, expectedLease, authorities.leases(), ownerClock);
            // Re-read actual Route/assignment after activation I/O, before constructing an exposed Source runtime.
            accept(oxia, tenant, publication, workerId);
            runtime = new TargetSourceApplyRuntime(
                    initialized, store, assignment, active, authorities, config.source(), nanos);
            runtime.configureAuthorityPreparation(
                    preparation, ownerClock, evidenceCatalog(content, null, resources, oxia));
            return new Activated(oxia, config, classes, resources, store, assignment, runtime);
        } catch (RuntimeException | Error failure) {
            if (runtime != null) {
                runtime.fence();
            }
            preparation.requestStop();
            throw new AssemblyFailure(failure, store, active == null ? expectedLease : active, preparation, null);
        }
    }

    /** Real same-DB reopen and protected no-ACK recovery; Source/Journal inputs retain their original provider ownership. */
    public static Recovery reopen(
            TargetWorkerOxiaRuntime oxia,
            TargetWorkerRuntimeConfig config,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ShardStore store,
            TargetQuotaScope scope,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            OwnerLease lease,
            SourceReplayCursor<? extends SourceReplayEntry> source,
            SourceReplaySuccessor actualSourceSuccessor,
            Content content,
            OwnerIdentity identity,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            Runnable recoveryProtection,
            Runnable journalProtection,
            LongSupplier ownerClock,
            LongSupplier nanos) {
        return reopenInternal(
                oxia,
                config,
                classes,
                resources,
                store,
                scope,
                tenant,
                publication,
                workerId,
                lease,
                source,
                actualSourceSuccessor,
                content,
                identity,
                journal,
                recoveryProtection,
                journalProtection,
                ownerClock,
                nanos,
                null,
                null);
    }

    /** Exact installed Target image recovery retains its original published Catalog/Pin throughout Source/Claim replay. */
    public static Recovery recoverInstalled(
            TargetWorkerOxiaRuntime oxia,
            TargetWorkerRuntimeConfig config,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ShardStore store,
            TargetQuotaScope scope,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            OwnerLease lease,
            SourceReplayCursor<? extends SourceReplayEntry> source,
            SourceReplaySuccessor actualSourceSuccessor,
            Content content,
            OwnerIdentity identity,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            Runnable recoveryProtection,
            Runnable journalProtection,
            LongSupplier ownerClock,
            LongSupplier nanos,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.protocol.RecoveryPin pin) {
        return reopenInternal(
                oxia,
                config,
                classes,
                resources,
                store,
                scope,
                tenant,
                publication,
                workerId,
                lease,
                source,
                actualSourceSuccessor,
                content,
                identity,
                journal,
                recoveryProtection,
                journalProtection,
                ownerClock,
                nanos,
                Objects.requireNonNull(catalog),
                Objects.requireNonNull(pin));
    }

    private static Recovery reopenInternal(
            TargetWorkerOxiaRuntime oxia,
            TargetWorkerRuntimeConfig config,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ShardStore store,
            TargetQuotaScope scope,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            OwnerLease lease,
            SourceReplayCursor<? extends SourceReplayEntry> source,
            SourceReplaySuccessor actualSourceSuccessor,
            Content content,
            OwnerIdentity identity,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            Runnable recoveryProtection,
            Runnable journalProtection,
            LongSupplier ownerClock,
            LongSupplier nanos,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.protocol.RecoveryPin pin) {
        requireResolvedScopeResources(content);
        config.requireRuntime(classes, resources);
        var assignment = accept(oxia, tenant, publication, workerId);
        requireLeaseContext(oxia, assignment, lease);
        requireScope(oxia, tenant, assignment, scope);
        var preparation = oxia.newSourcePreparation(
                content.provider(), content.physical(), content.lookupTimeout(), content.maximumBytes());
        var authorities = preparation.authorities(
                oxia.providers().leases(), actualSourceSuccessor, content.physical(), content.reads());
        TargetOwnerRecoveryCoordinator recovery = null;
        try {
            var guardedReads = startupReads(
                    oxia, tenant, publication, workerId, store, assignment, lease, content.reads(), ownerClock);
            var reopened = TargetStoreBootstrap.reopen(
                    store,
                    scope,
                    new TargetStoreBackend.WriteLimits(
                            config.source().records(), config.source().bytes()),
                    config.checkpoint().newReadBudget(nanos),
                    guardedReads,
                    config.checkpoint().ledger());
            bindOperationEnvelope(reopened.backend(), store, config, classes, resources, content);
            preparation.bindSourcePlanInputs(content.sourcePlanInputs());
            final var turn = new ReplayTurnBudget(
                    config.source().records(),
                    config.source().bytes(),
                    config.source().elapsedNanos());
            recovery = catalog == null
                    ? new TargetOwnerRecoveryCoordinator(
                            store,
                            reopened,
                            assignment,
                            lease,
                            authorities,
                            config.source(),
                            source,
                            recoveryProtection,
                            turn,
                            classes,
                            ownerClock,
                            nanos,
                            () -> config.checkpoint().newReadBudget(nanos),
                            config.checkpoint().ledger())
                    : new TargetOwnerRecoveryCoordinator(
                            store,
                            reopened,
                            assignment,
                            lease,
                            authorities,
                            config.source(),
                            source,
                            catalog,
                            pin,
                            recoveryProtection,
                            turn,
                            classes,
                            ownerClock,
                            nanos,
                            () -> config.checkpoint().newReadBudget(nanos),
                            config.checkpoint().ledger());
            recovery.configureAuthorityPreparation(preparation, evidenceCatalog(content, catalog, resources, oxia));
            recovery.configureClaimTakeover(identity, journal, journalProtection);
            return new Recovery(
                    oxia, config, classes, resources, store, assignment, tenant, publication, workerId, recovery);
        } catch (RuntimeException | Error failure) {
            preparation.requestStop();
            if (recovery != null) {
                recovery.stopAuthorityPreparation();
            }
            throw new AssemblyFailure(failure, store, lease, preparation, recovery);
        }
    }

    private static void requireResolvedScopeResources(Content content) {
        if (Objects.requireNonNull(content).scopeResources() != null) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "configured Scope resources require their actual owned Source startup");
        }
    }

    private static void bindOperationEnvelope(
            TargetStoreBackend backend,
            ShardStore store,
            TargetWorkerRuntimeConfig config,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            Content content) {
        final var actual = content.sourcePlanInputs();
        if (actual == null) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "configured Source requires its original Process plan/provider reservation");
        }
        actual.requireBinding(store, config, classes, resources, content.maximumBytes());
        if (content.lookupTimeout().toNanos() < config.source().elapsedNanos()) {
            throw new IllegalArgumentException(
                    "Source Native action exceeds its original authority-I/O timeout window");
        }
        // The actual writer kernel is installed only after the SDK binds its original OwnerIdentity.
        // Final Root/Native service qualification therefore runs on its owned I/O before READY.
        backend.bindSourceOperationEnvelope(actual);
    }

    public static final class Recovery {
        private final TargetWorkerOxiaRuntime oxia;
        private final TargetWorkerRuntimeConfig config;
        private final WorkClassExecutionRegistry classes;
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final SourceAssignment assignment;
        private final AuthenticatedTenantContext tenant;
        private final WorkerAssignmentAuthority.Publication publication;
        private final String workerId;
        private final TargetOwnerRecoveryCoordinator recovery;
        private Activated activated;
        private SourceReplaySuccessor activeSuccessor;

        private Recovery(
                TargetWorkerOxiaRuntime oxia,
                TargetWorkerRuntimeConfig config,
                WorkClassExecutionRegistry classes,
                SharedRocksDbResources resources,
                ShardStore store,
                SourceAssignment assignment,
                AuthenticatedTenantContext tenant,
                WorkerAssignmentAuthority.Publication publication,
                String workerId,
                TargetOwnerRecoveryCoordinator recovery) {
            this.oxia = oxia;
            this.config = config;
            this.classes = classes;
            this.resources = resources;
            this.store = store;
            this.assignment = assignment;
            this.tenant = tenant;
            this.publication = publication;
            this.workerId = workerId;
            this.recovery = recovery;
        }

        public TargetOwnerRecoveryCoordinator coordinator() {
            return recovery;
        }

        public synchronized Activated activateCompleted() {
            return activateCompleted(null);
        }

        public synchronized Activated activateCompleted(SourceReplaySuccessor actualActiveSuccessor) {
            accept(oxia, tenant, publication, workerId);
            if (activated != null) {
                if (activeSuccessor != actualActiveSuccessor) {
                    throw new IllegalStateException("active Source proof cannot be replaced");
                }
                return activated;
            }
            var runtime = actualActiveSuccessor == null
                    ? recovery.activeRuntime()
                    : recovery.activeRuntime(actualActiveSuccessor);
            activated = new Activated(oxia, config, classes, resources, store, assignment, runtime);
            activeSuccessor = actualActiveSuccessor;
            return activated;
        }
    }

    static SourceAssignment accept(
            TargetWorkerOxiaRuntime oxia,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId) {
        var providers = Objects.requireNonNull(oxia).providers();
        var expected = Objects.requireNonNull(publication).assignment();
        if (!expected.workerId().equals(workerId)) {
            throw new IllegalStateException("assignment is for another Worker");
        }
        if (!expected.routeBound()) {
            throw new IllegalStateException("Source assignment lacks its original signed Route");
        }
        var source = expected.sourceAssignment();
        var resolved = new RouteSourceAssignmentResolver(providers.routes())
                .exactResolved(
                        tenant,
                        source.shardId().routeIncarnation(),
                        source.shardId().partition(),
                        source.assignmentId(),
                        source.assignmentEpoch());
        new ProtocolActivationAuthorityCoordinator(providers.capabilities())
                .requireEligibleReaders(resolved.routeSnapshot().protocolTuple(), java.util.List.of(workerId));
        var observed = providers
                .assignments()
                .current(source.shardId())
                .orElseThrow(() -> new IllegalStateException("actual Source assignment is unavailable"));
        if (!observed.sameIdentity(publication)) {
            throw new IllegalStateException("actual Source assignment changed before acceptance");
        }
        observed.requireRouteProjection(resolved.routeSnapshot());
        return observed.assignment().sourceAssignment();
    }

    static TargetStoreBackend.ReadAuthority startupReads(
            TargetWorkerOxiaRuntime oxia,
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            ShardStore store,
            SourceAssignment assignment,
            OwnerLease expected,
            TargetStoreBackend.ReadAuthority physical,
            LongSupplier clock) {
        final TargetStoreBackend.ReadAuthority legacy = (metadata, scope) -> {
            if (!metadata.equals(store.metadata()) || !scope.shard().equals(assignment.shardId())) {
                throw new IllegalArgumentException("Source reopen changes its actual Store/Shard");
            }
            var actual = Objects.requireNonNull(physical.acquire(metadata, scope), "original physical read guard");
            return new TargetStoreBackend.CommitGuard() {
                private long lastNow = -1;

                @Override
                public synchronized void requireCurrent() {
                    actual.requireCurrent();
                    accept(oxia, tenant, publication, workerId);
                    requireLeaseContext(oxia, assignment, expected);
                    var current = oxia.providers()
                            .leases()
                            .current(assignment.shardId())
                            .orElseThrow(
                                    () -> new IllegalStateException("original Source Owner disappeared during reopen"));
                    long now = clock.getAsLong();
                    if (now < 0
                            || now < lastNow
                            || !expected.sameIdentity(current)
                            || !current.validAt(now)
                            || current.state() != ShardLifecycleState.ACQUIRING
                                    && current.state() != ShardLifecycleState.CATCHING_UP
                                    && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                            || store.isCloseStarted()
                            || Long.compareUnsigned(
                                            store.runtimeMetadata().lastOpenedOwnerEpoch(), current.ownerEpoch())
                                    > 0) {
                        throw new IllegalStateException("Source reopen lost its actual Owner/session/Store");
                    }
                    lastNow = now;
                    actual.requireCurrent();
                }

                @Override
                public void close() {
                    actual.close();
                }
            };
        };
        if (!(physical instanceof TargetStoreBackend.CheckpointReadAuthority original)) {
            return legacy;
        }
        record CheckpointOwnerRead(OwnerLease lease, long now) {}
        class CheckpointStartupReads
                implements TargetStoreBackend.ReadAuthority, TargetStoreBackend.CheckpointReadAuthority {
            @Override
            public TargetStoreBackend.CommitGuard acquire(
                    com.nereusstream.delay.store.StoreMetadata metadata, TargetQuotaScope scope) {
                return legacy.acquire(metadata, scope);
            }

            @Override
            public TargetStoreBackend.CheckpointReadHold acquireCheckpointRead(
                    com.nereusstream.delay.store.StoreMetadata metadata, TargetQuotaScope scope) {
                if (!metadata.equals(store.metadata()) || !scope.shard().equals(assignment.shardId())) {
                    throw new IllegalArgumentException("Source checkpoint read changes its actual Store/Shard");
                }
                final var acquired = requireExternalOwner(-1);
                final OwnerLease acquiredLease = acquired.lease();
                final var actual = Objects.requireNonNull(
                        original.acquireCheckpointRead(metadata, scope), "original physical checkpoint read hold");
                return new TargetStoreBackend.CheckpointReadHold() {
                    private long lastNow = acquired.now();

                    @Override
                    public void requireExternalCurrent() {
                        actual.requireExternalCurrent();
                        lastNow = requireExternalOwner(lastNow).now();
                        actual.requireExternalCurrent();
                        requireLocalCurrent();
                    }

                    @Override
                    public synchronized void requireLocalCurrent() {
                        requireLocalOwner();
                        actual.requireLocalCurrent();
                        requireLocalOwner();
                    }

                    private void requireLocalOwner() {
                        final long now = clock.getAsLong();
                        if (now < lastNow
                                || !acquiredLease.validAt(now)
                                || store.isCloseStarted()
                                || !metadata.equals(store.metadata())
                                || Long.compareUnsigned(
                                                store.runtimeMetadata().lastOpenedOwnerEpoch(),
                                                acquiredLease.ownerEpoch())
                                        > 0) {
                            throw new IllegalStateException(
                                    "Source checkpoint read lost its original local Owner/session/Store");
                        }
                        lastNow = now;
                    }

                    @Override
                    public void close() {
                        actual.close();
                    }

                    @Override
                    public boolean cleanupComplete() {
                        return actual.cleanupComplete();
                    }

                    @Override
                    public java.util.concurrent.CompletableFuture<Void> retryCleanup() {
                        return actual.retryCleanup();
                    }
                };
            }

            private CheckpointOwnerRead requireExternalOwner(long lastNow) {
                accept(oxia, tenant, publication, workerId);
                requireLeaseContext(oxia, assignment, expected);
                final var current = oxia.providers()
                        .leases()
                        .current(assignment.shardId())
                        .orElseThrow(() ->
                                new IllegalStateException("original Source Owner disappeared during checkpoint read"));
                final long now = clock.getAsLong();
                if (now < 0
                        || now < lastNow
                        || !expected.sameIdentity(current)
                        || !current.validAt(now)
                        || current.state() != ShardLifecycleState.ACQUIRING
                                && current.state() != ShardLifecycleState.CATCHING_UP
                                && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                        || store.isCloseStarted()
                        || Long.compareUnsigned(store.runtimeMetadata().lastOpenedOwnerEpoch(), current.ownerEpoch())
                                > 0) {
                    throw new IllegalStateException("Source checkpoint read lost its actual Owner/session/Store");
                }
                return new CheckpointOwnerRead(current, now);
            }
        }
        return new CheckpointStartupReads();
    }

    static void requireScope(
            TargetWorkerOxiaRuntime oxia,
            AuthenticatedTenantContext tenant,
            SourceAssignment assignment,
            TargetQuotaScope scope) {
        var route = new RouteSourceAssignmentResolver(oxia.providers().routes())
                .exactResolved(
                        tenant,
                        assignment.shardId().routeIncarnation(),
                        assignment.shardId().partition(),
                        assignment.assignmentId(),
                        assignment.assignmentEpoch())
                .routeSnapshot();
        if (scope.target() != null) {
            throw new IllegalArgumentException("Source runtime needs the exact Shard root scope");
        }
        if (!scope.shard().equals(assignment.shardId())
                || !Bytes.constantTimeEquals(scope.tenantScope(), route.tenantRoutingScope())) {
            throw new IllegalStateException("Source Store root differs from the authenticated Route tenant/Shard");
        }
    }

    private static void requireLeaseContext(
            TargetWorkerOxiaRuntime oxia, SourceAssignment assignment, OwnerLease lease) {
        if (lease.context() == null
                || !lease.shardId().equals(assignment.shardId())
                || !Bytes.constantTimeEquals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()
                || !Bytes.constantTimeEquals(
                        lease.context().sessionIdentity(), oxia.providers().sessionIdentity())) {
            throw new IllegalStateException("Source startup changes its actual assignment/Oxia session");
        }
    }

    /** No automatic Store/lease release: the original process root must settle I/O, close the Store, then decide lease release. */
    public static final class AssemblyFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient ShardStore store;
        private final transient OwnerLease lease;
        private final transient TargetSourceAuthorityPreparation preparation;
        private final transient TargetOwnerRecoveryCoordinator recovery;

        private AssemblyFailure(
                Throwable cause,
                ShardStore store,
                OwnerLease lease,
                TargetSourceAuthorityPreparation preparation,
                TargetOwnerRecoveryCoordinator recovery) {
            super("original Source assembly remains owned after failure", cause);
            this.store = store;
            this.lease = lease;
            this.preparation = preparation;
            this.recovery = recovery;
        }

        public ShardStore store() {
            return store;
        }

        public OwnerLease lease() {
            return lease;
        }

        public void retryContentCleanup() {
            preparation.retryCleanup();
            if (recovery != null) {
                recovery.retryAuthorityCleanup();
            }
        }

        public boolean contentIoExited() {
            return preparation.ioExited() && (recovery == null || recovery.authorityIoExited());
        }
    }
}
