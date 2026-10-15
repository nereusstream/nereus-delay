package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.TargetControlSnapshotInput;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Objects;
import java.util.Optional;
import java.util.Arrays;
import java.util.function.LongSupplier;

/** One original process-owned Source bootstrap, Store, lease and SDK handoff; no Source ACK is performed here. */
public final class TargetWorkerSourceStartup {
    public record Owner(AuthenticatedTenantContext tenant, WorkerAssignmentAuthority.Publication publication,
            String workerId, String ownerId, long leaseDurationMs, LongSupplier clock) {
        public Owner {
            Objects.requireNonNull(tenant); Objects.requireNonNull(publication); Objects.requireNonNull(clock);
            if (workerId == null || workerId.isBlank() || workerId.length() > 512
                    || ownerId == null || ownerId.isBlank() || ownerId.length() > 512
                    || leaseDurationMs < 1 || leaseDurationMs == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Source startup requires a finite original Owner identity/lease");
            }
        }
    }

    /** All authority callbacks consume original protected local material; collecting it remains outside Store locks. */
    public record Initial(TargetQuotaScope scope, byte[] lineage, PreparedControlOperation prepared,
            SourceReplayMutation entry, TargetQuotaGrantControlVerifier.Authority grants,
            TargetStoreBootstrap.StartAuthority start, TargetControlSnapshotInput snapshot,
            TargetStoreBootstrap.InitialControlAuthority snapshotAuthority) {
        public Initial {
            Objects.requireNonNull(scope); Objects.requireNonNull(prepared); Objects.requireNonNull(entry);
            Objects.requireNonNull(grants); Objects.requireNonNull(start); Objects.requireNonNull(snapshot);
            Objects.requireNonNull(snapshotAuthority);
            Bytes.requireLength(lineage, 32, "Source startup lineage"); lineage = Bytes.copy(lineage);
            if (scope.target() != null || !scope.shard().equals(entry.position().shardId())) {
                throw new IllegalArgumentException("Source startup needs its original initial Shard grant");
            }
        }
        @Override public byte[] lineage() { return Bytes.copy(lineage); }
    }

    private final TargetWorkerProcessRuntime process;
    private final TargetWorkerProcessRuntime.Assembly graph;
    private final Owner owner;
    private WorkerAssignmentAuthority.Publication publication;
    private final Initial initial;
    private final TargetQuotaScope scope;
    private final TargetWorkerSourceRuntimeFactory.Content content;
    private final SourceReplaySuccessor successor;
    private final LongSupplier nanos;
    private SourceAssignment assignment;
    private OwnerLease lease;
    private ShardStore store;
    private TargetWorkerSourceRuntimeFactory.Activated activation;
    private TargetWorkerSourceRuntimeFactory.AssemblyFailure failedAssembly;
    private TargetWorkerSourceRuntimeFactory.Recovery recovery;
    private TargetWorkerRecoverySource recoverySource;
    private CheckpointInputFuture.Cleanup recoveryOpeningCleanup;
    private TargetOwnedBackendGroup recoveryOpeningBackend;
    private com.nereusstream.delay.store.RecoveryCatalogAuthority checkpointCatalog;
    private com.nereusstream.delay.protocol.RecoveryPin checkpointPin;
    private boolean ownsCheckpointPin;
    private boolean checkpointPinConfirmed;
    private boolean checkpointPinReleased;
    private Throwable checkpointPinFailure;
    private com.nereusstream.delay.store.CheckpointRestoreWorkClassExecutor.Submission restoreSubmission;
    private Throwable recoveryCleanupFailure;
    private TargetOwnedBackendGroup backend;
    private TargetWorkerShardRuntime worker;
    private TargetOwnerDrainCoordinator.ReplayExit failedWorkerExit;
    private boolean stopping;
    private boolean storeClosed;
    private boolean leaseReleased;
    private boolean ownerLost;
    private boolean initializationRunning = true;
    private boolean failedCleanupComplete;
    private Thread constructionThread;
    private long lastNow = -1;
    private Throwable firstFailure;
    private PulsarPreparation pulsarPreparation;
    private boolean initialCommitted;
    private com.nereusstream.delay.protocol.OwnerIdentity registryIdentity;

    /** Only this startup can issue a receipt for its unexposed Source/runtime and actual Store barrier. */
    public synchronized Optional<PulsarPreparation> preparePulsarSource() {
        process.requireSourceAssemblyAdmission(this);
        if (pulsarPreparation != null) {
            pulsarPreparation.requireCurrent();
            return Optional.of(pulsarPreparation);
        }
        requireRecoveryReaderCurrent();
        if (initializationRunning || constructionThread != null || worker != null || firstFailure != null
                || !(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier)) {
            throw new IllegalStateException("P1 preparation requires its original unexposed startup");
        }
        if (recovery != null) {
            if (!recovery.coordinator().sourceReadyForReactivation()
                    || recoverySource == null || !recoverySource.cleanupComplete()) return Optional.empty();
            recovery.coordinator().stopAuthorityPreparation(); recovery.coordinator().retryAuthorityCleanup();
            if (!recovery.coordinator().authorityIoExited()) return Optional.empty();
        } else {
            return Optional.empty();
        }
        if (activation != null) {
            activation.runtime().fence(); activation.runtime().retryAuthorityCleanup();
            if (!activation.runtime().authorityIoExited()) return Optional.empty();
        }
        final var current = graph.oxia().providers().leases().current(assignment.shardId()).orElseThrow();
        if (!lease.sameIdentity(current) || current.state() != ShardLifecycleState.ACQUIRING
                && current.state() != ShardLifecycleState.CATCHING_UP
                && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Source preparation lost its original pre-fence Owner");
        }
        final var reads = TargetWorkerSourceRuntimeFactory.startupReads(graph.oxia(), owner.tenant(), publication,
                owner.workerId(), store, assignment, lease, content.reads(), owner.clock());
        final long sequence = store.latestSequenceNumber();
        TargetStoreBootstrap.reopen(store, scope,
                new TargetStoreBackend.WriteLimits(graph.config().source().records(), graph.config().source().bytes()),
                graph.config().checkpoint().newReadBudget(nanos), reads, graph.config().checkpoint().ledger());
        final var cut = Objects.requireNonNull(store.appliedShardLogPosition());
        if (!assignment.activationBarrier().reachedBy(cut)) return Optional.empty();
        pulsarPreparation = new PulsarPreparation(this, publication, current, cut, sequence);
        try { pulsarPreparation.fenceOriginal(); }
        catch (RuntimeException | Error failure) { retain(failure); throw new StartupFailure(failure, this); }
        return Optional.of(pulsarPreparation);
    }

    /** SDK preparation only; a frozen Owner never becomes an ordinary Source/Store authority. */
    public synchronized void requirePulsarOpeningAuthority() {
        if (pulsarPreparation == null) requireCurrent();
        else pulsarPreparation.requireCurrent();
    }

    public synchronized Construction beginPulsarClientOpening() {
        if (initializationRunning) throw new IllegalStateException("P1 client opening precedes its actual initial Store boundary");
        if (!(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier)) {
            throw new IllegalStateException("P1 client opening changes the original Source kind");
        }
        return beginConstruction(false);
    }

    public synchronized Construction beginPulsarConsumerOpening(PulsarPreparation original) {
        if (original != pulsarPreparation || constructionThread != null || worker != null) {
            throw new IllegalStateException("prepared P1 consumer construction belongs to another original call");
        }
        original.requireCurrent();
        final var current = Thread.currentThread();
        final var construction = new Construction(this, current, original);
        constructionThread = current;
        return construction;
    }

    public synchronized boolean needsPulsarRecovery() {
        requirePulsarOpeningAuthority();
        return store != null && !storeClosed && activation == null && recovery == null;
    }

    public synchronized boolean pulsarRecoveryPending() {
        requirePulsarOpeningAuthority();
        return recovery != null && recoverySource != null && activation == null;
    }

    public static final class PulsarPreparation {
        private final TargetWorkerSourceStartup startup;
        private final WorkerAssignmentAuthority.Publication originalPublication;
        private final OwnerLease originalLease;
        private final com.nereusstream.delay.protocol.SourcePosition cut;
        private final long sequence;
        private OwnerLease fencedLease;
        private ShardStore.FrozenSourceCut frozen;
        private final TargetWorkerSourceRuntimeFactory.Recovery previousRecovery;
        private final TargetWorkerRecoverySource previousReader;
        private final com.nereusstream.delay.protocol.RecoveryPin previousPin;
        private WorkerAssignment successor;
        private WorkerAssignmentAuthority.Publication published;
        private OwnerLease acquired;
        private com.nereusstream.delay.protocol.RecoveryPin replacementPin;
        private boolean openedSuccessor;
        private PulsarPreparation(TargetWorkerSourceStartup startup, WorkerAssignmentAuthority.Publication publication,
                OwnerLease lease, com.nereusstream.delay.protocol.SourcePosition cut, long sequence) {
            this.startup = startup; originalPublication = publication; originalLease = lease; this.cut = cut; this.sequence = sequence;
            previousRecovery = startup.recovery; previousReader = startup.recoverySource; previousPin = startup.checkpointPin;
        }
        private void fenceOriginal() {
            final var leases = startup.graph.oxia().providers().leases();
            fencedLease = leases.transitionOrRead(originalLease, ShardLifecycleState.FENCED)
                    .orElseThrow(() -> new IllegalStateException("original Source fence CAS was not observed"));
            if (!originalLease.sameIdentity(fencedLease) || fencedLease.state() != ShardLifecycleState.FENCED) {
                throw new IllegalStateException("Source fence changed the original Owner identity");
            }
            startup.lease = fencedLease;
            frozen = startup.store.freezeTargetSourceCut(cut, sequence);
            requireCurrent();
        }
        public com.nereusstream.delay.protocol.SourcePosition position() { requireCurrent(); return cut; }
        public SourceAssignment assignment() { requireCurrent(); return originalPublication.assignment().sourceAssignment(); }
        void requirePriorBackend(TargetOwnedBackendGroup actual) {
            requireCurrent();
            if (previousReader == null || !previousReader.cleanupComplete() || previousReader.backend() != actual) {
                throw new IllegalArgumentException("backend is not the original retired preparation reader's SDK owner");
            }
        }
        /** Advances one original consumer/authority handoff; the consumer supplies actual SDK facts, never publication authority. */
        public boolean advanceOwner(SourceRecordConsumer prepared) {
            synchronized (startup) {
                requireCurrent();
                if (prepared.ownedBackendGroup() != startup.backend || startup.backend == null) {
                    throw new IllegalArgumentException("prepared P1 consumer belongs to another original SDK owner");
                }
                final var barrier = prepared.preparedPulsarBarrier(this);
                if (successor == null) {
                    final var previous = originalPublication.assignment();
                    final var id = java.util.UUID.randomUUID();
                    final byte[] assignmentId = Bytes.concat(Bytes.u64beBits(id.getMostSignificantBits()), Bytes.u64beBits(id.getLeastSignificantBits()));
                    final var source = new SourceAssignment(previous.sourceAssignment().shardId(), assignmentId,
                            nextEpoch(previous.sourceAssignment().assignmentEpoch()), barrier);
                    new PulsarSourceReactivation(previous.routeSnapshotDigest(), previous.sourceAssignment(), source);
                    successor = new WorkerAssignment(previous.workerId(), source, nextEpoch(previous.placementEpoch()),
                            previous.capacityEnvelopeDigest(), previous.routeSnapshotDigest());
                } else if (!successor.sourceAssignment().activationBarrier().equals(barrier)) {
                    throw new IllegalStateException("prepared P1 connection changed after the original successor intent");
                }
                if (openedSuccessor) return true;
                if (published == null) {
                    frozen.closeOriginal();
                    if (!frozen.originalClosed()) return false;
                    startup.storeClosed = true;
                    final var providers = startup.graph.oxia().providers();
                    final var route = new RouteSourceAssignmentResolver(providers.routes()).exactResolved(startup.owner.tenant(),
                            originalLease.shardId().routeIncarnation(), originalLease.shardId().partition(),
                            originalLease.sourceAssignmentId(), originalLease.sourceAssignmentEpoch()).routeSnapshot();
                    final var transition = new PulsarSourceReactivation(originalPublication.assignment().routeSnapshotDigest(),
                            originalPublication.assignment().sourceAssignment(), successor.sourceAssignment());
                    final var coordinator = new PulsarSourceReactivationCoordinator(providers.routes(), providers.assignments(), providers.leases());
                    published = coordinator.publishSuccessor(new PulsarSourceReactivationCoordinator.FencedPlan(
                            originalPublication, originalLease, fencedLease, transition, route), successor, this::requireCurrent);
                    startup.publication = published; startup.leaseReleased = true;
                }
                prepared.preparedPulsarBarrier(this);
                if (acquired == null) {
                    acquired = TargetWorkerSourceRuntimeFactory.acquire(startup.graph.oxia(),
                            startup.owner.tenant(), published, startup.owner.workerId(), startup.owner.ownerId(),
                            startup.owner.leaseDurationMs(), startup.owner.clock());
                    startup.assignment = successor.sourceAssignment(); startup.lease = acquired; startup.leaseReleased = false;
                }
                if (!replacePin()) return false;
                if (!openedSuccessor) {
                    final var reopened = ShardStore.openTarget(startup.graph.config().store(), originalLease.shardId(), startup.graph.resources());
                    startup.store = reopened; startup.storeClosed = false;
                    if (!Arrays.equals(frozen.storeIncarnation(), reopened.metadata().storeIncarnation())
                            || !Bytes.constantTimeEquals(cut.canonicalBytes(), reopened.appliedShardLogPosition().canonicalBytes())) {
                        throw new IllegalStateException("successor Store changed the original positioned incarnation/cut");
                    }
                    startup.activation = null; startup.failedAssembly = null;
                    startup.recovery = null; startup.recoverySource = null;
                    startup.recoveryCleanupFailure = null;
                    openedSuccessor = true;
                }
                requireCurrent();
                return true;
            }
        }

        private boolean replacePin() {
            if (previousPin == null) return true;
            final var catalog = startup.checkpointCatalog;
            if (startup.ownsCheckpointPin) {
                if (replacementPin == null) {
                    final var floor = catalog.currentFloorRef().orElseThrow();
                    final var id = java.util.UUID.randomUUID();
                    replacementPin = new com.nereusstream.delay.protocol.RecoveryPin(
                            Bytes.concat(Bytes.u64beBits(id.getMostSignificantBits()), Bytes.u64beBits(id.getLeastSignificantBits())),
                            previousPin.shard(), new com.nereusstream.delay.protocol.OwnerIdentity(
                                    previousPin.owner().deploymentId(), previousPin.owner().workerRunId(), acquired.ownerEpoch(), acquired.leaseToken()),
                            previousPin.candidate(), floor, floor.catalogGeneration(), startup.graph.oxia().providers().sessionIdentity());
                }
                final var actual = catalog.replaceRecoveryPin(previousPin, replacementPin);
                if (!replacementPin.equals(actual)) throw new IllegalStateException("successor pin changed its exact request");
            } else {
                final var actual = catalog.activeRecoveryPin().orElseThrow();
                if (actual.equals(previousPin)) return false;
                if (!actual.shard().equals(previousPin.shard()) || !actual.candidate().equals(previousPin.candidate())
                        || actual.owner().ownerEpoch() != acquired.ownerEpoch()
                        || !Bytes.constantTimeEquals(actual.owner().leaseFencingDigest(), acquired.leaseToken())
                        || !Bytes.constantTimeEquals(actual.oxiaSessionIdentityDigest(), acquired.sessionIdentity())) {
                    throw new IllegalStateException("external successor pin lacks its actual new Owner/candidate/session");
                }
                replacementPin = actual;
            }
            startup.checkpointPin = replacementPin; startup.checkpointPinConfirmed = true;
            return true;
        }

        private static long nextEpoch(long epoch) {
            if (epoch == -1L) throw new IllegalStateException("Source reactivation epoch exhausted");
            return epoch + 1;
        }
        /** FENCED permits only this frozen-cut/SDK preparation check, never an ordinary Store write guard. */
        public void requireCurrent() {
            synchronized (startup) {
                startup.process.requireSourceAssemblyAdmission(startup);
                if (startup.stopping || startup.firstFailure != null || startup.pulsarPreparation != this || frozen == null) {
                    throw new IllegalStateException("original P1 preparation is not available");
                }
                final var providers = startup.graph.oxia().providers();
                final var publication = providers.assignments().current(originalLease.shardId()).orElseThrow();
                final var current = providers.leases().current(originalLease.shardId());
                long now = startup.owner.clock().getAsLong();
                if (!(published == null ? originalPublication : published).sameIdentity(publication)
                        || now < 0 || now < startup.lastNow
                        || !Bytes.constantTimeEquals(originalLease.sessionIdentity(), providers.sessionIdentity())) {
                    throw new IllegalStateException("P1 preparation lost its original publication/Owner/session/time");
                }
                if (published == null) {
                    final var actual = current.orElseThrow();
                    if (!fencedLease.sameIdentity(actual) || actual.state() != ShardLifecycleState.FENCED || !actual.validAt(now)) {
                        throw new IllegalStateException("P1 preparation lost its exact fenced Owner");
                    }
                } else if (acquired == null) {
                    if (current.isPresent()) throw new IllegalStateException("another Owner entered the successor acquisition gap");
                } else {
                    final var actual = current.orElseThrow();
                    if (!acquired.sameIdentity(actual) || !actual.validAt(now)
                            || actual.state() != ShardLifecycleState.ACQUIRING && actual.state() != ShardLifecycleState.CATCHING_UP
                                && actual.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                        throw new IllegalStateException("P1 preparation lost its actual successor Owner");
                    }
                }
                startup.lastNow = now;
                frozen.requireCurrent();
                previousRecovery.coordinator().requireSourcePreparationProtection();
                if (startup.checkpointCatalog != null) {
                    final var actual = startup.checkpointCatalog.activeRecoveryPin().orElseThrow();
                    if (!startup.ownsCheckpointPin && acquired != null && !actual.equals(previousPin) && replacementPin == null) {
                        if (!actual.shard().equals(previousPin.shard()) || !actual.candidate().equals(previousPin.candidate())
                                || actual.owner().ownerEpoch() != acquired.ownerEpoch()
                                || !Bytes.constantTimeEquals(actual.owner().leaseFencingDigest(), acquired.leaseToken())
                                || !Bytes.constantTimeEquals(actual.oxiaSessionIdentityDigest(), acquired.sessionIdentity())) {
                            throw new IllegalStateException("external pin changed outside the actual successor Owner/candidate/session");
                        }
                        replacementPin = actual;
                    }
                    if (!actual.equals(previousPin) && (replacementPin == null || !actual.equals(replacementPin))) {
                        throw new IllegalStateException("P1 preparation lost its original or exact replacement pin");
                    }
                    if (startup.checkpointPinConfirmed && replacementPin != null && startup.checkpointPin.equals(replacementPin)
                            && !actual.equals(replacementPin)) throw new IllegalStateException("confirmed successor pin was lost");
                }
            }
        }
    }

    public synchronized com.nereusstream.delay.protocol.OwnerIdentity currentPulsarOwnerIdentity() {
        requirePulsarOpeningAuthority();
        if (pulsarPreparation != null && pulsarPreparation.acquired == null) {
            throw new IllegalStateException("P1 successor Owner has not been acquired");
        }
        final var original = Objects.requireNonNull(registryIdentity, "original verified registry identity");
        return new com.nereusstream.delay.protocol.OwnerIdentity(original.deploymentId(), original.workerRunId(),
                lease.ownerEpoch(), lease.leaseToken());
    }

    TargetWorkerSourceStartup(TargetWorkerProcessRuntime process, TargetWorkerProcessRuntime.Assembly graph,
            Owner owner, Initial initial, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor successor, LongSupplier nanos) {
        this.process = process; this.graph = graph; this.owner = Objects.requireNonNull(owner); this.publication = owner.publication();
        this.initial = Objects.requireNonNull(initial); this.content = Objects.requireNonNull(content);
        this.scope = initial.scope();
        this.successor = Objects.requireNonNull(successor); this.nanos = nanos;
    }

    TargetWorkerSourceStartup(TargetWorkerProcessRuntime process, TargetWorkerProcessRuntime.Assembly graph,
            Owner owner, TargetQuotaScope scope, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor activeSuccessor, LongSupplier nanos) {
        this.process = process; this.graph = graph; this.owner = Objects.requireNonNull(owner); this.publication = owner.publication();
        this.initial = null; this.scope = Objects.requireNonNull(scope); this.content = Objects.requireNonNull(content);
        this.successor = Objects.requireNonNull(activeSuccessor); this.nanos = nanos;
        if (scope.target() != null) throw new IllegalArgumentException("Source reopen requires its original Shard scope");
    }

    /** Opens the actual same-DB Target Store under the newly acquired original assignment/session lease. */
    void openReopened() {
        acquireExisting(true);
    }

    void acquireCheckpoint() { acquireExisting(false); }

    /** Creates one exact ephemeral pin through this root's actual shared intent/Catalog backend. */
    public synchronized com.nereusstream.delay.protocol.RecoveryPin createCheckpointPin(String exactShardPrefix,
            byte[] pinId, com.nereusstream.delay.protocol.OwnerIdentity identity,
            com.nereusstream.delay.protocol.RecoveryCandidateRef candidate) {
        requireCurrent();
        if (initial != null || store != null || restoreSubmission != null || checkpointPin != null) {
            throw new IllegalStateException("original checkpoint pin request cannot be replaced");
        }
        if (identity.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalArgumentException("checkpoint pin identity changes its actual acquired Owner");
        }
        final var actual = graph.oxia().providers().checkpointPublication(exactShardPrefix, graph.config().checkpoint().physical());
        final var manifest = actual.manifest(candidate.checkpointId()).orElseThrow();
        if (!manifest.shardId().equals(scope.shard())
                || !Bytes.constantTimeEquals(manifest.manifestSha256(), candidate.manifestSha256())
                || !Bytes.constantTimeEquals(manifest.recoveryLineageId(), candidate.recoveryLineageId())) {
            throw new IllegalArgumentException("checkpoint pin candidate changes its original published image");
        }
        final var floor = actual.currentFloorRef().orElseThrow();
        final var requested = new com.nereusstream.delay.protocol.RecoveryPin(pinId,
                new com.nereusstream.delay.protocol.ShardSubject(scope.shard()), identity, candidate, floor,
                floor.catalogGeneration(), graph.oxia().providers().sessionIdentity());
        checkpointCatalog = actual; checkpointPin = requested; ownsCheckpointPin = true;
        return retryCheckpointPinCreation();
    }

    /** The original backend owns exact-CAS response-loss reconciliation; no new request or successful pin is guessed. */
    public synchronized com.nereusstream.delay.protocol.RecoveryPin retryCheckpointPinCreation() {
        requireCurrent();
        if (!ownsCheckpointPin || checkpointPin == null || checkpointPinReleased) throw new IllegalStateException("no original pin creation is pending");
        try {
            if (!checkpointPinConfirmed) {
                final var actual = checkpointCatalog.createRecoveryPin(checkpointPin);
                if (!checkpointPin.equals(actual)) throw new IllegalStateException("actual pin CAS changed its original request");
                checkpointPinConfirmed = true;
            }
            requireCheckpointPin(); return checkpointPin;
        } catch (RuntimeException | Error failure) {
            if (checkpointPinFailure == null) checkpointPinFailure = failure;
            throw failure;
        }
    }

    private void requireCheckpointPin() {
        if (checkpointPinReleased || !checkpointCatalog.activeRecoveryPin().orElseThrow().equals(checkpointPin)) {
            throw new IllegalStateException("original checkpoint pin is no longer active");
        }
    }

    /** Reopens only a previously installed image for original Catalog/Pin recovery; it never downloads over ACTIVE. */
    public synchronized void resumeInstalledCheckpoint() {
        if (!ownsCheckpointPin || !checkpointPinConfirmed) throw new IllegalStateException("original owned pin is not confirmed");
        resumeInstalledCheckpoint(checkpointCatalog, checkpointPin);
    }

    public synchronized void resumeInstalledCheckpoint(com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.protocol.RecoveryPin pin) {
        requireCurrent();
        if (initial != null || store != null || restoreSubmission != null || initializationRunning
                || checkpointPin != null && (checkpointCatalog != catalog || !checkpointPin.equals(pin))) {
            throw new IllegalStateException("original installed checkpoint startup cannot be replaced");
        }
        if (ownsCheckpointPin && !checkpointPinConfirmed) throw new IllegalStateException("original pin creation is not confirmed");
        requirePinOwner(pin);
        checkpointCatalog = Objects.requireNonNull(catalog); checkpointPin = pin;
        requireCheckpointPin();
        try {
            store = ShardStore.openTarget(graph.config().store(), scope.shard(), graph.resources());
            requireCurrent();
            final var base = store.recoveryMetadata().lineageBase();
            final var install = store.recoveryMetadata().installState();
            if (base == null || install == null || install.phase() == com.nereusstream.delay.protocol.RecoveryInstallPhase.STAGED
                    || install.phase() == com.nereusstream.delay.protocol.RecoveryInstallPhase.FRESH
                    || !Bytes.constantTimeEquals(base.storeIncarnation(), store.metadata().storeIncarnation())
                    || !Bytes.constantTimeEquals(base.checkpointId(), pin.candidate().checkpointId())
                    || !Bytes.constantTimeEquals(base.manifestSha256(), pin.candidate().manifestSha256())
                    || !Bytes.constantTimeEquals(base.recoveryLineageId(), pin.candidate().recoveryLineageId())
                    || pin.candidate().storeIncarnation() != null
                        && !Bytes.constantTimeEquals(pin.candidate().storeIncarnation(), store.metadata().storeIncarnation())) {
                throw new IllegalStateException("installed checkpoint changed its original base/Store/pin identity");
            }
            checkpointCatalog.validatePublishedRestoreCandidate(checkpointCatalog.manifest(base.checkpointId()).orElseThrow());
            requireCheckpointPin();
        } catch (RuntimeException | Error failure) { retain(failure); throw new StartupFailure(failure, this); }
    }

    private void requirePinOwner(com.nereusstream.delay.protocol.RecoveryPin pin) {
        if (!pin.shard().shardId().equals(scope.shard()) || pin.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(pin.owner().leaseFencingDigest(), lease.leaseToken())
                || !Bytes.constantTimeEquals(pin.oxiaSessionIdentityDigest(), graph.oxia().providers().sessionIdentity())) {
            throw new IllegalArgumentException("checkpoint pin changes its actual Source Owner/session/Shard");
        }
    }

    private void acquireExisting(boolean openStore) {
        try {
            assignment = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), publication, owner.workerId());
            TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, scope);
            lease = TargetWorkerSourceRuntimeFactory.acquire(graph.oxia(), owner.tenant(), publication,
                    owner.workerId(), owner.ownerId(), owner.leaseDurationMs(), owner.clock());
            requireCurrent();
            if (openStore) store = ShardStore.openTarget(graph.config().store(), scope.shard(), graph.resources());
            requireCurrent();
        } catch (RuntimeException | Error failure) {
            retain(failure); throw new StartupFailure(failure, this);
        } finally { synchronized (this) { initializationRunning = false; } }
    }

    /** Queues authenticated published download/install in the original bounded CHECKPOINT class. */
    public synchronized void restoreCheckpoint(com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images, Runnable protection) {
        if (!ownsCheckpointPin || !checkpointPinConfirmed) throw new IllegalStateException("original owned pin is not confirmed");
        restoreCheckpoint(request, checkpointPin, checkpointCatalog, downloader, images, protection);
    }

    public synchronized void restoreCheckpoint(com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.protocol.RecoveryPin pin, com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images, Runnable protection) {
        requireCurrent();
        if (initial != null || initializationRunning || store != null || restoreSubmission != null
                || checkpointPin != null && (checkpointCatalog != catalog || !checkpointPin.equals(pin))) {
            throw new IllegalStateException("original published checkpoint startup cannot be replaced");
        }
        Objects.requireNonNull(request); Objects.requireNonNull(pin); Objects.requireNonNull(catalog);
        if (ownsCheckpointPin && !checkpointPinConfirmed) throw new IllegalStateException("original pin creation is not confirmed");
        if (!request.manifest().shardId().equals(scope.shard()) || !pin.shard().shardId().equals(scope.shard())
                || pin.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(pin.owner().leaseFencingDigest(), lease.leaseToken())
                || !Bytes.constantTimeEquals(pin.oxiaSessionIdentityDigest(), graph.oxia().providers().sessionIdentity())) {
            throw new IllegalArgumentException("published checkpoint pin changes its actual Source Owner/session/Shard");
        }
        final var configured = graph.config().checkpoint();
        final Runnable held = () -> { requireCurrent(); Objects.requireNonNull(protection).run(); };
        final var authentication = new ShardStore.TargetRestoreAuthentication(configured.quota(), configured.ledger(),
                configured.semantics(), Objects.requireNonNull(images), held, configured.newReadBudget(nanos));
        final var coordinator = new com.nereusstream.delay.store.CheckpointRestoreCoordinator(graph.config().store(),
                scope.shard(), graph.resources(), downloader, catalog, configured.physical());
        final var executor = new com.nereusstream.delay.store.CheckpointRestoreWorkClassExecutor(graph.classes(), coordinator);
        checkpointCatalog = catalog; checkpointPin = pin;
        try {
            restoreSubmission = executor.submit(new com.nereusstream.delay.store.CheckpointRestoreWorkClassExecutor.RestoreRequest(
                    request, pin, authentication));
        } catch (RuntimeException | Error failure) { retain(failure); throw new StartupFailure(failure, this); }
    }

    /** Observes only the original restore action after its registry execution has returned. */
    public synchronized boolean finishCheckpointRestore() {
        requireCurrent();
        if (restoreSubmission == null) throw new IllegalStateException("published restore has not been queued");
        if (store != null) return true;
        if (graph.classes().state(restoreSubmission.task()).orElse(null)
                == com.nereusstream.delay.scheduler.WorkClassExecutionRegistry.ExecutionState.RUNNING) return false;
        final var outcome = restoreSubmission.outcome();
        if (outcome.isEmpty()) return false;
        if (outcome.get().failure() != null) {
            retain(outcome.get().failure()); throw new StartupFailure(outcome.get().failure(), this);
        }
        store = outcome.get().restored();
        try { requireCurrent(); return true; }
        catch (RuntimeException | Error failure) { retain(failure); throw new StartupFailure(failure, this); }
    }

    /** The cut is obtained under the acquired Owner and an actual bounded root/physical audit. */
    public synchronized com.nereusstream.delay.protocol.SourcePosition auditedRecoveryCut() {
        requireRecoveryReaderCurrent();
        if (initial != null && !initialCommitted || store == null || activation != null || recovery != null || worker != null
                || constructionThread != Thread.currentThread()) {
            throw new IllegalStateException("recovery cut requires its original startup construction and acquired Store");
        }
        final var reads = TargetWorkerSourceRuntimeFactory.startupReads(graph.oxia(), owner.tenant(),
                publication, owner.workerId(), store, assignment, lease, content.reads(), owner.clock());
        final var config = graph.config();
        com.nereusstream.delay.runtime.TargetStoreBootstrap.reopen(store, scope,
                new com.nereusstream.delay.store.TargetStoreBackend.WriteLimits(config.source().records(), config.source().bytes()),
                config.checkpoint().newReadBudget(nanos), reads, config.checkpoint().ledger());
        requireRecoveryReaderCurrent();
        return Objects.requireNonNull(store.appliedShardLogPosition(), "audited recovery Source cut");
    }

    public synchronized void requireRecoveryReaderCurrent() {
        requireCurrent();
        if (checkpointCatalog != null) {
            requireCheckpointPin();
            checkpointCatalog.validatePublishedRestoreCandidate(checkpointCatalog.manifest(
                    checkpointPin.candidate().checkpointId()).orElseThrow());
            requireCurrent();
        }
    }

    /** Adopts the exact verified reader before auditing/replaying; never takes an arbitrary iterator or issues ACK. */
    public void startRecovery(TargetWorkerRecoverySource source, com.nereusstream.delay.protocol.OwnerIdentity identity,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal, Runnable sourceProtection, Runnable journalProtection) {
        final Construction original;
        synchronized (this) {
            if (initial != null && !initialCommitted || initializationRunning || store == null
                    || recoverySource != null && recoverySource != source || recovery != null || activation != null) {
                throw new IllegalStateException("original same-DB recovery is unavailable or already started");
            }
            original = beginConstruction(false);
        }
        try (original) {
            original.requireCurrent();
            if (identity.ownerEpoch() != lease.ownerEpoch() || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())
                    || registryIdentity != null && (!Arrays.equals(registryIdentity.deploymentId(), identity.deploymentId())
                            || !Arrays.equals(registryIdentity.workerRunId(), identity.workerRunId()))) {
                throw new IllegalArgumentException("Source recovery changed its actual Owner or deployment/worker run identity");
            }
            registryIdentity = identity;
            Objects.requireNonNull(source).requireStartup(assignment, graph.config().maximumSourceFrameBytes(),
                    graph.classes(), graph.resources());
            synchronized (this) { recoverySource = source; }
            source.replayCursor().requireGuarded();
            recovery = checkpointCatalog == null ? TargetWorkerSourceRuntimeFactory.reopen(graph.oxia(), graph.config(), graph.classes(),
                    graph.resources(), store, scope, owner.tenant(), publication, owner.workerId(), lease,
                    source.replayCursor(), source.successorAfter(store.appliedShardLogPosition()), content, identity, journal,
                    sourceProtection, journalProtection, owner.clock(), nanos)
                    : TargetWorkerSourceRuntimeFactory.recoverInstalled(graph.oxia(), graph.config(), graph.classes(),
                            graph.resources(), store, scope, owner.tenant(), publication, owner.workerId(), lease,
                            source.replayCursor(), source.successorAfter(store.appliedShardLogPosition()), content, identity, journal,
                            sourceProtection, journalProtection, owner.clock(), nanos, checkpointCatalog, checkpointPin);
            if (assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier) {
                recovery.coordinator().bindReader(source, store.appliedShardLogPosition());
                if (pulsarPreparation == null) recovery.coordinator().deferForSourceReactivation();
            }
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerSourceRuntimeFactory.AssemblyFailure partial) {
                synchronized (this) { failedAssembly = partial; lease = partial.lease(); }
            }
            retain(failure); throw new StartupFailure(failure, this);
        }
    }

    /** One bounded no-ACK recovery turn; active Source remains unavailable while native reader cleanup is pending. */
    public OwnerRecoveryTurn runRecoveryTurn() {
        final Construction original;
        synchronized (this) {
            if (pulsarPreparation != null && recovery == pulsarPreparation.previousRecovery) {
                pulsarPreparation.requireCurrent();
                return recovery.coordinator().pausedSourceReactivationTurn();
            }
            if (recovery == null || recoverySource == null || activation != null) throw new IllegalStateException("original recovery is not pending");
            original = beginConstruction(false);
        }
        try (original) {
            original.requireCurrent();
            var turn = recovery.coordinator().runTurn();
            if (recovery.coordinator().sourceReadyForReactivation()) {
                synchronized (this) { lease = recovery.coordinator().sourcePreparationLease(); }
                try { recoverySource.close(); }
                catch (RuntimeException | Error failure) {
                    synchronized (this) { if (recoveryCleanupFailure == null) recoveryCleanupFailure = failure; }
                }
                if (recoverySource.cleanupComplete()) recovery.coordinator().releaseReaderForSourceReactivation(recoverySource);
                return turn;
            }
            if (turn.complete()) {
                synchronized (this) { lease = recovery.coordinator().activeLease(); }
                try { recoverySource.close(); }
                catch (RuntimeException | Error failure) {
                    synchronized (this) { if (recoveryCleanupFailure == null) recoveryCleanupFailure = failure; }
                    return turn;
                }
                if (recoverySource.cleanupComplete()) {
                    synchronized (this) { activation = recovery.activateCompleted(successor); }
                }
            }
            return turn;
        } catch (RuntimeException | Error failure) { retain(failure); throw new StartupFailure(failure, this); }
    }

    void initialize() {
        try {
            assignment = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), publication, owner.workerId());
            if (!assignment.shardId().equals(initial.scope().shard())) {
                throw new IllegalArgumentException("initial grant belongs to another accepted Source Shard");
            }
            TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, initial.scope());
            assignment.activationBarrier().validatePosition(initial.entry().position());
            if (assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier barrier) {
                if (initial.entry().sourceConnectionGeneration() == null || initial.entry().guardAttestationDigest() == null) {
                    throw new IllegalArgumentException("initial Pulsar grant lacks its original native proof");
                }
                barrier.validateSourceConnection(initial.entry().sourceConnectionGeneration(), initial.entry().guardAttestationDigest());
            } else if (initial.entry().sourceConnectionGeneration() != null || initial.entry().guardAttestationDigest() != null) {
                throw new IllegalArgumentException("initial Kafka grant carries a foreign connection proof");
            }
            lease = TargetWorkerSourceRuntimeFactory.acquire(graph.oxia(), owner.tenant(), publication,
                    owner.workerId(), owner.ownerId(), owner.leaseDurationMs(), owner.clock());
            requireCurrent();
            store = ShardStore.openTarget(graph.config().store(), assignment.shardId(), graph.resources());
            requireCurrent();
            var prepared = TargetStoreBootstrap.prepareWithControlSnapshot(store, initial.scope(), initial.lineage(),
                    new TargetStoreBackend.WriteLimits(graph.config().source().records(), graph.config().source().bytes()),
                    graph.config().checkpoint().newReadBudget(nanos), initial.prepared(), initial.entry().mutation(),
                    initial.entry().position(), initial.grants(), initial.start(), initial.snapshot(), initial.snapshotAuthority());
            var committed = TargetStoreBootstrap.commit(prepared, (metadata, scope, mutation) -> {
                requireCurrent();
                var original = Objects.requireNonNull(content.physical().acquire(metadata, scope, mutation));
                return new TargetStoreBackend.CommitGuard() {
                    @Override public void requireCurrent() { TargetWorkerSourceStartup.this.requireCurrent(); original.requireCurrent(); }
                    @Override public void close() { original.close(); }
                };
            });
            initialCommitted = true;
            if (assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier) return;
            activation = TargetWorkerSourceRuntimeFactory.initialize(graph.oxia(), graph.config(), graph.classes(),
                    graph.resources(), committed, store, owner.tenant(), publication, owner.workerId(), lease,
                    successor, content, owner.clock(), nanos);
            lease = activation.runtime().snapshotDrainLease();
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerSourceRuntimeFactory.AssemblyFailure partial) {
                failedAssembly = partial; lease = partial.lease();
            }
            retain(failure);
            throw new StartupFailure(failure, this);
        } finally { synchronized (this) { initializationRunning = false; } }
    }

    /** Actual Route/assignment/session/Owner reads are bounded by the original configured SDK timeout. */
    public synchronized void requireCurrent() {
        process.requireSourceAssemblyAdmission(this);
        if (stopping || firstFailure != null) throw new IllegalStateException("Source startup is stopped or failed");
        var accepted = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), publication, owner.workerId());
        var current = graph.oxia().providers().leases().current(accepted.shardId()).orElseThrow();
        long now = owner.clock().getAsLong();
        if (assignment == null || !assignment.sameIdentity(accepted) || lease == null || !lease.sameIdentity(current)
                || current.state() == ShardLifecycleState.FENCED || current.state() == ShardLifecycleState.FAILED
                || current.state() == ShardLifecycleState.UNASSIGNED
                || !Bytes.constantTimeEquals(current.sessionIdentity(), graph.oxia().providers().sessionIdentity())
                || !current.validAt(now) || now < 0 || now < lastNow
                || (recoverySource == null ? current.state() != lease.state()
                        : current.state() != ShardLifecycleState.ACQUIRING && current.state() != ShardLifecycleState.CATCHING_UP
                            && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                || store != null && Long.compareUnsigned(store.runtimeMetadata().lastOpenedOwnerEpoch(), lease.ownerEpoch()) > 0) {
            throw new IllegalStateException("Source startup lost its actual assignment/session/Owner/time authority");
        }
        lastNow = now;
        if (checkpointCatalog != null && checkpointPinConfirmed) requireCheckpointPin();
    }

    public synchronized TargetWorkerSourceRuntimeFactory.Activated activation() {
        if (activation == null || stopping || firstFailure != null) throw new IllegalStateException("Source activation is unavailable");
        return activation;
    }
    public synchronized ShardStore store() {
        if (store == null || storeClosed) throw new IllegalStateException("original Source Store is unavailable");
        return store;
    }
    public TargetWorkerProcessRuntime.Assembly graph() { return graph; }

    public synchronized SourceAssignment acceptedAssignment() {
        if (assignment == null) throw new IllegalStateException("original Source assignment has not been accepted");
        return assignment;
    }

    /** Actual reader factories own their original call until the cursor or native opening cleanup is retained. */
    public synchronized Construction beginRecoveryReaderConstruction() {
        if (initial != null && !initialCommitted || initializationRunning || store == null || recoverySource != null
                || recoveryOpeningCleanup != null || recovery != null || activation != null) {
            throw new IllegalStateException("original recovery reader construction is unavailable");
        }
        return beginConstruction(false);
    }

    public synchronized void retainRecoverySource(TargetWorkerRecoverySource original) {
        Objects.requireNonNull(original).requireStartup(assignment, graph.config().maximumSourceFrameBytes(),
                graph.classes(), graph.resources());
        if (recoverySource != null && recoverySource != original || recovery != null || activation != null) {
            throw new IllegalStateException("original recovery Source cannot be replaced");
        }
        recoverySource = original;
    }

    public synchronized void retainRecoveryOpeningCleanup(CheckpointInputFuture.Cleanup original) {
        if (recoveryOpeningCleanup != null && recoveryOpeningCleanup != original) {
            throw new IllegalStateException("original recovery opening cleanup cannot be replaced");
        }
        recoveryOpeningCleanup = Objects.requireNonNull(original);
    }

    public synchronized void retainRecoveryOpeningBackend(TargetOwnedBackendGroup original) {
        Objects.requireNonNull(original).requireWorkerGraph(graph.classes(), graph.resources());
        if (recoveryOpeningBackend != null && recoveryOpeningBackend != original) {
            throw new IllegalStateException("original recovery opening backend cannot be replaced");
        }
        recoveryOpeningBackend = original;
    }

    /** Original acquired/activated lease for binding the deployment's existing OwnerIdentity; never inferred on failure. */
    public synchronized OwnerLease ownerLease() {
        if (lease == null || stopping || firstFailure != null) throw new IllegalStateException("original acquired Source lease is unavailable");
        return lease;
    }

    /** Original SDK factory construction is single flight and must return before the Store/lease can retire. */
    public synchronized Construction beginWorkerConstruction() {
        return beginConstruction(true);
    }

    private synchronized Construction beginConstruction(boolean active) {
        requireCurrent(); if (active) activation();
        if (worker != null || constructionThread != null) throw new IllegalStateException("Source Worker construction is already owned");
        final var original = Thread.currentThread();
        final var construction = new Construction(this, original);
        constructionThread = original;
        return construction;
    }

    public static final class Construction implements AutoCloseable {
        private final TargetWorkerSourceStartup startup;
        private final Thread original;
        private final PulsarPreparation preparation;
        private boolean closed;
        private Construction(TargetWorkerSourceStartup startup, Thread original) { this(startup, original, null); }
        private Construction(TargetWorkerSourceStartup startup, Thread original, PulsarPreparation preparation) {
            this.startup = startup; this.original = original; this.preparation = preparation;
        }
        public void requireCurrent() {
            synchronized (startup) {
                if (closed || Thread.currentThread() != original || startup.constructionThread != original) {
                    throw new IllegalStateException("Source construction lost its original call ownership");
                }
                if (preparation == null) startup.requireCurrent();
                else preparation.requireCurrent();
            }
        }
        @Override public void close() {
            synchronized (startup) {
                if (Thread.currentThread() != original || startup.constructionThread != original && !closed) {
                    throw new IllegalStateException("Source construction exit belongs to another original call");
                }
                if (!closed) { startup.constructionThread = null; closed = true; }
            }
        }
    }

    /** The SDK factory registers its original owner before constructing or exposing the Worker. */
    public synchronized void registerBackend(TargetOwnedBackendGroup actual) {
        // Retain a late original allocation even if stop won the race; admission is checked separately.
        if (worker != null || backend != null && backend != actual) {
            throw new IllegalStateException("Source SDK owner cannot be replaced");
        }
        Objects.requireNonNull(actual).requireWorkerGraph(graph.classes(), graph.resources()); backend = actual;
    }

    public void admitWorker(TargetWorkerShardRuntime actual) {
        requireCurrent();
        graph.oxia().requireWorker(actual);
        synchronized (this) {
            activation().requireGraph(graph.classes(), graph.resources(), store());
            actual.requireSourceStartup(activation.runtime(), store);
            if (recoverySource != null) actual.retainRecoveryBackend(recoverySource.backend());
            if (pulsarPreparation != null && pulsarPreparation.previousReader != null) {
                actual.retainPriorRecoveryBackend(pulsarPreparation, pulsarPreparation.previousReader.backend());
            }
            if (worker != null || backend == null || actual.sourceBackendGroup() != backend
                    || !actual.shardId().equals(assignment.shardId())) {
                throw new IllegalStateException("Source Worker does not own its original SDK/Store graph");
            }
            worker = actual;
        }
    }

    synchronized TargetWorkerShardRuntime workerForHost(TargetWorkerProcessRuntime expected) {
        if (process != expected || worker == null || stopping || firstFailure != null
                || initializationRunning || constructionThread != null) {
            throw new IllegalStateException("process Source startup is not ready for its original Host");
        }
        return worker;
    }
    public synchronized Optional<Throwable> firstFailure() { return Optional.ofNullable(firstFailure); }
    public synchronized Optional<Throwable> recoveryCleanupFailure() { return Optional.ofNullable(recoveryCleanupFailure); }
    public synchronized Optional<Throwable> checkpointPinFailure() { return Optional.ofNullable(checkpointPinFailure); }
    public synchronized boolean activationReady() { return activation != null && !stopping && firstFailure == null; }
    public synchronized void retain(Throwable failure) {
        if (firstFailure == null) firstFailure = failure;
        else if (firstFailure != failure) {
            for (var prior : firstFailure.getSuppressed()) if (prior == failure) return;
            firstFailure.addSuppressed(failure);
        }
    }

    /** Retains the failed pre-Host graph. A transferred Worker must exit through the actual Host instead. */
    public synchronized boolean retryFailedStartupCleanup(TargetOwnerDrainCoordinator.Request request) {
        process.requireFailedSourceCleanup(this);
        if (failedCleanupComplete) return true;
        if (initializationRunning || constructionThread != null) return false;
        if (firstFailure == null) throw new IllegalStateException("successful startup is still admitted for assembly");
        stopping = true;
        try {
            if (restoreSubmission != null) {
                if (graph.classes().state(restoreSubmission.task()).orElse(null)
                        == com.nereusstream.delay.scheduler.WorkClassExecutionRegistry.ExecutionState.RUNNING
                        || restoreSubmission.outcome().isEmpty()) return false;
                if (store == null && restoreSubmission.outcome().orElseThrow().restored() != null) {
                    store = restoreSubmission.outcome().orElseThrow().restored();
                }
                final var failed = restoreSubmission.outcome().orElseThrow().failure();
                if (failed != null && !retryRestoreNativeCleanup(failed,
                        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()))) return false;
            }
            if (store != null && !storeClosed) store.fenceForUncleanClose();
            if (worker != null && failedWorkerExit == null) worker.quarantineBackendWrites();
            if (activation != null) {
                activation.runtime().fence(); activation.runtime().retryAuthorityCleanup();
                if (!activation.runtime().authorityIoExited()) return false;
            }
            if (failedAssembly != null) {
                failedAssembly.retryContentCleanup(); if (!failedAssembly.contentIoExited()) return false;
            }
            if (recovery != null) {
                recovery.coordinator().stopAuthorityPreparation(); recovery.coordinator().retryAuthorityCleanup();
                if (!recovery.coordinator().authorityIoExited()) return false;
            }
            if (pulsarPreparation != null) {
                final var prior = pulsarPreparation.previousRecovery;
                prior.coordinator().stopAuthorityPreparation(); prior.coordinator().retryAuthorityCleanup();
                if (!prior.coordinator().authorityIoExited()) return false;
                final var reader = pulsarPreparation.previousReader;
                reader.close(); if (!reader.cleanupComplete()) return false;
                final var oldBackend = reader.backend();
                final var currentReaderBackend = recoverySource == null ? null : recoverySource.backend();
                if (oldBackend != null && oldBackend != backend && oldBackend != currentReaderBackend) {
                    oldBackend.closeBeforeHostBinding(); if (!oldBackend.resourcesTerminated()) return false;
                }
            }
            if (recoveryOpeningCleanup != null) {
                if (recoveryOpeningBackend != null) {
                    recoveryOpeningBackend.closeBeforeHostBinding();
                    if (!recoveryOpeningBackend.resourcesTerminated()) return false;
                }
                recoveryOpeningCleanup.retryCleanup(); if (!recoveryOpeningCleanup.cleanupComplete()) return false;
            }
            if (recoverySource != null) {
                recoverySource.close(); if (!recoverySource.cleanupComplete()) return false;
                final var readerBackend = recoverySource.backend();
                if (readerBackend != null && readerBackend != backend) {
                    readerBackend.closeBeforeHostBinding(); if (!readerBackend.resourcesTerminated()) return false;
                }
            }
            if (backend != null) {
                backend.closeBeforeHostBinding(); if (!backend.resourcesTerminated()) return false;
            }
            if (worker != null) {
                for (var publication : worker.publicationBackendGroups()) {
                    publication.closeBeforeHostBinding(); if (!publication.resourcesTerminated()) return false;
                }
                if (failedWorkerExit == null) {
                    var exited = worker.exitQuarantinedForReplay(request, owner.clock());
                    if (exited.isEmpty()) return false;
                    failedWorkerExit = exited.get();
                }
                storeClosed = true; leaseReleased = failedWorkerExit.leaseReleased(); ownerLost = failedWorkerExit.ownerLost();
                if (!releaseOwnedCheckpointPin()) return false;
                failedCleanupComplete = true;
                return true;
            }
            if (!storeClosed && store != null) {
                if (!store.isWriteOutcomeUncertain()) store.flushForUncleanClose();
                store.close(); storeClosed = true;
            }
            if (lease != null && !leaseReleased && !ownerLost) {
                var current = graph.oxia().providers().leases().current(lease.shardId());
                if (current.isEmpty() || !lease.sameIdentity(current.get())) ownerLost = true;
                else if (!(leaseReleased = graph.oxia().providers().leases().release(current.get()))) return false;
            }
            if (!releaseOwnedCheckpointPin()) return false;
            // An uncertain acquisition remains in the original failure receipt; no successful lease is invented.
            failedCleanupComplete = true;
            return true;
        } catch (RuntimeException | Error failure) { retain(failure); return false; }
    }

    synchronized boolean failedCleanupComplete() { return failedCleanupComplete; }

    /** Only the original stopped Host can authorize the final pin release before its shared Oxia clients close. */
    boolean releaseCheckpointPinForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(graph.oxia());
        synchronized (this) { return releaseOwnedCheckpointPin(); }
    }

    private boolean releaseOwnedCheckpointPin() {
        if (!ownsCheckpointPin || checkpointPinReleased) return true;
        try {
            final var current = checkpointCatalog.activeRecoveryPin();
            if (pulsarPreparation != null && pulsarPreparation.replacementPin != null && current.isPresent()
                    && current.orElseThrow().equals(pulsarPreparation.replacementPin)) {
                checkpointCatalog.releaseRecoveryPin(pulsarPreparation.replacementPin);
                checkpointPinReleased = true; return true;
            }
            if (current.isEmpty() || !current.get().equals(checkpointPin)) {
                // Old protection is gone, including after response loss or a new owner's pin; never delete the successor.
                checkpointPinReleased = true; return true;
            }
            checkpointCatalog.releaseRecoveryPin(checkpointPin);
            checkpointPinReleased = true; return true;
        } catch (RuntimeException | Error failure) {
            if (checkpointPinFailure == null) checkpointPinFailure = failure;
            return false;
        }
    }

    private boolean retryRestoreNativeCleanup(Throwable failure, java.util.Set<Throwable> seen) {
        if (!seen.add(failure)) return true;
        boolean complete = true;
        if (failure instanceof CheckpointInputFuture.Cleanup cleanup) {
            cleanup.retryCleanup(); complete = cleanup.cleanupComplete();
        }
        if (failure.getCause() != null) complete &= retryRestoreNativeCleanup(failure.getCause(), seen);
        for (var suppressed : failure.getSuppressed()) complete &= retryRestoreNativeCleanup(suppressed, seen);
        return complete;
    }


    public static final class StartupFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerSourceStartup startup;
        private StartupFailure(Throwable failure, TargetWorkerSourceStartup startup) {
            super("Source bootstrap failed; original process-owned startup is retained", failure); this.startup = startup;
        }
        public TargetWorkerSourceStartup startup() { return startup; }
    }
}
