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
    private Throwable recoveryCleanupFailure;
    private TargetOwnedBackendGroup backend;
    private TargetWorkerShardRuntime worker;
    private boolean stopping;
    private boolean storeClosed;
    private boolean leaseReleased;
    private boolean ownerLost;
    private boolean initializationRunning = true;
    private boolean failedCleanupComplete;
    private Thread constructionThread;
    private long lastNow = -1;
    private Throwable firstFailure;

    TargetWorkerSourceStartup(TargetWorkerProcessRuntime process, TargetWorkerProcessRuntime.Assembly graph,
            Owner owner, Initial initial, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor successor, LongSupplier nanos) {
        this.process = process; this.graph = graph; this.owner = Objects.requireNonNull(owner);
        this.initial = Objects.requireNonNull(initial); this.content = Objects.requireNonNull(content);
        this.scope = initial.scope();
        this.successor = Objects.requireNonNull(successor); this.nanos = nanos;
    }

    TargetWorkerSourceStartup(TargetWorkerProcessRuntime process, TargetWorkerProcessRuntime.Assembly graph,
            Owner owner, TargetQuotaScope scope, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor activeSuccessor, LongSupplier nanos) {
        this.process = process; this.graph = graph; this.owner = Objects.requireNonNull(owner);
        this.initial = null; this.scope = Objects.requireNonNull(scope); this.content = Objects.requireNonNull(content);
        this.successor = Objects.requireNonNull(activeSuccessor); this.nanos = nanos;
        if (scope.target() != null) throw new IllegalArgumentException("Source reopen requires its original Shard scope");
    }

    /** Opens the actual same-DB Target Store under the newly acquired original assignment/session lease. */
    void openReopened() {
        try {
            assignment = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId());
            TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, scope);
            lease = TargetWorkerSourceRuntimeFactory.acquire(graph.oxia(), owner.tenant(), owner.publication(),
                    owner.workerId(), owner.ownerId(), owner.leaseDurationMs(), owner.clock());
            requireCurrent();
            store = ShardStore.openTarget(graph.config().store(), scope.shard(), graph.resources());
            requireCurrent();
        } catch (RuntimeException | Error failure) {
            retain(failure); throw new StartupFailure(failure, this);
        } finally { synchronized (this) { initializationRunning = false; } }
    }

    /** Adopts the exact verified reader before auditing/replaying; never takes an arbitrary iterator or issues ACK. */
    public void startRecovery(TargetWorkerRecoverySource source, com.nereusstream.delay.protocol.OwnerIdentity identity,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal, Runnable sourceProtection, Runnable journalProtection) {
        final Construction original;
        synchronized (this) {
            if (initial != null || initializationRunning || recoverySource != null || recovery != null || activation != null) {
                throw new IllegalStateException("original same-DB recovery is unavailable or already started");
            }
            original = beginConstruction(false);
        }
        try (original) {
            original.requireCurrent();
            Objects.requireNonNull(source).requireStartup(assignment, graph.config().maximumSourceFrameBytes(),
                    graph.classes(), graph.resources());
            synchronized (this) { recoverySource = source; }
            source.replayCursor().requireGuarded();
            recovery = TargetWorkerSourceRuntimeFactory.reopen(graph.oxia(), graph.config(), graph.classes(),
                    graph.resources(), store, scope, owner.tenant(), owner.publication(), owner.workerId(), lease,
                    source.replayCursor(), source.successorAfter(store.appliedShardLogPosition()), content, identity, journal,
                    sourceProtection, journalProtection, owner.clock(), nanos);
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
            if (recovery == null || recoverySource == null || activation != null) throw new IllegalStateException("original recovery is not pending");
            original = beginConstruction(false);
        }
        try (original) {
            original.requireCurrent();
            var turn = recovery.coordinator().runTurn();
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
            assignment = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId());
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
            lease = TargetWorkerSourceRuntimeFactory.acquire(graph.oxia(), owner.tenant(), owner.publication(),
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
            activation = TargetWorkerSourceRuntimeFactory.initialize(graph.oxia(), graph.config(), graph.classes(),
                    graph.resources(), committed, store, owner.tenant(), owner.publication(), owner.workerId(), lease,
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
        var accepted = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId());
        var current = graph.oxia().providers().leases().current(accepted.shardId()).orElseThrow();
        long now = owner.clock().getAsLong();
        if (assignment == null || !assignment.sameIdentity(accepted) || lease == null || !lease.sameIdentity(current)
                || !Bytes.constantTimeEquals(current.sessionIdentity(), graph.oxia().providers().sessionIdentity())
                || !current.validAt(now) || now < 0 || now < lastNow
                || (recoverySource == null ? current.state() != lease.state()
                        : current.state() != ShardLifecycleState.ACQUIRING && current.state() != ShardLifecycleState.CATCHING_UP
                            && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                || store != null && Long.compareUnsigned(store.runtimeMetadata().lastOpenedOwnerEpoch(), lease.ownerEpoch()) > 0) {
            throw new IllegalStateException("Source startup lost its actual assignment/session/Owner/time authority");
        }
        lastNow = now;
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
        private boolean closed;
        private Construction(TargetWorkerSourceStartup startup, Thread original) { this.startup = startup; this.original = original; }
        public void requireCurrent() {
            synchronized (startup) {
                if (closed || Thread.currentThread() != original || startup.constructionThread != original) {
                    throw new IllegalStateException("Source construction lost its original call ownership");
                }
                startup.requireCurrent();
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
            if (store != null && !storeClosed) store.fenceForUncleanClose();
            if (worker != null) worker.quarantineBackendWrites();
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
                var exited = worker.exitQuarantinedForReplay(request, owner.clock());
                if (exited.isEmpty()) return false;
                storeClosed = true; leaseReleased = exited.get().leaseReleased(); ownerLost = exited.get().ownerLost();
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
            // An uncertain acquisition remains in the original failure receipt; no successful lease is invented.
            failedCleanupComplete = true;
            return true;
        } catch (RuntimeException | Error failure) { retain(failure); return false; }
    }

    synchronized boolean failedCleanupComplete() { return failedCleanupComplete; }


    public static final class StartupFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerSourceStartup startup;
        private StartupFailure(Throwable failure, TargetWorkerSourceStartup startup) {
            super("Source bootstrap failed; original process-owned startup is retained", failure); this.startup = startup;
        }
        public TargetWorkerSourceStartup startup() { return startup; }
    }
}
