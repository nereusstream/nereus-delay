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
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** One original process-owned Source bootstrap, Store, lease and SDK handoff; no Source ACK is performed here. */
public final class TargetWorkerSourceStartup {
    public static final class FrameCapacityUnavailable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public FrameCapacityUnavailable(String message) {
            super(message);
        }
    }

    public static final class FrameOpening {
        private final TargetWorkerSourceStartup startup;
        private final ShardStore store;
        private final TargetQuotaScope scope;
        private final byte[] lineage;
        private final SourceAssignment assignment;
        private final OwnerLease owner;
        private final com.nereusstream.delay.protocol.SourcePosition cut;

        private FrameOpening(
                TargetWorkerSourceStartup startup,
                ShardStore store,
                TargetQuotaScope scope,
                byte[] lineage,
                SourceAssignment assignment,
                OwnerLease owner,
                com.nereusstream.delay.protocol.SourcePosition cut) {
            this.startup = startup;
            this.store = store;
            this.scope = scope;
            this.lineage = lineage.clone();
            this.assignment = assignment;
            this.owner = owner;
            this.cut = cut;
        }

        public SourceAssignment assignment() {
            return assignment;
        }

        public com.nereusstream.delay.protocol.SourcePosition cut() {
            return cut;
        }

        public TargetQuotaScope scope() {
            return scope;
        }

        public byte[] lineage() {
            return lineage.clone();
        }

        public void requireCurrent() {
            startup.requireCurrent();
            requireLocalCurrent();
        }

        public void requireLocalCurrent() {
            final var reserved = startup.sourcePlanReservation;
            final var sourceOwner = reserved == null ? null : reserved.localOwnerLease();
            final var current = sourceOwner == null ? startup.lease : sourceOwner;
            final var accepted = startup.assignment;
            if (startup.stopping
                    || startup.firstFailure != null
                    || startup.store != store
                    || store.nativeExposureFenceRaised()
                    || current == null
                    || !current.sameIdentity(owner)
                    || accepted == null
                    || !accepted.sameIdentity(assignment)
                    || !current.validAt(startup.owner.clock().getAsLong())
                    || Long.compareUnsigned(store.localOpenedOwnerEpoch(), owner.ownerEpoch()) > 0) {
                throw new IllegalStateException("complete Source frame hold lost its actual Store/Root/Owner/session");
            }
        }

        public void requireSource(com.nereusstream.delay.protocol.SourcePosition actual) {
            requireLocalCurrent();
            if (actual == null || !scope.shard().equals(actual.shardId()) || !cut.sameSourceIdentity(actual)) {
                throw new IllegalArgumentException("frame contract changed its exact physical Source");
            }
        }

        void requireSame(FrameOpening actual) {
            requireLocalCurrent();
            actual.requireLocalCurrent();
            if (startup != actual.startup
                    || store != actual.store
                    || !scope.equals(actual.scope)
                    || !Arrays.equals(lineage, actual.lineage)
                    || !assignment.sameIdentity(actual.assignment)
                    || !owner.sameIdentity(actual.owner)
                    || !cut.sameSourceIdentity(actual.cut)) {
                throw new IllegalArgumentException("Source frame contract changed its original opening");
            }
        }

        void requireReader(TargetStoreBackend.Reader actual) {
            actual.requireSourceFrameBinding(store, scope, lineage);
            requireSource(actual.source());
            if (actual.source().compareTo(cut) < 0) {
                throw new IllegalStateException("Native frame gate predates the original Source hold cut");
            }
        }

        void requireCheckpoint(com.nereusstream.delay.store.TargetCheckpointRootVerifier.RootProof actual) {
            requireLocalCurrent();
            if (!Arrays.equals(
                            store.metadata().storeIncarnation(),
                            actual.metadata().storeIncarnation())
                    || !Arrays.equals(
                            store.metadata().dbIdentity(), actual.metadata().dbIdentity())
                    || !scope.equals(actual.root().scope())
                    || !Arrays.equals(lineage, actual.root().recoveryLineage())) {
                throw new IllegalArgumentException(
                        "cold Source frame audit changed its actual Native Store/Root/lineage");
            }
            requireSource(actual.source());
        }

        void requireFrame(com.nereusstream.delay.protocol.SourcePosition actual, Long generation, byte[] attestation) {
            requireSource(actual);
            if (assignment.activationBarrier()
                    instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier barrier) {
                if (generation == null || attestation == null) {
                    throw new IllegalStateException("frame read lacks its actual guarded Source generation");
                }
                barrier.validateSourceConnection(generation, attestation);
            } else if (generation != null || attestation != null) {
                throw new IllegalStateException("Kafka frame carries a foreign Source generation");
            }
        }
    }

    public static final class FrameDecodeLoan implements CheckpointInputFuture.Cleanup {
        private final TargetWorkerSourceRuntimeFactory.SourceFrameContract frames;
        private final Runnable capacityRelease;
        private final java.util.function.Consumer<FrameDecodeLoan> exited;
        private final com.nereusstream.delay.protocol.SourcePosition position;
        private final long bytes, retained;
        private final Long generation;
        private final byte[] attestation;
        private volatile TargetWorkerSourceRuntimeFactory.FrameReadLoan physical;
        private volatile boolean physicalPending = true;
        private volatile boolean releaseRequested;
        private boolean released;
        private SourceReplayEntry decoded;

        private FrameDecodeLoan(
                TargetWorkerSourceRuntimeFactory.SourceFrameContract frames,
                Runnable capacityRelease,
                java.util.function.Consumer<FrameDecodeLoan> exited,
                com.nereusstream.delay.protocol.SourcePosition position,
                long bytes,
                long retained,
                Long generation,
                byte[] attestation) {
            this.frames = frames;
            this.capacityRelease = capacityRelease;
            this.exited = exited;
            this.position = position;
            this.bytes = bytes;
            this.retained = retained;
            this.generation = generation;
            this.attestation = attestation == null ? null : attestation.clone();
        }

        public void requireCurrent() {
            synchronized (this) {
                if (releaseRequested || released) {
                    throw new FrameCapacityUnavailable("frame decode loan is exiting");
                }
            }
            frames.requireFrame(position, bytes, generation, attestation);
            Objects.requireNonNull(physical, "original frame read/codec loan").requireCurrent();
        }

        public void retainDecoded(SourceReplayEntry actual) {
            requireCurrent();
            if (!position.sameSourceIdentity(actual.position())
                    || position.compareTo(actual.position()) != 0
                    || !Objects.equals(generation, actual.sourceConnectionGeneration())
                    || !Arrays.equals(attestation, actual.guardAttestationDigest())) {
                throw new IllegalStateException("decoded frame changed its actual Source/guard proof");
            }
            synchronized (this) {
                if (decoded != null && decoded != actual) {
                    throw new IllegalStateException("frame loan changed decoded entry");
                }
                decoded = actual;
            }
        }

        public void requestRelease() {
            synchronized (this) {
                releaseRequested = true;
            }
            final var actual = physical;
            if (actual != null) {
                actual.requestRelease();
            }
            cleanupComplete();
        }

        @Override
        public boolean cleanupComplete() {
            final var actual = physical;
            if (!releaseRequested || physicalPending || actual != null && !actual.cleanupComplete()) {
                return false;
            }
            synchronized (this) {
                if (released) {
                    return true;
                }
                released = true;
                decoded = null;
            }
            capacityRelease.run();
            exited.accept(this);
            return true;
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> retryCleanup() {
            requestRelease();
            return cleanupComplete()
                    ? java.util.concurrent.CompletableFuture.completedFuture(null)
                    : java.util.concurrent.CompletableFuture.failedFuture(
                            new FrameCapacityUnavailable("original frame cleanup remains live"));
        }
    }

    public record Owner(
            AuthenticatedTenantContext tenant,
            WorkerAssignmentAuthority.Publication publication,
            String workerId,
            String ownerId,
            long leaseDurationMs,
            LongSupplier clock) {
        public Owner {
            Objects.requireNonNull(tenant);
            Objects.requireNonNull(publication);
            Objects.requireNonNull(clock);
            if (workerId == null
                    || workerId.isBlank()
                    || workerId.length() > 512
                    || ownerId == null
                    || ownerId.isBlank()
                    || ownerId.length() > 512
                    || leaseDurationMs < 1
                    || leaseDurationMs == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Source startup requires a finite original Owner identity/lease");
            }
        }
    }

    /** All authority callbacks consume original protected local material; collecting it remains outside Store locks. */
    public record Initial(
            TargetQuotaScope scope,
            byte[] lineage,
            PreparedControlOperation prepared,
            SourceReplayMutation entry,
            TargetQuotaGrantControlVerifier.Authority grants,
            TargetStoreBootstrap.StartAuthority start,
            TargetControlSnapshotInput snapshot,
            TargetStoreBootstrap.InitialControlAuthority snapshotAuthority) {
        public Initial {
            Objects.requireNonNull(scope);
            Objects.requireNonNull(prepared);
            Objects.requireNonNull(entry);
            Objects.requireNonNull(grants);
            Objects.requireNonNull(start);
            Objects.requireNonNull(snapshot);
            Objects.requireNonNull(snapshotAuthority);
            Bytes.requireLength(lineage, 16, "Source startup lineage");
            lineage = Bytes.copy(lineage);
            if (scope.target() != null || !scope.shard().equals(entry.position().shardId())) {
                throw new IllegalArgumentException("Source startup needs its original initial Shard grant");
            }
        }

        @Override
        public byte[] lineage() {
            return Bytes.copy(lineage);
        }
    }

    private final TargetWorkerProcessRuntime process;
    private final TargetWorkerProcessRuntime.Assembly graph;
    private final Owner owner;
    private WorkerAssignmentAuthority.Publication publication;
    private final Initial initial;
    private final TargetQuotaScope scope;
    private final TargetWorkerSourceRuntimeFactory.Content content;
    private TargetWorkerSourceRuntimeFactory.Content scopedContent;
    private volatile TargetWorkerProcessRuntime.SourcePlanInputs sourcePlanReservation;
    private volatile TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames;
    private final java.util.Set<FrameDecodeLoan> frameDecodeLoans = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private TargetScopeResourceManager scopeResources;
    private OwnedCheckpointIo<TargetScopeResourceManager> scopeResourceOpening;
    private OwnedCheckpointIo<Boolean> scopeResourceClosing;
    private TargetWorkerProcessRuntime.ScopeInputs scopeInputReservation;
    private volatile boolean scopeResourcesClosing;
    private final SourceReplaySuccessor successor;
    private final LongSupplier nanos;
    private volatile SourceAssignment assignment;
    private volatile OwnerLease lease;
    private volatile ShardStore store;
    private TargetWorkerSourceRuntimeFactory.Activated activation;
    private TargetWorkerSourceRuntimeFactory.AssemblyFailure failedAssembly;
    private TargetWorkerSourceRuntimeFactory.Recovery recovery;
    private volatile TargetWorkerRecoverySource recoverySource;
    private volatile TargetWorkerRecoverySource localNativeReader;
    private final java.util.concurrent.atomic.AtomicLong recoveryLocalEpoch =
            new java.util.concurrent.atomic.AtomicLong();
    private OwnedCheckpointIo<Boolean> recoveryTailCapture;
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
    private volatile boolean stopping;
    private boolean storeClosed;
    private boolean leaseReleased;
    private boolean ownerLost;
    private boolean initializationRunning = true;
    private boolean failedCleanupComplete;
    private Thread constructionThread;
    private int authorityReadsInFlight;
    private volatile long lastNow = -1;
    private volatile Throwable firstFailure;
    private PulsarPreparation pulsarPreparation;
    private boolean initialCommitted;
    private com.nereusstream.delay.protocol.OwnerIdentity registryIdentity;
    // Set only from a genuinely committed/reopened Root, before its Store can close.
    private byte[] sourceExitLineage;
    private com.nereusstream.delay.protocol.SourcePosition sourceExitSource;
    private TargetWorkerCheckpointComposition checkpointComposition;
    private byte[] checkpointExpectedLineage;
    private final java.util.List<RecoveryInputsOpening> recoveryInputs = new java.util.ArrayList<>(2);
    private final java.util.Set<TargetOwnedBackendGroup> recoveryInputBackends =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private long recoveryInputBytesLimit;
    private volatile RecoveryInputsOpening currentRecoveryInputs;
    private final java.util.List<SloRecoveryOpening> sloRecoveries = new java.util.ArrayList<>(2);
    private SloRecoveryOpening currentSloRecovery;
    private boolean serviceJournalHandoffComplete;
    private TargetWorkerPublicationDispatch publicationDispatch;
    private TargetWorkerChannelInventory channelInventory;
    private boolean channelInputsReleased;
    private TargetPayloadSummaryLegacyPrefix.Provider payloadSummaryPrefixProvider;
    private TargetPayloadSummaryLegacyPrefix.Limits payloadSummaryPrefixLimits;
    private TargetWorkerProcessRuntime.PayloadSummaryPrefixInputs payloadSummaryPrefixReservation;
    private com.nereusstream.delay.protocol.TargetQuotaIncarnation payloadSummaryPrefixRoot;
    private com.nereusstream.delay.runtime.TargetStoreBootstrap.Reopened archiveRecoveryAudit;
    private com.nereusstream.delay.store.ShardStore archiveRecoveryAuditStore;
    private volatile TargetPayloadSummaryLegacyPrefix payloadSummaryPrefix;
    private TargetPayloadSummaryLegacyPrefix previousPayloadSummaryPrefix;
    private OwnedCheckpointIo<TargetPayloadSummaryLegacyPrefix> payloadSummaryPrefixOpening;
    private TargetWorkerRecoverySource payloadSummaryCapturedReader;
    private final java.util.List<CheckpointInputFuture.Cleanup> payloadSummaryPrefixPartial =
            new java.util.ArrayList<>();
    private com.nereusstream.delay.runtime.TargetSloWriterOrigin.Limits writerOriginLimits;
    private TargetWorkerProcessRuntime.WriterOriginInputs writerOriginReservation;
    private boolean activeWriterOriginConfigured;

    /** A provider registers actual Journal/Fence SDK owners before allocating native resources. */
    public synchronized void retainRecoveryInputBackend(TargetOwnedBackendGroup actual) {
        Objects.requireNonNull(actual).requireWorkerGraph(graph.classes(), graph.resources());
        if (actual == backend || recoveryInputBackends.contains(actual)) {
            return;
        }
        if (recoveryInputBackends.size() == 4) {
            throw new IllegalStateException("two Owner Journal/Fence SDK owner windows are already retained");
        }
        recoveryInputBackends.add(actual);
    }

    /** Uses the original finite process executor; the root retains late results and partial cleanup before exposure. */
    public synchronized RecoveryInputsOpening openRecoveryInputs(
            TargetSourceRecoveryInputs.Provider provider,
            com.nereusstream.delay.protocol.OwnerIdentity expectedOwner,
            long maximumRetainedBytes) {
        requireCurrent();
        Objects.requireNonNull(provider);
        Objects.requireNonNull(expectedOwner);
        if (worker != null
                || maximumRetainedBytes < 1
                || maximumRetainedBytes == Long.MAX_VALUE
                || maximumRetainedBytes > graph.config().resources().certifiedJvmHeapBytes()
                || expectedOwner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(expectedOwner.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalArgumentException(
                    "recovery inputs lack the actual startup Owner or finite retained-input memory bound");
        }
        for (var prior : recoveryInputs) {
            if (Arrays.equals(prior.owner.canonicalBytes(), expectedOwner.canonicalBytes())) {
                if (prior.provider != provider || prior.maximumBytes != maximumRetainedBytes) {
                    throw new IllegalStateException("the original Owner input provider/bound cannot be replaced");
                }
                return prior;
            }
        }
        if (recoveryInputs.size() == 2
                || recoveryInputBytesLimit != 0 && recoveryInputBytesLimit != maximumRetainedBytes) {
            throw new IllegalStateException("recovery inputs exceed the original two-Owner bounded startup");
        }
        recoveryInputBytesLimit = maximumRetainedBytes;
        long remainingBytes = maximumRetainedBytes;
        for (var prior : recoveryInputs) {
            if (prior.writer != null && !prior.handoffConfirmed) {
                throw new IllegalStateException(
                        "original recovery Journal writer has not normally closed before successor opening");
            }
            if (prior.fenceWriter != null && !prior.fenceHandoffConfirmed) {
                throw new IllegalStateException(
                        "original recovery Fence writer has not normally closed before successor opening");
            }
            if (!Arrays.equals(prior.owner.deploymentId(), expectedOwner.deploymentId())
                    || !Arrays.equals(prior.owner.workerRunId(), expectedOwner.workerRunId())) {
                throw new IllegalArgumentException("recovery provider changed its deployment/worker run identity");
            }
            remainingBytes = Math.subtractExact(
                    remainingBytes, prior.original == null ? prior.allocationBytes : prior.retainedBytes);
        }
        if (remainingBytes < 1) {
            throw new IllegalStateException("the original recovery input byte reservation is still occupied");
        }
        final long allocationBytes = remainingBytes;
        final var opening = new RecoveryInputsOpening(
                this, provider, expectedOwner, assignment, maximumRetainedBytes, allocationBytes);
        final SourceAssignment originalAssignment = assignment;
        recoveryInputs.add(opening);
        try {
            opening.io = OwnedCheckpointIo.submit(
                    guard -> {
                        final var value = Objects.requireNonNull(
                                provider.load(opening, expectedOwner, originalAssignment, allocationBytes, guard));
                        synchronized (this) {
                            opening.original = value;
                            opening.retainedBytes = value.retainedBytes();
                        }
                        guard.run();
                        opening.requireValue();
                        guard.run();
                        scopeContent();
                        captureSourceFrames();
                        guard.run();
                        return value;
                    },
                    this::requireCurrent,
                    graph.io());
            return opening;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
    }

    public static final class RecoveryInputsOpening {
        private final TargetWorkerSourceStartup startup;
        private final TargetSourceRecoveryInputs.Provider provider;
        private final com.nereusstream.delay.protocol.OwnerIdentity owner;
        private final SourceAssignment assignment;
        private final long maximumBytes;
        private final long allocationBytes;
        private long retainedBytes;
        private OwnedCheckpointIo<TargetSourceRecoveryInputs> io;
        private TargetSourceRecoveryInputs original;
        private boolean closed;
        private TargetSourceRecoveryInputs.JournalWriter writer;
        private JournalHandoff handoff;
        private boolean handoffConfirmed;
        private TargetRecoveryFenceWriter fenceWriter;
        private RecoveryFenceHandoff fenceHandoff;
        private boolean fenceHandoffConfirmed;

        private RecoveryInputsOpening(
                TargetWorkerSourceStartup startup,
                TargetSourceRecoveryInputs.Provider provider,
                com.nereusstream.delay.protocol.OwnerIdentity owner,
                SourceAssignment assignment,
                long maximumBytes,
                long allocationBytes) {
            this.startup = startup;
            this.provider = provider;
            this.owner = owner;
            this.maximumBytes = maximumBytes;
            this.allocationBytes = allocationBytes;
            this.assignment = assignment;
        }

        public Optional<TargetSourceRecoveryInputs> ready() {
            synchronized (startup) {
                if (io == null || !io.operationExited()) {
                    return Optional.empty();
                }
                final var result = io.receipt();
                if (!result.isDone()) {
                    return Optional.empty();
                }
                result.join();
                if (!io.ioExited()) {
                    return Optional.empty();
                }
                requireValue();
                return Optional.of(original);
            }
        }

        public com.nereusstream.delay.protocol.OwnerIdentity owner() {
            return owner;
        }

        public SourceAssignment assignment() {
            return assignment;
        }

        public TargetWorkerSourceStartup startup() {
            return startup;
        }
        /** The original registered handle also survives a factory's partial/late failure; no recursive hold validation. */
        public TargetRecoveryFenceWriter fenceWriter() {
            synchronized (startup) {
                return fenceWriter;
            }
        }

        public LocalBrokerMtlsResolver.SourceReceipt credential() {
            synchronized (startup) {
                requireValue();
                return original.credential();
            }
        }

        public int maximumFrameBytes() {
            synchronized (startup) {
                requireValue();
                return original.maximumFrameBytes();
            }
        }

        public FrameOpening frameOpening() {
            return startup.frameOpening();
        }

        /** Original strict prepare task only; native factories remain on that task's existing graph executor. */
        public void requireFenceFactory() {
            synchronized (startup) {
                startup.requireRecoveryReaderCurrent();
                requireValue();
                requireFencePreparation();
                original.credential().authorization().requireAssignment(assignment);
            }
        }

        private SloRecoveryOpening requireFencePreparation() {
            final var phase = startup.currentSloRecovery;
            if (closed
                    || startup.currentRecoveryInputs != this
                    || !startup.recoveryInputs.contains(this)
                    || !assignment.sameIdentity(startup.assignment)
                    || startup.lease == null
                    || owner.ownerEpoch() != startup.lease.ownerEpoch()
                    || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), startup.lease.leaseToken())
                    || startup.worker != null
                    || startup.activation != null
                    || startup.stopping
                    || startup.firstFailure != null
                    || phase == null
                    || phase.input != this
                    || !startup.sloRecoveries.contains(phase)
                    || !phase.qualificationRequired
                    || phase.originalAudit == null
                    || phase.preparationBound
                    || phase.preparationThread != Thread.currentThread()
                    || phase.preparation != null && phase.preparation.operationExited()
                    || startup.recoverySource != phase.source
                    || startup.recovery == null
                    || startup.recovery.coordinator() != phase.coordinator
                    || fenceHandoff != null) {
                throw new IllegalStateException("Fence SDK opening is not the original strict recovery preparation");
            }
            return phase;
        }

        /** Retain the actual producer generation before native allocation; backend ownership must already exist. */
        public void retainFenceWriter(TargetRecoveryFenceWriter actual) {
            synchronized (startup) {
                requireFenceFactory();
                Objects.requireNonNull(actual).requireBinding(this);
                if (fenceWriter != null && fenceWriter != actual
                        || actual.backend() != startup.backend
                                && !startup.recoveryInputBackends.contains(actual.backend())
                        || !actual.backend().holdsSourceCredential(original.credential())) {
                    throw new IllegalStateException(
                            "Fence writer lacks its original opening/Source credential/SDK owner");
                }
                fenceWriter = actual;
            }
        }

        /** Authoritative factory checks run outside the SDK's send/close locks. */
        public void requireFenceOpening(TargetRecoveryFenceWriter actual) {
            synchronized (startup) {
                requireFenceFactory();
                if (fenceWriter != actual) {
                    throw new IllegalStateException("Fence writer is not this opening's actual registered generation");
                }
                actual.requireBinding(this);
            }
        }

        /** Retains the exact immutable frame before any possible SDK append, including UNKNOWN/throwing returns. */
        public RecoveryFenceAppend prepareFenceAppend(
                TargetRecoveryFenceWriter actual, TargetTimeFenceMutationFactory.Prepared exact) {
            synchronized (startup) {
                requireFenceOpening(actual);
                final var phase = requireFencePreparation();
                phase.retainFence(Objects.requireNonNull(exact));
                if (phase.appendPermit == null) {
                    phase.appendPermit = new RecoveryFenceAppend(this, phase, actual, phase.exactFence);
                } else if (phase.appendPermit.writer != actual) {
                    throw new IllegalStateException(
                            "original recovery Fence cannot move to another producer generation");
                }
                phase.appendOutcome = ShardLogMutationAppender.AppendOutcome.unknown();
                return phase.appendPermit;
            }
        }

        /** Original provider path; the Root retains the frame and result even when native persistence is uncertain. */
        public ShardLogMutationAppender.AppendOutcome appendFence(
                TargetRecoveryFenceWriter actual, TargetTimeFenceMutationFactory.Prepared exact) {
            final var permit = prepareFenceAppend(actual, exact);
            final var appended =
                    Objects.requireNonNull(actual.append(permit.exact, permit), "actual recovery Fence append result");
            synchronized (startup) {
                permit.requireLocal(actual, permit.exact);
                permit.phase.appendOutcome = appended;
            }
            return appended;
        }

        /** An existing frame needs actual first/POS at the pre-append audit cut, not a caller DTO or append return. */
        public void retainPreviouslyAppliedFence(TargetTimeFenceMutationFactory.Prepared exact) {
            final SloRecoveryOpening phase;
            synchronized (startup) {
                requireFenceFactory();
                phase = requireFencePreparation();
            }
            phase.coordinator.requireSloRecoveryFencePreviouslyApplied(
                    phase.originalAudit, Objects.requireNonNull(exact));
            synchronized (startup) {
                requireFenceFactory();
                if (requireFencePreparation() != phase || phase.appendPermit != null) {
                    throw new IllegalStateException("existing recovery Fence changed its original pre-append phase");
                }
                phase.retainFence(exact);
                phase.existingFenceVerified = true;
            }
        }
        /** Retain the actual writer and its SDK cleanup before any native opening can begin. */
        public void retainJournalWriter(TargetSourceRecoveryInputs.JournalWriter actual) {
            synchronized (startup) {
                startup.requireCurrent();
                Objects.requireNonNull(actual).requireBinding(this);
                if (closed
                        || handoff != null
                        || writer != null && writer != actual
                        || !startup.recoveryInputs.contains(this)
                        || actual.backend() != startup.backend
                                && !startup.recoveryInputBackends.contains(actual.backend())) {
                    throw new IllegalStateException(
                            "recovery Journal writer lacks its exact original opening/SDK owner");
                }
                writer = actual;
                for (var prior : startup.recoveryInputs) {
                    if (prior == this || prior.writer == null) {
                        continue;
                    }
                    if (!prior.writer.producerName().equals(writer.producerName())
                            || !prior.writer.resource().sameIdentity(writer.resource())) {
                        throw new IllegalArgumentException(
                                "successor Journal changed its original resource/producer identity");
                    }
                }
                requireJournalOpening(actual);
            }
        }

        public void requireJournalOpening(TargetSourceRecoveryInputs.JournalWriter actual) {
            synchronized (startup) {
                startup.requireCurrent();
                if (writer != actual
                        || closed
                        || handoff != null
                        || !assignment.sameIdentity(startup.assignment)
                        || owner.ownerEpoch() != startup.lease.ownerEpoch()
                        || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), startup.lease.leaseToken())) {
                    throw new IllegalStateException("original recovery Journal opening is no longer admitted");
                }
                actual.requireBinding(this);
            }
        }

        private void requireValue() {
            synchronized (startup) {
                final var value = Objects.requireNonNull(original, "original input hold");
                if (closed
                        || !Arrays.equals(owner.canonicalBytes(), value.owner().canonicalBytes())
                        || value.credential() == null
                        || value.maximumFrameBytes() < 1
                        || value.maximumFrameBytes() > startup.graph.config().maximumSourceFrameBytes()
                        || value.retainedBytes() < 0
                        || value.retainedBytes() != retainedBytes
                        || retainedBytes > allocationBytes
                        || value.journal() != null
                                && (value.journalBackend() == null
                                        || writer == null
                                        || value.journalWriter() != writer
                                        || writer.journal() != value.journal()
                                        || writer.backend() != value.journalBackend())
                        || value.journal() == null && (writer != null || value.journalWriter() != null)) {
                    throw new IllegalStateException("provider changed its actual Owner/credential/frame/hold bound");
                }
                if (writer != null) {
                    writer.requireBinding(this);
                }
                final var inputFenceWriter = value.fenceWriter();
                final var phase = startup.currentSloRecovery;
                final boolean lateFenceOpening = inputFenceWriter == null
                        && fenceWriter != null
                        && phase != null
                        && phase.input == this
                        && phase.qualificationRequired
                        && phase.preparationThread != null
                        && !phase.preparationBound;
                if (inputFenceWriter != fenceWriter && !lateFenceOpening) {
                    throw new IllegalStateException("provider changed its original recovery Fence producer generation");
                }
                if (fenceWriter != null) {
                    fenceWriter.requireBinding(this);
                    if (fenceWriter.backend() != startup.backend
                                    && !startup.recoveryInputBackends.contains(fenceWriter.backend())
                            || !fenceWriter.backend().holdsSourceCredential(value.credential())) {
                        throw new IllegalStateException(
                                "provider did not retain its original Fence Source credential/SDK owner");
                    }
                }
                if (value.journalBackend() != null
                        && value.journalBackend() != startup.backend
                        && !startup.recoveryInputBackends.contains(value.journalBackend())) {
                    throw new IllegalStateException(
                            "provider did not retain its actual Journal SDK owner before opening");
                }
                long bytes = 0;
                for (var input : startup.recoveryInputs) {
                    if (input.original != null) {
                        if (input != this && input.original == value) {
                            throw new IllegalStateException("Owner input hold was rebound");
                        }
                        bytes = Math.addExact(bytes, input.retainedBytes);
                    }
                }
                if (bytes > maximumBytes) {
                    throw new IllegalStateException("two Owner input holds exceed their aggregate bound");
                }
                value.requireSourceCurrent();
                if (value.journal() != null) {
                    value.requireJournalCurrent();
                }
            }
        }
    }

    /** Both audits and the exact signed Fence remain with the original Owner/input through every I/O exit. */
    private static final class SloRecoveryOpening {
        private final TargetWorkerSourceStartup startup;
        private final TargetOwnerRecoveryCoordinator coordinator;
        private final TargetWorkerRecoverySource source;
        private final RecoveryInputsOpening input;
        private OwnedCheckpointIo<Boolean> preparation;
        private OwnedCheckpointIo<Boolean> completion;
        private Thread preparationThread;
        private TargetStoreBootstrap.SloRecoveryAudit originalAudit;
        private TargetStoreBootstrap.SloRecoveryAudit replayedAudit;
        private TargetTimeFenceMutationFactory.Prepared exactFence;
        private RecoveryFenceAppend appendPermit;
        private ShardLogMutationAppender.AppendOutcome appendOutcome;
        private boolean existingFenceVerified;
        private long fenceRetainedBytes;
        private boolean qualificationRequired;
        private boolean preparationBound;
        private boolean completionBound;

        private SloRecoveryOpening(TargetWorkerSourceStartup startup) {
            this.startup = startup;
            coordinator = startup.recovery.coordinator();
            source = startup.recoverySource;
            input = startup.currentRecoveryInputs;
        }

        private void requireCurrent() {
            synchronized (startup) {
                startup.requireRecoveryReaderCurrent();
                if (startup.currentSloRecovery != this
                        || startup.recovery == null
                        || startup.recovery.coordinator() != coordinator
                        || startup.recoverySource != source
                        || startup.currentRecoveryInputs != input
                        || !startup.sloRecoveries.contains(this)) {
                    throw new IllegalStateException("SLO recovery lost its original Owner/input/reader");
                }
                if (input != null) {
                    input.requireValue();
                }
            }
        }

        private long retainedBytes() {
            long bytes = fenceRetainedBytes;
            if (originalAudit != null) {
                bytes = Math.addExact(bytes, originalAudit.retainedBytes());
            }
            if (replayedAudit != null) {
                bytes = Math.addExact(bytes, replayedAudit.retainedBytes());
            }
            return bytes;
        }

        private void reserveFenceBytes() {
            synchronized (startup) {
                if (fenceRetainedBytes != 0) {
                    return;
                }
                final long maximumFrameBytes = startup.graph.config().maximumSourceFrameBytes();
                if (maximumFrameBytes >= startup.remainingSloRecoveryBytes()) {
                    throw new IllegalStateException("original SLO recovery cannot reserve its Fence and audit bytes");
                }
                // Any failed prepare retains this UNKNOWN allowance until the original input exits.
                fenceRetainedBytes = maximumFrameBytes;
            }
        }

        private void retainFence(TargetTimeFenceMutationFactory.Prepared exact) {
            if (!qualificationRequired
                    || originalAudit == null
                    || preparationBound
                    || fenceRetainedBytes == 0
                    || exact.mutation().type() != com.nereusstream.delay.protocol.SystemMutationType.TIME_FENCE
                    || !exact.body().shard().equals(originalAudit.scope().shard())
                    || !originalAudit.scope().equals(startup.scope)) {
                throw new IllegalStateException("recovery Fence lacks its original strict Scope/frame reservation");
            }
            final var frame = exact.mutation().encodeFrame();
            if (frame.length > fenceRetainedBytes
                    || input == null
                    || frame.length > input.original.maximumFrameBytes()) {
                throw new IllegalArgumentException(
                        "original SLO recovery Fence exceeds its reserved Source frame bound");
            }
            if (exactFence != null && !Arrays.equals(exactFence.mutation().encodeFrame(), frame)) {
                throw new IllegalStateException("original SLO recovery Fence cannot be replaced or resigned");
            }
            if (exactFence == null) {
                exactFence = exact;
            }
            fenceRetainedBytes = frame.length;
            startup.requireSloRecoveryByteBound();
        }

        private void prepare() {
            synchronized (startup) {
                requireCurrent();
                if (preparation != null) {
                    return;
                }
                preparation = OwnedCheckpointIo.submit(
                        guard -> {
                            synchronized (startup) {
                                preparationThread = Thread.currentThread();
                            }
                            try {
                                guard.run();
                                if (input != null) {
                                    input.requireValue();
                                    guard.run();
                                    qualificationRequired = input.original.requiresSloRecoveryQualification();
                                    input.requireValue();
                                    guard.run();
                                    if (qualificationRequired) {
                                        reserveFenceBytes();
                                    }
                                }
                                final var audit =
                                        coordinator.captureSloRecoveryAudit(startup.remainingSloRecoveryBytes(), false);
                                synchronized (startup) {
                                    originalAudit = audit;
                                    startup.sourceExitLineage = audit.lineage();
                                    startup.sourceExitSource = audit.cut().source();
                                    startup.requireSloRecoveryByteBound();
                                }
                                guard.run();
                                if (hasSlo(audit)) {
                                    qualificationRequired = true;
                                }
                                if (qualificationRequired) {
                                    if (input == null) {
                                        throw new IllegalStateException(
                                                "SLO recovery lacks its original protected input hold");
                                    }
                                    // A newly discovered Native obligation must reserve before its provider can append.
                                    reserveFenceBytes();
                                    input.requireValue();
                                    guard.run();
                                    // The original hold owns append/UNKNOWN and every native cleanup; no Source apply
                                    // is awaited here.
                                    final var fence = input.original.prepareSloRecoveryFence(audit, guard);
                                    if (fence != null) {
                                        final boolean alreadyRetained;
                                        synchronized (startup) {
                                            alreadyRetained = exactFence != null;
                                        }
                                        if (!alreadyRetained) {
                                            input.retainPreviouslyAppliedFence(fence);
                                        }
                                    }
                                    synchronized (startup) {
                                        if (fence == null && exactFence != null) {
                                            throw new IllegalStateException(
                                                    "original SLO recovery dropped its already retained Fence frame");
                                        }
                                        if (fence != null) {
                                            retainFence(fence);
                                        } else {
                                            fenceRetainedBytes = 0;
                                        }
                                        startup.requireSloRecoveryByteBound();
                                    }
                                    input.requireValue();
                                    guard.run();
                                    if (hasSlo(audit) && fence == null) {
                                        throw new IllegalStateException(
                                                "original SLO recovery returned no exact Fence for its retained domain");
                                    }
                                }
                                guard.run();
                                return Boolean.TRUE;
                            } finally {
                                synchronized (startup) {
                                    preparationThread = null;
                                }
                            }
                        },
                        this::requireCurrent,
                        startup.graph.io());
            }
        }

        private boolean bindPreparation() {
            final var io = preparation;
            if (io == null) {
                return false;
            }
            io.retryCleanup();
            if (!io.operationExited() || !io.receipt().isDone()) {
                return false;
            }
            io.receipt().join();
            if (!io.ioExited()) {
                return false;
            }
            requireCurrent();
            if (!preparationBound) {
                if (exactFence != null && appendPermit == null && !existingFenceVerified) {
                    throw new IllegalStateException(
                            "recovery Fence has neither its original SDK permit nor actual pre-cut APPLIED proof");
                }
                if (!startup.settleRecoveryFence(input, this)) {
                    return false;
                }
                coordinator.bindSloRecoveryPreparation(
                        Objects.requireNonNull(originalAudit),
                        exactFence,
                        input == null ? null : input.original,
                        qualificationRequired);
                preparationBound = true;
            }
            return true;
        }

        private void complete() {
            synchronized (startup) {
                requireCurrent();
                if (!preparationBound) {
                    throw new IllegalStateException("SLO recovery preparation has not actually exited");
                }
                if (completion != null) {
                    return;
                }
                completion = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            final var audit =
                                    coordinator.captureSloRecoveryAudit(startup.remainingSloRecoveryBytes(), true);
                            synchronized (startup) {
                                replayedAudit = audit;
                                startup.requireSloRecoveryByteBound();
                            }
                            guard.run();
                            if (input != null) {
                                input.requireValue();
                                guard.run();
                                final boolean requested = input.original.requiresSloRecoveryQualification();
                                input.requireValue();
                                guard.run();
                                if (requested && !qualificationRequired) {
                                    throw new IllegalStateException(
                                            "ordinary recovery cannot acquire SLO qualification after its preparation cut");
                                }
                            }
                            if (qualificationRequired && input == null) {
                                throw new IllegalStateException(
                                        "replayed SLO recovery lacks its original protected input hold");
                            }
                            if (!qualificationRequired
                                    && (exactFence != null || hasSlo(originalAudit) || hasSlo(audit))) {
                                throw new IllegalStateException(
                                        "ordinary recovery cannot close a retained or newly replayed SLO obligation");
                            }
                            coordinator.completeSloRecovery(audit, input == null ? null : input.original, guard);
                            guard.run();
                            return Boolean.TRUE;
                        },
                        this::requireCurrent,
                        startup.graph.io());
            }
        }

        private boolean bindCompletion() {
            final var io = completion;
            if (io == null) {
                return false;
            }
            io.retryCleanup();
            if (!io.operationExited() || !io.receipt().isDone()) {
                return false;
            }
            io.receipt().join();
            if (!io.ioExited()) {
                return false;
            }
            requireCurrent();
            if (!completionBound) {
                coordinator.bindSloRecoveryCompletion(
                        Objects.requireNonNull(replayedAudit), input == null ? null : input.original);
                completionBound = true;
            }
            return true;
        }

        private boolean operationsExited(boolean stop) {
            boolean exited = true;
            for (var io : new OwnedCheckpointIo<?>[] {preparation, completion}) {
                if (io == null) {
                    continue;
                }
                if (stop) {
                    io.requestStop();
                }
                exited &= io.operationExited();
            }
            return exited;
        }

        private boolean cleanupExited() {
            boolean exited = true;
            for (var io : new OwnedCheckpointIo<?>[] {preparation, completion}) {
                if (io == null) {
                    continue;
                }
                io.retryCleanup();
                exited &= io.ioExited();
            }
            return exited;
        }

        private static boolean hasSlo(TargetStoreBootstrap.SloRecoveryAudit audit) {
            return !audit.generations().isEmpty() || !audit.writers().isEmpty();
        }
    }

    /** One aggregate bound covers both phases and both retained Owner handles; input holds keep their own bound. */
    private synchronized long remainingSloRecoveryBytes() {
        long remaining = content.maximumBytes();
        for (var opening : sloRecoveries) {
            remaining = Math.subtractExact(remaining, opening.retainedBytes());
        }
        if (remaining < 1) {
            throw new IllegalStateException("original SLO recovery audit/Fence byte reservation is occupied");
        }
        return remaining;
    }

    private synchronized void requireSloRecoveryByteBound() {
        long bytes = 0;
        for (var opening : sloRecoveries) {
            bytes = Math.addExact(bytes, opening.retainedBytes());
        }
        if (bytes > content.maximumBytes()) {
            throw new IllegalStateException("retained SLO recovery audits/Fences exceed their aggregate bound");
        }
    }

    /** Actual accepted reader for the original strict factory; a replacement reader cannot authorize this producer. */
    public TargetWorkerRecoverySource recoverySourceForFence(RecoveryInputsOpening input) {
        synchronized (this) {
            if (input == null || input.startup != this) {
                throw new IllegalArgumentException("Fence reader belongs to another original startup");
            }
            input.requireFenceFactory();
            final var phase = input.requireFencePreparation();
            Objects.requireNonNull(recoverySource)
                    .requireStartup(
                            input.assignment,
                            graph.config().maximumSourceFrameBytes(),
                            graph.classes(),
                            graph.resources());
            if (recoverySource != phase.source) {
                throw new IllegalStateException("Fence writer lost its actual original Source reader");
            }
            return recoverySource;
        }
    }

    /** Exact type-4 SDK permission created only after the Root retained this immutable frame. */
    public static final class RecoveryFenceAppend {
        private final RecoveryInputsOpening input;
        private final SloRecoveryOpening phase;
        private final TargetRecoveryFenceWriter writer;
        private final TargetTimeFenceMutationFactory.Prepared exact;
        private final TargetSourceRecoveryInputs original;
        private final OwnerLease owner;
        private final ShardStore store;

        private RecoveryFenceAppend(
                RecoveryInputsOpening input,
                SloRecoveryOpening phase,
                TargetRecoveryFenceWriter writer,
                TargetTimeFenceMutationFactory.Prepared exact) {
            this.input = input;
            this.phase = phase;
            this.writer = writer;
            this.exact = exact;
            original = input.original;
            owner = input.startup.lease;
            store = input.startup.store;
        }

        private void requireLocal(
                TargetRecoveryFenceWriter actual, TargetTimeFenceMutationFactory.Prepared actualFrame) {
            final var startup = input.startup;
            final long now = startup.owner.clock().getAsLong();
            if (actual != writer
                    || input.fenceWriter != actual
                    || phase.appendPermit != this
                    || input.original != original
                    || input.closed
                    || input.fenceHandoff != null
                    || startup.currentRecoveryInputs != input
                    || startup.currentSloRecovery != phase
                    || startup.recoverySource != phase.source
                    || startup.store != store
                    || startup.storeClosed
                    || startup.worker != null
                    || startup.activation != null
                    || startup.stopping
                    || startup.firstFailure != null
                    || !phase.qualificationRequired
                    || phase.originalAudit == null
                    || phase.preparationBound
                    || phase.preparationThread != Thread.currentThread()
                    || phase.exactFence != exact
                    || startup.lease == null
                    || !owner.sameIdentity(startup.lease)
                    || !input.assignment.sameIdentity(startup.assignment)
                    || now < startup.lastNow
                    || !startup.lease.validAt(now)
                    || actualFrame == null
                    || actualFrame.mutation().type() != com.nereusstream.delay.protocol.SystemMutationType.TIME_FENCE
                    || !Arrays.equals(
                            exact.mutation().encodeFrame(),
                            actualFrame.mutation().encodeFrame())) {
                throw new IllegalStateException(
                        "recovery Fence SDK handoff lost its actual original frame/producer/Source/Owner");
            }
        }

        /** Local retained guards only; no authoritative lookup, Store view or SDK acquisition under native send locks. */
        public void beforeSdk(TargetRecoveryFenceWriter actual, TargetTimeFenceMutationFactory.Prepared actualFrame) {
            synchronized (input.startup) {
                requireLocal(actual, actualFrame);
            }
            original.requireSourceCurrent();
            synchronized (input.startup) {
                requireLocal(actual, actualFrame);
            }
        }
    }

    /** Normal close of only the original recovery Fence producer; never business-write or shared-client authority. */
    public static final class RecoveryFenceHandoff {
        private final RecoveryInputsOpening input;
        private final SloRecoveryOpening phase;
        private final TargetRecoveryFenceWriter writer;
        private final OwnerLease owner;

        private RecoveryFenceHandoff(RecoveryInputsOpening input, SloRecoveryOpening phase) {
            this.input = input;
            this.phase = phase;
            writer = input.fenceWriter;
            owner = input.startup.lease;
        }

        public void requireWriter(TargetRecoveryFenceWriter actual) {
            final var startup = input.startup;
            synchronized (startup) {
                final long now = startup.owner.clock().getAsLong();
                if (actual != writer
                        || input.fenceWriter != actual
                        || input.fenceHandoff != this
                        || input.closed
                        || startup.currentRecoveryInputs != input
                        || startup.currentSloRecovery != phase
                        || startup.recoverySource != phase.source
                        || phase.input != input
                        || phase.preparationBound
                        || phase.preparationThread != null
                        || phase.preparation == null
                        || !phase.preparation.operationExited()
                        || !phase.preparation.ioExited()
                        || startup.worker != null
                        || startup.activation != null
                        || startup.stopping
                        || startup.firstFailure != null
                        || startup.lease == null
                        || !owner.sameIdentity(startup.lease)
                        || !input.assignment.sameIdentity(startup.assignment)
                        || now < startup.lastNow
                        || !startup.lease.validAt(now)) {
                    throw new IllegalStateException(
                            "Fence close is not this original completed preparation's producer handoff");
                }
            }
        }
    }

    private boolean settleRecoveryFence(RecoveryInputsOpening input, SloRecoveryOpening phase) {
        if (input == null) {
            return true;
        }
        final RecoveryFenceHandoff permit;
        final TargetRecoveryFenceWriter actual;
        synchronized (this) {
            phase.requireCurrent();
            input.requireValue();
            if (input.fenceHandoffConfirmed) {
                return true;
            }
            actual = input.fenceWriter;
            if (actual == null) {
                input.fenceHandoffConfirmed = true;
                return true;
            }
            if (input.fenceHandoff == null) {
                input.fenceHandoff = new RecoveryFenceHandoff(input, phase);
            }
            permit = input.fenceHandoff;
            permit.requireWriter(actual);
        }
        final boolean closed = actual.closeForHandoff(permit);
        final boolean exited = actual.operationsExited();
        synchronized (this) {
            permit.requireWriter(actual);
            if (closed && !exited) {
                throw new IllegalStateException(
                        "normally closed Fence producer still has original native/callback cleanup work");
            }
            if (closed) {
                input.fenceHandoffConfirmed = true;
            }
            return closed;
        }
    }

    private void requireRecoveryFenceHandoffs() {
        for (var input : recoveryInputs) {
            if (input.fenceWriter != null && !input.fenceHandoffConfirmed) {
                throw new IllegalStateException("original recovery Fence producer normal handoff is pending");
            }
        }
    }

    /** Private root-issued close permission; no apply/ACK/append authority and no hold release. */
    public static final class JournalHandoff {
        private final RecoveryInputsOpening input;
        private final PulsarPreparation preparation;
        private final long sequence;
        private final com.nereusstream.delay.protocol.SourcePosition cut;

        private JournalHandoff(RecoveryInputsOpening input, PulsarPreparation preparation) {
            this.input = input;
            this.preparation = preparation;
            sequence = input.startup.store.latestSequenceNumber();
            cut = input.startup.store.appliedShardLogPosition();
        }

        public void requireWriter(TargetSourceRecoveryInputs.JournalWriter actual) {
            final var startup = input.startup;
            synchronized (startup) {
                if (input.handoff != this
                        || input.writer != actual
                        || input.closed
                        || startup.worker != null
                        || startup.constructionThread != null
                        || input.io == null
                        || !input.io.operationExited()
                        || !input.io.ioExited()) {
                    throw new IllegalStateException("Journal handoff belongs to another original recovery writer");
                }
                actual.requireBinding(input);
                if (preparation != null) {
                    if (startup.pulsarPreparation != preparation || preparation.previousInputs != input) {
                        throw new IllegalStateException("Journal handoff lost its exact frozen preparation");
                    }
                    preparation.requireCurrent();
                } else {
                    startup.requireRecoveryReaderCurrent();
                    if (startup.currentRecoveryInputs != input
                            || startup.activation == null
                            || startup.recovery == null
                            || !startup.recovery.coordinator().readyForJournalHandoff()
                            || startup.recoverySource == null
                            || !startup.recoverySource.cleanupComplete()) {
                        throw new IllegalStateException(
                                "service Journal handoff requires complete original Source/Claim recovery");
                    }
                }
                if (startup.storeClosed
                        || startup.store.latestSequenceNumber() != sequence
                        || !Arrays.equals(
                                cut.canonicalBytes(),
                                startup.store.appliedShardLogPosition().canonicalBytes())) {
                    throw new IllegalStateException("Journal handoff changed its original Store cut");
                }
                for (var held : startup.recoveryInputs) {
                    if (held.original != null) {
                        held.requireValue();
                    }
                }
            }
        }
    }

    private boolean settleRecoveryJournal(RecoveryInputsOpening input, PulsarPreparation preparation) {
        try {
            final JournalHandoff permit;
            synchronized (this) {
                if (input == null) {
                    return true;
                } // Component path with no deployment recovery input hold.
                input.requireValue();
                if (input.handoffConfirmed) {
                    return true;
                }
                if (input.writer == null) {
                    input.handoffConfirmed = true;
                    return true;
                }
                if (input.handoff == null) {
                    input.handoff = new JournalHandoff(input, preparation);
                }
                permit = input.handoff;
                permit.requireWriter(input.writer);
            }
            // Native/Journal locks never run under the startup monitor.
            final boolean complete = input.writer.closeForHandoff(permit);
            synchronized (this) {
                permit.requireWriter(input.writer);
                if (complete) {
                    input.handoffConfirmed = true;
                }
                return complete;
            }
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw failure;
        }
    }

    /** Same-Owner second handoff, before the Worker/Host can admit business Source or publishing. */
    public boolean prepareServiceJournalHandoff() {
        try {
            final RecoveryInputsOpening input;
            synchronized (this) {
                requireRecoveryReaderCurrent();
                if (activation == null || constructionThread != null || worker != null) {
                    return false;
                }
                if (recovery != null) {
                    recovery.coordinator().stopAuthorityPreparation();
                    recovery.coordinator().retryAuthorityCleanup();
                    if (!recovery.coordinator().readyForJournalHandoff()
                            || recoverySource == null
                            || !recoverySource.cleanupComplete()) {
                        return false;
                    }
                }
                input = currentRecoveryInputs;
                requireRecoveryFenceHandoffs();
            }
            if (!settleRecoveryJournal(input, null)) {
                return false;
            }
            synchronized (this) {
                requireRecoveryReaderCurrent();
                serviceJournalHandoffComplete = true;
            }
            return true;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
    }

    /** Active Journal must reopen the same physical namespace/producer after all normal recovery writer closes. */
    public synchronized void requireActiveJournalOpening(
            com.nereusstream.delay.adapter.PulsarJournalResource resource, String producerName) {
        requireCurrent();
        if (!recoveryInputs.isEmpty() && !serviceJournalHandoffComplete) {
            throw new IllegalStateException("service Journal handoff is pending");
        }
        for (var input : recoveryInputs) {
            if (input.writer != null) {
                input.requireValue();
                if (!input.handoffConfirmed
                        || !producerName.equals(input.writer.producerName())
                        || !resource.sameIdentity(input.writer.resource())) {
                    throw new IllegalStateException(
                            "active Journal lacks the original normally closed recovery namespace/producer");
                }
            }
        }
    }

    private boolean recoveryInputOperationsExited(boolean stop) {
        boolean exited = true;
        for (var opening : sloRecoveries) {
            exited &= opening.operationsExited(stop);
        }
        if (recoveryTailCapture != null) {
            if (stop) {
                recoveryTailCapture.requestStop();
            }
            exited &= recoveryTailCapture.operationExited();
        }
        for (var input : recoveryInputs) {
            if (input.io == null) {
                continue;
            }
            if (stop) {
                input.io.requestStop();
            }
            exited &= input.io.operationExited();
        }
        return exited;
    }

    private boolean recoveryInputCleanupExited() {
        boolean exited = true;
        for (var opening : sloRecoveries) {
            exited &= opening.cleanupExited();
        }
        if (recoveryTailCapture != null) {
            recoveryTailCapture.retryCleanup();
            exited &= recoveryTailCapture.ioExited();
        }
        for (var input : recoveryInputs) {
            if (input.io == null) {
                continue;
            }
            input.io.retryCleanup();
            exited &= input.io.ioExited();
        }
        return exited;
    }

    private boolean releaseRecoveryInputs() {
        if (!recoveryInputOperationsExited(true) || !recoveryInputCleanupExited()) {
            return false;
        }
        if (!releaseFrameDecodeLoans()) {
            return false;
        }
        boolean released = true;
        for (var input : recoveryInputs) {
            if (input.original == null || input.closed) {
                continue;
            }
            try {
                input.original.close();
                input.closed = true;
            } catch (RuntimeException | Error failure) {
                retain(failure);
                released = false;
            }
        }
        if (released && channelInventory != null && !channelInputsReleased) {
            channelInventory.releaseAfterExit();
            process.releaseChannelInventory(this, channelInventory);
            channelInputsReleased = true;
        }
        if (released) {
            sloRecoveries.clear();
            currentSloRecovery = null;
        }
        return released;
    }

    void requireRecoveryInputBackend(TargetOwnedBackendGroup actual) {
        requireCurrent();
        if (!recoveryInputBackends.contains(actual)) {
            throw new IllegalArgumentException("SDK owner is not an original recovery input allocation");
        }
    }

    /** Only this startup can issue a receipt for its unexposed Source/runtime and actual Store barrier. */
    public synchronized Optional<PulsarPreparation> preparePulsarSource() {
        process.requireSourceAssemblyAdmission(this);
        if (pulsarPreparation != null) {
            pulsarPreparation.requireCurrent();
            return Optional.of(pulsarPreparation);
        }
        requireRecoveryReaderCurrent();
        if (initializationRunning
                || constructionThread != null
                || worker != null
                || firstFailure != null
                || !(assignment.activationBarrier()
                        instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier)) {
            throw new IllegalStateException("P1 preparation requires its original unexposed startup");
        }
        if (!recoveryInputOperationsExited(false) || !recoveryInputCleanupExited()) {
            return Optional.empty();
        }
        requireRecoveryFenceHandoffs();
        if (recovery != null) {
            if (!recovery.coordinator().sourceReadyForReactivation()
                    || recoverySource == null
                    || !recoverySource.cleanupComplete()) {
                return Optional.empty();
            }
            recovery.coordinator().stopAuthorityPreparation();
            recovery.coordinator().retryAuthorityCleanup();
            if (!recovery.coordinator().authorityIoExited()) {
                return Optional.empty();
            }
            if (!retirePayloadSummaryRange()) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
        if (activation != null) {
            activation.runtime().fence();
            activation.runtime().retryAuthorityCleanup();
            if (!activation.runtime().authorityIoExited()) {
                return Optional.empty();
            }
        }
        final var current =
                graph.oxia().providers().leases().current(assignment.shardId()).orElseThrow();
        if (!lease.sameIdentity(current)
                || current.state() != ShardLifecycleState.ACQUIRING
                        && current.state() != ShardLifecycleState.CATCHING_UP
                        && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Source preparation lost its original pre-fence Owner");
        }
        final var reads = TargetWorkerSourceRuntimeFactory.startupReads(
                graph.oxia(),
                owner.tenant(),
                publication,
                owner.workerId(),
                store,
                assignment,
                lease,
                content.reads(),
                owner.clock());
        final long sequence = store.latestSequenceNumber();
        TargetStoreBootstrap.reopen(
                store,
                scope,
                new TargetStoreBackend.WriteLimits(
                        graph.config().source().records(),
                        graph.config().source().bytes()),
                graph.config().checkpoint().newReadBudget(nanos),
                reads,
                graph.config().checkpoint().ledger());
        final var cut = Objects.requireNonNull(store.appliedShardLogPosition());
        if (!assignment.activationBarrier().reachedBy(cut)) {
            return Optional.empty();
        }
        pulsarPreparation = new PulsarPreparation(this, publication, current, cut, sequence);
        try {
            pulsarPreparation.fenceOriginal();
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
        return Optional.of(pulsarPreparation);
    }

    /** SDK preparation only; a frozen Owner never becomes an ordinary Source/Store authority. */
    public synchronized void requirePulsarOpeningAuthority() {
        if (pulsarPreparation == null) {
            requireCurrent();
        } else {
            pulsarPreparation.requireCurrent();
        }
    }

    public synchronized Construction beginPulsarClientOpening() {
        if (initializationRunning || backend != null) {
            throw new IllegalStateException(
                    "P1 client opening requires its actual initial Store boundary and original SDK owner");
        }
        if (!(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier)) {
            throw new IllegalStateException("P1 client opening changes the original Source kind");
        }
        return beginConstruction(false);
    }

    /** The actual K1 consumer may open before recovery activation, but never as a replacement SDK owner. */
    public synchronized Construction beginKafkaClientOpening() {
        requireCurrent();
        if (initializationRunning
                || backend != null
                || !(assignment.activationBarrier()
                        instanceof com.nereusstream.delay.protocol.KafkaActivationBarrier)) {
            throw new IllegalStateException(
                    "K1 client opening requires its original initialized Source boundary and SDK owner");
        }
        return beginConstruction(false);
    }

    public synchronized boolean needsKafkaRecovery() {
        requireCurrent();
        if (!(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.KafkaActivationBarrier)) {
            throw new IllegalStateException("K1 recovery changes its original Source kind");
        }
        return store != null && !storeClosed && activation == null && recovery == null;
    }

    public synchronized boolean kafkaRecoveryPending() {
        requireCurrent();
        if (!(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.KafkaActivationBarrier)) {
            throw new IllegalStateException("K1 recovery changes its original Source kind");
        }
        return recovery != null && recoverySource != null && activation == null;
    }

    public synchronized Construction beginPulsarConsumerOpening(PulsarPreparation original) {
        if (original != pulsarPreparation || constructionThread != null || worker != null) {
            throw new IllegalStateException("prepared P1 consumer construction belongs to another original call");
        }
        original.requireCurrent();
        final var current = Thread.currentThread();
        final var construction = new Construction(this, current, original);
        process.beginSourceConstruction(this, construction);
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
        private final RecoveryInputsOpening previousInputs;
        private WorkerAssignment successor;
        private WorkerAssignmentAuthority.Publication published;
        private OwnerLease acquired;
        private com.nereusstream.delay.protocol.RecoveryPin replacementPin;
        private boolean openedSuccessor;

        private PulsarPreparation(
                TargetWorkerSourceStartup startup,
                WorkerAssignmentAuthority.Publication publication,
                OwnerLease lease,
                com.nereusstream.delay.protocol.SourcePosition cut,
                long sequence) {
            this.startup = startup;
            originalPublication = publication;
            originalLease = lease;
            this.cut = cut;
            this.sequence = sequence;
            previousRecovery = startup.recovery;
            previousReader = startup.recoverySource;
            previousPin = startup.checkpointPin;
            previousInputs = startup.currentRecoveryInputs;
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

        public com.nereusstream.delay.protocol.SourcePosition position() {
            requireCurrent();
            return cut;
        }

        public SourceAssignment assignment() {
            requireCurrent();
            return originalPublication.assignment().sourceAssignment();
        }

        void requirePriorBackend(TargetOwnedBackendGroup actual) {
            requireCurrent();
            if (previousReader == null || !previousReader.cleanupComplete() || previousReader.backend() != actual) {
                throw new IllegalArgumentException(
                        "backend is not the original retired preparation reader's SDK owner");
            }
        }
        /** Advances one original consumer/authority handoff; the consumer supplies actual SDK facts, never publication authority. */
        public boolean advanceOwner(SourceRecordConsumer prepared) {
            if (!startup.settleRecoveryJournal(previousInputs, this)) {
                return false;
            }
            if (!startup.releaseFrameDecodeLoans()) {
                return false;
            }
            synchronized (startup) {
                requireCurrent();
                if (!startup.recoveryInputOperationsExited(false) || !startup.recoveryInputCleanupExited()) {
                    return false;
                }
                if (prepared.ownedBackendGroup() != startup.backend || startup.backend == null) {
                    throw new IllegalArgumentException("prepared P1 consumer belongs to another original SDK owner");
                }
                final var barrier = prepared.preparedPulsarBarrier(this);
                if (successor == null) {
                    final var previous = originalPublication.assignment();
                    final var id = java.util.UUID.randomUUID();
                    final byte[] assignmentId = Bytes.concat(
                            Bytes.u64beBits(id.getMostSignificantBits()),
                            Bytes.u64beBits(id.getLeastSignificantBits()));
                    final var source = new SourceAssignment(
                            previous.sourceAssignment().shardId(),
                            assignmentId,
                            nextEpoch(previous.sourceAssignment().assignmentEpoch()),
                            barrier);
                    new PulsarSourceReactivation(previous.routeSnapshotDigest(), previous.sourceAssignment(), source);
                    successor = new WorkerAssignment(
                            previous.workerId(),
                            source,
                            nextEpoch(previous.placementEpoch()),
                            previous.capacityEnvelopeDigest(),
                            previous.routeSnapshotDigest());
                } else if (!successor.sourceAssignment().activationBarrier().equals(barrier)) {
                    throw new IllegalStateException(
                            "prepared P1 connection changed after the original successor intent");
                }
                if (openedSuccessor) {
                    return true;
                }
                if (published == null) {
                    frozen.closeOriginal();
                    if (!frozen.originalClosed()) {
                        return false;
                    }
                    startup.storeClosed = true;
                    final var providers = startup.graph.oxia().providers();
                    final var route = new RouteSourceAssignmentResolver(providers.routes())
                            .exactResolved(
                                    startup.owner.tenant(),
                                    originalLease.shardId().routeIncarnation(),
                                    originalLease.shardId().partition(),
                                    originalLease.sourceAssignmentId(),
                                    originalLease.sourceAssignmentEpoch())
                            .routeSnapshot();
                    final var transition = new PulsarSourceReactivation(
                            originalPublication.assignment().routeSnapshotDigest(),
                            originalPublication.assignment().sourceAssignment(),
                            successor.sourceAssignment());
                    final var coordinator = new PulsarSourceReactivationCoordinator(
                            providers.routes(), providers.assignments(), providers.leases());
                    published = coordinator.publishSuccessor(
                            new PulsarSourceReactivationCoordinator.FencedPlan(
                                    originalPublication, originalLease, fencedLease, transition, route),
                            successor,
                            this::requireCurrent);
                    startup.publication = published;
                    startup.leaseReleased = true;
                }
                prepared.preparedPulsarBarrier(this);
                if (acquired == null) {
                    acquired = TargetWorkerSourceRuntimeFactory.acquire(
                            startup.graph.oxia(),
                            startup.owner.tenant(),
                            published,
                            startup.owner.workerId(),
                            startup.owner.ownerId(),
                            startup.owner.leaseDurationMs(),
                            startup.owner.clock());
                    startup.assignment = successor.sourceAssignment();
                    startup.lease = acquired;
                    startup.leaseReleased = false;
                }
                if (!replacePin()) {
                    return false;
                }
                if (!openedSuccessor) {
                    final var previousStore = startup.store;
                    final var reopened = ShardStore.openTarget(
                            startup.graph.config().store(), originalLease.shardId(), startup.graph.resources());
                    if (startup.sourcePlanReservation != null) {
                        startup.sourcePlanReservation.rebindAfterStoreExit(previousStore, reopened);
                    }
                    if (startup.sourceFrames != null) {
                        startup.sourceFrames.close();
                        startup.sourceFrames = null;
                    }
                    startup.store = reopened;
                    startup.storeClosed = false;
                    startup.requireMigrationAdoption();
                    if (!Arrays.equals(
                                    frozen.storeIncarnation(),
                                    reopened.metadata().storeIncarnation())
                            || !Bytes.constantTimeEquals(
                                    cut.canonicalBytes(),
                                    reopened.appliedShardLogPosition().canonicalBytes())) {
                        throw new IllegalStateException(
                                "successor Store changed the original positioned incarnation/cut");
                    }
                    startup.activation = null;
                    startup.failedAssembly = null;
                    startup.recovery = null;
                    startup.recoverySource = null;
                    startup.recoveryTailCapture = null;
                    startup.currentSloRecovery = null;
                    startup.recoveryCleanupFailure = null;
                    openedSuccessor = true;
                }
                requireCurrent();
                return true;
            }
        }

        private boolean replacePin() {
            if (previousPin == null) {
                return true;
            }
            final var catalog = startup.checkpointCatalog;
            if (startup.ownsCheckpointPin) {
                if (replacementPin == null) {
                    final var floor = catalog.currentFloorRef().orElseThrow();
                    final var id = java.util.UUID.randomUUID();
                    replacementPin = new com.nereusstream.delay.protocol.RecoveryPin(
                            Bytes.concat(
                                    Bytes.u64beBits(id.getMostSignificantBits()),
                                    Bytes.u64beBits(id.getLeastSignificantBits())),
                            previousPin.shard(),
                            new com.nereusstream.delay.protocol.OwnerIdentity(
                                    previousPin.owner().deploymentId(),
                                    previousPin.owner().workerRunId(),
                                    acquired.ownerEpoch(),
                                    acquired.leaseToken()),
                            previousPin.candidate(),
                            floor,
                            floor.catalogGeneration(),
                            startup.graph.oxia().providers().sessionIdentity());
                }
                final var actual = catalog.replaceRecoveryPin(previousPin, replacementPin);
                if (!replacementPin.equals(actual)) {
                    throw new IllegalStateException("successor pin changed its exact request");
                }
            } else {
                final var actual = catalog.activeRecoveryPin().orElseThrow();
                if (actual.equals(previousPin)) {
                    return false;
                }
                if (!actual.shard().equals(previousPin.shard())
                        || !actual.candidate().equals(previousPin.candidate())
                        || actual.owner().ownerEpoch() != acquired.ownerEpoch()
                        || !Bytes.constantTimeEquals(actual.owner().leaseFencingDigest(), acquired.leaseToken())
                        || !Bytes.constantTimeEquals(actual.oxiaSessionIdentityDigest(), acquired.sessionIdentity())) {
                    throw new IllegalStateException(
                            "external successor pin lacks its actual new Owner/candidate/session");
                }
                replacementPin = actual;
            }
            startup.checkpointPin = replacementPin;
            startup.checkpointPinConfirmed = true;
            return true;
        }

        private static long nextEpoch(long epoch) {
            if (epoch == -1L) {
                throw new IllegalStateException("Source reactivation epoch exhausted");
            }
            return epoch + 1;
        }
        /** FENCED permits only this frozen-cut/SDK preparation check, never an ordinary Store write guard. */
        public void requireCurrent() {
            synchronized (startup) {
                startup.process.requireSourceAssemblyAdmission(startup);
                if (startup.stopping
                        || startup.firstFailure != null
                        || startup.pulsarPreparation != this
                        || frozen == null) {
                    throw new IllegalStateException("original P1 preparation is not available");
                }
                final var providers = startup.graph.oxia().providers();
                final var publication =
                        providers.assignments().current(originalLease.shardId()).orElseThrow();
                final var current = providers.leases().current(originalLease.shardId());
                long now = startup.owner.clock().getAsLong();
                if (!(published == null ? originalPublication : published).sameIdentity(publication)
                        || now < 0
                        || now < startup.lastNow
                        || !Bytes.constantTimeEquals(originalLease.sessionIdentity(), providers.sessionIdentity())) {
                    throw new IllegalStateException("P1 preparation lost its original publication/Owner/session/time");
                }
                if (published == null) {
                    final var actual = current.orElseThrow();
                    if (!fencedLease.sameIdentity(actual)
                            || actual.state() != ShardLifecycleState.FENCED
                            || !actual.validAt(now)) {
                        throw new IllegalStateException("P1 preparation lost its exact fenced Owner");
                    }
                } else if (acquired == null) {
                    if (current.isPresent()) {
                        throw new IllegalStateException("another Owner entered the successor acquisition gap");
                    }
                } else {
                    final var actual = current.orElseThrow();
                    if (!acquired.sameIdentity(actual)
                            || !actual.validAt(now)
                            || actual.state() != ShardLifecycleState.ACQUIRING
                                    && actual.state() != ShardLifecycleState.CATCHING_UP
                                    && actual.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                        throw new IllegalStateException("P1 preparation lost its actual successor Owner");
                    }
                }
                startup.lastNow = now;
                frozen.requireCurrent();
                previousRecovery.coordinator().requireSourcePreparationProtection();
                if (startup.checkpointCatalog != null) {
                    final var actual =
                            startup.checkpointCatalog.activeRecoveryPin().orElseThrow();
                    if (!startup.ownsCheckpointPin
                            && acquired != null
                            && !actual.equals(previousPin)
                            && replacementPin == null) {
                        if (!actual.shard().equals(previousPin.shard())
                                || !actual.candidate().equals(previousPin.candidate())
                                || actual.owner().ownerEpoch() != acquired.ownerEpoch()
                                || !Bytes.constantTimeEquals(actual.owner().leaseFencingDigest(), acquired.leaseToken())
                                || !Bytes.constantTimeEquals(
                                        actual.oxiaSessionIdentityDigest(), acquired.sessionIdentity())) {
                            throw new IllegalStateException(
                                    "external pin changed outside the actual successor Owner/candidate/session");
                        }
                        replacementPin = actual;
                    }
                    if (!actual.equals(previousPin) && (replacementPin == null || !actual.equals(replacementPin))) {
                        throw new IllegalStateException("P1 preparation lost its original or exact replacement pin");
                    }
                    if (startup.checkpointPinConfirmed
                            && replacementPin != null
                            && startup.checkpointPin.equals(replacementPin)
                            && !actual.equals(replacementPin)) {
                        throw new IllegalStateException("confirmed successor pin was lost");
                    }
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
        return new com.nereusstream.delay.protocol.OwnerIdentity(
                original.deploymentId(), original.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
    }

    TargetWorkerSourceStartup(
            TargetWorkerProcessRuntime process,
            TargetWorkerProcessRuntime.Assembly graph,
            Owner owner,
            Initial initial,
            TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor successor,
            LongSupplier nanos) {
        this.process = process;
        this.graph = graph;
        this.owner = Objects.requireNonNull(owner);
        this.publication = owner.publication();
        this.initial = Objects.requireNonNull(initial);
        this.content = Objects.requireNonNull(content);
        this.scope = initial.scope();
        this.successor = Objects.requireNonNull(successor);
        this.nanos = nanos;
    }

    TargetWorkerSourceStartup(
            TargetWorkerProcessRuntime process,
            TargetWorkerProcessRuntime.Assembly graph,
            Owner owner,
            TargetQuotaScope scope,
            TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor activeSuccessor,
            LongSupplier nanos) {
        this.process = process;
        this.graph = graph;
        this.owner = Objects.requireNonNull(owner);
        this.publication = owner.publication();
        this.initial = null;
        this.scope = Objects.requireNonNull(scope);
        this.content = Objects.requireNonNull(content);
        this.successor = Objects.requireNonNull(activeSuccessor);
        this.nanos = nanos;
        if (scope.target() != null) {
            throw new IllegalArgumentException("Source reopen requires its original Shard scope");
        }
    }

    /** Optional deployment owners are acquired on the original bounded I/O graph, never converted from configuration. */
    private TargetWorkerSourceRuntimeFactory.Content scopeContent() {
        if (content.scopeResources() == null) {
            return content.withSourcePlanInputs(sourcePlanReservation());
        }
        final OwnedCheckpointIo<TargetScopeResourceManager> opening;
        synchronized (this) {
            if (scopedContent != null) {
                return scopedContent;
            }
            if (scopeResourcesClosing) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable("original Scope owner is closing");
            }
            if (scopeResourceOpening == null) {
                final var inputs = content.scopeResources();
                final long reservedBytes = TargetScopeResourceManager.maximumResidentBytes(
                        inputs, content.maximumBytes(), content.maximumBytes());
                scopeInputReservation = process.reserveScopeInputs(this, reservedBytes);
                final var reservation = scopeInputReservation;
                final var actualSource =
                        Objects.requireNonNull(store.appliedShardLogPosition(), "actual Source-paid Store frontier");
                final Runnable local = () -> {
                    if (stopping || scopeResourcesClosing || firstFailure != null) {
                        throw new IllegalStateException("original Scope process allocation is no longer admitted");
                    }
                    reservation.requireCurrent();
                    graph.resources().requireRuntimeBusinessAdmission();
                };
                scopeResourceOpening = OwnedCheckpointIo.submitResource(
                        guard -> {
                            guard.run();
                            final var registrations = graph.oxia().providers().registrations();
                            final var storage = registrations.scopeResourceStorage(
                                    inputs.configuration().policy());
                            final var actual = new TargetScopeDeploymentAllocation.ActualBinding(
                                    process, actualSource, storage.namespaceBinding(), storage.keyPrefix());
                            final var manager = TargetScopeResourceManager.attach(registrations, actual, inputs, local);
                            guard.run();
                            return manager;
                        },
                        this::requireCurrent,
                        TargetScopeResourceManager::close,
                        graph.io());
            }
            opening = scopeResourceOpening;
        }
        // Source construction is outside Worker/Store monitors; the original handle retains late/failed acquisition.
        final TargetScopeResourceManager manager;
        try {
            manager = opening.receipt()
                    .get(
                            content.scopeResources().configuration().limits().requestTimeoutNanos(),
                            java.util.concurrent.TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new StartupFailure(interrupted, this);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException failure) {
            throw new StartupFailure(failure, this);
        }
        if (!opening.operationExited() || !opening.ioExited()) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original Scope allocation acquisition has not exited");
        }
        final var registrations =
                graph.oxia().providers().registrations().withSloScopes(manager.protections(), content.maximumBytes());
        final var provider = new TargetScopeSourceAuthorityProvider(
                content.provider(),
                registrations,
                manager,
                content.maximumBytes(),
                content.maximumBytes(),
                owner.clock());
        final var resolved = new TargetWorkerSourceRuntimeFactory.Content(
                        provider,
                        content.physical(),
                        content.reads(),
                        content.lookupTimeout(),
                        content.maximumBytes(),
                        content.evidencePublication())
                .withSourcePlanInputs(sourcePlanReservation());
        synchronized (this) {
            if (scopeResources != null && scopeResources != manager) {
                throw new IllegalStateException("original Scope allocation changed");
            }
            scopeResources = manager;
            if (scopedContent == null) {
                scopedContent = resolved;
            }
            return scopedContent;
        }
    }

    private synchronized TargetWorkerProcessRuntime.SourcePlanInputs sourcePlanReservation() {
        if (sourcePlanReservation == null) {
            sourcePlanReservation = process.reserveSourcePlanInputs(
                    this, Objects.requireNonNull(store, "original Source Store"), content.maximumBytes());
        }
        return sourcePlanReservation;
    }

    /** Actual fixed Root points under the original Source read authority, on its existing I/O flight. */
    public FrameOpening frameOpening() {
        final ShardStore actual;
        final SourceAssignment accepted;
        final OwnerLease ownerLease;
        synchronized (this) {
            actual = Objects.requireNonNull(store);
            accepted = Objects.requireNonNull(assignment);
            ownerLease = Objects.requireNonNull(lease);
        }
        requireCurrent();
        final var budget = graph.config().checkpoint().newReadBudget(nanos);
        try (final var guard = content.reads().acquire(actual.metadata(), scope)) {
            final var captured = actual.readWithBudget(budget, () -> {
                        guard.requireCurrent();
                        final byte[] bookKey =
                                com.nereusstream.delay.protocol.TargetQuotaBookkeeping.keyFor(scope.shard());
                        final byte[] bookPayload = com.nereusstream.delay.store.TargetValueEnvelope.decode(
                                        actual.get(com.nereusstream.delay.store.ColumnFamily.META, bookKey),
                                        com.nereusstream.delay.protocol.TargetQuotaBookkeeping.VALUE_TYPE)
                                .payload();
                        // The encoded owner only selects the Root; the same payload is fully checked against that
                        // actual Root below.
                        final byte[] rootKey = com.nereusstream.delay.protocol.TargetQuotaBookkeeping.decode(
                                        bookPayload)
                                .owner()
                                .key();
                        rootKey[0] = com.nereusstream.delay.store.TargetKeyCodec.QUOTA_INCARNATION_TAG;
                        final var root = com.nereusstream.delay.protocol.TargetQuotaIncarnation.decodeForStore(
                                rootKey,
                                com.nereusstream.delay.store.TargetValueEnvelope.decode(
                                                actual.get(com.nereusstream.delay.store.ColumnFamily.META, rootKey),
                                                com.nereusstream.delay.protocol.TargetQuotaIncarnation.VALUE_TYPE)
                                        .payload(),
                                scope.shard(),
                                scope.tenantScope());
                        final var book = com.nereusstream.delay.protocol.TargetQuotaBookkeeping.decodeForStore(
                                bookKey, bookPayload, root.identity(), scope.tenantScope());
                        final byte[] aggregateKey = com.nereusstream.delay.protocol.TargetQuotaAggregate.genesis(
                                        scope.shard(), root.identity().accountingIncarnation())
                                .key();
                        final var aggregate = com.nereusstream.delay.protocol.TargetQuotaAggregate.decodeForStore(
                                aggregateKey,
                                com.nereusstream.delay.store.TargetValueEnvelope.decode(
                                                actual.get(
                                                        com.nereusstream.delay.store.ColumnFamily.META, aggregateKey),
                                                com.nereusstream.delay.protocol.TargetQuotaAggregate.VALUE_TYPE)
                                        .payload(),
                                scope.shard());
                        if (!root.identity().equals(book.owner())
                                || !Arrays.equals(
                                        root.identity().accountingIncarnation(), aggregate.accountingIncarnation())
                                || !Arrays.equals(
                                        root.accounting().canonicalBytes(),
                                        book.accounting().canonicalBytes())) {
                            throw new IllegalStateException(
                                    "Source frame capture changed its actual Root/accounting binding");
                        }
                        root.latestMutation().requireAtOrBefore(aggregate.mutation());
                        book.mutation().requireAtOrBefore(aggregate.mutation());
                        guard.requireCurrent();
                        return new FrameOpening(
                                this,
                                actual,
                                scope,
                                root.recoveryLineage(),
                                accepted,
                                ownerLease,
                                Objects.requireNonNull(actual.appliedShardLogPosition()));
                    })
                    .value();
            requireCurrent();
            captured.requireLocalCurrent();
            return captured;
        }
    }

    TargetWorkerSourceRuntimeFactory.SourceFrameContract captureSourceFrames() {
        final var opening = frameOpening();
        final var retained = sourceFrames;
        if (retained != null) {
            retained.requireOpening(opening);
            return retained;
        }
        TargetWorkerSourceRuntimeFactory.SourceFrameContract actual = null;
        final RecoveryInputsOpening input;
        synchronized (this) {
            input = recoveryInputs.stream()
                    .filter(value -> value.original != null
                            && value.assignment.sameIdentity(assignment)
                            && value.owner.ownerEpoch() == lease.ownerEpoch()
                            && Arrays.equals(value.owner.leaseFencingDigest(), lease.leaseToken()))
                    .findFirst()
                    .orElse(null);
        }
        if (input != null
                && input.original != null
                && input.assignment.sameIdentity(assignment)
                && input.owner.ownerEpoch() == lease.ownerEpoch()
                && Arrays.equals(input.owner.leaseFencingDigest(), lease.leaseToken())) {
            try {
                actual = input.original.sourceFrames();
            } catch (TargetSourceAuthorityPreparation.InputUnavailable missing) {
                /* same original provider below */
            }
        }
        if (actual == null) {
            final var selected = scopedContent == null ? content : scopedContent;
            actual = Objects.requireNonNull(selected.provider().captureSourceFrames(opening));
        }
        actual.requireOpening(opening);
        sourcePlanReservation().bindSourceFrames(actual);
        sourcePlanReservation.prepareDecodeWindow();
        sourceFrames = actual;
        return actual;
    }

    TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrameContract() {
        final var actual = sourceFrames;
        if (actual == null) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "actual Source frame qualification is not captured");
        }
        actual.requireLocalCurrent();
        return actual;
    }

    TargetWorkerProcessRuntime.SourcePlanInputs prepareMigrationFrameInputs(TargetMigrationProcessDriver original) {
        process.requireMigrationRecoveryAdmission(original, this, store);
        scopeContent();
        captureSourceFrames();
        return sourcePlanReservation();
    }

    public FrameDecodeLoan acquireFrameDecode(
            com.nereusstream.delay.protocol.SourcePosition actual,
            long completeFrameBytes,
            Long generation,
            byte[] guardAttestation) {
        final var frames = sourceFrames;
        final var reserved = sourcePlanReservation;
        if (frames == null) {
            return process.migrationHistoryFrameDecode(this, actual, completeFrameBytes, generation, guardAttestation);
        }
        if (reserved == null) {
            throw new FrameCapacityUnavailable("same original Source frame capacity is not yet available");
        }
        frames.requireFrame(actual, completeFrameBytes, generation, guardAttestation);
        final var decode = com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.frameDecode(completeFrameBytes);
        reserved.borrowDecode(decode.retainedBytes());
        return borrowFrameDecode(
                frames,
                actual,
                completeFrameBytes,
                generation,
                guardAttestation,
                decode,
                () -> reserved.releaseDecode(decode.retainedBytes()),
                frameDecodeLoans);
    }

    static FrameDecodeLoan borrowFrameDecode(
            TargetWorkerSourceRuntimeFactory.SourceFrameContract frames,
            com.nereusstream.delay.protocol.SourcePosition actual,
            long bytes,
            Long generation,
            byte[] attestation,
            com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds decode,
            Runnable release,
            java.util.Set<FrameDecodeLoan> retained) {
        final var loan = new FrameDecodeLoan(
                frames, release, retained::remove, actual, bytes, decode.retainedBytes(), generation, attestation);
        retained.add(loan);
        try {
            loan.physical = frames.borrow(actual, bytes, generation, attestation, decode);
            if (loan.releaseRequested) {
                loan.physical.requestRelease();
            }
            loan.physicalPending = false;
            loan.requireCurrent();
            return loan;
        } catch (RuntimeException | Error failure) {
            loan.physicalPending = false;
            loan.requestRelease();
            throw failure;
        }
    }

    private boolean releaseFrameDecodeLoans() {
        for (final var loan : frameDecodeLoans) {
            loan.requestRelease();
            if (!loan.cleanupComplete()) {
                return false;
            }
        }
        return frameDecodeLoans.isEmpty();
    }

    /** The configured Source producer uses the same managed registry and original allocation as Source replay. */
    public TargetScopeExposureSource.Issuer scopeExposureIssuer(TargetWorkerShardRuntime actualWorker) {
        final TargetScopeResourceManager manager;
        synchronized (this) {
            if (content.scopeResources() == null) {
                return null;
            }
            if (worker != actualWorker || scopeResources == null || scopeResourcesClosing || stopping) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable(
                        "configured Source exposure lacks its actual owned Scope manager/Worker");
            }
            manager = scopeResources;
        }
        final var issuer = graph.oxia()
                .providers()
                .registrations()
                .withSloScopes(manager.protections(), content.maximumBytes())
                .sourceExposureIssuer(actualWorker, content.maximumBytes(), content.maximumBytes(), owner.clock());
        return (mutation, sdk) -> {
            // Bound Native capture before it allocates facts. The successful permit then owns its resource-protection
            // slot.
            final var captureSlot = manager.borrowOriginalOwner();
            try {
                final var permit = issuer.acquire(mutation, sdk);
                captureSlot.close();
                return permit;
            } catch (RuntimeException | Error failure) {
                if (failure instanceof CheckpointInputFuture.Cleanup cleanup && !cleanup.cleanupComplete()) {
                    throw new ScopeIssuerCleanup(captureSlot, cleanup, failure);
                }
                captureSlot.close();
                throw failure;
            }
        };
    }

    private static final class ScopeIssuerCleanup extends IllegalStateException
            implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetScopeDeploymentAllocation.Loan captureSlot;
        private final transient CheckpointInputFuture.Cleanup original;
        private transient java.util.concurrent.CompletableFuture<Void> retry;
        private boolean complete;

        private ScopeIssuerCleanup(
                TargetScopeDeploymentAllocation.Loan captureSlot,
                CheckpointInputFuture.Cleanup original,
                Throwable failure) {
            super("original bounded Scope capture slot remains retained", failure);
            this.captureSlot = captureSlot;
            this.original = original;
        }

        @Override
        public synchronized boolean cleanupComplete() {
            if (!complete && original.cleanupComplete() && (retry == null || CheckpointInputFuture.ioExited(retry))) {
                captureSlot.close();
                complete = true;
            }
            return complete;
        }

        @Override
        public synchronized java.util.concurrent.CompletableFuture<Void> retryCleanup() {
            if (cleanupComplete()) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
            try {
                if (retry == null || retry.isDone() && CheckpointInputFuture.ioExited(retry)) {
                    retry = Objects.requireNonNull(original.retryCleanup(), "original Scope capture cleanup future");
                }
                return retry.thenRun(() -> {
                    if (!cleanupComplete()) {
                        throw new IllegalStateException("original Scope capture cleanup remains pending");
                    }
                });
            } catch (RuntimeException | Error failure) {
                return java.util.concurrent.CompletableFuture.failedFuture(failure);
            }
        }
    }

    /** Called only after the original producers/SDK consumers have actually exited. */
    boolean closeScopeResourcesAfterDependencies() {
        scopeResourcesClosing = true;
        try {
            if (!closePayloadSummaryPrefixAfterReader()) {
                return false;
            }
            final var active = activation;
            if (active != null) {
                active.runtime().retryAuthorityCleanup();
                if (!active.runtime().nativeReadLoansExited()) {
                    return false;
                }
                if (writerOriginReservation != null && !active.runtime().authorityIoExited()) {
                    return false;
                }
            }
            final var replay = recovery;
            if (replay != null) {
                replay.coordinator().retryAuthorityCleanup();
                if (!replay.coordinator().nativeReadLoansExited()) {
                    return false;
                }
                if (writerOriginReservation != null && !replay.coordinator().authorityIoExited()) {
                    return false;
                }
            }
            final var previous = pulsarPreparation == null ? null : pulsarPreparation.previousRecovery;
            if (previous != null && previous != replay) {
                previous.coordinator().retryAuthorityCleanup();
                if (!previous.coordinator().nativeReadLoansExited()) {
                    return false;
                }
                if (writerOriginReservation != null && !previous.coordinator().authorityIoExited()) {
                    return false;
                }
            }
            if (writerOriginReservation != null) {
                writerOriginReservation.release();
                writerOriginReservation = null;
            }
            final OwnedCheckpointIo<TargetScopeResourceManager> opening;
            synchronized (this) {
                opening = scopeResourceOpening;
            }
            if (opening != null) {
                opening.requestStop();
                opening.retryCleanup();
                if (!opening.operationExited() || !opening.ioExited()) {
                    return false;
                }
            }
            final TargetScopeResourceManager manager;
            synchronized (this) {
                if (scopeResources == null && opening != null) {
                    scopeResources = opening.completedValue().orElse(null);
                }
                manager = scopeResources;
                if (manager != null && scopeResourceClosing == null) {
                    scopeResourceClosing = OwnedCheckpointIo.submit(
                            ignored -> {
                                manager.close();
                                return true;
                            },
                            () -> {},
                            graph.io());
                }
            }
            final OwnedCheckpointIo<Boolean> closing;
            synchronized (this) {
                closing = scopeResourceClosing;
            }
            if (closing != null) {
                closing.retryCleanup();
                if (!closing.operationExited() || !closing.ioExited()) {
                    return false;
                }
                if (!manager.cleanupComplete()) {
                    synchronized (this) {
                        if (scopeResourceClosing == closing) {
                            scopeResourceClosing = null;
                        }
                    }
                    return false;
                }
            }
            synchronized (this) {
                if (scopeInputReservation != null) {
                    scopeInputReservation.release();
                    scopeInputReservation = null;
                }
            }
            return true;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            return false;
        }
    }

    /** Opens the actual same-DB Target Store under the newly acquired original assignment/session lease. */
    void openReopened() {
        acquireExisting(true);
    }

    void acquireCheckpoint() {
        acquireExisting(false);
    }

    /** Creates one exact ephemeral pin through this root's actual shared intent/Catalog backend. */
    public synchronized com.nereusstream.delay.protocol.RecoveryPin createCheckpointPin(
            String exactShardPrefix,
            byte[] pinId,
            com.nereusstream.delay.protocol.OwnerIdentity identity,
            com.nereusstream.delay.protocol.RecoveryCandidateRef candidate) {
        requireCurrent();
        if (initial != null || store != null || restoreSubmission != null || checkpointPin != null) {
            throw new IllegalStateException("original checkpoint pin request cannot be replaced");
        }
        if (identity.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalArgumentException("checkpoint pin identity changes its actual acquired Owner");
        }
        final var actual = graph.oxia()
                .providers()
                .checkpointPublication(
                        exactShardPrefix, graph.config().checkpoint().physical());
        final var manifest = actual.manifest(candidate.checkpointId()).orElseThrow();
        if (!manifest.shardId().equals(scope.shard())
                || !Bytes.constantTimeEquals(manifest.manifestSha256(), candidate.manifestSha256())
                || !Bytes.constantTimeEquals(manifest.recoveryLineageId(), candidate.recoveryLineageId())) {
            throw new IllegalArgumentException("checkpoint pin candidate changes its original published image");
        }
        final var floor = actual.currentFloorRef().orElseThrow();
        final var requested = new com.nereusstream.delay.protocol.RecoveryPin(
                pinId,
                new com.nereusstream.delay.protocol.ShardSubject(scope.shard()),
                identity,
                candidate,
                floor,
                floor.catalogGeneration(),
                graph.oxia().providers().sessionIdentity());
        checkpointCatalog = actual;
        checkpointPin = requested;
        ownsCheckpointPin = true;
        return retryCheckpointPinCreation();
    }

    /** The original backend owns exact-CAS response-loss reconciliation; no new request or successful pin is guessed. */
    public synchronized com.nereusstream.delay.protocol.RecoveryPin retryCheckpointPinCreation() {
        requireCurrent();
        if (!ownsCheckpointPin || checkpointPin == null || checkpointPinReleased) {
            throw new IllegalStateException("no original pin creation is pending");
        }
        try {
            if (!checkpointPinConfirmed) {
                final var actual = checkpointCatalog.createRecoveryPin(checkpointPin);
                if (!checkpointPin.equals(actual)) {
                    throw new IllegalStateException("actual pin CAS changed its original request");
                }
                checkpointPinConfirmed = true;
            }
            requireCheckpointPin();
            return checkpointPin;
        } catch (RuntimeException | Error failure) {
            if (checkpointPinFailure == null) {
                checkpointPinFailure = failure;
            }
            throw failure;
        }
    }

    private void requireCheckpointPin() {
        if (checkpointPinReleased
                || !checkpointCatalog.activeRecoveryPin().orElseThrow().equals(checkpointPin)) {
            throw new IllegalStateException("original checkpoint pin is no longer active");
        }
    }

    /** Reopens only a previously installed image for original Catalog/Pin recovery; it never downloads over ACTIVE. */
    public synchronized void resumeInstalledCheckpoint() {
        if (!ownsCheckpointPin || !checkpointPinConfirmed) {
            throw new IllegalStateException("original owned pin is not confirmed");
        }
        resumeInstalledCheckpoint(checkpointCatalog, checkpointPin);
    }

    public synchronized void resumeInstalledCheckpoint(
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.protocol.RecoveryPin pin) {
        requireCurrent();
        if (initial != null
                || store != null
                || restoreSubmission != null
                || initializationRunning
                || checkpointPin != null && (checkpointCatalog != catalog || !checkpointPin.equals(pin))) {
            throw new IllegalStateException("original installed checkpoint startup cannot be replaced");
        }
        if (ownsCheckpointPin && !checkpointPinConfirmed) {
            throw new IllegalStateException("original pin creation is not confirmed");
        }
        requirePinOwner(pin);
        checkpointCatalog = Objects.requireNonNull(catalog);
        checkpointPin = pin;
        requireCheckpointPin();
        try {
            store = ShardStore.openTarget(graph.config().store(), scope.shard(), graph.resources());
            requireMigrationAdoption();
            requireCurrent();
            final var base = store.recoveryMetadata().lineageBase();
            final var install = store.recoveryMetadata().installState();
            if (base == null
                    || install == null
                    || install.phase() == com.nereusstream.delay.protocol.RecoveryInstallPhase.STAGED
                    || install.phase() == com.nereusstream.delay.protocol.RecoveryInstallPhase.FRESH
                    || !Bytes.constantTimeEquals(
                            base.storeIncarnation(), store.metadata().storeIncarnation())
                    || !Bytes.constantTimeEquals(
                            base.checkpointId(), pin.candidate().checkpointId())
                    || !Bytes.constantTimeEquals(
                            base.manifestSha256(), pin.candidate().manifestSha256())
                    || !Bytes.constantTimeEquals(
                            base.recoveryLineageId(), pin.candidate().recoveryLineageId())
                    || pin.candidate().storeIncarnation() != null
                            && !Bytes.constantTimeEquals(
                                    pin.candidate().storeIncarnation(),
                                    store.metadata().storeIncarnation())) {
                throw new IllegalStateException("installed checkpoint changed its original base/Store/pin identity");
            }
            checkpointCatalog.validatePublishedRestoreCandidate(
                    checkpointCatalog.manifest(base.checkpointId()).orElseThrow());
            requireCheckpointPin();
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
    }

    private void requirePinOwner(com.nereusstream.delay.protocol.RecoveryPin pin) {
        if (!pin.shard().shardId().equals(scope.shard())
                || pin.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(pin.owner().leaseFencingDigest(), lease.leaseToken())
                || !Bytes.constantTimeEquals(
                        pin.oxiaSessionIdentityDigest(),
                        graph.oxia().providers().sessionIdentity())) {
            throw new IllegalArgumentException("checkpoint pin changes its actual Source Owner/session/Shard");
        }
    }

    private void acquireExisting(boolean openStore) {
        try {
            assignment = TargetWorkerSourceRuntimeFactory.accept(
                    graph.oxia(), owner.tenant(), publication, owner.workerId());
            TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, scope);
            lease = TargetWorkerSourceRuntimeFactory.acquire(
                    graph.oxia(),
                    owner.tenant(),
                    publication,
                    owner.workerId(),
                    owner.ownerId(),
                    owner.leaseDurationMs(),
                    owner.clock());
            requireCurrent();
            if (openStore) {
                store = ShardStore.openTarget(graph.config().store(), scope.shard(), graph.resources());
                requireMigrationAdoption();
            }
            requireCurrent();
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        } finally {
            synchronized (this) {
                initializationRunning = false;
            }
        }
    }

    /** Queues authenticated published download/install in the original bounded CHECKPOINT class. */
    public synchronized void restoreCheckpoint(
            com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images,
            Runnable protection) {
        if (!ownsCheckpointPin || !checkpointPinConfirmed) {
            throw new IllegalStateException("original owned pin is not confirmed");
        }
        queueCheckpoint(null, request, checkpointPin, checkpointCatalog, downloader, images, protection);
    }

    /** Uses the newly acquired Owner's current Pin while retaining the intent's immutable original provenance. */
    public synchronized void repairCheckpoint(
            java.util.UUID exactTarget,
            com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images,
            Runnable protection) {
        if (!ownsCheckpointPin || !checkpointPinConfirmed) {
            throw new IllegalStateException("original owned pin is not confirmed");
        }
        queueCheckpoint(
                Objects.requireNonNull(exactTarget),
                request,
                checkpointPin,
                checkpointCatalog,
                downloader,
                images,
                protection);
    }

    public synchronized void repairCheckpoint(
            java.util.UUID exactTarget,
            com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.protocol.RecoveryPin pin,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images,
            Runnable protection) {
        queueCheckpoint(Objects.requireNonNull(exactTarget), request, pin, catalog, downloader, images, protection);
    }

    public synchronized void restoreCheckpoint(
            com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.protocol.RecoveryPin pin,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images,
            Runnable protection) {
        queueCheckpoint(null, request, pin, catalog, downloader, images, protection);
    }

    private void queueCheckpoint(
            java.util.UUID repairTarget,
            com.nereusstream.delay.store.CheckpointDownloadRequest request,
            com.nereusstream.delay.protocol.RecoveryPin pin,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.store.CheckpointDownloadAdapter downloader,
            com.nereusstream.delay.store.TargetCheckpointRootVerifier.ImageAuthority images,
            Runnable protection) {
        requireCurrent();
        if (initial != null
                || initializationRunning
                || store != null
                || restoreSubmission != null
                || checkpointPin != null && (checkpointCatalog != catalog || !checkpointPin.equals(pin))) {
            throw new IllegalStateException("original published checkpoint startup cannot be replaced");
        }
        Objects.requireNonNull(request);
        Objects.requireNonNull(pin);
        Objects.requireNonNull(catalog);
        if (ownsCheckpointPin && !checkpointPinConfirmed) {
            throw new IllegalStateException("original pin creation is not confirmed");
        }
        if (!request.manifest().shardId().equals(scope.shard())
                || !pin.shard().shardId().equals(scope.shard())
                || pin.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(pin.owner().leaseFencingDigest(), lease.leaseToken())
                || !Bytes.constantTimeEquals(
                        pin.oxiaSessionIdentityDigest(),
                        graph.oxia().providers().sessionIdentity())) {
            throw new IllegalArgumentException(
                    "published checkpoint pin changes its actual Source Owner/session/Shard");
        }
        final var configured = graph.config().checkpoint();
        final Runnable held = () -> {
            requireCurrent();
            Objects.requireNonNull(protection).run();
        };
        final var authentication = new ShardStore.TargetRestoreAuthentication(
                configured.quota(),
                configured.ledger(),
                configured.semantics(),
                Objects.requireNonNull(images),
                held,
                configured.newReadBudget(nanos));
        final var coordinator = new com.nereusstream.delay.store.CheckpointRestoreCoordinator(
                graph.config().store(), scope.shard(), graph.resources(), downloader, catalog, configured.physical());
        final var executor =
                new com.nereusstream.delay.store.CheckpointRestoreWorkClassExecutor(graph.classes(), coordinator);
        checkpointCatalog = catalog;
        checkpointPin = pin;
        try {
            restoreSubmission =
                    executor.submit(new com.nereusstream.delay.store.CheckpointRestoreWorkClassExecutor.RestoreRequest(
                            request, pin, authentication, repairTarget));
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
    }

    /** Observes only the original restore action after its registry execution has returned. */
    public synchronized boolean finishCheckpointRestore() {
        requireCurrent();
        if (restoreSubmission == null) {
            throw new IllegalStateException("published restore has not been queued");
        }
        if (store != null) {
            return true;
        }
        if (graph.classes().state(restoreSubmission.task()).orElse(null)
                == com.nereusstream.delay.scheduler.WorkClassExecutionRegistry.ExecutionState.RUNNING) {
            return false;
        }
        final var outcome = restoreSubmission.outcome();
        if (outcome.isEmpty()) {
            return false;
        }
        if (outcome.get().failure() != null) {
            retain(outcome.get().failure());
            throw new StartupFailure(outcome.get().failure(), this);
        }
        store = outcome.get().restored();
        try {
            requireMigrationAdoption();
            requireCurrent();
            return true;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
    }

    /** The cut is obtained under the acquired Owner and an actual bounded root/physical audit. */
    public synchronized com.nereusstream.delay.protocol.SourcePosition auditedRecoveryCut() {
        requireRecoveryReaderCurrent();
        if (initial != null && !initialCommitted
                || store == null
                || activation != null
                || recovery != null
                || worker != null
                || constructionThread != Thread.currentThread()) {
            throw new IllegalStateException(
                    "recovery cut requires its original startup construction and acquired Store");
        }
        final var audited = archiveRecoveryAuditStore == store && archiveRecoveryAudit != null
                ? store.withReadView(archiveRecoveryAudit.view(), () -> archiveRecoveryAudit)
                : auditRecoveryStore(false);
        archiveRecoveryAudit = null;
        archiveRecoveryAuditStore = null;
        payloadSummaryPrefixRoot = audited.root();
        requireRecoveryReaderCurrent();
        return Objects.requireNonNull(store.appliedShardLogPosition(), "audited recovery Source cut");
    }

    private com.nereusstream.delay.runtime.TargetStoreBootstrap.Reopened auditRecoveryStore(
            boolean retainArchiveDependencies) {
        final var reads = TargetWorkerSourceRuntimeFactory.startupReads(
                graph.oxia(),
                owner.tenant(),
                publication,
                owner.workerId(),
                store,
                assignment,
                lease,
                content.reads(),
                owner.clock());
        final var config = graph.config();
        final var writes = new com.nereusstream.delay.store.TargetStoreBackend.WriteLimits(
                config.source().records(), config.source().bytes());
        final var audited = retainArchiveDependencies
                ? com.nereusstream.delay.runtime.TargetStoreBootstrap.reopenForArchiveRecovery(
                        store,
                        scope,
                        writes,
                        config.checkpoint().newReadBudget(nanos),
                        reads,
                        config.checkpoint().ledger())
                : com.nereusstream.delay.runtime.TargetStoreBootstrap.reopen(
                        store,
                        scope,
                        writes,
                        config.checkpoint().newReadBudget(nanos),
                        reads,
                        config.checkpoint().ledger());
        requireRecoveryReaderCurrent();
        return audited;
    }

    /** The registered post-T Driver executes the existing complete audit before its receiver's multi-turn audit. */
    synchronized com.nereusstream.delay.runtime.TargetStoreBootstrap.Reopened archiveRecoveryAudit(
            TargetMigrationProcessDriver original) {
        process.requireMigrationRecoveryAdmission(original, this, store);
        requireRecoveryReaderCurrent();
        if (original.opening().archivePreview() == null
                || constructionThread != null
                || activation != null
                || recovery != null
                || worker != null
                || store == null) {
            throw new IllegalStateException("archive Native dependency audit is not this original pre-reader recovery");
        }
        if (archiveRecoveryAuditStore != store || archiveRecoveryAudit == null) {
            archiveRecoveryAudit = auditRecoveryStore(true);
            archiveRecoveryAuditStore = store;
        }
        return store.withReadView(archiveRecoveryAudit.view(), () -> archiveRecoveryAudit);
    }

    /** Material acquisition is an original CHECKPOINT/OwnedIO flight, before opening the first actual Source reader. */
    public boolean prepareMigrationCompatibility() {
        return process.prepareMigrationRecovery(this);
    }

    public boolean migrationHistoryRequired() {
        return process.migrationHistoryRequired(this);
    }

    /** Literal limits and genuine old-writer freeze provider are retained before the actual C reader opens. */
    public synchronized void configurePayloadSummaryLegacyPrefix(
            TargetPayloadSummaryLegacyPrefix.Provider provider, TargetPayloadSummaryLegacyPrefix.Limits limits) {
        requireCurrent();
        Objects.requireNonNull(provider);
        Objects.requireNonNull(limits);
        if (activation != null || recovery != null || worker != null || payloadSummaryPrefixProvider != null) {
            throw new IllegalStateException(
                    "legacy summary prefix requires its original controlled startup, before replay");
        }
        payloadSummaryPrefixReservation = process.reservePayloadSummaryPrefix(this, limits.maximumRetainedBytes());
        payloadSummaryPrefixProvider = provider;
        payloadSummaryPrefixLimits = limits;
    }

    public synchronized void configureWriterOrigin(com.nereusstream.delay.runtime.TargetSloWriterOrigin.Limits limits) {
        requireCurrent();
        Objects.requireNonNull(limits);
        if (worker != null || recovery != null || writerOriginLimits != null) {
            throw new IllegalStateException(
                    "writer origin preparation limits bind once on original controlled Source startup");
        }
        writerOriginReservation = process.reserveWriterOrigin(this, limits.maximumRetainedBytes());
        writerOriginLimits = limits;
        if (activation != null && registryIdentity != null) {
            activation.runtime().configureWriterOrigin(limits, registryIdentity, owner.clock());
            activeWriterOriginConfigured = true;
        }
    }

    /** Two real C-reader passes: the first captures/closes R, the second independently replays only that protected prefix. */
    public Optional<TargetWorkerRecoverySource> payloadSummaryRecoverySource(TargetWorkerRecoverySource original) {
        final OwnedCheckpointIo<TargetPayloadSummaryLegacyPrefix> acquired;
        synchronized (this) {
            requireRecoveryReaderCurrent();
            if (payloadSummaryPrefixProvider == null) {
                return Optional.of(original);
            }
            if (payloadSummaryPrefixOpening == null) {
                if (payloadSummaryPrefixRoot == null) {
                    throw new IllegalStateException("controlled prefix lacks its actual bounded Native root audit");
                }
                final var opening = new PayloadSummaryPrefixOpening(this);
                payloadSummaryPrefixOpening = OwnedCheckpointIo.submitResource(
                        guard -> {
                            guard.run();
                            final var previous = previousPayloadSummaryPrefix;
                            final var actual = previous == null
                                    ? TargetPayloadSummaryLegacyPrefix.open(
                                            opening, payloadSummaryPrefixProvider, payloadSummaryPrefixLimits)
                                    : TargetPayloadSummaryLegacyPrefix.openNext(
                                            opening,
                                            payloadSummaryPrefixProvider,
                                            payloadSummaryPrefixLimits,
                                            previous);
                            payloadSummaryPrefix = actual;
                            guard.run();
                            return actual;
                        },
                        this::requireRecoveryReaderCurrent,
                        TargetPayloadSummaryLegacyPrefix::close,
                        graph.io());
            }
            acquired = payloadSummaryPrefixOpening;
        }
        acquired.retryCleanup();
        if (!acquired.operationExited() || !acquired.ioExited()) {
            return Optional.empty();
        }
        final var prefix = acquired.receipt().join();
        if (payloadSummaryCapturedReader != null && payloadSummaryCapturedReader != original) {
            return Optional.of(prefix.source(original));
        }
        if (prefix.captureReaderClosed(original)) {
            payloadSummaryCapturedReader = original;
            return Optional.empty();
        }
        // A new actual second C reader is identified by the private prefix after the first native reader really exits.
        if (prefix.capture(original)) {
            synchronized (this) {
                payloadSummaryCapturedReader = original;
                if (recoverySource == original) {
                    recoverySource = null;
                }
            }
            return Optional.empty();
        }
        return Optional.empty();
    }

    public synchronized boolean payloadSummaryCaptureReaderClosed(TargetWorkerRecoverySource original) {
        final var prefix = payloadSummaryPrefix;
        if (prefix == null || !prefix.captureReaderClosed(original)) {
            return false;
        }
        payloadSummaryCapturedReader = original;
        if (recoverySource == original) {
            recoverySource = null;
        }
        return true;
    }

    public static final class PayloadSummaryPrefixOpening implements TargetPayloadSummaryLegacyPrefix.Opening {
        private final TargetWorkerSourceStartup startup;
        private final ShardStore store;
        private final SourceAssignment assignment;
        private final OwnerLease owner;

        private PayloadSummaryPrefixOpening(TargetWorkerSourceStartup startup) {
            this.startup = startup;
            store = startup.store;
            assignment = startup.assignment;
            owner = startup.lease;
        }

        @Override
        public TargetWorkerProcessRuntime.Assembly graph() {
            return startup.graph;
        }

        @Override
        public ShardStore store() {
            return startup.store();
        }

        @Override
        public TargetQuotaScope scope() {
            return startup.scope;
        }

        @Override
        public byte[] lineage() {
            return root().recoveryLineage();
        }

        @Override
        public SourceAssignment assignment() {
            return startup.acceptedAssignment();
        }

        @Override
        public OwnerLease ownerLease() {
            return startup.ownerLease();
        }

        @Override
        public com.nereusstream.delay.protocol.TargetQuotaIncarnation root() {
            return Objects.requireNonNull(startup.payloadSummaryPrefixRoot, "actual audited Native root");
        }

        @Override
        public LongSupplier nanos() {
            return startup.nanos;
        }

        @Override
        public LongSupplier clock() {
            return startup.owner.clock();
        }

        @Override
        public TargetPayloadSummaryLegacyPrefix.Limits limits() {
            return startup.payloadSummaryPrefixLimits;
        }

        @Override
        public void requireCurrent() {
            startup.requireRecoveryReaderCurrent();
            startup.payloadSummaryPrefixReservation.requireCurrent();
        }

        @Override
        public void requireLocalCurrent() {
            final var current = startup.lease;
            final long now = startup.owner.clock().getAsLong();
            if (startup.stopping
                    || startup.firstFailure != null
                    || startup.store != store
                    || store.nativeExposureFenceRaised()
                    || startup.assignment == null
                    || !startup.assignment.sameIdentity(assignment)
                    || current == null
                    || !current.sameIdentity(owner)
                    || !current.validAt(now)
                    || now < startup.lastNow) {
                throw new IllegalStateException("original controlled prefix local Owner/Store/Source was revoked");
            }
            startup.payloadSummaryPrefixReservation.requireCurrent();
        }

        @Override
        public void retainCleanup(CheckpointInputFuture.Cleanup original) {
            synchronized (startup) {
                if (!startup.payloadSummaryPrefixPartial.contains(original)) {
                    if (startup.payloadSummaryPrefixPartial.size() >= 4) {
                        throw new IllegalStateException("controlled prefix partial acquisition bound exhausted");
                    }
                    startup.payloadSummaryPrefixPartial.add(Objects.requireNonNull(original));
                }
            }
        }
    }

    private boolean closePayloadSummaryPrefixAfterReader() {
        final var opening = payloadSummaryPrefixOpening;
        if (opening != null) {
            opening.requestStop();
            opening.retryCleanup();
            if (!opening.operationExited() || !opening.ioExited()) {
                return false;
            }
        }
        final var prefix = payloadSummaryPrefix;
        if (prefix != null) {
            prefix.requestStop();
            prefix.retryCleanup();
            if (!prefix.cleanupComplete()) {
                return false;
            }
        }
        for (var partial : payloadSummaryPrefixPartial) {
            partial.retryCleanup();
            if (!partial.cleanupComplete()) {
                return false;
            }
        }
        if (payloadSummaryPrefixReservation != null) {
            payloadSummaryPrefixReservation.release();
            payloadSummaryPrefixReservation = null;
            payloadSummaryPrefixPartial.clear();
            payloadSummaryPrefix = null;
            payloadSummaryPrefixOpening = null;
            payloadSummaryPrefixRoot = null;
            payloadSummaryCapturedReader = null;
            previousPayloadSummaryPrefix = null;
        }
        return true;
    }
    /** Returns only the local finite-range child loan; its durable old-writer freeze/retention remains owned. */
    private boolean retirePayloadSummaryRange() {
        final var prefix = payloadSummaryPrefix;
        if (prefix == null) {
            return true;
        }
        prefix.requestStop();
        prefix.retryCleanup();
        if (!prefix.ioExited() || !prefix.cleanupComplete()) {
            return false;
        }
        final var acquired = payloadSummaryPrefixOpening;
        if (acquired != null) {
            acquired.retryCleanup();
            if (!acquired.operationExited() || !acquired.ioExited()) {
                return false;
            }
        }
        previousPayloadSummaryPrefix = prefix;
        payloadSummaryPrefix = null;
        payloadSummaryPrefixOpening = null;
        payloadSummaryCapturedReader = null;
        payloadSummaryPrefixPartial.removeIf(CheckpointInputFuture.Cleanup::cleanupComplete);
        return true;
    }

    /** The SDK positions at C only for the privately registered historical shadow reconstruction. */
    public synchronized com.nereusstream.delay.protocol.SourcePosition recoveryReaderCut() {
        final var actual = auditedRecoveryCut();
        final var history = process.migrationReaderCut(this);
        return history == null ? actual : history;
    }

    /** Retains the C reader until its real native close; the next factory turn must open a fresh reader at Native's cut. */
    public boolean advanceMigrationHistory(TargetWorkerRecoverySource original) {
        synchronized (this) {
            if (recoverySource != original || recovery != null || activation != null) {
                throw new IllegalStateException("migration history is not this startup's original pre-recovery reader");
            }
        }
        if (!process.advanceMigrationHistory(this, original)) {
            return false;
        }
        synchronized (this) {
            if (!original.cleanupComplete()) {
                return false;
            }
            if (recoverySource != original) {
                throw new IllegalStateException("migration history reader changed during actual close");
            }
            recoverySource = null;
        }
        return true;
    }

    /** Pure local ownership/expiry check for the retained input hold, never a lease RPC under a native lock. */
    synchronized void requireMigrationLocalCurrent() {
        process.requireSourceAssemblyAdmission(this);
        if (stopping
                || firstFailure != null
                || lease == null
                || !lease.validAt(owner.clock().getAsLong())) {
            throw new IllegalStateException("original migration recovery Owner is stopped or expired");
        }
    }

    public void requireRecoveryReaderCurrent() {
        final var original = beginAuthorityRead();
        try {
            requireCurrent();
            if (original.catalog() != null) {
                requireObservedPin(original);
                original.catalog()
                        .validatePublishedRestoreCandidate(original.catalog()
                                .manifest(original.pin().candidate().checkpointId())
                                .orElseThrow());
                requireCurrent();
            }
            synchronized (this) {
                requireSameAuthorityRead(original);
            }
        } catch (RuntimeException | Error failure) {
            recoveryLocalEpoch.incrementAndGet();
            throw failure;
        } finally {
            endAuthorityRead();
        }
    }

    /** Minted only for the actual retained native reader after its original full owned-I/O validation. */
    public synchronized RecoveryReaderLocalProtection recoveryReaderLocalProtection(TargetWorkerRecoverySource actual) {
        if (actual != recoverySource
                || actual != localNativeReader
                || stopping
                || firstFailure != null
                || store == null
                || lease == null) {
            throw new IllegalStateException("local reader protection requires the original retained actual Source");
        }
        return new RecoveryReaderLocalProtection(this, actual, store, assignment, lease, recoveryLocalEpoch.get());
    }

    public void revokeRecoveryReaderLocalProtection(TargetWorkerRecoverySource actual) {
        if (localNativeReader == actual) {
            recoveryLocalEpoch.incrementAndGet();
        }
    }

    public static final class RecoveryReaderLocalProtection {
        private final TargetWorkerSourceStartup startup;
        private final TargetWorkerRecoverySource reader;
        private final ShardStore store;
        private final SourceAssignment assignment;
        private final OwnerLease owner;
        private final long epoch;

        private RecoveryReaderLocalProtection(
                TargetWorkerSourceStartup startup,
                TargetWorkerRecoverySource reader,
                ShardStore store,
                SourceAssignment assignment,
                OwnerLease owner,
                long epoch) {
            this.startup = startup;
            this.reader = reader;
            this.store = store;
            this.assignment = assignment;
            this.owner = owner;
            this.epoch = epoch;
        }

        public void requireCurrent() {
            startup.requireLocalRecoveryIdentity(store, assignment, owner, epoch);
            if (startup.localNativeReader != reader) {
                throw new IllegalStateException("actual recovery native reader was replaced");
            }
            startup.graph.resources().requireRuntimeBusinessAdmission();
            startup.requireLocalRecoveryIdentity(store, assignment, owner, epoch);
            if (startup.localNativeReader != reader) {
                throw new IllegalStateException("actual recovery native reader was replaced");
            }
        }
    }

    private void requireLocalRecoveryIdentity(
            ShardStore actualStore, SourceAssignment accepted, OwnerLease original, long epoch) {
        final var current = lease;
        final var currentAssignment = assignment;
        final long now = owner.clock().getAsLong();
        if (stopping
                || firstFailure != null
                || recoveryLocalEpoch.get() != epoch
                || store != actualStore
                || actualStore.nativeExposureFenceRaised()
                || current == null
                || !current.sameIdentity(original)
                || currentAssignment == null
                || !currentAssignment.sameIdentity(accepted)
                || current.state() != ShardLifecycleState.ACQUIRING
                        && current.state() != ShardLifecycleState.CATCHING_UP
                        && current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || now < lastNow
                || !current.validAt(now)) {
            throw new IllegalStateException("original recovery local Source/Store/Owner/session admission was revoked");
        }
    }

    /** Adopts the exact verified reader before auditing/replaying; never takes an arbitrary iterator or issues ACK. */
    public void startRecovery(
            TargetWorkerRecoverySource source,
            com.nereusstream.delay.protocol.OwnerIdentity identity,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            Runnable sourceProtection,
            Runnable journalProtection) {
        final Construction original;
        final TargetWorkerRecoverySource rawPayloadSummaryReader;
        TargetSourceRecoveryInputs localInput = null;
        synchronized (this) {
            if (initial != null && !initialCommitted
                    || initializationRunning
                    || store == null
                    || recoverySource != null
                            && recoverySource != source
                            && (payloadSummaryPrefix == null || !payloadSummaryPrefix.isSource(recoverySource, source))
                    || recovery != null
                    || activation != null) {
                throw new IllegalStateException("original same-DB recovery is unavailable or already started");
            }
            rawPayloadSummaryReader = recoverySource;
            original = beginConstruction(false);
        }
        try (original) {
            original.requireCurrent();
            requireMigrationAdoption();
            if (identity.ownerEpoch() != lease.ownerEpoch()
                    || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())
                    || registryIdentity != null
                            && (!Arrays.equals(registryIdentity.deploymentId(), identity.deploymentId())
                                    || !Arrays.equals(registryIdentity.workerRunId(), identity.workerRunId()))) {
                throw new IllegalArgumentException(
                        "Source recovery changed its actual Owner or deployment/worker run identity");
            }
            registryIdentity = identity;
            synchronized (this) {
                if (!recoveryInputs.isEmpty()) {
                    final var input = recoveryInputs.stream()
                            .filter(value -> Arrays.equals(value.owner.canonicalBytes(), identity.canonicalBytes()))
                            .findFirst()
                            .orElseThrow(() ->
                                    new IllegalStateException("recovery lacks its exact per-Owner input opening"));
                    input.requireValue();
                    if (input.io == null || !input.io.operationExited() || !input.io.ioExited()) {
                        throw new IllegalStateException("original recovery input opening has not exited");
                    }
                    if (input.original.journal() != journal || input.handoff != null) {
                        throw new IllegalStateException("recovery changed or reopened its original Journal input");
                    }
                    currentRecoveryInputs = input;
                    localInput = input.original;
                }
            }
            Objects.requireNonNull(source)
                    .requireStartup(
                            assignment, graph.config().maximumSourceFrameBytes(), graph.classes(), graph.resources());
            synchronized (this) {
                recoverySource = source;
            }
            source.replayCursor().requireGuarded();
            final var sourceContent = scopeContent();
            captureSourceFrames();
            recovery = checkpointCatalog == null
                    ? TargetWorkerSourceRuntimeFactory.reopen(
                            graph.oxia(),
                            graph.config(),
                            graph.classes(),
                            graph.resources(),
                            store,
                            scope,
                            owner.tenant(),
                            publication,
                            owner.workerId(),
                            lease,
                            source.replayCursor(),
                            source.successorAfter(store.appliedShardLogPosition()),
                            sourceContent,
                            identity,
                            journal,
                            sourceProtection,
                            journalProtection,
                            owner.clock(),
                            nanos)
                    : TargetWorkerSourceRuntimeFactory.recoverInstalled(
                            graph.oxia(),
                            graph.config(),
                            graph.classes(),
                            graph.resources(),
                            store,
                            scope,
                            owner.tenant(),
                            publication,
                            owner.workerId(),
                            lease,
                            source.replayCursor(),
                            source.successorAfter(store.appliedShardLogPosition()),
                            sourceContent,
                            identity,
                            journal,
                            sourceProtection,
                            journalProtection,
                            owner.clock(),
                            nanos,
                            checkpointCatalog,
                            checkpointPin);
            if (localInput != null) {
                recovery.coordinator().configureLocalRecoveryProtection(source, localInput);
            }
            if (scopeResources != null) {
                recovery.coordinator().configureScopeResources(scopeResources);
            }
            process.bindMigration(this, recovery.coordinator());
            if (writerOriginLimits != null) {
                recovery.coordinator().configureWriterOrigin(writerOriginLimits, identity, owner.clock());
            }
            if (payloadSummaryPrefix != null && payloadSummaryPrefix.isSource(rawPayloadSummaryReader, source)) {
                recovery.coordinator().configurePayloadSummaryLegacyPrefix(payloadSummaryPrefix);
            }
            if (assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier) {
                recovery.coordinator().bindReader(source, store.appliedShardLogPosition());
                if (pulsarPreparation == null) {
                    recovery.coordinator().deferForSourceReactivation();
                }
            }
            recovery.coordinator().bindReplayTail(source);
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerSourceRuntimeFactory.AssemblyFailure partial) {
                synchronized (this) {
                    failedAssembly = partial;
                    lease = partial.lease();
                }
            }
            retain(failure);
            throw new StartupFailure(failure, this);
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
            if (recovery == null || recoverySource == null || activation != null) {
                throw new IllegalStateException("original recovery is not pending");
            }
            original = beginConstruction(false);
        }
        try (original) {
            original.requireCurrent();
            final var coordinator = recovery.coordinator();
            if (coordinator.needsSloRecoveryPreparation()) {
                final SloRecoveryOpening opening;
                synchronized (this) {
                    if (currentSloRecovery == null) {
                        if (sloRecoveries.size() == 2) {
                            throw new IllegalStateException(
                                    "two original SLO recovery Owner handles are already retained");
                        }
                        currentSloRecovery = new SloRecoveryOpening(this);
                        sloRecoveries.add(currentSloRecovery);
                    }
                    opening = currentSloRecovery;
                }
                opening.prepare();
                if (!opening.bindPreparation()) {
                    return coordinator.replayTailCaptureTurn();
                }
            }
            if (coordinator.awaitingReplayTailCapture() && recoveryTailCapture == null) {
                final var source = recoverySource;
                recoveryTailCapture = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            source.captureReplayTail();
                            guard.run();
                            return Boolean.TRUE;
                        },
                        this::requireRecoveryReaderCurrent,
                        graph.io());
            }
            final var capture = recoveryTailCapture;
            if (capture != null) {
                capture.retryCleanup();
                if (!capture.operationExited() || !capture.receipt().isDone()) {
                    return coordinator.replayTailCaptureTurn();
                }
                capture.receipt().join();
                if (!capture.ioExited()) {
                    return coordinator.replayTailCaptureTurn();
                }
                coordinator.replayTailCaptured(recoverySource);
            }
            if (coordinator.awaitingSloRecoveryCompletion()) {
                final var opening = Objects.requireNonNull(currentSloRecovery, "original SLO recovery handle");
                opening.complete();
                if (!opening.bindCompletion()) {
                    return coordinator.sloRecoveryCompletionTurn();
                }
            }
            original.requireCurrent();
            var turn = coordinator.runTurn();
            if (coordinator.sourceReadyForReactivation()) {
                synchronized (this) {
                    lease = coordinator.sourcePreparationLease();
                }
                try {
                    recoverySource.close();
                } catch (RuntimeException | Error failure) {
                    synchronized (this) {
                        if (recoveryCleanupFailure == null) {
                            recoveryCleanupFailure = failure;
                        }
                    }
                }
                if (recoverySource.cleanupComplete()) {
                    coordinator.releaseReaderForSourceReactivation(recoverySource);
                }
                return turn;
            }
            if (turn.complete()) {
                synchronized (this) {
                    lease = coordinator.activeLease();
                }
                try {
                    recoverySource.close();
                } catch (RuntimeException | Error failure) {
                    synchronized (this) {
                        if (recoveryCleanupFailure == null) {
                            recoveryCleanupFailure = failure;
                        }
                    }
                    return turn;
                }
                if (recoverySource.cleanupComplete()) {
                    if (!retirePayloadSummaryRange()) {
                        return turn;
                    }
                    coordinator.retryAuthorityCleanup();
                    if (!coordinator.authorityIoExited()) {
                        return turn;
                    }
                    synchronized (this) {
                        activation = recovery.activateCompleted(successor);
                        activeWriterOriginConfigured = writerOriginLimits != null;
                    }
                }
            }
            return turn;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            throw new StartupFailure(failure, this);
        }
    }

    void initialize() {
        try {
            assignment = TargetWorkerSourceRuntimeFactory.accept(
                    graph.oxia(), owner.tenant(), publication, owner.workerId());
            if (!assignment.shardId().equals(initial.scope().shard())) {
                throw new IllegalArgumentException("initial grant belongs to another accepted Source Shard");
            }
            TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, initial.scope());
            assignment.activationBarrier().validatePosition(initial.entry().position());
            if (assignment.activationBarrier()
                    instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier barrier) {
                if (initial.entry().sourceConnectionGeneration() == null
                        || initial.entry().guardAttestationDigest() == null) {
                    throw new IllegalArgumentException("initial Pulsar grant lacks its original native proof");
                }
                barrier.validateSourceConnection(
                        initial.entry().sourceConnectionGeneration(),
                        initial.entry().guardAttestationDigest());
            } else if (initial.entry().sourceConnectionGeneration() != null
                    || initial.entry().guardAttestationDigest() != null) {
                throw new IllegalArgumentException("initial Kafka grant carries a foreign connection proof");
            }
            lease = TargetWorkerSourceRuntimeFactory.acquire(
                    graph.oxia(),
                    owner.tenant(),
                    publication,
                    owner.workerId(),
                    owner.ownerId(),
                    owner.leaseDurationMs(),
                    owner.clock());
            requireCurrent();
            store = ShardStore.createTarget(graph.config().store(), assignment.shardId(), graph.resources());
            requireCurrent();
            sourcePlanReservation();
            var prepared = TargetStoreBootstrap.prepareWithControlSnapshot(
                    store,
                    initial.scope(),
                    initial.lineage(),
                    new TargetStoreBackend.WriteLimits(
                            graph.config().source().records(),
                            graph.config().source().bytes()),
                    graph.config().checkpoint().newReadBudget(nanos),
                    initial.prepared(),
                    initial.entry().mutation(),
                    initial.entry().position(),
                    initial.grants(),
                    initial.start(),
                    initial.snapshot(),
                    initial.snapshotAuthority());
            var committed = TargetStoreBootstrap.commit(prepared, (metadata, scope, mutation) -> {
                requireCurrent();
                var original = Objects.requireNonNull(content.physical().acquire(metadata, scope, mutation));
                return new TargetStoreBackend.CommitGuard() {
                    @Override
                    public void requireCurrent() {
                        TargetWorkerSourceStartup.this.requireCurrent();
                        original.requireCurrent();
                    }

                    @Override
                    public void close() {
                        original.close();
                    }
                };
            });
            synchronized (this) {
                sourceExitLineage = committed.root().recoveryLineage();
                sourceExitSource = initial.entry().position();
            }
            initialCommitted = true;
            if (assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier) {
                return;
            }
            final var sourceContent = scopeContent();
            activation = TargetWorkerSourceRuntimeFactory.initialize(
                    graph.oxia(),
                    graph.config(),
                    graph.classes(),
                    graph.resources(),
                    committed,
                    store,
                    owner.tenant(),
                    publication,
                    owner.workerId(),
                    lease,
                    successor,
                    sourceContent,
                    owner.clock(),
                    nanos);
            if (scopeResources != null) {
                scopeResources.bindSource(activation.runtime(), owner.clock());
            }
            lease = activation.runtime().snapshotDrainLease();
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerSourceRuntimeFactory.AssemblyFailure partial) {
                failedAssembly = partial;
                lease = partial.lease();
            }
            retain(failure);
            throw new StartupFailure(failure, this);
        } finally {
            synchronized (this) {
                initializationRunning = false;
            }
        }
    }

    /** Actual Route/assignment/session/Owner reads are bounded by the original configured SDK timeout. */
    public void requireCurrent() {
        try {
            process.requireSourceAssemblyAdmission(this);
            requireCurrentFacts();
            process.requireSourceAssemblyAdmission(this);
        } catch (RuntimeException | Error failure) {
            recoveryLocalEpoch.incrementAndGet();
            throw failure;
        }
    }

    private record AuthorityRead(
            SourceAssignment assignment,
            OwnerLease lease,
            ShardStore store,
            TargetWorkerRecoverySource reader,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.protocol.RecoveryPin pin,
            boolean pinConfirmed,
            boolean pinReleased) {}

    private AuthorityRead beginAuthorityRead() {
        synchronized (this) {
            if (stopping || firstFailure != null || assignment == null || lease == null) {
                throw new IllegalStateException("Source startup is stopped, failed or lacks its original Owner");
            }
            final var original = new AuthorityRead(
                    assignment,
                    lease,
                    store,
                    recoverySource,
                    checkpointCatalog,
                    checkpointPin,
                    checkpointPinConfirmed,
                    checkpointPinReleased);
            authorityReadsInFlight = Math.incrementExact(authorityReadsInFlight);
            return original;
        }
    }

    private void endAuthorityRead() {
        synchronized (this) {
            authorityReadsInFlight--;
        }
    }

    /** Called only under the short startup state lock; no external authority is read here. */
    private void requireSameAuthorityRead(AuthorityRead original) {
        if (stopping
                || firstFailure != null
                || assignment == null
                || !assignment.sameIdentity(original.assignment())
                || lease == null
                || !lease.sameIdentity(original.lease())
                || store != original.store()
                || recoverySource != original.reader()
                || checkpointCatalog != original.catalog()
                || checkpointPin != original.pin()
                || checkpointPinConfirmed != original.pinConfirmed()
                || checkpointPinReleased != original.pinReleased()) {
            throw new IllegalStateException("Source startup changed during its original authority read");
        }
    }

    private static void requireObservedPin(AuthorityRead original) {
        if (original.pinReleased()
                || original.pin() == null
                || !original.catalog().activeRecoveryPin().orElseThrow().equals(original.pin())) {
            throw new IllegalStateException("original checkpoint pin is no longer active");
        }
    }

    /** Same original assignment/session/lease facts, read outside the startup state monitor. */
    private OwnerLease requireCurrentFacts() {
        final var original = beginAuthorityRead();
        try {
            final var accepted = TargetWorkerSourceRuntimeFactory.accept(
                    graph.oxia(), owner.tenant(), publication, owner.workerId());
            final var observed = graph.oxia()
                    .providers()
                    .leases()
                    .current(accepted.shardId())
                    .orElseThrow();
            final var session = graph.oxia().providers().sessionIdentity();
            final long storeOwner = original.store() == null
                    ? 0
                    : original.store().runtimeMetadata().lastOpenedOwnerEpoch();
            if (original.catalog() != null && original.pinConfirmed()) {
                requireObservedPin(original);
            }
            synchronized (this) {
                requireSameAuthorityRead(original);
                final long now = owner.clock().getAsLong();
                if (!assignment.sameIdentity(accepted)
                        || !lease.sameIdentity(observed)
                        || observed.state() == ShardLifecycleState.FENCED
                        || observed.state() == ShardLifecycleState.FAILED
                        || observed.state() == ShardLifecycleState.UNASSIGNED
                        || !Bytes.constantTimeEquals(observed.sessionIdentity(), session)
                        || !observed.validAt(now)
                        || now < 0
                        || now < lastNow
                        || (recoverySource == null
                                ? observed.state() != lease.state()
                                : observed.state() != ShardLifecycleState.ACQUIRING
                                        && observed.state() != ShardLifecycleState.CATCHING_UP
                                        && observed.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                        || original.store() != null && Long.compareUnsigned(storeOwner, lease.ownerEpoch()) > 0) {
                    throw new IllegalStateException(
                            "Source startup lost its actual assignment/session/Owner/time authority");
                }
                // A concurrent successful check or Owner transition already published its own real observation.
                // Do not overwrite it with this earlier in-flight response.
                if (lease == original.lease()
                        || lease.state() == observed.state()
                                && observed.expiresAtEpochMs() > lease.expiresAtEpochMs()) {
                    lease = observed;
                }
                if (!lease.validAt(now)) {
                    throw new IllegalStateException("the retained actual Owner expired during authority validation");
                }
                lastNow = now;
                return lease;
            }
        } catch (RuntimeException | Error failure) {
            recoveryLocalEpoch.incrementAndGet();
            throw failure;
        } finally {
            endAuthorityRead();
        }
    }

    public TargetMigrationMaintenance configureMigrationMaintenance(
            TargetWorkerShardRuntime actual,
            TargetMigrationMaintenanceConfiguration configuration,
            TargetMigrationMaintenanceInputs.Provider provider,
            TargetDestinationSdkBudget sdk,
            java.util.function.Function<TargetMigrationSourceOpening, TargetMigrationTransport> transport) {
        synchronized (this) {
            requireCurrent();
            requireAdmittedWorker(actual);
        }
        return process.configureMigrationMaintenance(this, actual, configuration, provider, sdk, transport);
    }

    public TargetSourceAuthorityLoader.HeldInputs loadMigrationMaintenanceSource(
            TargetMigrationMaintenanceInputs.Provider provider, SourceReplayMutation original, Runnable current) {
        return process.loadMigrationMaintenanceSource(this, provider, original, current);
    }

    public TargetPayloadSummaryInitializationMaintenance configurePayloadSummaryInitialization(
            TargetWorkerShardRuntime actual,
            OwnedSourceSystemWriter writer,
            TargetPayloadSummaryInitializationMaintenance.Configuration configuration,
            TargetPayloadSummaryInitializationMaintenance.Inputs.Provider provider) {
        synchronized (this) {
            requireCurrent();
            requireAdmittedWorker(actual);
        }
        return process.configurePayloadSummaryInitialization(this, actual, writer, configuration, provider);
    }

    synchronized com.nereusstream.delay.store.TargetStoreBackend.ReadAuthority payloadSummaryMaintenanceReads(
            TargetPayloadSummaryInitializationMaintenance actual, TargetWorkerShardRuntime actualWorker) {
        process.requirePayloadSummaryMaintenance(this, actualWorker, actual);
        requirePayloadSummaryMaintenanceLocalCurrent(actual, actualWorker);
        return activation.runtime().migrationMaintenanceReads(owner.clock());
    }

    synchronized void requirePayloadSummaryMaintenanceCurrent(
            TargetPayloadSummaryInitializationMaintenance actual, TargetWorkerShardRuntime actualWorker) {
        process.requirePayloadSummaryMaintenance(this, actualWorker, actual);
        if (worker != actualWorker) {
            throw new IllegalStateException("summary initialization changed its actual Source Worker");
        }
        lease = requireCurrentFacts();
    }

    synchronized void requirePayloadSummaryMaintenanceLocalCurrent(
            TargetPayloadSummaryInitializationMaintenance actual, TargetWorkerShardRuntime actualWorker) {
        process.requirePayloadSummaryMaintenance(this, actualWorker, actual);
        requireMaintenanceLocalFacts(actualWorker);
    }

    synchronized com.nereusstream.delay.store.TargetStoreBackend migrationMaintenanceBackend(
            TargetWorkerShardRuntime actual) {
        requireCurrent();
        requireAdmittedWorker(actual);
        return activation().runtime().migrationMaintenanceBackend(store, graph.resources());
    }

    synchronized com.nereusstream.delay.store.TargetStoreBackend.ReadAuthority migrationMaintenanceReads(
            TargetMigrationMaintenance actual, TargetWorkerShardRuntime actualWorker) {
        process.requireMigrationMaintenance(this, actualWorker, actual);
        requireMigrationMaintenanceLocalCurrent(actual, actualWorker);
        return activation.runtime().migrationMaintenanceReads(owner.clock());
    }

    synchronized com.nereusstream.delay.protocol.OwnerIdentity migrationMaintenanceOwner(
            TargetWorkerShardRuntime actual) {
        requireCurrent();
        requireAdmittedWorker(actual);
        final var original = Objects.requireNonNull(registryIdentity, "actual registered Source Owner identity");
        final var identity = new com.nereusstream.delay.protocol.OwnerIdentity(
                original.deploymentId(), original.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
        return actual.requirePublicationOwner(identity, owner.clock());
    }

    LongSupplier migrationMaintenanceClock() {
        return owner.clock();
    }

    synchronized Runnable managementMaintenanceLocalToken(
            TargetWorkerShardRuntime actual, com.nereusstream.delay.protocol.OwnerIdentity identity) {
        requireCurrent();
        requireAdmittedWorker(actual);
        return activation.runtime().managementMaintenanceLocalGuard(store, graph.resources(), identity, owner.clock());
    }

    synchronized void requireMigrationMaintenanceCurrent(
            TargetMigrationMaintenance actual, TargetWorkerShardRuntime actualWorker) {
        process.requireMigrationMaintenance(this, actualWorker, actual);
        if (worker != actualWorker) {
            throw new IllegalStateException("live migration changed its actual Source Worker");
        }
        lease = requireCurrentFacts();
    }

    synchronized void requireMigrationMaintenanceLocalCurrent(
            TargetMigrationMaintenance actual, TargetWorkerShardRuntime actualWorker) {
        process.requireMigrationMaintenance(this, actualWorker, actual);
        requireMaintenanceLocalFacts(actualWorker);
    }

    private void requireMaintenanceLocalFacts(TargetWorkerShardRuntime actualWorker) {
        if (worker != actualWorker
                || stopping
                || firstFailure != null
                || storeClosed
                || store == null
                || store.isCloseStarted()
                || lease == null
                || !lease.validAt(owner.clock().getAsLong())
                || lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || registryIdentity == null
                || lease.ownerEpoch() != registryIdentity.ownerEpoch()
                || !Arrays.equals(lease.leaseToken(), registryIdentity.leaseFencingDigest())) {
            throw new IllegalStateException("live migration lost its actual local Worker/Owner/Store token");
        }
    }

    public synchronized TargetWorkerSourceRuntimeFactory.Activated activation() {
        if (activation == null || stopping || firstFailure != null) {
            throw new IllegalStateException("Source activation is unavailable");
        }
        if (!activation.runtime().operationEnvelopeReady()) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "Source activation lacks its completed actual Native service qualification");
        }
        return activation;
    }

    public synchronized ShardStore store() {
        if (store == null || storeClosed) {
            throw new IllegalStateException("original Source Store is unavailable");
        }
        return store;
    }

    public TargetWorkerProcessRuntime.Assembly graph() {
        return graph;
    }

    boolean hasMigrationStore(ShardStore actual) {
        return store == actual;
    }

    /** Native A and its exact adopted assignment are both required on reopen, restore and successor-reader paths. */
    private void requireMigrationAdoption() {
        final var actual = Objects.requireNonNull(store, "original opened Source Store");
        final var budget = new com.nereusstream.delay.store.BoundedReadBudget(
                2, content.maximumBytes(), content.lookupTimeout().toNanos(), nanos);
        actual.readWithBudget(budget, () -> {
            final var key = com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard());
            final byte[] raw = actual.get(com.nereusstream.delay.store.ColumnFamily.META, key);
            final var root = raw == null
                    ? null
                    : com.nereusstream.delay.protocol.TargetMigrationRoot.decode(
                            com.nereusstream.delay.store.TargetValueEnvelope.decode(
                                            raw, com.nereusstream.delay.protocol.TargetMigrationRoot.VALUE_TYPE)
                                    .payload());
            publication.requireMigration(actual.metadata(), root);
            process.bindMigrationRoot(this, actual, root);
            return Boolean.TRUE;
        });
    }

    // Final graph/Scope identities only; retained-current guards may call these while holding a Native Store view.
    TargetWorkerProcessRuntime sourceProcessForExitBinding() {
        return process;
    }

    TargetQuotaScope sourceScopeForExitBinding() {
        return scope;
    }

    /** Tracks the Process-owned composition; this binding alone grants no Native lineage or read rights. */
    void retainCheckpointComposition(TargetWorkerCheckpointComposition original, byte[] expectedLineage) {
        process.requireCheckpointComposition(this, Objects.requireNonNull(original), expectedLineage);
        synchronized (this) {
            if (stopping
                    || firstFailure != null
                    || checkpointComposition != null && checkpointComposition != original) {
                throw new IllegalStateException("original checkpoint composition is no longer admitted or differs");
            }
            Bytes.requireLength(expectedLineage, 16, "original checkpoint lineage");
            checkpointComposition = original;
            checkpointExpectedLineage = Bytes.copy(expectedLineage);
        }
    }

    /** Immutable local snapshot only; never reads a closed Store or re-enters stopped Source/Owner admission. */
    synchronized SourceExitBinding sourceExitBinding() {
        final var runtime = activation == null ? null : activation.runtime();
        final var actualLease = runtime == null ? lease : runtime.snapshotDrainLease();
        final var actualAssignment = runtime == null ? assignment : runtime.acceptedAssignment();
        // Same verified deployment/run plus the actual active-runtime (or acquired startup) lease,
        // as publicationOwnerIdentity does. This does not attribute every old SERVICE writer to one Owner.
        final var verified = registryIdentity == null || actualLease == null
                ? null
                : new com.nereusstream.delay.protocol.OwnerIdentity(
                        registryIdentity.deploymentId(),
                        registryIdentity.workerRunId(),
                        actualLease.ownerEpoch(),
                        actualLease.leaseToken());
        return new SourceExitBinding(
                process, graph, scope, actualAssignment, verified, sourceExitLineage, sourceExitSource);
    }

    /** Internal identity material, not an exit receipt; only the actual Process can certify its completed cleanup. */
    static final class SourceExitBinding {
        final TargetWorkerProcessRuntime process;
        final TargetWorkerProcessRuntime.Assembly graph;
        final TargetQuotaScope scope;
        final SourceAssignment assignment;
        final com.nereusstream.delay.protocol.OwnerIdentity verifiedOwner;
        final byte[] lineage;
        final com.nereusstream.delay.protocol.SourcePosition source;

        private SourceExitBinding(
                TargetWorkerProcessRuntime process,
                TargetWorkerProcessRuntime.Assembly graph,
                TargetQuotaScope scope,
                SourceAssignment assignment,
                com.nereusstream.delay.protocol.OwnerIdentity verifiedOwner,
                byte[] lineage,
                com.nereusstream.delay.protocol.SourcePosition source) {
            this.process = process;
            this.graph = graph;
            this.scope = scope;
            this.assignment = assignment;
            this.verifiedOwner = verifiedOwner;
            this.lineage = lineage == null ? null : Bytes.copy(lineage);
            this.source = source == null
                    ? null
                    : com.nereusstream.delay.protocol.TargetSourcePosition.decode(source.canonicalBytes());
        }
    }

    /** Explicit deployment composition; uses this Source's original writer, process I/O and input envelope. */
    public TargetResourceReclaimMaintenance configureResourceReclaimMaintenance(
            TargetWorkerShardRuntime actual,
            OwnedSourceSystemWriter writer,
            TargetResourceReclaimMaintenance.Configuration configuration,
            TargetResourceReclaimMaintenance.InputProvider provider,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock) {
        try (var construction = beginWorkerResourceConstruction(actual)) {
            construction.requireCurrent();
            if (graph.config().source().counters() < 4) {
                throw new IllegalArgumentException("Native GC requires the original Source root/owner counter window");
            }
            final var reservation = process.reserveReclaimInputs(this, configuration.maximumInputBytes());
            try {
                final var original = new TargetResourceReclaimMaintenance(
                        actual,
                        writer,
                        configuration,
                        provider,
                        reservation,
                        graph.io(),
                        time,
                        clock,
                        nanos,
                        () -> actual.requireRetirementReadCurrent(clock));
                actual.registerResourceReclaimMaintenance(original, writer);
                return original;
            } catch (RuntimeException | Error failure) {
                reservation.release();
                throw failure;
            }
        }
    }

    /** Original process-wide window; Source factories cannot replace pending generations or UNKNOWN appends. */
    public synchronized SystemWriterAppendBudget systemWriterBudget() {
        requireCurrent();
        return process.systemWriterBudget(this);
    }

    public synchronized TargetWorkerPublicationDispatch publicationDispatch(
            TargetWorkerPublicationDispatch.Configuration configuration) {
        requireCurrent();
        publicationDispatch = process.publicationDispatch(this, configuration);
        return publicationDispatch;
    }

    public TargetWorkerChannelInventory beginChannelInventory(TargetWorkerChannelInventory.Limits limits) {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            synchronized (this) {
                if (channelInventory != null) {
                    if (!channelInventory.limits().equals(limits)) {
                        throw new IllegalStateException("original inventory limits cannot be replaced");
                    }
                    return channelInventory;
                }
                final var original = worker.newChannelInventory(limits, owner.clock(), nanos);
                process.reserveChannelInventory(this, original);
                channelInventory = original;
                return original;
            }
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    public TargetWorkerChannelInventory.Turn advanceChannelInventory() {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            final TargetWorkerChannelInventory original;
            synchronized (this) {
                original = Objects.requireNonNull(channelInventory, "original channel inventory");
            }
            final var turn = original.runTurn();
            construction.requireCurrent();
            if (turn.status() == TargetWorkerChannelInventory.Status.COMPLETE) {
                worker.retainChannelInventory(original);
            }
            return turn;
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    /** Exact retained facts for the actual SDK factory on the original construction call. */
    public synchronized TargetWorkerChannelInventory.Entry publicationChannel(
            TargetWorkerShardRuntime actual, byte[] digest, boolean newWork) {
        requireCurrent();
        requireAdmittedWorker(actual);
        if (constructionThread != Thread.currentThread()) {
            throw new IllegalStateException("channel lookup requires its original SDK construction");
        }
        return actual.requirePublicationInventoryChannel(digest, newWork);
    }

    public synchronized com.nereusstream.delay.protocol.OwnerIdentity publicationOwnerIdentity(
            TargetWorkerShardRuntime actual) {
        requireCurrent();
        requireAdmittedWorker(actual);
        if (constructionThread != Thread.currentThread()) {
            throw new IllegalStateException("Owner lookup requires its original SDK construction");
        }
        final var original = Objects.requireNonNull(registryIdentity, "original verified Source registry identity");
        final var current = new com.nereusstream.delay.protocol.OwnerIdentity(
                original.deploymentId(), original.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
        return actual.requirePublicationOwner(current, owner.clock());
    }

    /** Configured K2 obtains CONTROL backing only from the same original process owner, before Host transfer. */
    public void requireReceiptManagement(TargetWorkerProcessRuntime.ReceiptManagement original) {
        process.requireReceiptManagement(this, original);
    }

    public void requireReceiptManagementRead(TargetWorkerProcessRuntime.ReceiptManagement original) {
        process.requireReceiptManagementRead(this, original);
    }
    /** Select only original recovery/publication paths; a caller cannot nominate another Catalog fork. */
    public synchronized com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend
            originalEvidenceRecoveryCatalog(TargetWorkerShardRuntime actual) {
        requireAdmittedWorker(actual);
        final var published = actual.originalEvidenceRecoveryCatalog();
        if (checkpointCatalog != null) {
            if (!(checkpointCatalog
                    instanceof com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend restored)) {
                throw new IllegalStateException("original recovery Catalog lacks its actual managed publication SDK");
            }
            if (published != null) {
                restored.requireSameOriginalEvidenceAuthority(published);
            }
            return restored;
        }
        return Objects.requireNonNull(published, "original Source has no managed checkpoint publication authority");
    }

    public void requireEvidenceRetirementReadCurrent(
            TargetWorkerShardRuntime actual, TargetWorkerProcessRuntime.ReceiptManagement management) {
        requireReceiptManagementRead(management);
        synchronized (this) {
            requireAdmittedWorker(actual);
        }
        actual.requireRetirementReadCurrent(owner.clock());
        requireReceiptManagementRead(management);
    }

    public com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent readEvidenceRetirementIntent(
            TargetWorkerShardRuntime actual,
            TargetWorkerProcessRuntime.ReceiptManagement management,
            com.nereusstream.delay.protocol.SystemMutation selected,
            com.nereusstream.delay.store.BoundedReadBudget reads) {
        requireEvidenceRetirementReadCurrent(actual, management);
        final var intent = actual.readAppliedRetirementIntent(reads, selected, owner.clock());
        requireEvidenceRetirementReadCurrent(actual, management);
        return intent;
    }

    public com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedConfirmation
            readEvidenceRetirementConfirmation(
                    TargetWorkerShardRuntime actual,
                    TargetWorkerProcessRuntime.ReceiptManagement management,
                    com.nereusstream.delay.protocol.SystemMutation selected,
                    com.nereusstream.delay.store.BoundedReadBudget reads) {
        requireEvidenceRetirementReadCurrent(actual, management);
        final var confirmed = actual.readAppliedRetirementConfirmation(reads, selected, owner.clock());
        requireEvidenceRetirementReadCurrent(actual, management);
        return confirmed;
    }

    public synchronized void retainPublicationDispatch(
            TargetWorkerShardRuntime actual, TargetWorkerPublicationDispatch dispatch) {
        requireCurrent();
        requireAdmittedWorker(actual);
        if (constructionThread != Thread.currentThread()) {
            throw new IllegalStateException("dispatch binding requires its original SDK construction");
        }
        Objects.requireNonNull(dispatch, "original process publication dispatch");
        process.requirePublicationDispatch(this, dispatch);
        if (publicationDispatch != null && publicationDispatch != dispatch) {
            throw new IllegalStateException("original publication dispatch cannot be replaced");
        }
        publicationDispatch = dispatch;
    }

    /** Resolves an actual OPEN inventory identity under this Root's original Owner before SDK creation. */
    public TargetDestinationActivation preparePublicationChannel(
            byte[] digest,
            LocalBrokerMtlsResolver resolver,
            com.nereusstream.delay.runtime.ProfileCatalog profiles,
            com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time) {
        final byte[] originalDigest = Bytes.copy(digest);
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            final var entry = worker.requireOpenPublicationChannel(originalDigest);
            final var current = publicationOwnerIdentity(worker);
            // Actual bounded file/PKIX work is outside the Startup/Store monitors.
            final var material =
                    Objects.requireNonNull(resolver).resolve(entry.identity(), entry.physical(), profiles, trust, time);
            construction.requireCurrent();
            worker.requireOpenPublicationChannel(originalDigest);
            final var activation = new TargetDestinationActivation(
                    worker,
                    current,
                    entry.identity(),
                    entry.physical(),
                    profiles,
                    trust,
                    material,
                    reads,
                    time,
                    owner.clock());
            activation.run();
            construction.requireCurrent();
            return activation;
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    /** Actual BASELINE/managed opening over this Root's private Worker and shared physical/routing owner. */
    public TargetDestinationChannelRuntime openPublicationChannel(
            TargetWorkerPublicationDispatch dispatch,
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            com.nereusstream.delay.protocol.CanonicalTargetPartition physical,
            TargetDestinationSdkBudget budget,
            com.nereusstream.delay.protocol.CapacityVector cost,
            long maximumPhysicalBytes,
            TargetDestinationChannelRuntime.Open factory,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            process.requirePublicationDispatch(this, dispatch);
            synchronized (this) {
                publicationDispatch = dispatch;
            }
            final var result = new TargetDestinationChannelRuntime(
                    worker,
                    channel,
                    physical,
                    budget,
                    cost,
                    dispatch.physical(),
                    dispatch.mux(),
                    graph.classes(),
                    graph.resources(),
                    graph.io(),
                    reads,
                    owner.clock(),
                    maximumPhysicalBytes,
                    factory);
            construction.requireCurrent();
            return result;
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    public boolean finishPublicationChannel(TargetDestinationChannelRuntime original) {
        try {
            final TargetWorkerPublicationDispatch dispatch;
            synchronized (this) {
                requireAdmittedWorker(worker);
                dispatch = Objects.requireNonNull(publicationDispatch, "original process dispatch");
                worker.requirePublicationResource(original);
                original.requireDispatch(worker, dispatch);
            }
            if (!original.finishStartup()) {
                return false;
            }
            dispatch.mux().requireEndpoint(original.channelIdentity(), original);
            return true;
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    /** Uses the original process pool/Source writer; collector custody is independent of business Source ACK. */
    public void configureSloCustodyExport(
            OwnedSourceSystemWriter writer,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads,
            com.nereusstream.delay.runtime.TargetSloExportPageReader.Limits pageLimits,
            TargetSloCustodyExportMaintenance.Provider provider,
            long maximumBytes) {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            worker.configureSloCustodyExport(
                    writer, graph.io(), reads, owner.clock(), pageLimits, provider, maximumBytes);
            construction.requireCurrent();
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    /** Original process I/O and already admitted Source writer; deployment supplies held UTC/signer/budget material. */
    public void configureSloFinalMaintenance(
            OwnedSourceSystemWriter writer,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads,
            TargetSloFinalMaintenance.Limits pageLimits,
            TargetSloFinalMaintenance.Provider provider,
            long maximumBytes) {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            worker.configureSloFinalMaintenance(
                    writer, graph.io(), reads, owner.clock(), pageLimits, provider, maximumBytes);
            construction.requireCurrent();
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    /** Original process I/O and already admitted Source writer; deployment supplies held UTC/signer/budget material. */
    public void configureSloFinalContinuations(
            OwnedSourceSystemWriter writer, TargetSloFinalContinuation.Provider provider, long maximumBytes) {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            worker.configureSloFinalContinuations(writer, graph.io(), provider, maximumBytes);
            construction.requireCurrent();
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    /** The original construction retains a concrete backend before any native SDK work is queued. */
    public void retainPublicationBackend(
            TargetWorkerShardRuntime actualWorker,
            TargetOwnedBackendGroup original,
            TargetDestinationSdkBudget budget) {
        synchronized (this) {
            if (worker != actualWorker
                    || constructionThread != Thread.currentThread()
                    || stopping
                    || firstFailure != null) {
                throw new IllegalStateException("publication opening requires its original active construction");
            }
        }
        Objects.requireNonNull(original).requireWorkerGraph(graph.classes(), graph.resources());
        Objects.requireNonNull(budget).requireWorker(graph.classes(), graph.resources());
        actualWorker.retainPublicationBackend(original, budget);
    }

    /** Builds the actual single-flight driver and its mandatory recovery/Journal maintenance, never a supplied runtime. */
    public TargetPulsarPublishRuntime configurePublishing(
            OwnedSourceSystemWriter writer, TargetWorkerPublishingConfiguration configuration) {
        try (var construction = beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            final var dispatch = Objects.requireNonNull(publicationDispatch, "original process publication dispatch");
            process.requirePublicationDispatch(this, dispatch);
            final var journal = worker.requireOwnedPublishing(writer, dispatch, configuration);
            final var result = configuration.install(worker, writer, journal, dispatch, graph.io());
            construction.requireCurrent();
            return result;
        } catch (RuntimeException | Error failure) {
            failResourceStartup(failure);
            throw failure;
        }
    }

    public synchronized SourceAssignment acceptedAssignment() {
        if (assignment == null) {
            throw new IllegalStateException("original Source assignment has not been accepted");
        }
        return assignment;
    }

    /** Actual reader factories own their original call until the cursor or native opening cleanup is retained. */
    public synchronized Construction beginRecoveryReaderConstruction() {
        if (initial != null && !initialCommitted
                || initializationRunning
                || store == null
                || recoverySource != null
                || recoveryOpeningCleanup != null
                || recovery != null
                || activation != null) {
            throw new IllegalStateException("original recovery reader construction is unavailable");
        }
        return beginConstruction(false);
    }

    public void retainRecoverySource(TargetWorkerRecoverySource original) {
        final var expected = beginAuthorityRead();
        try {
            Objects.requireNonNull(original)
                    .requireStartup(
                            expected.assignment(),
                            graph.config().maximumSourceFrameBytes(),
                            graph.classes(),
                            graph.resources());
            synchronized (this) {
                requireSameAuthorityRead(expected);
                if (recoverySource != null && recoverySource != original || recovery != null || activation != null) {
                    throw new IllegalStateException("original recovery Source cannot be replaced");
                }
                recoverySource = original;
                localNativeReader = original;
            }
        } catch (RuntimeException | Error failure) {
            recoveryLocalEpoch.incrementAndGet();
            throw failure;
        } finally {
            endAuthorityRead();
        }
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

    /** Retains the signed Source deployment/Owner identity on its original actual SDK construction. */
    public synchronized void bindSourceIdentity(
            LocalBrokerMtlsResolver.SourceReceipt receipt,
            com.nereusstream.delay.protocol.OwnerIdentity identity,
            com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence now) {
        requireCurrent();
        if (constructionThread != Thread.currentThread() || worker != null) {
            throw new IllegalStateException("Source identity binding requires its original SDK construction");
        }
        receipt.authorization().requireAssignment(assignment);
        receipt.requireActive(now);
        if (!Bytes.constantTimeEquals(
                        identity.deploymentId(), receipt.authorization().deploymentId())
                || identity.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())
                || registryIdentity != null
                        && !Arrays.equals(registryIdentity.canonicalBytes(), identity.canonicalBytes())) {
            throw new IllegalArgumentException("Source identity changes its signed deployment or original Owner");
        }
        registryIdentity = identity;
        if (activation != null && writerOriginLimits != null && !activeWriterOriginConfigured) {
            activation.runtime().configureWriterOrigin(writerOriginLimits, identity, owner.clock());
            activeWriterOriginConfigured = true;
        }
    }

    /** Original acquired/activated lease for binding the deployment's existing OwnerIdentity; never inferred on failure. */
    public synchronized OwnerLease ownerLease() {
        if (lease == null || stopping || firstFailure != null) {
            throw new IllegalStateException("original acquired Source lease is unavailable");
        }
        return lease;
    }

    /** Original SDK factory construction is single flight and must return before the Store/lease can retire. */
    public synchronized Construction beginWorkerConstruction() {
        requireRecoveryFenceHandoffs();
        if (!recoveryInputs.isEmpty() && !serviceJournalHandoffComplete) {
            throw new IllegalStateException("recovery Journal normal close is required before Worker construction");
        }
        for (var input : recoveryInputs) {
            input.requireValue();
        }
        return beginConstruction(true);
    }

    /** Registers actual SDK resources before this root can expose the Worker to Host composition. */
    public synchronized Construction beginWorkerResourceConstruction(TargetWorkerShardRuntime actual) {
        requireCurrent();
        requireAdmittedWorker(actual);
        if (constructionThread != null) {
            throw new IllegalStateException("original resource construction is already owned");
        }
        final var current = Thread.currentThread();
        final var construction = new Construction(this, current);
        process.beginSourceConstruction(this, construction);
        constructionThread = current;
        return construction;
    }

    public synchronized void requireAdmittedWorker(TargetWorkerShardRuntime actual) {
        if (worker != actual || stopping || firstFailure != null) {
            throw new IllegalStateException("actual Source Worker is unavailable");
        }
        graph.oxia().requireWorker(actual);
    }

    private synchronized Construction beginConstruction(boolean active) {
        requireCurrent();
        if (active) {
            activation();
        }
        if (worker != null || constructionThread != null) {
            throw new IllegalStateException("Source Worker construction is already owned");
        }
        final var original = Thread.currentThread();
        final var construction = new Construction(this, original);
        process.beginSourceConstruction(this, construction);
        constructionThread = original;
        return construction;
    }

    public static final class Construction implements AutoCloseable {
        private final TargetWorkerSourceStartup startup;
        private final Thread original;
        private final PulsarPreparation preparation;
        private boolean closed;

        private Construction(TargetWorkerSourceStartup startup, Thread original) {
            this(startup, original, null);
        }

        private Construction(TargetWorkerSourceStartup startup, Thread original, PulsarPreparation preparation) {
            this.startup = startup;
            this.original = original;
            this.preparation = preparation;
        }

        public void requireCurrent() {
            synchronized (startup) {
                if (closed || Thread.currentThread() != original || startup.constructionThread != original) {
                    throw new IllegalStateException("Source construction lost its original call ownership");
                }
                if (preparation == null) {
                    startup.requireCurrent();
                } else {
                    preparation.requireCurrent();
                }
            }
        }

        @Override
        public void close() {
            synchronized (startup) {
                if (Thread.currentThread() != original || startup.constructionThread != original && !closed) {
                    throw new IllegalStateException("Source construction exit belongs to another original call");
                }
                if (!closed) {
                    startup.process.endSourceConstruction(this);
                    startup.constructionThread = null;
                    closed = true;
                }
            }
        }
    }

    /** The SDK factory registers its original owner before constructing or exposing the Worker. */
    public synchronized void registerBackend(TargetOwnedBackendGroup actual) {
        // Retain a late original allocation even if stop won the race; admission is checked separately.
        if (worker != null || backend != null && backend != actual) {
            throw new IllegalStateException("Source SDK owner cannot be replaced");
        }
        Objects.requireNonNull(actual).requireWorkerGraph(graph.classes(), graph.resources());
        backend = actual;
    }

    public void admitWorker(TargetWorkerShardRuntime actual) {
        requireCurrent();
        graph.oxia().requireWorker(actual);
        synchronized (this) {
            if (checkpointComposition != null) {
                if (sourceExitLineage == null || !Arrays.equals(sourceExitLineage, checkpointExpectedLineage)) {
                    throw new IllegalArgumentException(
                            "checkpoint composition differs from the actual reopened Native lineage");
                }
                process.requireCheckpointComposition(this, checkpointComposition, sourceExitLineage);
            }
            activation().requireGraph(graph.classes(), graph.resources(), store());
            actual.requireSourceStartup(activation.runtime(), store);
            if (!recoveryInputOperationsExited(false) || !recoveryInputCleanupExited()) {
                throw new IllegalStateException("original recovery input opening/cleanup is still pending");
            }
            requireRecoveryFenceHandoffs();
            if (!recoveryInputs.isEmpty() && !serviceJournalHandoffComplete) {
                throw new IllegalStateException("recovery Journal handoff is pending before Worker admission");
            }
            for (var input : recoveryInputs) {
                input.requireValue();
            }
            for (var inputBackend : recoveryInputBackends) {
                actual.retainRecoveryInputBackend(this, inputBackend);
            }
            if (recoverySource != null) {
                actual.retainRecoveryBackend(recoverySource.backend());
            }
            if (pulsarPreparation != null && pulsarPreparation.previousReader != null) {
                actual.retainPriorRecoveryBackend(pulsarPreparation, pulsarPreparation.previousReader.backend());
            }
            if (worker != null
                    || backend == null
                    || actual.sourceBackendGroup() != backend
                    || !actual.shardId().equals(assignment.shardId())) {
                throw new IllegalStateException("Source Worker does not own its original SDK/Store graph");
            }
            worker = actual;
        }
        final var manager = scopeResources;
        if (manager != null) {
            manager.bindWorker(actual, owner.clock());
        }
    }

    synchronized TargetWorkerShardRuntime workerForHost(TargetWorkerProcessRuntime expected) {
        if (process != expected
                || worker == null
                || stopping
                || firstFailure != null
                || initializationRunning
                || constructionThread != null) {
            throw new IllegalStateException("process Source startup is not ready for its original Host");
        }
        return worker;
    }

    public synchronized Optional<Throwable> firstFailure() {
        return Optional.ofNullable(firstFailure);
    }

    public synchronized Optional<Throwable> recoveryCleanupFailure() {
        return Optional.ofNullable(recoveryCleanupFailure);
    }

    public synchronized Optional<Throwable> checkpointPinFailure() {
        return Optional.ofNullable(checkpointPinFailure);
    }

    public boolean activationReady() {
        final TargetWorkerSourceRuntimeFactory.Activated actual;
        synchronized (this) {
            if (activation == null || stopping || firstFailure != null) {
                return false;
            }
            actual = activation;
        }
        if (!actual.runtime().prepareServiceEnvelope()) {
            return false;
        }
        synchronized (this) {
            return activation == actual
                    && !stopping
                    && firstFailure == null
                    && actual.runtime().operationEnvelopeReady();
        }
    }

    public synchronized void retain(Throwable failure) {
        if (firstFailure == null) {
            firstFailure = failure;
        } else if (firstFailure != failure) {
            for (var prior : firstFailure.getSuppressed()) {
                if (prior == failure) {
                    return;
                }
            }
            firstFailure.addSuppressed(failure);
        }
    }

    /** Resource observation can finish after Host composition; failure then uses that same original Host exit. */
    public void failResourceStartup(Throwable failure) {
        retain(failure);
        final var actual = process.host();
        if (actual.isPresent()) {
            try {
                actual.orElseThrow().quarantineOwnedBackends();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) {
                    retain(cleanup);
                }
            }
        }
    }

    /** Retains the failed pre-Host graph. A transferred Worker must exit through the actual Host instead. */
    public synchronized boolean retryFailedStartupCleanup(TargetOwnerDrainCoordinator.Request request) {
        process.requireFailedSourceCleanup(this);
        if (failedCleanupComplete) {
            return true;
        }
        if (initializationRunning || constructionThread != null || authorityReadsInFlight != 0) {
            return false;
        }
        if (firstFailure == null) {
            throw new IllegalStateException("successful startup is still admitted for assembly");
        }
        stopping = true;
        archiveRecoveryAudit = null;
        archiveRecoveryAuditStore = null;
        try {
            if (!recoveryInputOperationsExited(true)) {
                return false;
            }
            if (restoreSubmission == null && !ShardStore.retryTargetOpeningCleanup(firstFailure)) {
                return false;
            }
            if (restoreSubmission != null) {
                if (graph.classes().state(restoreSubmission.task()).orElse(null)
                                == com.nereusstream.delay.scheduler.WorkClassExecutionRegistry.ExecutionState.RUNNING
                        || restoreSubmission.outcome().isEmpty()) {
                    return false;
                }
                if (store == null && restoreSubmission.outcome().orElseThrow().restored() != null) {
                    store = restoreSubmission.outcome().orElseThrow().restored();
                }
                final var failed = restoreSubmission.outcome().orElseThrow().failure();
                if (failed != null
                        && !retryRestoreNativeCleanup(
                                failed, java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()))) {
                    return false;
                }
            }
            if (store != null && !storeClosed) {
                store.fenceForUncleanClose();
            }
            if (worker != null && failedWorkerExit == null) {
                worker.quarantineBackendWrites();
            }
            if (activation != null) {
                activation.runtime().fence();
                activation.runtime().retryAuthorityCleanup();
                if (!activation.runtime().authorityIoExited()) {
                    return false;
                }
            }
            if (failedAssembly != null) {
                failedAssembly.retryContentCleanup();
                if (!failedAssembly.contentIoExited()) {
                    return false;
                }
            }
            if (recovery != null) {
                recovery.coordinator().stopAuthorityPreparation();
                recovery.coordinator().retryAuthorityCleanup();
                if (!recovery.coordinator().authorityIoExited()) {
                    return false;
                }
            }
            if (pulsarPreparation != null) {
                final var prior = pulsarPreparation.previousRecovery;
                prior.coordinator().stopAuthorityPreparation();
                prior.coordinator().retryAuthorityCleanup();
                if (!prior.coordinator().authorityIoExited()) {
                    return false;
                }
                final var reader = pulsarPreparation.previousReader;
                reader.close();
                if (!reader.cleanupComplete()) {
                    return false;
                }
                final var oldBackend = reader.backend();
                final var currentReaderBackend = recoverySource == null ? null : recoverySource.backend();
                if (oldBackend != null && oldBackend != backend && oldBackend != currentReaderBackend) {
                    oldBackend.closeBeforeHostBinding();
                    if (!oldBackend.resourcesTerminated()) {
                        return false;
                    }
                }
            }
            if (recoveryOpeningCleanup != null) {
                if (recoveryOpeningBackend != null) {
                    recoveryOpeningBackend.closeBeforeHostBinding();
                    if (!recoveryOpeningBackend.resourcesTerminated()) {
                        return false;
                    }
                }
                recoveryOpeningCleanup.retryCleanup();
                if (!recoveryOpeningCleanup.cleanupComplete()) {
                    return false;
                }
            }
            if (recoverySource != null) {
                recoverySource.close();
                if (!recoverySource.cleanupComplete()) {
                    return false;
                }
                final var readerBackend = recoverySource.backend();
                if (readerBackend != null && readerBackend != backend) {
                    readerBackend.closeBeforeHostBinding();
                    if (!readerBackend.resourcesTerminated()) {
                        return false;
                    }
                }
            }
            if (backend != null) {
                backend.closeBeforeHostBinding();
                if (!backend.resourcesTerminated()) {
                    return false;
                }
            }
            for (var inputBackend : recoveryInputBackends) {
                inputBackend.closeBeforeHostBinding();
                if (!inputBackend.resourcesTerminated()) {
                    return false;
                }
            }
            if (!recoveryInputCleanupExited()) {
                return false;
            }
            if (worker != null) {
                for (var publication : worker.publicationBackendGroups()) {
                    publication.closeBeforeHostBinding();
                    if (!publication.resourcesTerminated()) {
                        return false;
                    }
                }
                if (!process.closeSourceCheckpointComposition(this)) {
                    return false;
                }
                if (!closeScopeResourcesAfterDependencies()) {
                    return false;
                }
                if (failedWorkerExit == null) {
                    var exited = worker.exitQuarantinedForReplay(request, owner.clock());
                    if (exited.isEmpty()) {
                        return false;
                    }
                    failedWorkerExit = exited.get();
                }
                storeClosed = true;
                leaseReleased = failedWorkerExit.leaseReleased();
                ownerLost = failedWorkerExit.ownerLost();
                if (!releaseRecoveryInputs() || !releaseOwnedCheckpointPin()) {
                    return false;
                }
                if (sourcePlanReservation != null) {
                    sourcePlanReservation.release();
                }
                failedCleanupComplete = true;
                return true;
            }
            if (!process.closeSourceCheckpointComposition(this)) {
                return false;
            }
            if (!closeScopeResourcesAfterDependencies()) {
                return false;
            }
            if (!storeClosed && store != null) {
                if (!store.isWriteOutcomeUncertain()) {
                    store.flushForUncleanClose();
                }
                store.close();
                storeClosed = true;
            }
            if (lease != null && !leaseReleased && !ownerLost) {
                var current = graph.oxia().providers().leases().current(lease.shardId());
                if (current.isEmpty() || !lease.sameIdentity(current.get())) {
                    ownerLost = true;
                } else if (!(leaseReleased = graph.oxia().providers().leases().release(current.get()))) {
                    return false;
                }
            }
            if (!releaseRecoveryInputs() || !releaseOwnedCheckpointPin()) {
                return false;
            }
            if (sourcePlanReservation != null) {
                sourcePlanReservation.release();
            }
            // An uncertain acquisition remains in the original failure receipt; no successful lease is invented.
            failedCleanupComplete = true;
            return true;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            return false;
        }
    }

    synchronized boolean failedCleanupComplete() {
        return failedCleanupComplete;
    }

    /** Only the original stopped Host can authorize the final pin release before its shared Oxia clients close. */
    boolean releaseCheckpointPinForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(graph.oxia());
        synchronized (this) {
            if (!releaseRecoveryInputs() || !releaseOwnedCheckpointPin()) {
                return false;
            }
            if (sourcePlanReservation != null) {
                sourcePlanReservation.release();
            }
            return true;
        }
    }

    private boolean releaseOwnedCheckpointPin() {
        if (!ownsCheckpointPin || checkpointPinReleased) {
            return true;
        }
        try {
            final var current = checkpointCatalog.activeRecoveryPin();
            if (pulsarPreparation != null
                    && pulsarPreparation.replacementPin != null
                    && current.isPresent()
                    && current.orElseThrow().equals(pulsarPreparation.replacementPin)) {
                checkpointCatalog.releaseRecoveryPin(pulsarPreparation.replacementPin);
                checkpointPinReleased = true;
                return true;
            }
            if (current.isEmpty() || !current.get().equals(checkpointPin)) {
                // Old protection is gone, including after response loss or a new owner's pin; never delete the
                // successor.
                checkpointPinReleased = true;
                return true;
            }
            checkpointCatalog.releaseRecoveryPin(checkpointPin);
            checkpointPinReleased = true;
            return true;
        } catch (RuntimeException | Error failure) {
            if (checkpointPinFailure == null) {
                checkpointPinFailure = failure;
            }
            return false;
        }
    }

    private boolean retryRestoreNativeCleanup(Throwable failure, java.util.Set<Throwable> seen) {
        if (!seen.add(failure)) {
            return true;
        }
        boolean complete = true;
        if (failure instanceof CheckpointInputFuture.Cleanup cleanup) {
            cleanup.retryCleanup();
            complete = cleanup.cleanupComplete();
        }
        if (failure.getCause() != null) {
            complete &= retryRestoreNativeCleanup(failure.getCause(), seen);
        }
        for (var suppressed : failure.getSuppressed()) {
            complete &= retryRestoreNativeCleanup(suppressed, seen);
        }
        return complete;
    }

    public static final class StartupFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerSourceStartup startup;

        private StartupFailure(Throwable failure, TargetWorkerSourceStartup startup) {
            super("Source bootstrap failed; original process-owned startup is retained", failure);
            this.startup = startup;
        }

        public TargetWorkerSourceStartup startup() {
            return startup;
        }
    }
}
