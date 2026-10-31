package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.TargetMigrationImporter;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** One fenced maintenance flight on the original Process I/O, WorkClass, Store and native resource graph. */
public final class TargetMigrationProcessDriver {
    public enum State { OPENING, MATERIAL, IMPORT, SEAL, SOURCE, REGISTER, APPEND, REPLAY, ADOPT, HISTORY, HISTORY_CLOSE, ARCHIVE, CLOSING, COMPLETE, FAILED }

    private final TargetWorkerProcessRuntime process;
    private final TargetWorkerProcessRuntime.Assembly graph;
    private final TargetMigrationConfiguration configuration;
    private final TargetWorkerSourceStartup.Owner owner;
    private final TargetMigrationInputs.Provider provider;
    private final TargetDestinationSdkBudget sdk;
    private final LongSupplier nanos;
    private final Opening opening = new Opening(this);
    private final List<TargetOwnedBackendGroup> backends = new ArrayList<>();
    private final List<TargetWorkerRecoverySource> readers = new ArrayList<>();
    private final List<CheckpointInputFuture.Cleanup> partial = new ArrayList<>();
    private final Map<TargetOwnedBackendGroup, CompletableFuture<Void>> backendCloses = new IdentityHashMap<>();
    private volatile State state = State.OPENING;
    private volatile boolean stopping;
    private volatile Throwable firstFailure;
    private SourceAssignment assignment;
    private OwnerLease lease;
    private OwnerIdentity identity;
    private TargetMigrationInputs inputs;
    private TargetMigrationTransport transport;
    private java.util.function.Function<Opening, TargetMigrationTransport> transportFactory;
    private TargetMigrationControlWriter writer;
    private TargetWorkerRecoverySource reader;
    private SourcePosition readerCut;
    private SourceReplayCursor<SourceReplayEntry> sourceCursor;
    private SourceReplaySuccessor sourceSuccessor;
    private ShardStore.LegacyCheckpointReplayCopy legacyCopy;
    private ShardStore target;
    private DelayShard legacy;
    private LegacyMigrationBundle.Material material;
    private LegacyMigrationBundle.Sealed bundle;
    private TargetMigrationPlan plan;
    private TargetMigrationImporter importer;
    private TargetMigrationImporter.PreparedCandidate candidate;
    private List<TargetMigrationImporter.ImportedRow> rows = List.of();
    private List<TargetMigrationCustodyGuard> guards = List.of();
    private int rowOffset;
    private int guardOffset;
    private TargetMigrationControlMutationFactory.Prepared exact;
    private TargetMigrationImporter.StagedActivation staged;
    private AppendPermit appendPermit;
    private ShardLogMutationAppender.AppendOutcome appendOutcome;
    private Throwable appendFailure;
    private boolean appendTried;
    private volatile boolean exposed;
    private SourcePosition synchronizedThrough;
    private SourceReplayEntry pending;
    private boolean pendingConsumed;
    private volatile TargetWorkerSourceStartup.FrameCapacityUnavailable frameCapacityFailure;
    private TargetMigrationStore.Activated activated;
    private TargetMigrationAdoption.Adopted adopted;
    private TargetMigrationAdoption adoption;
    private boolean adoptionStarted;
    private boolean captured;
    private OwnedCheckpointIo<Boolean> operation;
    private OwnedCheckpointIo<Boolean> cleanup;
    private WorkClassTask queued;
    private long turn;
    private boolean leaseReleased;
    private boolean reservationReleased;
    private volatile boolean sourceHandoffComplete;
    private SourcePosition lastSeen;
    private SourcePosition minimumReplayCut;
    private boolean handoffReading;
    private TargetMigrationCompatibilityRuntime compatibility;
    private boolean compatibilityRecovery;
    private volatile TargetWorkerSourceStartup reopenedSource;
    private TargetMigrationRoot recoveredRoot;
    private SourcePosition recoveredTargetCut;
    private TargetMigrationArchiveState archivePreview;
    private com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover archiveTakeover;
    private TargetMigrationArchiveHeld.Cursor archiveCursor;
    private TargetMigrationArchiveHeld receivedArchive;
    private TargetMigrationArchiveHeld.NativeCursor archiveNativeCursor;
    private TargetMigrationArchiveHeld.RuntimeHold archiveHold;
    private BoundedReadBudget archiveBudget;
    private CheckpointInputFuture.Cleanup archiveRuntimeCleanup;
    private TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames;
    private final java.util.Set<TargetWorkerSourceStartup.FrameDecodeLoan> frameDecodeLoans=java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Original old-domain reader binding. Only its real Driver can mint it after opening the authenticated copy. */
    public static final class FrameOpening {
        private final TargetMigrationProcessDriver driver;
        private final TargetMigrationInputs held;
        private final ShardStore.LegacyCheckpointReplayCopy copy;
        private final ShardStore store;
        private final CheckpointManifest manifest;
        private final SourceAssignment assignment;
        private final OwnerLease owner;
        private final SourcePosition cut;
        private final byte[] lineage;
        private FrameOpening(TargetMigrationProcessDriver driver) {
            this.driver=driver;held=Objects.requireNonNull(driver.inputs);copy=Objects.requireNonNull(driver.legacyCopy);
            store=copy.store();manifest=Objects.requireNonNull(held.manifest());assignment=driver.assignment;
            owner=driver.compatibilityRecovery?driver.reopenedSource.ownerLease():driver.lease;
            final var plan=Objects.requireNonNull(driver.plan);cut=plan.baseline().source();lineage=plan.oldLineage();
            if(manifest.storeFormatVersion()!=1||!store.shardId().equals(plan.scope().shard())
                    ||!Arrays.equals(store.metadata().storeIncarnation(),plan.oldStore())
                    ||!Arrays.equals(store.metadata().dbIdentity(),plan.oldDbIdentity())
                    ||!manifest.sourceStoreIncarnation().equals(store.metadata().storeIncarnationUuid())
                    ||!Arrays.equals(manifest.dbIdentity(),store.metadata().dbIdentity())
                    ||!Arrays.equals(manifest.recoveryLineageId(),lineage)
                    ||!Arrays.equals(manifest.manifestSha256(),plan.oldManifest().resource().manifestSha256())
                    ||!Arrays.equals(manifest.appliedShardLogPosition().canonicalBytes(),cut.canonicalBytes()))
                throw new IllegalStateException("migration Source frame hold changed its authenticated old Store/Manifest/lineage/cut");
        }
        public TargetMigrationSourceOpening acquisition(){return driver.opening;}
        public Object actualLegacyStore(){return store;}
        public StoreMetadata legacyMetadata(){return store.metadata();}
        public CheckpointManifest manifest(){return manifest;}
        public SourceAssignment assignment(){return assignment;}
        public SourcePosition cut(){return cut;}
        public byte[] lineage(){return lineage.clone();}
        public void requireCurrent(){driver.requireCurrent();requireLocalCurrent();}
        public void requireLocalCurrent(){
            driver.requireLocalCurrent();
            final var current=driver.compatibilityRecovery?driver.reopenedSource.ownerLease():driver.lease;
            if(driver.inputs!=held||driver.legacyCopy!=copy||store.isCloseStarted()
                    ||!assignment.sameIdentity(driver.assignment)||current==null||!owner.sameIdentity(current))
                throw new IllegalStateException("original migration read frame acquisition was replaced or exited");
        }
        public void requireSource(SourcePosition actual){
            requireLocalCurrent();
            if(actual==null||!cut.sameSourceIdentity(actual))throw new IllegalArgumentException("migration frame changed its original physical Source");
        }
        void requireFrame(SourcePosition actual,Long generation,byte[] attestation){
            requireSource(actual);
            if(actual.compareTo(cut)<=0)throw new IllegalArgumentException("migration frame predates its actual baseline");
            if(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier){
                if(generation==null||attestation==null)throw new IllegalStateException("migration frame lacks its actual Source generation");
                barrier.validateSourceConnection(generation,attestation);
            }else if(generation!=null||attestation!=null)throw new IllegalStateException("Kafka migration frame carries a foreign Source generation");
        }
    }

    private void captureSourceFrames(){
        if(sourceFrames!=null){sourceFrames.requireLocalCurrent();return;}
        final var binding=new FrameOpening(this);
        sourceFrames=TargetWorkerSourceRuntimeFactory.SourceFrameContract.capture(binding,
                Objects.requireNonNull(inputs.originalManagementSource()),inputs::requireLocalCurrent);
        // This remains the original held-input share; the Source owner must already pay the complete codec window.
        if(inputs.retainedBytes()>configuration.maximumRetainedBytes())
            throw new TargetSourceAuthorityPreparation.InputUnavailable("migration Source codec material exceeds its original Held reservation");
    }

    private TargetWorkerSourceStartup.FrameDecodeLoan acquireFrameDecode(SourcePosition actual,long bytes,Long generation,byte[] attestation){
        requireLocalCurrent();
        final var frames=sourceFrames;
        if(frames==null)throw new TargetWorkerSourceStartup.FrameCapacityUnavailable("original migration Source frame qualification is not yet captured");
        frames.requireFrame(actual,bytes,generation,attestation);
        final var decode=com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.frameDecode(bytes);
        return TargetWorkerSourceStartup.borrowFrameDecode(frames,actual,bytes,generation,attestation,decode,()->{},frameDecodeLoans);
    }

    TargetMigrationProcessDriver(TargetWorkerProcessRuntime process, TargetWorkerProcessRuntime.Assembly graph,
            TargetMigrationConfiguration configuration, TargetWorkerSourceStartup.Owner owner,
            TargetMigrationInputs.Provider provider, TargetDestinationSdkBudget sdk, LongSupplier nanos) {
        this.process = Objects.requireNonNull(process); this.graph = Objects.requireNonNull(graph);
        this.configuration = Objects.requireNonNull(configuration); this.owner = Objects.requireNonNull(owner);
        this.provider = Objects.requireNonNull(provider); this.sdk = Objects.requireNonNull(sdk);
        this.nanos = Objects.requireNonNull(nanos);
        if (configuration.taskBytes() < graph.config().maximumSourceApplyTaskBytes())
            throw new IllegalArgumentException("migration WorkClass bytes omit the original maximum decoded Source apply window");
        graph.classes().requireTaskServiceable(new WorkClassTask(WorkClass.CHECKPOINT,
                "migration-" + Bytes.hex(configuration.migrationId()), configuration.taskBytes()),
                graph.config().checkpoint().settlementTurn());
    }

    public State state() { return state; }
    public TargetMigrationConfiguration configuration() { return configuration; }
    public Opening opening() { return opening; }
    LongSupplier ownerClock() { return owner.clock(); }
    boolean compatibilityRecovery() { return compatibilityRecovery; }
    void recoverCompatibility() { compatibilityRecovery = true; }

    /** Binds the actual opened/restored Native root before any original material or reader is acquired. */
    void attachReopened(TargetWorkerSourceStartup source, ShardStore actual, TargetMigrationRoot root) {
        if (reopenedSource != null) {
            if (reopenedSource != source || target != actual || !Objects.equals(recoveredRoot, root))
                throw new IllegalStateException("reopened compatibility owner changed its actual Native Store/root");
            return;
        }
        if (!compatibilityRecovery || state != State.OPENING || reopenedSource != null && reopenedSource != source
                || root == null || !root.activated() || !root.plan().scope().equals(configuration.scope())
                || !Arrays.equals(root.plan().migrationId(), configuration.migrationId()))
            throw new IllegalStateException("normal migration recovery requires its exact actual activated Native root");
        TargetMigrationStore.requireIdentity(actual.metadata(), root.plan());
        target = actual; recoveredRoot = root; plan = root.plan();
        recoveredTargetCut = Objects.requireNonNull(actual.appliedShardLogPosition());
        assignment = source.acceptedAssignment();
        reopenedSource = source;
    }

    synchronized boolean prepareCompatibilityRecovery() {
        if (!compatibilityRecovery || reopenedSource == null) throw new IllegalStateException("migration recovery lacks its opened Source Store");
        advance(); return state == State.HISTORY && operation == null && queued == null || serviceAdmitted();
    }

    boolean historyRequired() { return compatibilityRecovery && !sourceHandoffComplete; }

    SourcePosition recoveryReaderCut() {
        if (!compatibilityRecovery || state != State.HISTORY || reader != null)
            throw new IllegalStateException("original compatibility history reader is not ready to open");
        inputs.requireReplayRetention(plan, recoveredTargetCut);
        return plan.baseline().source();
    }

    /** The actual startup reader is retained by both original owners until native close, never borrowed as a DTO. */
    synchronized boolean advanceHistory(TargetWorkerRecoverySource actual) {
        if (!compatibilityRecovery) throw new IllegalStateException("not an original compatibility recovery");
        if (reader == null) {
            if (state != State.HISTORY) throw new IllegalStateException("history material is not ready");
            actual.requireStartup(assignment, graph.config().maximumSourceFrameBytes(), graph.classes(), graph.resources());
            actual.requireReaderBinding(assignment, plan.baseline().source());
            reader = actual; readers.add(actual); readerCut = plan.baseline().source(); lastSeen = readerCut;
            sourceSuccessor = actual.successorAfter(readerCut); sourceCursor = actual.replayCursor();
        } else if (reader != actual) throw new IllegalStateException("history recovery changed its original reader");
        advance(); return serviceAdmitted();
    }
    public Optional<Throwable> firstFailure() { return Optional.ofNullable(firstFailure); }
    public Optional<TargetWorkerSourceStartup.FrameCapacityUnavailable> frameCapacityFailure(){return Optional.ofNullable(frameCapacityFailure);}
    public boolean activationExposed() { return exposed || staged != null; }
    public synchronized Optional<SourcePosition> baseline() {
        return inputs == null ? Optional.empty() : Optional.of(archivePreview == null ? inputs.manifest().appliedShardLogPosition() : plan.baseline().source());
    }

    /** Called by the configured K1/P1 factory path. A Provider cannot substitute a completed SDK DTO. */
    public synchronized void bindTransport(TargetMigrationTransport actual) {
        requireCurrent(); Objects.requireNonNull(actual).requireBinding(opening);
        if (transport != null && transport != actual) throw new IllegalStateException("migration transport changed its original SDK owner");
        transport = actual;
    }

    /** The configured service supplies its actual K1/P1 factory; allocation runs on this original owned I/O. */
    public synchronized void openTransport(java.util.function.Function<Opening, TargetMigrationTransport> actualFactory) {
        requireCurrent();
        if (transportFactory != null || transport != null) throw new IllegalStateException("original migration SDK factory already retained");
        transportFactory = Objects.requireNonNull(actualFactory);
    }

    /** Schedules one bounded original task, then polls its actual I/O completion on later startup turns. */
    public synchronized State advance() {
        if (state == State.COMPLETE) {
            if (operation != null) {
                operation.retryCleanup();
                if (operation.operationExited() && operation.ioExited()) { operation.receipt().join(); operation = null; }
            }
            if (queued != null && graph.classes().state(queued).isEmpty()) queued = null;
            if (cleanup != null && cleanup.operationExited() && cleanup.ioExited()) {
                cleanup.receipt().join(); cleanup = null;
            }
            return state;
        }
        if (stopping || state == State.FAILED || state == State.CLOSING) {
            retryCleanup(); return state;
        }
        try {
            requireCurrent();
            if (operation != null) {
                operation.retryCleanup();
                if (!operation.operationExited()) return state;
                // Observe failure before waiting for its cleanup: the registered backend may need stop to exit.
                operation.receipt().join();
                if (!operation.ioExited()) return state;
                operation = null;
            }
            if (queued != null) {
                if (graph.classes().state(queued).isPresent()) return state;
                queued = null;
            }
            if (compatibilityRecovery && state == State.HISTORY && reader == null) return state;
            if (state == State.SOURCE && transport == null && transportFactory == null) return state;
            final var task = new WorkClassTask(WorkClass.CHECKPOINT,
                    "migration-" + Bytes.hex(configuration.migrationId()) + '-' + Long.toUnsignedString(++turn),
                    configuration.taskBytes());
            graph.classes().submit(task, () -> {
                synchronized (TargetMigrationProcessDriver.this) {
                    if (stopping) return;
                    try {
                        operation = OwnedCheckpointIo.submit(guard -> {
                            try{step(guard);frameCapacityFailure=null;}
                            catch(TargetWorkerSourceStartup.FrameCapacityUnavailable unavailable){frameCapacityFailure=unavailable;}
                            return true;
                        },
                                this::requireCurrent, graph.io());
                    } catch (RuntimeException | Error failure) { retain(failure); throw failure; }
                }
            });
            queued = task;
            return state;
        } catch (RuntimeException | Error failure) {
            retain(failure); stopping = true; state = State.FAILED;
            throw new Failure(failure, this);
        }
    }

    private void step(Runnable current) {
        current.run();
        switch (state) {
            case OPENING -> openInputs(current);
            case MATERIAL -> { if (compatibilityRecovery) recoverMaterial(current); else buildMaterial(current); }
            case ARCHIVE -> recoverArchive(current);
            case HISTORY -> recoverHistory(current);
            case HISTORY_CLOSE -> {
                reader.close();
                if (!reader.cleanupComplete()) return;
                inputs.requireReplayRetention(plan, Objects.requireNonNull(minimumReplayCut));
                compatibility = new TargetMigrationCompatibilityRuntime(process, this, graph, configuration, inputs,
                        plan, bundle, material, legacyCopy, legacy, candidate.candidate(), nanos);
                sourceHandoffComplete = true; state = State.COMPLETE;
            }
            case IMPORT -> importBatch(current);
            case SEAL -> {
                candidate = importer.seal(inputs.candidateSealer(), configuration.newReadBudget(nanos));
                final var control = inputs.controlPreparation();
                exact = control.factory().prepare(control.operationId(), new TargetMigrationActivateRequest(plan,
                        candidate.candidate(), inputs.rootIssuerAuthorization(plan)), control.authenticated(), control.exactScope(),
                        control.registrationRetryUntil(), control.sourceRetryUntil());
                state = State.SOURCE;
            }
            case SOURCE -> {
                captureSourceFrames();
                if (transport == null) bindTransport(transportFactory.apply(opening));
                if (!transport.advance()) return;
                final var actualReader = Objects.requireNonNull(transport.reader());
                final var actualWriter = Objects.requireNonNull(transport.writer());
                requireRetained(actualReader, actualWriter);
                actualReader.requireStartup(assignment, graph.config().maximumSourceFrameBytes(), graph.classes(), graph.resources());
                actualReader.requireReaderBinding(assignment, synchronizedThrough);
                reader = actualReader; writer = actualWriter;
                readerCut = synchronizedThrough;
                lastSeen = synchronizedThrough;
                sourceSuccessor = reader.successorAfter(readerCut);
                sourceCursor = reader.replayCursor();
                state = staged == null ? State.REGISTER : State.REPLAY;
            }
            case REGISTER -> {
                final var copies = Objects.requireNonNull(writer.copyContract(), "actual SDK submission copy bound");
                final var exposure = TargetMigrationSourceExposure.acquire(opening, inputs.originalManagementSource(),
                        inputs.originalManagementRegistry(), exact, writer, configuration.maximumRetainedBytes());
                if (staged == null) staged = importer.stageActivation(candidate, exact.control(), exact.mutation(),
                        configuration.newReadBudget(nanos), inputs.commits());
                staged.requireCurrent();
                inputs.registration().register(exact.control());
                inputs.registration().validateMutation(exact.control(), exact.control().targets().getFirst(), exact.mutation());
                current.run(); appendPermit = new AppendPermit(this, writer, exact, copies, exposure); state = State.APPEND;
            }
            case APPEND -> {
                staged.requireCurrent(); current.run();
                appendTried = true;
                // Set before invoking the original writer: even an exception can follow native SDK exposure.
                try { appendOutcome = writer.append(exact, appendPermit); }
                catch (RuntimeException | Error failure) {
                    if (!exposed) throw failure;
                    appendFailure = failure;
                    appendOutcome = ShardLogMutationAppender.AppendOutcome.unknown();
                }
                if (appendOutcome.disposition() == ShardLogMutationAppender.AppendDisposition.DEFINITIVELY_NOT_PERSISTED) {
                    throw new IllegalStateException("original ACTIVATE was definitively rejected; maintenance remains fenced");
                }
                if (captured) {
                    transport.openReaderAfter(synchronizedThrough); captured = false; state = State.SOURCE;
                } else state = State.REPLAY;
            }
            case REPLAY -> replayOne(current);
            case ADOPT -> {
                if (!adoptionStarted) {
                    adoption = Objects.requireNonNull(inputs.adoption());
                    adoption.begin(activated, opening); adoptionStarted = true;
                }
                final var result = adoption.advance();
                if (result.isEmpty()) return;
                final var actual = result.orElseThrow();
                actual.requireApplied(activated); actual.requireCurrent();
                if (!actual.plan().equals(plan) || actual.checkpoint().storeFormatVersion() != 2
                        || !actual.checkpoint().sourceStoreIncarnation().equals(target.metadata().storeIncarnationUuid())
                        || actual.checkpoint().appliedShardLogPosition().compareTo(activated.migration().activatedAt().source()) < 0)
                    throw new IllegalStateException("metadata adoption changed actual A or old→new Store identity");
                adopted = actual; state = State.CLOSING;
            }
            default -> throw new IllegalStateException("migration has no admitted operation in " + state);
        }
        current.run();
    }

    private void openInputs(Runnable current) {
        if (!compatibilityRecovery) {
            assignment = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId());
            TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, configuration.scope());
            lease = TargetWorkerSourceRuntimeFactory.acquire(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId(),
                    owner.ownerId(), owner.leaseDurationMs(), owner.clock());
        }
        current.run();
        if (compatibilityRecovery) archivePreview = com.nereusstream.delay.runtime.TargetMigrationArchiveStore.peekTakeover(
                target, plan, configuration.newReadBudget(nanos));
        final var acquired = Objects.requireNonNull(provider.open(opening), "original migration input hold");
        synchronized (this) {
            if (inputs == null) { inputs = acquired; throw new IllegalStateException("migration Provider did not retain its actual hold before allocation"); }
            if (inputs != acquired) throw new IllegalStateException("migration Provider replaced its original held acquisition");
        }
        inputs.requireCurrent();
        if (inputs.retainedBytes() > configuration.maximumRetainedBytes()) throw new IllegalStateException("migration hold exceeds its Process reservation");
        if (archivePreview != null) { synchronizedThrough = recoveredTargetCut; state = State.MATERIAL; return; }
        final var manifest = inputs.manifest();
        if (manifest.storeFormatVersion() != 1 || !manifest.shardId().equals(configuration.scope().shard()))
            throw new IllegalArgumentException("migration requires its protected original legacy Manifest");
        assignment.activationBarrier().validatePosition(manifest.appliedShardLogPosition());
        synchronizedThrough = manifest.appliedShardLogPosition(); state = State.MATERIAL;
    }

    /** Revalidates the original C inventory against the already published fixed objects; writes no candidate or activation. */
    private void recoverMaterial(Runnable current) {
        if (!recoveredRoot.equals(readMigrationRoot())) throw new IllegalStateException("opened migration root changed during recovery");
        if (archivePreview != null) {
            final var attempt = Objects.requireNonNull(TargetMigrationStore.readAttemptNative(target, plan, configuration.newReadBudget(nanos)));
            if (!Arrays.equals(attempt.input().canonicalEnvelope(), recoveredRoot.activation().canonicalEnvelope()))
                throw new IllegalStateException("archive reopen changed original exact A/Native69");
            importer = TargetMigrationImporter.resume(target, plan, inputs.importLimits(), inputs, configuration.newReadBudget(nanos));
            candidate = importer.resumeSealed(attempt.candidate(), configuration.newReadBudget(nanos));
            final var backend = TargetStoreBackend.forMigration(target, plan.scope(), importer.root().identity().accountingIncarnation(),
                    plan.newLineage(), inputs.importLimits().writes(), candidate);
            backend.bindSourceOperationEnvelope(reopenedSource.prepareMigrationFrameInputs(this));
            archiveTakeover = Objects.requireNonNull(com.nereusstream.delay.runtime.TargetMigrationArchiveStore.readAppliedTakeover(
                    backend, plan.scope(), plan.newLineage(), plan, configuration.newReadBudget(nanos), inputs.reads()), "actual Native72 takeover/first/POS");
            if (!archiveTakeover.state().equals(archivePreview)) throw new IllegalStateException("preview differs from actual authenticated takeover");
            archiveCursor = Objects.requireNonNull(inputs.archiveRecovery(opening, archiveTakeover));
            archiveBudget = Objects.requireNonNull(inputs.archiveReads());
            state = State.ARCHIVE; return;
        }
        final var view = LegacyCheckpointStateInventory.inspectComplete(configuration.legacyCheckpoint(),
                configuration.scope().shard(), inputs.manifest(), inputs.physicalLimits(), inputs.inventoryLimits());
        material = LegacyMigrationBundle.prepare(view, inputs, inputs.bundleLimits());
        if (!"READY".equals(material.summary()) || !material.baseline().equals(plan.baseline())
                || !Arrays.equals(view.inventory().physicalProof().metadata().storeIncarnation(), plan.oldStore())
                || !Arrays.equals(view.inventory().physicalProof().metadata().dbIdentity(), plan.oldDbIdentity())
                || !Arrays.equals(inputs.manifest().manifestSha256(), plan.oldManifest().resource().manifestSha256()))
            throw new IllegalStateException("original checkpoint/material no longer matches durable migration C");
        final var mapping = LegacyMigrationBundle.readCompanion(plan.mapping().resource(), LegacyMigrationBundle.Section.MAPPING,
                material, inputs, inputs.bundleLimits(), inputs.immutableObjects());
        final var custody = LegacyMigrationBundle.readCompanion(plan.custody().resource(), LegacyMigrationBundle.Section.CUSTODY,
                material, inputs, inputs.bundleLimits(), inputs.immutableObjects());
        final var conservation = LegacyMigrationBundle.readCompanion(plan.conservation().resource(), LegacyMigrationBundle.Section.CONSERVATION,
                material, inputs, inputs.bundleLimits(), inputs.immutableObjects());
        final List<byte[]> expectedMapping = new ArrayList<>();
        for (var value : material.mappings()) expectedMapping.add(value.canonicalBytes());
        final List<byte[]> expectedCustody = new ArrayList<>();
        for (var value : material.view().entries()) expectedCustody.add(Bytes.concat(Bytes.u8(1), value.canonicalBytes()));
        for (var value : material.dispositions()) expectedCustody.add(Bytes.concat(Bytes.u8(2), value.canonicalBytes()));
        for (var value : material.entities()) expectedCustody.add(Bytes.concat(Bytes.u8(3), value.canonicalBytes()));
        for (var value : material.externalRecords()) expectedCustody.add(Bytes.concat(Bytes.u8(4), value.canonicalBytes()));
        final var mappingRecords = mapping.records(); final var custodyRecords = custody.records();
        final var conservationRecords = conservation.records();
        final var expectedConservation = List.of(Bytes.concat(material.baseline().canonicalBytes(),
                inputs.manifest().manifestSha256(), material.conservation().canonicalBytes()));
        long buffers = material.retainedBytes();
        for (var records : List.of(mappingRecords, custodyRecords, conservationRecords))
            for (var bytes : records) buffers = Math.addExact(buffers, Math.multiplyExact(2L, bytes.length));
        for (var records : List.of(expectedMapping, expectedCustody, expectedConservation))
            for (var bytes : records) buffers = Math.addExact(buffers, bytes.length);
        if (buffers > configuration.maximumRetainedBytes() || buffers > inputs.retainedBytes())
            throw new IllegalStateException("fixed original companions and their comparison buffers exceed the original charged flight");
        requireRecords(mappingRecords, expectedMapping); requireRecords(custodyRecords, expectedCustody);
        requireRecords(conservationRecords, expectedConservation);
        bundle = new LegacyMigrationBundle.Sealed(material, mapping.companion(), custody.companion(), conservation.companion());
        legacyCopy = ShardStore.openLegacyCheckpointReplayCopy(graph.config().store(), configuration.scope().shard(),
                graph.resources(), configuration.legacyCheckpoint(), inputs.manifest(), inputs.physicalLimits());
        legacy = inputs.legacyApplierFactory().open(legacyCopy.store());
        if (!legacy.isBackedBy(legacyCopy.store())) throw new IllegalStateException("reopened old interpreter changed its actual shadow");
        final var attempt = Objects.requireNonNull(TargetMigrationStore.readAttemptNative(target, plan,
                configuration.newReadBudget(nanos)), "original durable activation attempt");
        if (!Arrays.equals(attempt.input().canonicalEnvelope(), recoveredRoot.activation().canonicalEnvelope()))
            throw new IllegalStateException("actual Native65/69 disagree about original A");
        importer = TargetMigrationImporter.resume(target, plan, inputs.importLimits(), inputs, configuration.newReadBudget(nanos));
        candidate = importer.resumeSealed(attempt.candidate(), configuration.newReadBudget(nanos));
        exact = TargetMigrationControlMutationFactory.Prepared.restore(attempt.control(), attempt.input());
        activated = TargetMigrationStore.resumeActivated(candidate, attempt.control(), attempt.input(),
                recoveredRoot.activatedAt().source(), inputs.activationAuthority(), configuration.newReadBudget(nanos));
        inputs.requireCompatibilityCurrent(plan, target.metadata()); inputs.requireReplayRetention(plan, recoveredTargetCut);
        captureSourceFrames();current.run(); state = State.HISTORY;
    }
    private void recoverArchive(Runnable current) {
        if (receivedArchive == null) {
            final var received = archiveCursor.advance(archiveBudget);
            if (received.isEmpty()) return;
            receivedArchive = received.orElseThrow();
            archiveNativeCursor = receivedArchive.prepareNativeInputs(opening.archiveDependencies(), archiveBudget);
            return;
        }
        if (!archiveNativeCursor.advance()) return;
        opening.archiveDependencies(); // Recheck the same complete Native read view before publishing the receiver handoff.
        compatibility = new TargetMigrationCompatibilityRuntime(process, this, graph, configuration, inputs,
                plan, null, null, null, null, candidate.candidate(), nanos);
        archiveHold = receivedArchive.acquireRuntime(archiveTakeover, compatibility.archiveRuntimeOpening());
        compatibility.reopenArchive(target, importer, candidate, archiveTakeover, archiveHold);
        archiveNativeCursor = null; receivedArchive = null;
        current.run(); minimumReplayCut = recoveredTargetCut; sourceHandoffComplete = true; state = State.COMPLETE;
    }

    private static void requireRecords(List<byte[]> actual, List<byte[]> expected) {
        if (actual.size() != expected.size()) throw new IllegalStateException("fixed migration companion changed its complete original records");
        for (int index = 0; index < actual.size(); index++) if (!Arrays.equals(actual.get(index), expected.get(index)))
            throw new IllegalStateException("fixed migration companion differs from protected original C material");
    }

    /** Rebuilds only the old shadow. Persisted Native68 or actual first/POS determines each historical owner. */
    private void recoverHistory(Runnable current) {
        if (reader == null) return;
        if(pendingConsumed){completeFrameDelivery();return;}
        if (!captured) { reader.captureReplayTail(); captured = true; }
        if (!sourceCursor.hasNext()) {
            final var through = reader.capturedTailReadThrough();
            if (through.isEmpty()) return;
            minimumReplayCut = through.orElseThrow();
            if (!Arrays.equals(minimumReplayCut.canonicalBytes(), lastSeen.canonicalBytes())
                    || !Arrays.equals(legacyCopy.store().appliedShardLogPosition().canonicalBytes(), recoveredTargetCut.canonicalBytes())
                    || legacyCopy.store().shardMutationSequence() != target.shardMutationSequence())
                throw new SourceReplayGapException("original C history did not reconstruct the actual opened Native frontier");
            inputs.requireReplayRetention(plan, minimumReplayCut); state = State.HISTORY_CLOSE; return;
        }
        if(pending==null)pending=sourceCursor.peek();
        final var entry = pending;
        reader.requireReaderEntry(entry, assignment, readerCut); sourceSuccessor.validate(lastSeen, entry.position());
        lastSeen = entry.position();
        if (entry.position().compareTo(recoveredTargetCut) > 0) {completeFrameDelivery();return;}
        if (inputs.classifySource(entry, plan) == TargetMigrationInputs.SourceDomain.UNKNOWN)
            throw new TargetSourceAuthorityPreparation.InputUnavailable("historical original writer/producer domain is unknown");
        final boolean prefix = target.readWithBudget(configuration.newReadBudget(nanos), () -> target.get(ColumnFamily.META,
                TargetMigrationPrefixRecord.keyFor(plan.migrationId(), TargetQuotaMutation.increment(legacyCopy.store().shardMutationSequence()))) != null).value();
        if (prefix) {
            // The old domain was the actual owner of this Source occurrence, even if the entity converted later.
            switch (entry) {
                case SourceReplayRecord command -> legacy.apply(command.command(), entry.position());
                case SourceReplayMutation mutation -> legacy.applySystemMutation(mutation.mutation(), entry.position(), inputs.legacySystemVerificationKey());
            }
            if (!importer.replayPersistedPrefix(candidate, entry, legacyCopy.store(), configuration.newReadBudget(nanos)))
                throw new IllegalStateException("historical old-domain Source lacks its exact durable Native68");
        } else {
            final var proof = com.nereusstream.delay.runtime.TargetMigrationTargetApplied.read(legacyCopy, target, plan, entry,
                    configuration.newReadBudget(nanos), () -> inputs.requireCompatibilityCurrent(plan, target.metadata()));
            legacy.applyMigrationTargetPosition(proof);
        }
        current.run();completeFrameDelivery();
    }

    private void completeFrameDelivery(){
        pendingConsumed=true;
        if(!SourceApplyCoordinator.sameEntry(pending,sourceCursor.peek()))
            throw new IllegalStateException("migration Source changed its actually consumed frame");
        sourceCursor.next();pending=null;pendingConsumed=false;
    }

    private void buildMaterial(Runnable current) {
        final var view = LegacyCheckpointStateInventory.inspectComplete(configuration.legacyCheckpoint(),
                configuration.scope().shard(), inputs.manifest(), inputs.physicalLimits(), inputs.inventoryLimits());
        material = LegacyMigrationBundle.prepare(view, inputs, inputs.bundleLimits());
        if (!"READY".equals(material.summary())) throw new IllegalStateException("original migration material is blocked: "
                + material.blockers() + " " + material.conflicts());
        bundle = LegacyMigrationBundle.seal(material, inputs, inputs.bundleLimits(), inputs.immutableObjects());
        current.run();
        legacyCopy = ShardStore.openLegacyCheckpointReplayCopy(graph.config().store(), configuration.scope().shard(),
                graph.resources(), configuration.legacyCheckpoint(), inputs.manifest(), inputs.physicalLimits());
        legacy = inputs.legacyApplierFactory().open(legacyCopy.store());
        if (!legacy.isBackedBy(legacyCopy.store())) throw new IllegalArgumentException("migration legacy applier changed its original Store");
        final var oldSnapshot = Objects.requireNonNull(legacyCopy.store().controlSnapshot(), "original protected legacy control snapshot");
        final var shadowTuples = new ArrayList<>(oldSnapshot.protocolTuples());
        if (!shadowTuples.contains(ProtocolTuple.targetMigrationShadowProgress())) shadowTuples.add(ProtocolTuple.targetMigrationShadowProgress());
        // Reject a candidate before A if its original finite snapshot cannot express the shadow reader restriction.
        new CompatibleControlSnapshot(oldSnapshot.shard(), shadowTuples, oldSnapshot.profiles(), oldSnapshot.initialQuotaGrant());
        final var active = graph.config().store().rootPath().toAbsolutePath().normalize().resolve("shards")
                .resolve(configuration.scope().shard().routeIncarnation().uuid().toString())
                .resolve(Integer.toUnsignedString(configuration.scope().shard().partition())).resolve("ACTIVE");
        final boolean existing = java.nio.file.Files.exists(active, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        target = existing ? ShardStore.openMigrationTarget(graph.config().store(), configuration.scope().shard(), graph.resources())
                : ShardStore.createTarget(graph.config().store(), configuration.scope().shard(), graph.resources());
        final var a = inputs.planAuthority();
        plan = new TargetMigrationPlan(configuration.scope(), configuration.migrationId(), a.generation(), a.predecessor(),
                legacyCopy.store().metadata().storeIncarnation(), legacyCopy.store().metadata().dbIdentity(), 1,
                a.oldLineage(), material.baseline(), a.oldManifest(), target.metadata().storeIncarnation(),
                target.metadata().dbIdentity(), 2, a.newLineage(),
                new TargetMigrationObjectReference(bundle.mapping().manifest()),
                new TargetMigrationObjectReference(bundle.custody().manifest()),
                new TargetMigrationObjectReference(bundle.conservation().manifest()), a.closedWriterDomain(),
                a.managementAuthorityDigest(), a.readerEvidence());
        final var persisted = existing ? readMigrationRoot() : null;
        if (persisted != null && !persisted.plan().equals(plan))
            throw new IllegalStateException("recovery changed the original migration identity/plan/candidate Store");
        importer = persisted == null
                ? TargetMigrationImporter.begin(target, plan, inputs.accounting(), inputs.importLimits(), inputs, configuration.newReadBudget(nanos))
                : TargetMigrationImporter.resume(target, plan, inputs.importLimits(), inputs, configuration.newReadBudget(nanos));
        material = LegacyMigrationBundle.project(bundle, inputs, plan, inputs.bundleLimits());
        final var attempt = persisted == null ? null : TargetMigrationStore.readAttemptNative(target, plan,
                configuration.newReadBudget(nanos));
        if (attempt != null) {
            candidate = importer.resumeSealed(attempt.candidate(), configuration.newReadBudget(nanos));
            staged = importer.resumeStaged(candidate, configuration.newReadBudget(nanos));
            exact = TargetMigrationControlMutationFactory.Prepared.restore(staged.control(), staged.input());
            exposed = true;
            state = State.SOURCE;
            return;
        }
        rows = TargetMigrationImporter.importedRows(plan, importer.root(),
                LegacyMigrationBundle.baselineInputs(material, bundle, importer.root(), inputs), inputs.importLimits(),
                inputs.maximumRetainedOriginBytes());
        guards = LegacyMigrationBundle.baselineGuards(material, bundle, plan, importer.root(), inputs);
        long bytes = Math.addExact(material.retainedBytes(), Math.multiplyExact(128L, material.entities().size()));
        for (var row : rows) bytes = Math.addExact(bytes, Math.addExact(row.key().length,
                Math.addExact(row.value().length, row.origin().canonicalBytes().length)));
        for (var guard : guards) bytes = Math.addExact(bytes, guard.canonicalBytes().length);
        if (bytes > configuration.maximumRetainedBytes()) throw new IllegalStateException("complete migration material/import rows exceed original reservation");
        state = State.IMPORT;
    }

    private void importBatch(Runnable current) {
        final int maximum = Math.min(configuration.maximumRecordsPerTurn(), inputs.importLimits().writes().maximumRecords());
        final var batchRows = new ArrayList<TargetMigrationImporter.ImportedRow>();
        final var batchGuards = new ArrayList<TargetMigrationCustodyGuard>();
        long bytes = 0; int records = 0;
        while (rowOffset + batchRows.size() < rows.size()) {
            final var row = rows.get(rowOffset + batchRows.size());
            final long next = Math.addExact(bytes, row.key().length + (long) row.value().length
                    + row.origin().key().length + row.origin().canonicalBytes().length + 32);
            if (records + 2 > maximum || next > configuration.maximumBytesPerTurn()) break;
            batchRows.add(row); bytes = next; records += 2;
        }
        while (guardOffset + batchGuards.size() < guards.size()) {
            final var guard = guards.get(guardOffset + batchGuards.size());
            final long next = Math.addExact(bytes, guard.key().length + (long) guard.canonicalBytes().length + 16);
            if (records + 1 > maximum || next > configuration.maximumBytesPerTurn()) break;
            batchGuards.add(guard); bytes = next; records++;
        }
        if (records == 0 && (rowOffset < rows.size() || guardOffset < guards.size()))
            throw new IllegalStateException("one authenticated import row exceeds the original bounded turn");
        current.run(); importer.importBatch(batchRows, batchGuards, configuration.newReadBudget(nanos));
        rowOffset += batchRows.size(); guardOffset += batchGuards.size();
        if (rowOffset == rows.size() && guardOffset == guards.size()) state = State.SEAL;
    }

    private void replayOne(Runnable current) {
        if(pendingConsumed){completeFrameDelivery();return;}
        if (!captured) { reader.captureReplayTail(); captured = true; }
        final var cursor = sourceCursor;
        if (pending == null) {
            if (!cursor.hasNext()) {
                // UNKNOWN and an observed empty tail never reopen the old writer.
                if (activated != null) {
                    final var captured = reader.capturedTailReadThrough();
                    if (captured.isEmpty()) return;
                    minimumReplayCut = captured.orElseThrow();
                    if (!Arrays.equals(minimumReplayCut.canonicalBytes(), lastSeen.canonicalBytes()))
                        throw new IllegalStateException("captured migration prefix lost its actual decoded final Source frame");
                    inputs.requireReplayRetention(plan, minimumReplayCut);
                    state = State.ADOPT; return;
                }
                if (!reader.replayTailReached(synchronizedThrough)) return;
                if (!appendTried && !readMigrationRoot().activated()) {
                    // The same original management debit must independently permit its one native claim.
                    // An empty prefix grants neither a new debit nor permission to repeat an old UNKNOWN claim.
                    state = State.REGISTER; return;
                }
                transport.openReaderAfter(synchronizedThrough); captured = false; state = State.SOURCE; return;
            }
            pending = cursor.peek();
        }
        final var entry = pending;
        reader.requireReaderEntry(entry, assignment, readerCut);
        sourceSuccessor.validate(lastSeen, entry.position());
        lastSeen = entry.position();
        if (handoffReading) { completeFrameDelivery(); return; }
        if (activated != null && entry.position().compareTo(target.appliedShardLogPosition()) > 0) {
            // Preserve the actual full captured requirement; the original CATCHING_UP runtime applies it.
            handoffReading = true; completeFrameDelivery(); return;
        }
        if (entry instanceof SourceReplayMutation mutation && TargetMigrationControlBody.isControl(mutation.mutation())) {
            if (!Arrays.equals(mutation.mutation().encodeFrame(), exact.mutation().encodeFrame()))
                throw new IllegalStateException("intervening migration operation changed the exact retained ACTIVATE");
            if (appendOutcome != null && appendOutcome.sourcePosition() != null) {
                assignment.activationBarrier().validatePosition(appendOutcome.sourcePosition());
                if (!appendOutcome.sourcePosition().sameSourceIdentity(entry.position()))
                    throw new IllegalStateException("SDK receipt changed the original physical migration Source");
            }
            final var root = readMigrationRoot();
            if (root.activated()) {
                if (activated == null) activated = TargetMigrationStore.resumeActivated(candidate, exact.control(),
                        mutation.mutation(), root.activatedAt().source(), inputs.activationAuthority(), configuration.newReadBudget(nanos));
                TargetMigrationStore.applyActivationDuplicate(activated, mutation, configuration.newReadBudget(nanos), inputs.commits());
            } else {
                if (!Arrays.equals(target.appliedShardLogPosition().canonicalBytes(), synchronizedThrough.canonicalBytes())
                        || !Arrays.equals(legacyCopy.store().appliedShardLogPosition().canonicalBytes(), synchronizedThrough.canonicalBytes()))
                    throw new IllegalStateException("old and candidate Stores have not closed the complete C→A predecessor prefix");
                final var prepared = TargetMigrationStore.prepareActivation(candidate, inputs.importLimits(),
                        configuration.newReadBudget(nanos), exact.control(), mutation.mutation(), entry.position(), inputs.activationAuthority());
                current.run(); activated = TargetMigrationStore.commitActivation(prepared, inputs.commits());
            }
            final var shadowProof = com.nereusstream.delay.runtime.TargetMigrationTargetApplied.read(legacyCopy, target,
                    plan, entry, configuration.newReadBudget(nanos), inputs::requireLocalCurrent);
            legacy.applyMigrationTargetPosition(shadowProof);
            synchronizedThrough = entry.position(); completeFrameDelivery(); return;
        }
        if (activated != null && hasActualTargetPosition(entry)) {
            final var domain = inputs.classifySource(entry, plan);
            if (domain == TargetMigrationInputs.SourceDomain.UNKNOWN)
                throw new IllegalStateException("recovered Source writer/producer domain is unknown");
            final var proof = com.nereusstream.delay.runtime.TargetMigrationTargetApplied.read(legacyCopy, target, plan,
                    entry, configuration.newReadBudget(nanos), () -> inputs.requireCompatibilityCurrent(plan, target.metadata()));
            legacy.applyMigrationTargetPosition(proof);
            synchronizedThrough = entry.position(); completeFrameDelivery(); return;
        }
        applyLegacyPrefix(entry, current);
        synchronizedThrough = entry.position(); completeFrameDelivery();
    }

    private boolean hasActualTargetPosition(SourceReplayEntry entry) {
        return target.readWithBudget(configuration.newReadBudget(nanos), () -> target.get(ColumnFamily.DEDUPE,
                Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                        entry.position().canonicalBytes())) != null).value();
    }

    private void applyLegacyPrefix(SourceReplayEntry entry, Runnable current) {
        // Each old frame is interpreted by the original decoder/applier. It is never re-signed as Target input.
        final SourceReplayOutcome outcome = switch (entry) {
            case SourceReplayRecord command -> {
                final var actual = legacy.apply(command.command(), command.position());
                yield SourceReplayOutcome.command(entry.position(), new com.nereusstream.delay.runtime.CommandResult(
                        actual.applyStatus(), actual.stableCode(), actual.generation(), actual.stateVersion(),
                        actual.messageStatus(), entry.position().canonicalBytes()));
            }
            case SourceReplayMutation mutation -> {
                final var actual = legacy.applySystemMutation(mutation.mutation(), mutation.position(),
                        inputs.legacySystemVerificationKey());
                yield SourceReplayOutcome.systemMutation(entry.position(), com.nereusstream.delay.runtime.SystemMutationResult.from(
                        mutation.mutation(), actual.applyStatus(), actual.stableCode(), entry.position().canonicalBytes()));
            }
        };
        current.run();
        if (Arrays.equals(entry.position().canonicalBytes(), synchronizedThrough.canonicalBytes())) return;
        if (importer.replayPersistedPrefix(candidate, entry, legacyCopy.store(), configuration.newReadBudget(nanos))) return;
        final var projection = LegacyMigrationBundle.projectPrefix(bundle, inputs, entry, outcome, legacyCopy.store(),
                candidate, inputs.immutableObjects(), inputs.inventoryLimits(), inputs.bundleLimits(), configuration.newReadBudget(nanos));
        importer.applyLegacyPrefix(candidate, legacyCopy.store(), projection,
                configuration.newReadBudget(nanos), inputs.commits());
    }

    private TargetMigrationRoot readMigrationRoot() {
        return TargetMigrationStore.readRootNative(target, configuration.newReadBudget(nanos));
    }

    private synchronized void requireRetained(TargetWorkerRecoverySource source, TargetMigrationControlWriter control) {
        if (!readers.contains(source) || writer != control || !backends.contains(source.backend())
                || !backends.contains(control.backend())) throw new IllegalStateException("migration SDK handles were not retained before exposure");
        control.requireBinding(opening);
    }

    private void requireCurrent() {
        if (compatibilityRecovery) {
            process.requireMigrationRecoveryAdmission(this, reopenedSource, target);
            if (stopping || firstFailure != null) throw new IllegalStateException("original compatibility recovery stopped");
            reopenedSource.requireCurrent();
            if (inputs != null) {
                if (archivePreview == null) inputs.requireCompatibilityCurrent(plan, target.metadata());
                else if (archiveTakeover == null) inputs.requireCurrent();
                else inputs.requireArchiveCurrent(plan, target.metadata(), archiveTakeover);
            }
            return;
        }
        process.requireMigrationAdmission(this);
        if (stopping || firstFailure != null) throw new IllegalStateException("original migration maintenance owner stopped");
        if (lease != null) {
            var current = graph.oxia().providers().leases().current(configuration.scope().shard()).orElseThrow();
            final long now = owner.clock().getAsLong();
            if (!lease.sameIdentity(current) || !current.validAt(now)
                    || current.state() != ShardLifecycleState.ACQUIRING && current.state() != ShardLifecycleState.CATCHING_UP
                    || !Arrays.equals(current.sessionIdentity(), graph.oxia().providers().sessionIdentity()))
                throw new IllegalStateException("migration lost its original fenced maintenance Owner/session");
            if (current.expiresAtEpochMs() - now < owner.leaseDurationMs() / 2)
                current = graph.oxia().providers().leases().renew(current, now, owner.leaseDurationMs())
                        .orElseThrow(() -> new IllegalStateException("original maintenance lease renewal was not confirmed"));
            lease = current;
        }
        if (inputs != null) inputs.requireCurrent();
    }

    /** No Registry, Owner RPC, Store read or allocation is permitted from an SDK handoff lock. */
    private void requireLocalCurrent() {
        if (compatibilityRecovery) {
            process.requireMigrationRecoveryAdmission(this, reopenedSource, target);
            reopenedSource.requireMigrationLocalCurrent();
            if (stopping || firstFailure != null) throw new IllegalStateException("original compatibility recovery stopped");
            if (inputs != null) {
                if (archiveTakeover == null) inputs.requireLocalCurrent();
                else inputs.requireArchiveLocalCurrent(plan, target.metadata(), archiveTakeover);
            }
            return;
        }
        process.requireMigrationAdmission(this);
        final var actualLease = lease;
        if (stopping || firstFailure != null || actualLease == null || !actualLease.validAt(owner.clock().getAsLong())
                || actualLease.state() != ShardLifecycleState.ACQUIRING && actualLease.state() != ShardLifecycleState.CATCHING_UP)
            throw new IllegalStateException("original local migration Owner is stopped, expired or no longer in maintenance");
        final var actualInputs = Objects.requireNonNull(inputs, "original migration local protection hold");
        actualInputs.requireLocalCurrent();
    }

    public void requestStop() {
        final WorkClassTask task;
        final OwnedCheckpointIo<Boolean> running;
        final TargetMigrationTransport actualTransport;
        final TargetMigrationControlWriter actualWriter;
        final TargetMigrationInputs actualInputs;
        final TargetMigrationAdoption actualAdoption;
        final TargetMigrationCompatibilityRuntime actualCompatibility;
        synchronized (this) {
            stopping = true; task = queued; running = operation;
            actualTransport = transport; actualWriter = writer; actualInputs = inputs;
            actualAdoption = adoption;
            actualCompatibility = compatibility;
        }
        if (task != null) graph.classes().withdrawQueued(task);
        if (running != null) running.requestStop();
        if (actualCompatibility != null) actualCompatibility.requestStop();
        if (actualTransport != null) stopOriginal(actualTransport::requestStop);
        if (actualWriter != null) stopOriginal(actualWriter::requestStop);
        if (actualInputs != null) stopOriginal(actualInputs::requestStop);
        if (actualAdoption != null) stopOriginal(actualAdoption::requestStop);
    }

    private void stopOriginal(Runnable stop) {
        try { stop.run(); } catch (RuntimeException | Error failure) { retain(failure); }
    }

    /** Actual original I/O, late SDK handles and retained protections precede lease and Process reservation release. */
    public synchronized boolean retryCleanup() {
        if (!stopping && state != State.CLOSING) return false;
        if (queued != null) {
            graph.classes().withdrawQueued(queued); graph.classes().discardFailed(queued);
            if (graph.classes().state(queued).isPresent()) return false;
            queued = null;
        }
        if (operation != null) {
            if (stopping) operation.requestStop(); operation.retryCleanup();
            if (!operation.operationExited()) return false;
            try { operation.receipt().join(); } catch (RuntimeException failure) { retain(failure); }
            if (operation.ioExited()) operation = null;
        }
        if (cleanup != null) {
            cleanup.retryCleanup(); if (!cleanup.operationExited() || !cleanup.ioExited()) return false;
            try { cleanup.receipt().join(); } catch (RuntimeException failure) { retain(failure); }
            cleanup = null;
        }
        if (reservationReleased) return true;
        try {
            cleanup = OwnedCheckpointIo.submit(ignored -> closeOriginals(), () -> {}, graph.io());
            return false;
        } catch (java.util.concurrent.RejectedExecutionException full) {
            if (graph.io().isShutdown()) retain(full); return false;
        }
    }

    private boolean closeOriginals() {
        if (transport != null) transport.requestStop();
        if (writer != null) writer.requestStop();
        for (var original : readers) original.close();
        for (var original : backends) {
            original.freezeRegistrations();
            final var closing = original.closeBeforeHostBinding(); backendCloses.put(original, closing);
        }
        boolean nativeExited = true;
        for (var original : backends) {
            final var closing = backendCloses.get(original);
            if (!closing.isDone() || !original.resourcesTerminated() || !original.nativeResourcesTerminated()) nativeExited = false;
            else closing.join();
        }
        for (var original : partial) { original.retryCleanup(); if (!original.cleanupComplete()) nativeExited = false; }
        if (!nativeExited) return false;
        final var originalOperation = operation;
        if (originalOperation != null && !originalOperation.ioExited()) return false;
        if (transport != null && (!transport.ioExited() || !transport.cleanupComplete())) return false;
        if (writer != null && (!writer.operationsExited() || !writer.cleanupComplete())) return false;
        for (var original : readers) if (!original.cleanupComplete()) return false;
        for(var loan:frameDecodeLoans){loan.requestRelease();if(!loan.cleanupComplete())return false;}
        if (compatibility != null && stopping) {
            compatibility.requestStop();
            if (!compatibility.ioExited()) return false;
        }
        if (stopping && archiveRuntimeCleanup != null) {
            final var actual = (TargetMigrationArchiveHeld.RuntimeHold) archiveRuntimeCleanup;
            actual.close(); actual.retryCleanup();
            if (!actual.ioExited() || !actual.cleanupComplete()) return false;
        }
        if (stopping && archiveCursor != null) {
            archiveCursor.close(); archiveCursor.retryCleanup();
            if (!archiveCursor.cleanupComplete()) return false;
        }
        if (adoption != null) {
            adoption.requestStop(); adoption.close(); adoption.retryCleanup();
            if (!adoption.ioExited() || !adoption.cleanupComplete()) return false;
        }
        if (!compatibilityRecovery && target != null && !target.isClosed()) target.close();
        if (!compatibilityRecovery && target != null && !target.isClosed()) return false;
        if (lease != null && !leaseReleased) {
            final var actual = graph.oxia().providers().leases().current(lease.shardId());
            if (actual.isPresent() && actual.orElseThrow().sameIdentity(lease)
                    && !graph.oxia().providers().leases().release(lease)) return false;
            leaseReleased = true;
        }
        if (!stopping && adopted != null) {
            if (compatibility == null) compatibility = new TargetMigrationCompatibilityRuntime(process, this, graph,
                    configuration, inputs, plan, bundle, material, legacyCopy, legacy, candidate.candidate(), nanos);
            sourceHandoffComplete = true; state = State.COMPLETE;
            return true;
        }
        if (legacyCopy != null) legacyCopy.close();
        if (inputs != null) {
            inputs.requestStop(); inputs.close(); inputs.retryCleanup();
            if (!inputs.ioExited() || !inputs.cleanupComplete()) return false;
        }
        if (!reservationReleased) { process.releaseMigrationReservation(this); reservationReleased = true; }
        synchronized (this) {
            if (adopted != null && firstFailure == null) state = State.COMPLETE;
        }
        return true;
    }

    public synchronized boolean serviceAdmitted() {
        return sourceHandoffComplete && (compatibilityRecovery || adopted != null) && firstFailure == null && !stopping
                && (operation == null || operation.operationExited() && operation.ioExited()) && queued == null
                && (cleanup == null || cleanup.operationExited() && cleanup.ioExited());
    }
    public TargetMigrationCompatibilityRuntime.ArchiveOpening openArchiveHold(TargetMigrationMaintenance.Opening actual,
            com.nereusstream.delay.runtime.TargetMigrationRetirementStore.AppliedIntent intention,
            com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.Limits limits, BoundedReadBudget cumulative) {
        return compatibility().archiveOpening(actual, intention, limits, cumulative);
    }
    public boolean exitAfterArchiveTakeover(com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetSourceApplyRuntime runtime) {
        return compatibility().exitAfterArchiveTakeover(actual, runtime);
    }
    public boolean archiveTakeoverActive(com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetSourceApplyRuntime runtime) {
        return compatibility().archiveTakeoverActive(actual, runtime);
    }
    synchronized void archiveActivated(com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover actual) {
        if (!actual.migration().plan().equals(plan)
                || archiveTakeover != null && !archiveTakeover.takeoverAt().equals(actual.takeoverAt()))
            throw new IllegalStateException("archive runtime changed its actual same-plan takeover");
        archiveTakeover = actual;
    }
    void retainArchiveRuntimeCleanup(CheckpointInputFuture.Cleanup actual) {
        synchronized (this) {
            if (!(actual instanceof TargetMigrationArchiveHeld.RuntimeHold))
                throw new IllegalArgumentException("archive runtime cleanup must be the actual acquired new-owner child loan");
            if (archiveRuntimeCleanup != null && archiveRuntimeCleanup != actual)
                throw new IllegalStateException("original archive runtime acquisition cannot be replaced");
            archiveRuntimeCleanup = Objects.requireNonNull(actual);
        }
    }
    synchronized TargetMigrationCompatibilityRuntime compatibility() {
        if (!serviceAdmitted() || compatibility == null) throw new IllegalStateException("original migration compatibility custody is unavailable");
        if (archiveTakeover == null) inputs.requireReplayRetention(plan, Objects.requireNonNull(minimumReplayCut));
        else inputs.requireArchiveCurrent(plan, target.metadata(), archiveTakeover);
        return compatibility;
    }
    synchronized SourcePosition requiredReplayCut() {
        if (!serviceAdmitted()) throw new IllegalStateException("migration prefix has no actual handoff requirement");
        return minimumReplayCut;
    }
    public synchronized TargetMigrationAdoption.Adopted adopted() {
        if (!serviceAdmitted()) throw new IllegalStateException("migration has not adopted actual A and exited its original maintenance resources");
        return adopted;
    }
    public synchronized boolean cleanupComplete() { return reservationReleased; }

    private synchronized void retain(Throwable failure) {
        if (firstFailure == null) firstFailure = failure;
        else if (firstFailure != failure) {
            for (var prior : firstFailure.getSuppressed()) if (prior == failure || prior.getCause() == failure) return;
            if (firstFailure.getSuppressed().length < 16) firstFailure.addSuppressed(failure);
        }
    }

    /** Narrow original SDK acquisition capability; the Process retains it before any Provider/SDK call. */
    public static final class Opening implements TargetMigrationSourceOpening {
        private final TargetMigrationProcessDriver driver;
        private Opening(TargetMigrationProcessDriver driver) { this.driver = driver; }
        public TargetWorkerProcessRuntime.Assembly graph() { return driver.graph; }
        public TargetMigrationConfiguration configuration() { return driver.configuration; }
        public long maximumRetainedInputBytes() { return driver.configuration.maximumRetainedBytes(); }
        public boolean compatibilityRecovery() { return driver.compatibilityRecovery; }
        public TargetMigrationRoot recoveredRoot() { return Objects.requireNonNull(driver.recoveredRoot, "actual reopened Native65"); }
        /** Resource preview only; actual first/POS/reader36 and receiver audit are checked before service. */
        public TargetMigrationArchiveState archivePreview() { return driver.archivePreview; }
        public TargetCheckpointDependencies archiveDependencies() {
            if (!driver.compatibilityRecovery || driver.archivePreview == null)
                throw new IllegalStateException("only the actual post-T recovery owns Native archive dependencies");
            return driver.reopenedSource.archiveRecoveryAudit(driver).dependencies();
        }
        public void requireCurrent() { driver.requireCurrent(); }
        public void requireLocalCurrent() { driver.requireLocalCurrent(); }
        public SourceAssignment assignment() { return Objects.requireNonNull(driver.assignment, "accepted maintenance Source"); }
        public SourceAssignment acceptedAssignment() { return assignment(); }
        public OwnerLease ownerLease() { return driver.compatibilityRecovery ? driver.reopenedSource.ownerLease()
                : Objects.requireNonNull(driver.lease, "actual maintenance lease"); }
        public OwnerIdentity ownerIdentity() { return Objects.requireNonNull(driver.identity, "actual signed Source identity"); }
        public SourcePosition baseline() { return driver.baseline().orElseThrow(); }
        public TargetWorkerSourceStartup.FrameDecodeLoan acquireFrameDecode(SourcePosition actual,long bytes,Long generation,byte[] attestation){
            return driver.acquireFrameDecode(actual,bytes,generation,attestation);
        }
        public TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames(){
            requireLocalCurrent();
            if(driver.archivePreview!=null)return driver.reopenedSource.sourceFrameContract();
            final var actual=driver.sourceFrames;
            if(actual==null)throw new TargetSourceAuthorityPreparation.InputUnavailable("original migration frame material is not captured");
            actual.requireLocalCurrent();return actual;
        }
        public TargetDestinationSdkBudget sdkBudget() { return driver.sdk; }
        public SystemWriterAppendBudget systemWriterBudget() { return driver.process.migrationSystemWriterBudget(driver); }
        public SystemWriterAppendBudget systemWriters() { return systemWriterBudget(); }
        public void retainInputs(TargetMigrationInputs actual) {
            synchronized (driver) {
                requireCurrent();
                if (driver.inputs != null && driver.inputs != actual) throw new IllegalStateException("original migration hold already retained");
                driver.inputs = Objects.requireNonNull(actual);
            }
        }
        public void retainBackend(TargetOwnedBackendGroup actual) {
            Objects.requireNonNull(actual).requireWorkerGraph(driver.graph.classes(), driver.graph.resources());
            synchronized (driver) {
                requireCurrent();
                if (driver.backends.contains(actual)) return;
                driver.readers.removeIf(TargetWorkerRecoverySource::cleanupComplete);
                driver.backends.removeIf(prior -> {
                    if (prior == (driver.writer == null ? null : driver.writer.backend())
                            || !prior.resourcesTerminated() || !prior.nativeResourcesTerminated()
                            || driver.readers.stream().anyMatch(source -> source.backend() == prior)) return false;
                    driver.backendCloses.remove(prior); return true;
                });
                if (driver.backends.size() >= driver.configuration.maximumBackends()) throw new IllegalStateException("migration native owner bound exhausted");
                driver.backends.add(actual);
            }
        }
        public void retainRecoverySource(TargetWorkerRecoverySource actual) {
            synchronized (driver) {
                requireCurrent();
                if (!driver.backends.contains(actual.backend())) throw new IllegalStateException("migration reader backend was not retained before opening");
                driver.readers.removeIf(TargetWorkerRecoverySource::cleanupComplete);
                if (!driver.readers.contains(actual)) {
                    if (driver.readers.size() >= 2) throw new IllegalStateException("migration retains two original reader windows already");
                    driver.readers.add(actual);
                }
            }
        }
        public void retainControlWriter(TargetMigrationControlWriter actual) {
            synchronized (driver) {
                if (driver.compatibilityRecovery) throw new IllegalStateException("normal compatibility recovery cannot open an ACTIVATE writer");
                requireCurrent(); Objects.requireNonNull(actual).requireBinding(this);
                if (!driver.backends.contains(actual.backend()) || driver.writer != null && driver.writer != actual)
                    throw new IllegalStateException("migration changed its retained original CONTROL producer");
                driver.writer = actual;
            }
        }
        public void retainCleanup(CheckpointInputFuture.Cleanup actual) {
            synchronized (driver) {
                if (!driver.partial.contains(actual)) {
                    if (driver.partial.size() >= driver.configuration.maximumBackends()) throw new IllegalStateException("migration partial native cleanup bound exhausted");
                    driver.partial.add(Objects.requireNonNull(actual));
                }
            }
        }
        public void bindSourceIdentity(LocalBrokerMtlsResolver.SourceReceipt receipt, OwnerIdentity actual,
                TrustedUtcIntervalEvidence now) {
            requireCurrent(); receipt.authorization().requireAssignment(assignment()); receipt.requireActive(now);
            synchronized (driver) {
                if (!Arrays.equals(actual.deploymentId(), receipt.authorization().deploymentId())
                        || actual.ownerEpoch() != ownerLease().ownerEpoch()
                        || !Arrays.equals(actual.leaseFencingDigest(), ownerLease().leaseToken())
                        || driver.identity != null && !Arrays.equals(driver.identity.canonicalBytes(), actual.canonicalBytes()))
                    throw new IllegalArgumentException("migration SDK identity differs from signed deployment or actual maintenance Owner");
                driver.identity = actual;
            }
        }
    }

    /** Reusable only for this original exact append call; it records SDK exposure before native handoff. */
    public static final class AppendPermit implements TargetMigrationAppendPermit {
        private final TargetMigrationProcessDriver driver;
        private final TargetMigrationControlWriter writer;
        private final TargetMigrationControlMutationFactory.Prepared exact;
        private final TargetScopeExposureSource.CopyContract copies;
        private final TargetMigrationSourceExposure exposure;
        private boolean claimed;
        private AppendPermit(TargetMigrationProcessDriver driver, TargetMigrationControlWriter writer,
                TargetMigrationControlMutationFactory.Prepared exact, TargetScopeExposureSource.CopyContract copies,
                TargetMigrationSourceExposure exposure) {
            this.driver = driver; this.writer = writer; this.exact = exact; this.copies = copies; this.exposure = exposure;
        }
        public void beforeSdk(TargetMigrationControlWriter actual, TargetMigrationControlMutationFactory.Prepared frame) {
            beforeSdk(actual, frame, actual.copyContract());
        }
        public void beforeSdk(TargetMigrationControlWriter actual, TargetMigrationControlMutationFactory.Prepared frame,
                TargetScopeExposureSource.CopyContract actualCopies) {
            driver.requireLocalCurrent(); Objects.requireNonNull(driver.staged, "durable original activation attempt").requireLocalCurrent();
            synchronized (this) {
                if (driver.appendPermit != this || actual != writer || frame != exact
                        || driver.writer != writer || driver.state != State.APPEND || !copies.equals(actualCopies))
                    throw new IllegalStateException("native migration append changed its original producer/Prepared/root");
                if (!claimed) { exposure.claimNative(actual, frame, actualCopies); claimed = true; }
                else exposure.requireCurrent();
                driver.exposed = true;
            }
        }
    }

    public static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetMigrationProcessDriver driver;
        private Failure(Throwable cause, TargetMigrationProcessDriver driver) {
            super("migration failed; original maintenance owner and uncertain activation are retained", cause); this.driver = driver;
        }
        public TargetMigrationProcessDriver driver() { return driver; }
    }
}
