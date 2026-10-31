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
    public enum State { OPENING, MATERIAL, IMPORT, SEAL, SOURCE, REGISTER, APPEND, REPLAY, ADOPT, CLOSING, COMPLETE, FAILED }

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
    public Optional<Throwable> firstFailure() { return Optional.ofNullable(firstFailure); }
    public boolean activationExposed() { return exposed || staged != null; }
    public synchronized Optional<SourcePosition> baseline() {
        return inputs == null ? Optional.empty() : Optional.of(inputs.manifest().appliedShardLogPosition());
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
            if (state == State.SOURCE && transport == null && transportFactory == null) return state;
            final var task = new WorkClassTask(WorkClass.CHECKPOINT,
                    "migration-" + Bytes.hex(configuration.migrationId()) + '-' + Long.toUnsignedString(++turn),
                    configuration.taskBytes());
            graph.classes().submit(task, () -> {
                synchronized (TargetMigrationProcessDriver.this) {
                    if (stopping) return;
                    try {
                        operation = OwnedCheckpointIo.submit(guard -> { step(guard); return true; },
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
            case MATERIAL -> buildMaterial(current);
            case IMPORT -> importBatch(current);
            case SEAL -> {
                candidate = importer.seal(inputs.candidateSealer(), configuration.newReadBudget(nanos));
                final var control = inputs.controlPreparation();
                exact = control.factory().prepare(control.operationId(), new TargetMigrationActivateRequest(plan,
                        candidate.candidate()), control.authenticated(), control.exactScope(),
                        control.registrationRetryUntil(), control.sourceRetryUntil());
                state = State.SOURCE;
            }
            case SOURCE -> {
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
        assignment = TargetWorkerSourceRuntimeFactory.accept(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId());
        TargetWorkerSourceRuntimeFactory.requireScope(graph.oxia(), owner.tenant(), assignment, configuration.scope());
        lease = TargetWorkerSourceRuntimeFactory.acquire(graph.oxia(), owner.tenant(), owner.publication(), owner.workerId(),
                owner.ownerId(), owner.leaseDurationMs(), owner.clock());
        current.run();
        final var acquired = Objects.requireNonNull(provider.open(opening), "original migration input hold");
        synchronized (this) {
            if (inputs == null) { inputs = acquired; throw new IllegalStateException("migration Provider did not retain its actual hold before allocation"); }
            if (inputs != acquired) throw new IllegalStateException("migration Provider replaced its original held acquisition");
        }
        inputs.requireCurrent();
        if (inputs.retainedBytes() > configuration.maximumRetainedBytes()) throw new IllegalStateException("migration hold exceeds its Process reservation");
        final var manifest = inputs.manifest();
        if (manifest.storeFormatVersion() != 1 || !manifest.shardId().equals(configuration.scope().shard()))
            throw new IllegalArgumentException("migration requires its protected original legacy Manifest");
        assignment.activationBarrier().validatePosition(manifest.appliedShardLogPosition());
        synchronizedThrough = manifest.appliedShardLogPosition(); state = State.MATERIAL;
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
            pending = cursor.next();
        }
        final var entry = pending;
        reader.requireReaderEntry(entry, assignment, readerCut);
        sourceSuccessor.validate(lastSeen, entry.position());
        lastSeen = entry.position();
        if (handoffReading) { pending = null; return; }
        if (activated != null && entry.position().compareTo(target.appliedShardLogPosition()) > 0) {
            // Preserve the actual full captured requirement; the original CATCHING_UP runtime applies it.
            handoffReading = true; pending = null; return;
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
            pending = null; synchronizedThrough = entry.position(); return;
        }
        if (activated != null && hasActualTargetPosition(entry)) {
            final var domain = inputs.classifySource(entry, plan);
            if (domain == TargetMigrationInputs.SourceDomain.UNKNOWN)
                throw new IllegalStateException("recovered Source writer/producer domain is unknown");
            final var proof = com.nereusstream.delay.runtime.TargetMigrationTargetApplied.read(legacyCopy, target, plan,
                    entry, configuration.newReadBudget(nanos), () -> inputs.requireCompatibilityCurrent(plan, target.metadata()));
            legacy.applyMigrationTargetPosition(proof);
            pending = null; synchronizedThrough = entry.position(); return;
        }
        applyLegacyPrefix(entry, current);
        pending = null; synchronizedThrough = entry.position();
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
        if (compatibility != null && stopping) {
            compatibility.requestStop();
            if (!compatibility.ioExited()) return false;
        }
        if (adoption != null) {
            adoption.requestStop(); adoption.close(); adoption.retryCleanup();
            if (!adoption.ioExited() || !adoption.cleanupComplete()) return false;
        }
        if (target != null && !target.isClosed()) target.close();
        if (target != null && !target.isClosed()) return false;
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
        return sourceHandoffComplete && adopted != null && firstFailure == null && !stopping
                && (cleanup == null || cleanup.operationExited() && cleanup.ioExited());
    }
    synchronized TargetMigrationCompatibilityRuntime compatibility() {
        if (!serviceAdmitted() || compatibility == null) throw new IllegalStateException("original migration compatibility custody is unavailable");
        inputs.requireReplayRetention(plan, Objects.requireNonNull(minimumReplayCut));
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
    public static final class Opening {
        private final TargetMigrationProcessDriver driver;
        private Opening(TargetMigrationProcessDriver driver) { this.driver = driver; }
        public TargetWorkerProcessRuntime.Assembly graph() { return driver.graph; }
        public TargetMigrationConfiguration configuration() { return driver.configuration; }
        public void requireCurrent() { driver.requireCurrent(); }
        public void requireLocalCurrent() { driver.requireLocalCurrent(); }
        public SourceAssignment assignment() { return Objects.requireNonNull(driver.assignment, "accepted maintenance Source"); }
        public SourceAssignment acceptedAssignment() { return assignment(); }
        public OwnerLease ownerLease() { return Objects.requireNonNull(driver.lease, "actual maintenance lease"); }
        public OwnerIdentity ownerIdentity() { return Objects.requireNonNull(driver.identity, "actual signed Source identity"); }
        public SourcePosition baseline() { return driver.baseline().orElseThrow(); }
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
    public static final class AppendPermit {
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
