package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/** Same Process's retained old custody and interpreter; only Source records can advance either domain. */
public final class TargetMigrationCompatibilityRuntime {
    enum Route { TARGET, NATIVE_OLD_COMMAND, LEGACY_FIRST, LEGACY, ARCHIVE_LOOKUP, ARCHIVE_FIRST, ARCHIVE_CLOSED }
    private record PreparedLegacy(SourceReplayOutcome outcome, LegacyMigrationBundle.TailProjection projection,
            boolean alreadyApplied) {}
    private final TargetWorkerProcessRuntime process;
    private final TargetMigrationProcessDriver root;
    private final TargetWorkerProcessRuntime.Assembly graph;
    private final TargetMigrationConfiguration configuration;
    private final TargetMigrationInputs inputs;
    private final TargetMigrationPlan plan;
    private final LegacyMigrationBundle.Sealed bundle;
    private final ShardStore.LegacyCheckpointReplayCopy shadow;
    private final DelayShard old;
    private final TargetMigrationObjectReference sealedCandidate;
    private final Set<DelayMessageId> converted = new HashSet<>();
    private TargetSourceApplyRuntime runtime;
    private ShardStore target;
    private StoreMetadata targetMetadata;
    private TargetMigrationImporter importer;
    private TargetMigrationImporter.PreparedCandidate candidate;
    private SourceReplayEntry selected;
    private Route route;
    private NativeOwnedCommand nativeCommand;
    private OwnedCheckpointIo<PreparedLegacy> preparation;
    private OwnedCheckpointIo<Boolean> bridge;
    private PreparedLegacy prepared;
    private boolean closedEntry;
    private volatile boolean stopping;
    private boolean bindingStopped;
    private final LongSupplier nanos;
    private ArchiveOpening archiveOpening;
    private volatile TargetMigrationArchiveStore.AppliedTakeover takeover;
    private TargetMigrationArchiveHeld.RuntimeHold archiveOwner;
    private TargetStoreBackend archiveBackend;
    private volatile Runnable localArchiveOwner;
    private OwnedCheckpointIo<TargetMigrationArchiveHeld.RuntimeHold> takingOver;
    private OwnedCheckpointIo<ArchiveReplay> archivePreparation;
    private ArchiveReplay preparedArchive;
    private TargetMigrationArchiveClosedContract.Qualification archiveClosure;
    private TargetMigrationArchiveClosedContract.Decision closedDecision;
    private TargetMigrationArchiveReplayMaterial closedMaterial;
    private OwnedCheckpointIo<Boolean> legacyExit;
    private TargetMigrationInputs.LegacyRuntimeLoan legacyLoan;
    private boolean legacyExited;
    private final ArchiveRuntimeOpening runtimeOpening = new ArchiveRuntimeOpening();
    public record ArchiveReplay(TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetMigrationArchiveRow logical, TargetMigrationArchiveRow auxiliary, TargetMigrationArchiveStore.ReplayInputs inputs,
            TargetMigrationImporter.Limits limits, TargetStoreBackend.CommitAuthority commits) {}
    public record ArchiveClosed(TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetMigrationArchiveClosedContract.Decision decision, TargetMigrationArchiveReplayMaterial inputs,
            TargetMigrationImporter.Limits limits, TargetStoreBackend.CommitAuthority commits) {}
    private record ReplaySnapshot(TargetMigrationArchiveState head, TargetQuotaMutation before, long fence) {}
    TargetMigrationArchiveHeld.RuntimeOpening archiveRuntimeOpening() { return runtimeOpening; }
    private final class ArchiveRuntimeOpening implements TargetMigrationArchiveHeld.RuntimeOpening {
        public TargetWorkerProcessRuntime.Assembly graph() { return graph; }
        public void requireCurrent() { requireLocalCurrent(); }
        public void requireLocalCurrent() {
            TargetMigrationCompatibilityRuntime.this.requireLocalCurrent();
        }
        public void retainCleanup(CheckpointInputFuture.Cleanup actual) { root.retainArchiveRuntimeCleanup(actual); }
        public TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames(){
            return runtime==null?root.opening().sourceFrames():runtime.sourceFrameContract();
        }
    }
    void reopenArchive(ShardStore actual, TargetMigrationImporter originalImporter,
            TargetMigrationImporter.PreparedCandidate originalCandidate, TargetMigrationArchiveStore.AppliedTakeover actualT,
            TargetMigrationArchiveHeld.RuntimeHold received) {
        if (!actualT.migration().plan().equals(plan)) throw new IllegalStateException("archive reopen changed original migration");
        target = actual; targetMetadata = actual.metadata(); importer = originalImporter; candidate = originalCandidate; takeover = actualT; archiveOwner = received;
        archiveBackend = TargetStoreBackend.forMigration(actual, plan.scope(), importer.root().identity().accountingIncarnation(),
                plan.newLineage(), inputs.importLimits().writes(), candidate);
        legacyExited = true;
    }
    synchronized boolean activateArchive(TargetSourceApplyRuntime actual, SourceReplayEntry exactT,
            TargetMigrationArchiveStore.AppliedTakeover applied, TargetMigrationArchiveReceived received) {
        requireBinding(actual);
        if (!applied.migration().plan().equals(plan) || !(exactT instanceof SourceReplayMutation marker)
                || !Arrays.equals(applied.state().input().canonicalEnvelope(), marker.mutation().canonicalEnvelope()))
            throw new IllegalStateException("archive transition changed its actual same-plan Source T");
        if (takeover != null) return takeover.takeoverAt().equals(applied.takeoverAt()) && archiveOwner != null;
        if (takingOver == null) {
            takingOver = OwnedCheckpointIo.submit(current -> {
                current.run(); applied.requireCurrent();
                final var acquired = received.acquireRuntime(applied, runtimeOpening);
                inputs.requireArchiveCurrent(plan, target.metadata(), applied); acquired.requireCurrent(); current.run(); return acquired;
            }, this::requireCurrent, graph.io());
            return false;
        }
        takingOver.retryCleanup(); if (!takingOver.operationExited() || !takingOver.ioExited()) return false;
        final var acquired = takingOver.receipt().join();
        archiveBackend = TargetStoreBackend.forMigration(target, plan.scope(), importer.root().identity().accountingIncarnation(),
                plan.newLineage(), inputs.importLimits().writes(), candidate);
        actual.bindArchiveSourceEnvelope(archiveBackend);
        archiveOwner = acquired; takeover = applied; takingOver = null; root.archiveActivated(applied); return true;
    }

    /** The actual original graph, never a caller-assembled pair of Stores or a completed transfer callback. */
    synchronized ArchiveOpening archiveOpening(TargetMigrationMaintenance.Opening actual,
            TargetMigrationRetirementStore.AppliedIntent intention, TargetMigrationArchivePreparation.Limits limits,
            BoundedReadBudget cumulative) {
        requireCurrent(); Objects.requireNonNull(actual).requireCurrent(); Objects.requireNonNull(intention).requireCurrent();
        if (actual.graph().classes() != graph.classes() || actual.graph().resources() != graph.resources()
                || !intention.migration().plan().equals(plan)
                || !actual.configuration().scope().equals(plan.scope()))
            throw new IllegalStateException("archive acquisition changed the original Process/Store/plan/intent");
        if (archiveOpening == null || archiveOpening.closed) archiveOpening = new ArchiveOpening(actual, intention, limits, cumulative);
        else if (!archiveOpening.intention.intentAt().equals(intention.intentAt())
                || !Arrays.equals(archiveOpening.intention.intention().digest(), intention.intention().digest()))
            throw new IllegalStateException("archive graph is already borrowed by another exact retirement intent");
        return archiveOpening;
    }

    public final class ArchiveOpening implements TargetMigrationArchivePreparation.Held {
        private final TargetMigrationMaintenance.Opening acquisition;
        private final TargetMigrationRetirementStore.AppliedIntent intention;
        private final TargetMigrationArchivePreparation.Limits limits;
        private final BoundedReadBudget cumulative;
        private boolean closed;
        private ArchiveOpening(TargetMigrationMaintenance.Opening acquisition, TargetMigrationRetirementStore.AppliedIntent intention,
                TargetMigrationArchivePreparation.Limits limits, BoundedReadBudget cumulative) {
            this.acquisition = acquisition; this.intention = intention; this.limits = Objects.requireNonNull(limits); this.cumulative = Objects.requireNonNull(cumulative);
        }
        public TargetMigrationRetirementStore.AppliedIntent intent() { requireLocalCurrent(); return intention; }
        public ShardStore.LegacyCheckpointReplayCopy shadow() { requireLocalCurrent(); return shadow; }
        public ShardStore target() { requireLocalCurrent(); return target; }
        public LegacyMigrationBundle.Sealed sealed() { requireLocalCurrent(); return bundle; }
        public LegacyMigrationBundle.Held original() { requireLocalCurrent(); return inputs; }
        public LegacyMigrationBundle.ImmutableObjects objects() { requireLocalCurrent(); return inputs.immutableObjects(); }
        public TargetMigrationArchivePreparation.Limits limits() { return limits; }
        public BoundedReadBudget reads() { return cumulative; }
        public TargetMigrationArchiveClosedContract.Materials closedMaterials() {
            requireLocalCurrent(); return inputs.archiveClosedMaterials(intention);
        }
        public SourceReplayEntry source(SourcePosition at, BoundedReadBudget cumulative) {
            requireCurrent(); final var record = inputs.archiveSource(at, cumulative);
            if (!Arrays.equals(record.position().canonicalBytes(), at.canonicalBytes())) throw new IllegalStateException("archive source content changed its exact protected physical point");
            requireCurrent(); return record;
        }
        public void requireCurrent() { requireLocalCurrent(); acquisition.requireCurrent(); intention.requireCurrent(); TargetMigrationCompatibilityRuntime.this.requireCurrent(); }
        public void requireLocalCurrent() {
            if (closed || archiveOpening != this) throw new IllegalStateException("original archive graph loan is closed");
            TargetMigrationCompatibilityRuntime.this.requireLocalCurrent();
        }
        public long retainedBytes() { return inputs.retainedBytes(); }
        /** Only this graph loan exits; Source/HTTP/legacy custody remain in the original root until actual T/exit. */
        public void close() { closed = true; }
    }

    TargetMigrationCompatibilityRuntime(TargetWorkerProcessRuntime process, TargetMigrationProcessDriver root,
            TargetWorkerProcessRuntime.Assembly graph, TargetMigrationConfiguration configuration,
            TargetMigrationInputs inputs, TargetMigrationPlan plan, LegacyMigrationBundle.Sealed bundle,
            LegacyMigrationBundle.Material material, ShardStore.LegacyCheckpointReplayCopy shadow, DelayShard old,
            TargetMigrationObjectReference sealedCandidate, LongSupplier nanos) {
        this.process = process; this.root = root; this.graph = graph; this.configuration = configuration;
        this.inputs = inputs; this.plan = plan; this.bundle = bundle; this.shadow = shadow; this.old = old;
        this.sealedCandidate = sealedCandidate; this.nanos = nanos;
        if (material != null) for (var entity : material.entities()) if (entity.disposition() == LegacyMigrationBundle.Disposition.CONVERTED)
            converted.add(entity.messageId());
    }

    synchronized void bind(TargetSourceApplyRuntime actual, ShardStore actualStore) {
        if (stopping || runtime != null && runtime != actual && !ioExited(runtime))
            throw new IllegalStateException("previous original migration compatibility binding has not exited");
        if (runtime == actual) return;
        process.requireMigrationCompatibility(root, actual, actualStore);
        requireInputsCurrent(actualStore.metadata());
        if (!Arrays.equals(actualStore.metadata().storeIncarnation(), plan.newStore())
                || !Arrays.equals(actualStore.metadata().dbIdentity(), plan.newDbIdentity()))
            throw new IllegalArgumentException("compatibility runtime changed actual new Store identity");
        target = actualStore; targetMetadata = actualStore.metadata();
        runtime = Objects.requireNonNull(actual); bindingStopped = false;
        localArchiveOwner = actual.migrationArchiveLocalGuard(actualStore, graph.resources(), root.ownerClock());
        importer = TargetMigrationImporter.resume(target, plan, inputs.importLimits(), new CompatibilityHeld(), configuration.newReadBudget(nanos));
        candidate = importer.resumeSealed(sealedCandidate, configuration.newReadBudget(nanos));
        if (takeover != null) {
            archiveBackend = TargetStoreBackend.forMigration(target, plan.scope(), importer.root().identity().accountingIncarnation(),
                    plan.newLineage(), inputs.importLimits().writes(), candidate);
            actual.bindArchiveSourceEnvelope(archiveBackend);
            final var rebound = Objects.requireNonNull(TargetMigrationArchiveStore.readAppliedTakeover(archiveBackend,
                    plan.scope(), plan.newLineage(), plan, configuration.newReadBudget(nanos), inputs.reads()));
            if (!rebound.state().equals(takeover.state())) throw new IllegalStateException("reopened Source changed the actual archive takeover");
            takeover = rebound; root.archiveActivated(rebound);
        }
        selected = null; route = null; nativeCommand = null; preparation = null; bridge = null; prepared = null; archivePreparation = null; preparedArchive = null; closedDecision = null; closedMaterial = null; archiveClosure = null; closedEntry = false;
    }

    synchronized Route route(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireBinding(actual); requireCurrent();
        if (selected != null) {
            if (SourceApplyCoordinator.sameEntry(selected, entry)) return route;
            if (!closedEntry) throw new IllegalStateException("migration compatibility retains another original Source entry");
            selected = null; route = null; nativeCommand = null; prepared = null; preparation = null; bridge = null; archivePreparation = null; preparedArchive = null; closedDecision = null; closedMaterial = null;
            closedEntry = false;
        }
        final var domain = Objects.requireNonNull(inputs.classifySource(entry, plan), "original Source writer domain");
        if (domain == TargetMigrationInputs.SourceDomain.UNKNOWN)
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original producer/writer domain is unknown; retain Source and custody");
        selected = entry;
        if (takeover != null) { route = Route.ARCHIVE_LOOKUP; return route; }
        if (entry instanceof SourceReplayRecord command) {
            if (shadow.store().get(ColumnFamily.DEDUPE, KeyCodec.dedupeCommand(command.command().commandId())) != null)
                route = Route.LEGACY_FIRST;
            else if (domain == TargetMigrationInputs.SourceDomain.TARGET) route = Route.TARGET;
            else if (nativeOwned(command.command().delayMessageId())) {
                route = Route.NATIVE_OLD_COMMAND; nativeCommand = new NativeOwnedCommand(this, command);
            } else route = Route.LEGACY;
        } else route = domain == TargetMigrationInputs.SourceDomain.TARGET ? Route.TARGET : Route.LEGACY;
        return route;
    }

    private boolean nativeOwned(DelayMessageId id) {
        return target.readWithBudget(configuration.newReadBudget(nanos), () -> {
            if (target.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(id)) != null) return false;
            return converted.contains(id) || target.get(ColumnFamily.ID, TargetKeyCodec.message(id)) != null
                    || target.get(ColumnFamily.META, Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG,TargetKeyCodec.KEY_FORMAT},id.bytes())) != null;
        }).value();
    }

    synchronized NativeOwnedCommand nativeCommand(TargetSourceApplyRuntime actual, SourceReplayRecord entry) {
        return route(actual, entry) == Route.NATIVE_OLD_COMMAND ? nativeCommand : null;
    }

    /** Old-domain I/O runs on the original finite pool; the Source slot remains owned and unacknowledged. */
    synchronized boolean prepare(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        final var selectedRoute = route(actual, entry);
        if (selectedRoute == Route.ARCHIVE_LOOKUP || selectedRoute == Route.ARCHIVE_FIRST || selectedRoute == Route.ARCHIVE_CLOSED)
            return prepareArchive(actual, entry);
        if (selectedRoute == Route.TARGET || selectedRoute == Route.NATIVE_OLD_COMMAND) return true;
        if (preparation == null) {
            preparation = OwnedCheckpointIo.submit(current -> {
                current.run();
                final var outcome = oldApply(entry);
                current.run();
                if (importer.replayPersistedPrefix(candidate, entry, shadow.store(), configuration.newReadBudget(nanos)))
                    return new PreparedLegacy(outcome, null, true);
                final var projection = selectedRoute == Route.LEGACY_FIRST
                        ? LegacyMigrationBundle.projectRetainedResult(bundle, inputs, entry, outcome, shadow.store(), candidate,
                                inputs.immutableObjects(), inputs.inventoryLimits(), inputs.bundleLimits(), configuration.newReadBudget(nanos))
                        : LegacyMigrationBundle.projectPrefix(bundle, inputs, entry, outcome, shadow.store(), candidate,
                                inputs.immutableObjects(), inputs.inventoryLimits(), inputs.bundleLimits(), configuration.newReadBudget(nanos));
                current.run(); return new PreparedLegacy(outcome, projection, false);
            }, this::requireCurrent, graph.io());
            return false;
        }
        preparation.retryCleanup();
        if (!preparation.operationExited()) return false;
        final var value = preparation.receipt().join();
        if (!preparation.ioExited()) return false;
        prepared = value; return true;
    }

    private boolean prepareArchive(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        if (preparedArchive != null) return true;
        if (archivePreparation == null) {
            archivePreparation = OwnedCheckpointIo.submit(current -> {
                current.run(); takeover.requireCurrent(); archiveOwner.requireCurrent();
                final var domain = inputs.classifySource(entry, plan);
                if (domain == TargetMigrationInputs.SourceDomain.UNKNOWN)
                    throw new TargetSourceAuthorityPreparation.InputUnavailable("archive input has no original frozen writer/producer domain");
                final TargetMigrationArchiveStore.LogicalPair pair;
                if (entry instanceof SourceReplayRecord command) pair = TargetMigrationArchiveStore.readCommandPair(archiveBackend,
                        plan.scope(), plan.newLineage(), command.command().commandId(), configuration.newReadBudget(nanos), inputs.reads());
                else {
                    final var input = ((SourceReplayMutation) entry).mutation();
                    final byte[] fence = input.type() == SystemMutationType.TIME_FENCE ? TargetTimeFenceBody.decode(input.canonicalBody()).proofId() : null;
                    pair = TargetMigrationArchiveStore.readSystemPair(archiveBackend, plan.scope(), plan.newLineage(),
                            input.systemMutationId(), fence, configuration.newReadBudget(nanos), inputs.reads());
                }
                final var logical = pair == null ? null : pair.first();
                final var auxiliary = pair == null ? null : pair.auxiliary();
                if (logical == null) {
                    if (entry.position().compareTo(target.appliedShardLogPosition()) <= 0) {
                        final var physical = TargetMigrationArchiveStore.read(archiveBackend, plan.scope(), plan.newLineage(),
                                KeyCodec.dedupePosition(entry.position().canonicalBytes()), configuration.newReadBudget(nanos), inputs.reads());
                        if (physical != null && physical.sourceBranch() == TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY) {
                            route = Route.ARCHIVE_CLOSED;
                            return new ArchiveReplay(takeover, null, auxiliary, null, inputs.importLimits(), inputs.commits());
                        }
                    }
                    if (auxiliary != null) route = Route.ARCHIVE_CLOSED;
                    else if (domain == TargetMigrationInputs.SourceDomain.TARGET) route = Route.TARGET;
                    else if (entry instanceof SourceReplayRecord command && nativeOwned(command.command().delayMessageId())) {
                        route = Route.NATIVE_OLD_COMMAND; nativeCommand = new NativeOwnedCommand(this, command);
                    } else route = Route.ARCHIVE_CLOSED;
                    if (route == Route.ARCHIVE_CLOSED) {
                        final var snapshot = readArchiveSnapshot();
                        if (archiveClosure == null) {
                            final var intention = Objects.requireNonNull(TargetMigrationRetirementStore.readAppliedIntent(archiveBackend,
                                    plan.scope(), plan.newLineage(), plan, configuration.newReadBudget(nanos), inputs.reads()));
                            final var materials = Objects.requireNonNull(inputs.archiveClosedMaterials(intention));
                            archiveClosure = TargetMigrationArchiveClosedContract.qualify(materials, takeover.migration(), takeover.retirement(), takeover.state());
                        }
                        closedDecision = TargetMigrationArchiveClosedContract.decide(archiveClosure, entry, auxiliary, snapshot.before(), snapshot.fence());
                        closedMaterial = archiveOwner.prepareClosed(takeover, snapshot.head(), closedDecision);
                        return new ArchiveReplay(takeover, null, auxiliary, closedMaterial, inputs.importLimits(), inputs.commits());
                    }
                    return new ArchiveReplay(takeover, null, auxiliary, null, inputs.importLimits(), inputs.commits());
                }
                if (entry.position().compareTo(target.appliedShardLogPosition()) <= 0) {
                    route = Route.ARCHIVE_FIRST;
                    return new ArchiveReplay(takeover, logical, auxiliary, null, inputs.importLimits(), inputs.commits());
                }
                final var snapshot = readArchiveSnapshot();
                final SourceReplayOutcome replayOutcome;
                if (entry instanceof SourceReplayRecord command) {
                    final var result = TargetMigrationArchiveSemantics.commandOutcome(logical.entry(), command.command(), entry.position(), snapshot.fence());
                    replayOutcome = SourceReplayOutcome.command(entry.position(), new CommandResult(result.applyStatus(), result.stableCode(),
                            result.generation(), result.stateVersion(), result.messageStatus(), entry.position().canonicalBytes()));
                } else {
                    final var input = ((SourceReplayMutation) entry).mutation();
                    final var result = TargetMigrationArchiveSemantics.systemOutcome(logical.entry(), input, entry.position(), snapshot.fence());
                    replayOutcome = SourceReplayOutcome.systemMutation(entry.position(), SystemMutationResult.from(input, result.applyStatus(), result.stableCode(), entry.position().canonicalBytes()));
                }
                final var original = archiveOwner.prepareReplay(takeover, snapshot.head(), snapshot.before(), entry, logical, replayOutcome, snapshot.fence());
                original.requireCurrent();
                final var protectedInputs = new TargetMigrationArchiveStore.ReplayInputs() {
                    public TargetMigrationRecordReference sourceRecord(SourceReplayEntry value) { requireEntry(value); return original.sourceRecord(value); }
                    public TargetMigrationObjectReference inventory() { requireCurrent(); return original.inventory(); }
                    public void requireFence(long frozen) { requireCurrent(); original.requireFence(frozen); }
                    public void requirePredecessor(TargetMigrationArchiveState head, TargetQuotaMutation before) { requireCurrent(); original.requirePredecessor(head,before); }
                    public void requireReplay(TargetMigrationArchiveStore.AppliedTakeover at, TargetMigrationArchiveRow row, SourceReplayEntry value, SourceReplayOutcome outcome) {
                        requireEntry(value);
                        if (!at.takeoverAt().equals(takeover.takeoverAt()) || !row.equals(logical)) throw new IllegalStateException("archive physical replay changed exact T/old first");
                        original.requireReplay(at,row,value,outcome); requireCurrent();
                    }
                    private void requireEntry(SourceReplayEntry value) { requireCurrent(); if(!SourceApplyCoordinator.sameEntry(entry,value))throw new IllegalStateException("archive acquisition changed its original physical Source frame"); }
                    public void requireCurrent() { runtimeOpening.requireLocalCurrent(); archiveOwner.requireLocalCurrent(); original.requireCurrent(); }
                };
                route = Route.ARCHIVE_FIRST;
                return new ArchiveReplay(takeover, logical, auxiliary, protectedInputs, inputs.importLimits(), inputs.commits());
            }, this::requireCurrent, graph.io());
            return false;
        }
        archivePreparation.retryCleanup(); if (!archivePreparation.operationExited() || !archivePreparation.ioExited()) return false;
        preparedArchive = archivePreparation.receipt().join(); return true;
    }
    synchronized ArchiveReplay archiveReplay(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireBinding(actual);
        if (route(actual,entry) != Route.ARCHIVE_FIRST || preparedArchive == null || preparedArchive.logical() == null)
            throw new TargetSourceAuthorityPreparation.Required();
        return preparedArchive;
    }
    synchronized ArchiveClosed archiveClosed(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireBinding(actual);
        if (route(actual,entry) != Route.ARCHIVE_CLOSED || preparedArchive == null
                || entry.position().compareTo(target.appliedShardLogPosition()) > 0 && (closedDecision == null || closedMaterial == null))
            throw new TargetSourceAuthorityPreparation.Required();
        return new ArchiveClosed(takeover, closedDecision, closedMaterial, preparedArchive.limits(), preparedArchive.commits());
    }
    private ReplaySnapshot readArchiveSnapshot() {
        final var read = archiveBackend.prepareRead(configuration.newReadBudget(nanos), reader -> {
            final var raw = Objects.requireNonNull(reader.get(ColumnFamily.META, TargetMigrationArchiveState.headKey(plan.migrationId())));
            return new ReplaySnapshot(TargetMigrationArchiveState.decode(TargetValueEnvelope.decode(raw, TargetMigrationArchiveState.VALUE_TYPE).payload()),
                    reader.aggregate().mutation(), reader.closedIngressDeadlineThrough());
        });
        return archiveBackend.completeRead(read, inputs.reads());
    }

    synchronized SourceReplayOutcome apply(TargetSourceApplyRuntime actual, SourceReplayEntry entry,
            TargetStoreBackend.CommitAuthority actualWrites, BoundedReadBudget budget) {
        requireBinding(actual); requireCurrent();
        if (prepared == null || selected == null || !SourceApplyCoordinator.sameEntry(selected, entry))
            throw new IllegalStateException("old Source record has not completed original bounded preparation");
        if (!prepared.alreadyApplied()) importer.applyLegacyPrefix(candidate, shadow.store(), prepared.projection(), budget, actualWrites);
        return prepared.outcome();
    }

    synchronized void rebuild(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireBinding(actual);
        if (selected == null || !SourceApplyCoordinator.sameEntry(selected, entry)
                || preparation != null && (!preparation.operationExited() || !preparation.ioExited()))
            throw new IllegalStateException("cannot replace live original migration preparation");
        preparation = null; prepared = null;
    }

    /** A Target commit is not ACKable until its same original shadow has durably progressed past the exact frame. */
    synchronized boolean complete(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        if (takeover != null) {
            requireBinding(actual); requireCurrent();
            if (archivePreparation != null && (!archivePreparation.operationExited() || !archivePreparation.ioExited())) return false;
            closedEntry = true; return true;
        }
        if (selected != null && SourceApplyCoordinator.sameEntry(selected, entry) && closedEntry) return true;
        final var selectedRoute = route(actual, entry);
        if (selectedRoute == Route.TARGET || selectedRoute == Route.NATIVE_OLD_COMMAND) {
            if (bridge == null) {
                bridge = OwnedCheckpointIo.submit(current -> {
                    current.run();
                    final var proof = TargetMigrationTargetApplied.read(shadow, target, plan, entry,
                            configuration.newReadBudget(nanos), this::requireCurrent);
                    old.applyMigrationTargetPosition(proof); current.run(); return true;
                }, this::requireCurrent, graph.io());
                return false;
            }
            bridge.retryCleanup(); if (!bridge.operationExited()) return false;
            bridge.receipt().join(); if (!bridge.ioExited()) return false;
        } else if (preparation == null || !preparation.operationExited() || !preparation.ioExited()) return false;
        if (!Arrays.equals(shadow.store().appliedShardLogPosition().canonicalBytes(), entry.position().canonicalBytes()))
            throw new IllegalStateException("old and Target source processing has not closed its exact ordered steps");
        closedEntry = true;
        return true;
    }

    private SourceReplayOutcome oldApply(SourceReplayEntry entry) {
        return switch (entry) {
            case SourceReplayRecord command -> {
                final var result = old.apply(command.command(), entry.position());
                yield SourceReplayOutcome.command(entry.position(), new CommandResult(result.applyStatus(), result.stableCode(),
                        result.generation(), result.stateVersion(), result.messageStatus(), entry.position().canonicalBytes()));
            }
            case SourceReplayMutation mutation -> {
                final var result = old.applySystemMutation(mutation.mutation(), entry.position(), inputs.legacySystemVerificationKey());
                yield SourceReplayOutcome.systemMutation(entry.position(), SystemMutationResult.from(mutation.mutation(),
                        result.applyStatus(), result.stableCode(), entry.position().canonicalBytes()));
            }
        };
    }

    private void requireBinding(TargetSourceApplyRuntime actual) {
        if (runtime != actual || bindingStopped || stopping) throw new IllegalStateException("migration compatibility lost its original Source binding");
    }
    private void requireCurrent() {
        requireLocalCurrent();
        process.requireMigrationCompatibility(root, runtime, target);
        requireInputsCurrent(targetMetadata);
    }
    private void requireLocalCurrent() {
        if (stopping || bindingStopped) throw new IllegalStateException("original migration compatibility owner stopped");
        final var token = localArchiveOwner;
        if (runtime == null) root.opening().requireLocalCurrent();
        else if (token == null) throw new IllegalStateException("original Source local owner token is unavailable");
        else token.run();
        final var applied = takeover;
        if (applied == null) inputs.requireLocalCurrent();
        else inputs.requireArchiveLocalCurrent(plan, targetMetadata, applied);
    }
    /** Drives the exact old-only close after actual T. Shared Inputs and the receiver remain owned by this root. */
    synchronized boolean exitAfterArchiveTakeover(TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetSourceApplyRuntime actualRuntime) {
        if (runtime != actualRuntime || !actual.migration().plan().equals(plan)
                || takeover != null && !takeover.takeoverAt().equals(actual.takeoverAt()))
            throw new IllegalStateException("old runtime exit requires its actual same-root takeover/new owner");
        if (takeover == null || archiveOwner == null) return false;
        if (legacyExited) return true;
        if (preparation != null && (!preparation.operationExited() || !preparation.ioExited())
                || bridge != null && (!bridge.operationExited() || !bridge.ioExited())
                || takingOver != null && (!takingOver.operationExited() || !takingOver.ioExited())) return false;
        if (legacyExit == null) {
            legacyExit = OwnedCheckpointIo.submit(ignored -> {
                actual.requireCurrent(); archiveOwner.requireCurrent();
                if (legacyLoan == null) legacyLoan = Objects.requireNonNull(inputs.legacyRuntimeLoan());
                legacyLoan.requireTransfer(actual, archiveOwner);
                if (archiveOpening != null && !archiveOpening.closed) return false;
                if (shadow != null) shadow.close();
                legacyLoan.requestStop(); legacyLoan.close(); legacyLoan.retryCleanup();
                return legacyLoan.ioExited() && legacyLoan.cleanupComplete();
            }, runtimeOpening::requireCurrent, graph.io());
            return false;
        }
        legacyExit.retryCleanup(); if (!legacyExit.operationExited() || !legacyExit.ioExited()) return false;
        final boolean exited = legacyExit.receipt().join(); legacyExit = null;
        if (exited) legacyExited = true;
        return legacyExited;
    }
    synchronized boolean archiveTakeoverActive(TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetSourceApplyRuntime actualRuntime) {
        if (runtime != actualRuntime || !actual.migration().plan().equals(plan)
                || takeover != null && !takeover.takeoverAt().equals(actual.takeoverAt()))
            throw new IllegalStateException("archive activation lookup changed its actual runtime/takeover");
        return takeover != null && archiveOwner != null && takingOver == null && !stopping && !bindingStopped;
    }
    private void requireInputsCurrent(StoreMetadata metadata) {
        if (takeover == null) inputs.requireCompatibilityCurrent(plan, metadata);
        else inputs.requireArchiveCurrent(plan, metadata, takeover);
    }
    synchronized void stopBinding(TargetSourceApplyRuntime actual) {
        if (runtime != actual) return;
        bindingStopped = true;
        if (preparation != null) preparation.requestStop();
        if (bridge != null) bridge.requestStop();
        if (archivePreparation != null) archivePreparation.requestStop();
        if (takingOver != null) takingOver.requestStop();
        if (legacyExit != null) legacyExit.requestStop();
    }
    synchronized boolean ioExited(TargetSourceApplyRuntime actual) {
        if (runtime != actual) return true;
        if (preparation != null) preparation.retryCleanup();
        if (bridge != null) bridge.retryCleanup();
        if (archivePreparation != null) archivePreparation.retryCleanup();
        if (takingOver != null) takingOver.retryCleanup();
        if (legacyExit != null) legacyExit.retryCleanup();
        return (preparation == null || preparation.operationExited() && preparation.ioExited())
                && (bridge == null || bridge.operationExited() && bridge.ioExited())
                && (archivePreparation == null || archivePreparation.operationExited() && archivePreparation.ioExited())
                && (takingOver == null || takingOver.operationExited() && takingOver.ioExited())
                && (legacyExit == null || legacyExit.operationExited() && legacyExit.ioExited());
    }
    synchronized Throwable failure(TargetSourceApplyRuntime actual) {
        if (runtime != actual) return null;
        for (var operation : new OwnedCheckpointIo<?>[] {preparation, bridge, archivePreparation, takingOver, legacyExit}) if (operation != null && operation.operationExited()) {
            try { operation.receipt().join(); } catch (RuntimeException failed) { return failed; }
        }
        return null;
    }
    synchronized void requestStop() {
        stopping = true;
        if (runtime != null) stopBinding(runtime);
    }
    synchronized boolean ioExited() { return runtime == null || ioExited(runtime); }

    /** Reuses original content protections while the live Source supplies its own new Owner admission. */
    private final class CompatibilityHeld implements TargetMigrationImporter.Held {
        @Override public void requireExternalCurrent(TargetMigrationPlan exact, StoreMetadata metadata) {
            if (!plan.equals(exact) || !Arrays.equals(metadata.storeIncarnation(), plan.newStore()))
                throw new IllegalStateException("compatibility import changed original plan/Store");
            requireInputsCurrent(metadata);
        }
        @Override public void requireLocalCurrent() { TargetMigrationCompatibilityRuntime.this.requireLocalCurrent(); }
        @Override public void requireRow(TargetMigrationPlan exact, TargetMigrationImporter.ImportedRow row) {
            inputs.requireRow(exact, row);
        }
        @Override public void requireGuard(TargetMigrationPlan exact, TargetMigrationCustodyGuard guard) {
            inputs.requireGuard(exact, guard);
        }
        @Override public void requireComplete(TargetMigrationPlan exact, TargetMigrationImporter.ImportSummary summary) {
            inputs.requireComplete(exact, summary);
        }
        @Override public void requireCandidate(TargetMigrationPlan exact, TargetMigrationObjectReference object, ShardStore.ReadView cut) {
            inputs.requireCandidate(exact, object, cut);
        }
        @Override public void requirePrefix(TargetMigrationPlan exact, SourceReplayEntry original, ShardStore after,
                java.util.List<TargetStoreBackend.Edit> projection) { inputs.requirePrefix(exact, original, after, projection); }
        @Override public void requireActivationAttempt(TargetMigrationPlan exact, PreparedControlOperation control, SystemMutation input) {
            inputs.requireActivationAttempt(exact, control, input);
        }
    }

    /** Only this original compatibility owner can select a previously un-applied old command for Native interpretation. */
    public static final class NativeOwnedCommand {
        private final TargetMigrationCompatibilityRuntime owner;
        private final SourceReplayRecord original;
        private NativeOwnedCommand(TargetMigrationCompatibilityRuntime owner, SourceReplayRecord original) {
            this.owner = owner; this.original = original;
        }
        public void requireCurrent(TargetStoreBackend.Reader reader, PreparedCommand command, SourcePosition at) {
            owner.requireLocalCurrent();
            if (owner.nativeCommand != this || owner.route != Route.NATIVE_OLD_COMMAND
                    || !Arrays.equals(command.canonicalFrameDigest(), original.command().canonicalFrameDigest())
                    || !Arrays.equals(at.canonicalBytes(), original.position().canonicalBytes())
                    || !Arrays.equals(reader.metadata().storeIncarnation(), owner.plan.newStore())
                    || (owner.takeover == null ? owner.shadow.store().get(ColumnFamily.DEDUPE, KeyCodec.dedupeCommand(command.commandId())) != null
                        : reader.get(ColumnFamily.META,TargetMigrationArchiveRow.keyFor(KeyCodec.dedupeCommand(command.commandId()))) != null))
                throw new IllegalStateException("Native legacy-command permit changed original frame/ownership/first absence");
            final var actual = TargetMigrationStore.requireActivated(reader, java.util.List.of(), owner.plan.scope(),
                    owner.plan.newLineage(), reader.aggregate().mutation());
            if (!actual.plan().equals(owner.plan)) throw new IllegalStateException("Native old command changed actual migration root");
            if (reader.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(command.delayMessageId())) != null
                    || !owner.converted.contains(command.delayMessageId())
                            && reader.get(ColumnFamily.ID, TargetKeyCodec.message(command.delayMessageId())) == null
                            && reader.get(ColumnFamily.META, Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG,TargetKeyCodec.KEY_FORMAT},command.delayMessageId().bytes())) == null)
                throw new IllegalStateException("command is not owned by the actual Native entity domain");
            owner.inputs.requireNativeOwnedContract(owner.plan, reader, command, at); owner.requireLocalCurrent();
        }
    }
}
