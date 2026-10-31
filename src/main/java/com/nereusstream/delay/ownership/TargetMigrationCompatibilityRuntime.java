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
    enum Route { TARGET, NATIVE_OLD_COMMAND, LEGACY_FIRST, LEGACY }
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

    TargetMigrationCompatibilityRuntime(TargetWorkerProcessRuntime process, TargetMigrationProcessDriver root,
            TargetWorkerProcessRuntime.Assembly graph, TargetMigrationConfiguration configuration,
            TargetMigrationInputs inputs, TargetMigrationPlan plan, LegacyMigrationBundle.Sealed bundle,
            LegacyMigrationBundle.Material material, ShardStore.LegacyCheckpointReplayCopy shadow, DelayShard old,
            TargetMigrationObjectReference sealedCandidate, LongSupplier nanos) {
        this.process = process; this.root = root; this.graph = graph; this.configuration = configuration;
        this.inputs = inputs; this.plan = plan; this.bundle = bundle; this.shadow = shadow; this.old = old;
        this.sealedCandidate = sealedCandidate; this.nanos = nanos;
        for (var entity : material.entities()) if (entity.disposition() == LegacyMigrationBundle.Disposition.CONVERTED)
            converted.add(entity.messageId());
    }

    synchronized void bind(TargetSourceApplyRuntime actual, ShardStore actualStore) {
        if (stopping || runtime != null && runtime != actual && !ioExited(runtime))
            throw new IllegalStateException("previous original migration compatibility binding has not exited");
        if (runtime == actual) return;
        inputs.requireCompatibilityCurrent(plan, actualStore.metadata());
        if (!Arrays.equals(actualStore.metadata().storeIncarnation(), plan.newStore())
                || !Arrays.equals(actualStore.metadata().dbIdentity(), plan.newDbIdentity()))
            throw new IllegalArgumentException("compatibility runtime changed actual new Store identity");
        target = actualStore;
        importer = TargetMigrationImporter.resume(target, plan, inputs.importLimits(), new CompatibilityHeld(), configuration.newReadBudget(nanos));
        candidate = importer.resumeSealed(sealedCandidate, configuration.newReadBudget(nanos));
        runtime = Objects.requireNonNull(actual); bindingStopped = false;
        selected = null; route = null; nativeCommand = null; preparation = null; bridge = null; prepared = null; closedEntry = false;
    }

    synchronized Route route(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireBinding(actual); requireCurrent();
        if (selected != null) {
            if (SourceApplyCoordinator.sameEntry(selected, entry)) return route;
            if (!closedEntry) throw new IllegalStateException("migration compatibility retains another original Source entry");
            selected = null; route = null; nativeCommand = null; prepared = null; preparation = null; bridge = null;
            closedEntry = false;
        }
        final var domain = Objects.requireNonNull(inputs.classifySource(entry, plan), "original Source writer domain");
        if (domain == TargetMigrationInputs.SourceDomain.UNKNOWN)
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original producer/writer domain is unknown; retain Source and custody");
        selected = entry;
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
            return converted.contains(id) || target.get(ColumnFamily.ID, TargetKeyCodec.message(id)) != null;
        }).value();
    }

    synchronized NativeOwnedCommand nativeCommand(TargetSourceApplyRuntime actual, SourceReplayRecord entry) {
        return route(actual, entry) == Route.NATIVE_OLD_COMMAND ? nativeCommand : null;
    }

    /** Old-domain I/O runs on the original finite pool; the Source slot remains owned and unacknowledged. */
    synchronized boolean prepare(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        final var selectedRoute = route(actual, entry);
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
        if (stopping || bindingStopped) throw new IllegalStateException("original migration compatibility owner stopped");
        process.requireMigrationCompatibility(root, runtime, target);
        if (runtime != null) runtime.requireMigrationLocalCurrent();
        inputs.requireCompatibilityCurrent(plan, target.metadata());
    }
    synchronized void stopBinding(TargetSourceApplyRuntime actual) {
        if (runtime != actual) return;
        bindingStopped = true;
        if (preparation != null) preparation.requestStop();
        if (bridge != null) bridge.requestStop();
    }
    synchronized boolean ioExited(TargetSourceApplyRuntime actual) {
        if (runtime != actual) return true;
        if (preparation != null) preparation.retryCleanup();
        if (bridge != null) bridge.retryCleanup();
        return (preparation == null || preparation.operationExited() && preparation.ioExited())
                && (bridge == null || bridge.operationExited() && bridge.ioExited());
    }
    synchronized Throwable failure(TargetSourceApplyRuntime actual) {
        if (runtime != actual) return null;
        for (var operation : new OwnedCheckpointIo<?>[] {preparation, bridge}) if (operation != null && operation.operationExited()) {
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
            inputs.requireCompatibilityCurrent(plan, metadata);
        }
        @Override public void requireLocalCurrent() { inputs.requireCompatibilityCurrent(plan, target.metadata()); }
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
            owner.requireCurrent();
            if (owner.nativeCommand != this || owner.route != Route.NATIVE_OLD_COMMAND
                    || !Arrays.equals(command.canonicalFrameDigest(), original.command().canonicalFrameDigest())
                    || !Arrays.equals(at.canonicalBytes(), original.position().canonicalBytes())
                    || !Arrays.equals(reader.metadata().storeIncarnation(), owner.plan.newStore())
                    || owner.shadow.store().get(ColumnFamily.DEDUPE, KeyCodec.dedupeCommand(command.commandId())) != null)
                throw new IllegalStateException("Native legacy-command permit changed original frame/ownership/first absence");
            final var actual = TargetMigrationStore.requireActivated(reader, java.util.List.of(), owner.plan.scope(),
                    owner.plan.newLineage(), reader.aggregate().mutation());
            if (!actual.plan().equals(owner.plan)) throw new IllegalStateException("Native old command changed actual migration root");
            if (reader.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(command.delayMessageId())) != null
                    || !owner.converted.contains(command.delayMessageId())
                            && reader.get(ColumnFamily.ID, TargetKeyCodec.message(command.delayMessageId())) == null)
                throw new IllegalStateException("command is not owned by the actual Native entity domain");
            owner.inputs.requireNativeOwnedContract(owner.plan, reader, command, at); owner.requireCurrent();
        }
    }
}
