package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.RecoveryInstallPhase;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.runtime.HeadReadIncompleteException;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.RecoveryCatalogAuthority;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Protected bounded Source recovery of an already authenticated Target checkpoint/local recovery Store.
 * The caller owns the accepted no-ACK Source cursor and all Catalog/Pin, original image/semantic/evidence,
 * assignment, physical and session protections. This class neither creates nor releases those authorities.
 */
public final class TargetOwnerRecoveryCoordinator {
    private final ShardStore store;
    private final TargetStoreBootstrap.Reopened initial;
    private final SourceAssignment assignment;
    private final TargetSourceApplyRuntime.Authorities authorities;
    private final TargetSourceApplyRuntime.Limits limits;
    private final SourceReplayCursor<? extends SourceReplayEntry> source;
    private final RecoveryCatalogAuthority catalog;
    private final RecoveryPin pin;
    private final CheckpointManifest manifest;
    private final Runnable protection;
    private final ReplayTurnBudget turnBudget;
    private final WorkClassExecutionRegistry workClasses;
    private final LongSupplier clock;
    private final LongSupplier monotonic;
    private final Supplier<BoundedReadBudget> auditBudgets;
    private final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits;
    private OwnerLease lease;
    private TargetSourceApplyRuntime replay;
    private SourceApplyWorkClassExecutor sourceApply;
    private SourceReplayEntry pendingEntry;
    private SourceApplyWorkClassExecutor.Submission pending;
    private boolean complete;
    private int turnNumber;
    private long lastNow = -1;

    /** Protection rechecks held snapshots at native guards; provider acquisition/full reads remain outside monitors. */
    public TargetOwnerRecoveryCoordinator(ShardStore store, TargetStoreBootstrap.Reopened reopened,
            SourceAssignment assignment, OwnerLease lease, TargetSourceApplyRuntime.Authorities authorities,
            TargetSourceApplyRuntime.Limits limits, SourceReplayCursor<? extends SourceReplayEntry> source,
            RecoveryCatalogAuthority catalog, RecoveryPin pin, Runnable protection, ReplayTurnBudget turnBudget,
            WorkClassExecutionRegistry workClasses, LongSupplier clock, LongSupplier monotonic,
            Supplier<BoundedReadBudget> auditBudgets, TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        this.store = Objects.requireNonNull(store, "store");
        initial = Objects.requireNonNull(reopened, "reopened");
        initial.backend().requireStore(store);
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.authorities = Objects.requireNonNull(authorities, "authorities");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.source = Objects.requireNonNull(source, "source");
        this.source.requireGuarded();
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.pin = Objects.requireNonNull(pin, "pin");
        final var heldProtection = Objects.requireNonNull(protection, "protection");
        this.protection = () -> {
            this.source.requireCurrent();
            heldProtection.run();
            this.source.requireCurrent();
        };
        this.turnBudget = Objects.requireNonNull(turnBudget, "turnBudget");
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.monotonic = Objects.requireNonNull(monotonic, "monotonic");
        this.auditBudgets = Objects.requireNonNull(auditBudgets, "auditBudgets");
        this.ledgerLimits = Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        manifest = catalog.manifest(pin.candidate().checkpointId())
                .orElseThrow(() -> new IllegalStateException("Target recovery candidate is not published"));
        final var base = store.recoveryMetadata().lineageBase();
        if (store.metadata().storeFormatVersion() != 2 || base == null
                || store.recoveryMetadata().installState() == null
                || !store.shardId().equals(assignment.shardId()) || !store.shardId().equals(lease.shardId())
                || lease.context() == null || !Arrays.equals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()
                || pin.owner().ownerEpoch() != lease.ownerEpoch()
                || !pin.shard().shardId().equals(store.shardId())
                || !Arrays.equals(base.storeIncarnation(), store.metadata().storeIncarnation())
                || !Arrays.equals(base.checkpointId(), pin.candidate().checkpointId())
                || !Arrays.equals(base.manifestSha256(), pin.candidate().manifestSha256())
                || !Arrays.equals(base.recoveryLineageId(), initial.root().recoveryLineage())
                || !Arrays.equals(base.recoveryLineageId(), pin.candidate().recoveryLineageId())
                || !Arrays.equals(manifest.manifestSha256(), pin.candidate().manifestSha256())) {
            throw new IllegalArgumentException("Target recovery lacks exact Store/base/assignment/Owner/Pin identity");
        }
        assignment.activationBarrier().validatePosition(store.appliedShardLogPosition());
    }

    /** One retained Source action at a time; reaching the authenticated barrier, not cursor exhaustion, completes. */
    public synchronized OwnerRecoveryTurn runTurn() {
        final var outcomes = new ArrayList<SourceReplayOutcome>();
        final int turn = Math.incrementExact(turnNumber);
        final long started = monotonic.getAsLong();
        long bytes = 0;
        try {
            if (complete) {
                requireLease(true);
                return new OwnerRecoveryTurn(List.of(), true, turnNumber);
            }
            requireCatalog();
            while (true) {
                if (pending == null && assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())) {
                    finish();
                    complete = true;
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, true, turn);
                }
                beginReplay();
                replay.requireRecoveryOwner(clock);
                if (outcomes.size() >= turnBudget.maxRecords() || bytes >= turnBudget.maxCanonicalBytes()
                        || elapsed(started) >= turnBudget.maxElapsedNanos()) {
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn);
                }
                if (pendingEntry == null) {
                    if (!source.hasNext()) {
                        throw new SourceReplayGapException("Target Source ended before its barrier");
                    }
                    pendingEntry = Objects.requireNonNull(source.peek(), "source entry");
                } else if (!sameEntry(pendingEntry, source.peek())) {
                    throw new IllegalStateException("Target recovery retained Source entry changed");
                }
                final long charged = Math.addExact(pendingEntry.position().canonicalBytes().length,
                        frame(pendingEntry).length);
                if (charged > turnBudget.maxCanonicalBytes()) {
                    throw new IllegalArgumentException("Target recovery entry exceeds its finite byte budget");
                }
                if (charged > turnBudget.maxCanonicalBytes() - bytes) {
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn);
                }
                if (pending == null) { pending = sourceApply.submitRecovery(pendingEntry, clock); }
                Throwable dispatchFailure = null;
                try {
                    workClasses.runTurn(new SchedulerBudget(1, pending.task().bytes(),
                            Math.max(1, turnBudget.maxElapsedNanos() - elapsed(started))));
                } catch (RuntimeException | Error failure) {
                    if (pending.outcome().isEmpty()) { throw failure; }
                    dispatchFailure = failure;
                }
                final var applied = pending.outcome().orElse(null);
                if (applied == null) {
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn, true, pending.task());
                }
                if (applied.failure() instanceof HeadReadIncompleteException incomplete) {
                    pending = null;
                    if (dispatchFailure != null) { throwUnchecked(dispatchFailure); }
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn, false, null, incomplete.reason());
                }
                if (applied.failure() != null) { throwUnchecked(applied.failure()); }
                if (!sameEntry(pendingEntry, source.peek())) {
                    throw new IllegalStateException("Target recovery Source changed before cursor advance");
                }
                source.next();
                outcomes.add(Objects.requireNonNull(applied.result(), "applied result"));
                pendingEntry = null;
                pending = null;
                bytes = Math.addExact(bytes, charged);
                if (dispatchFailure != null) { throwUnchecked(dispatchFailure); }
            }
        } catch (ReadIncompleteException incomplete) {
            turnNumber = turn;
            return new OwnerRecoveryTurn(outcomes, false, turn, false, null, incomplete.reason());
        } catch (RuntimeException | Error failure) {
            if (replay != null) { replay.fence(); }
            throw failure;
        }
    }

    private void beginReplay() {
        if (replay != null) { return; }
        final var current = requireLease(false);
        if (current.state() != ShardLifecycleState.CATCHING_UP) {
            lease = authorities.leases().transitionOrRead(current, ShardLifecycleState.CATCHING_UP)
                    .orElseThrow(() -> new IllegalStateException("Target recovery CATCHING_UP CAS was not observed"));
        }
        requireLease(false);
        if (Long.compareUnsigned(store.runtimeMetadata().lastOpenedOwnerEpoch(), lease.ownerEpoch()) > 0) {
            throw new IllegalStateException("Target recovery Store already has a newer Owner epoch");
        }
        protection.run();
        if (store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()) {
            store.recordOpenedOwnerEpoch(lease.ownerEpoch());
        }
        requireLease(false);
        replay = TargetSourceApplyRuntime.recovery(initial, store, assignment, lease, authorities, limits,
                monotonic, protection);
        sourceApply = new SourceApplyWorkClassExecutor(workClasses, replay);
    }

    private void finish() {
        final var current = requireLease(true);
        if (current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) { beginReplay(); }
        requireCatalog();
        final var audited = TargetStoreBootstrap.reopen(store, initial.root().scope(),
                new TargetStoreBackend.WriteLimits(limits.records(), limits.bytes()),
                Objects.requireNonNull(auditBudgets.get(), "completion audit budget"), (metadata, scope) -> heldGuard(),
                ledgerLimits);
        if (store.recoveryMetadata().installState().phase() == RecoveryInstallPhase.INSTALLED) {
            try (var guard = heldGuard()) {
                store.completeTargetRecovery(audited.view(), store.recoveryMetadata().lineageBase(), guard);
            }
        }
        requireCatalog();
        lease = TargetWorkerOwnerActivation.activate(audited, store, assignment, lease, authorities.leases(), clock);
        requireLease(true);
        if (lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Target recovery activation did not retain ACTIVE Owner");
        }
        if (replay != null) { replay.fence(); }
    }

    private TargetStoreBackend.CommitGuard heldGuard() {
        protection.run();
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                protection.run();
                requireLease(true);
                if (!assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())) {
                    throw new IllegalStateException("Target recovery completion lost its Source barrier");
                }
            }

            @Override
            public void close() {}
        };
    }

    private OwnerLease requireLease(boolean allowActive) {
        protection.run();
        final long now = clock.getAsLong();
        final var current = authorities.leases().current(store.shardId())
                .orElseThrow(() -> new IllegalStateException("Target recovery Owner disappeared"));
        if (now < 0 || now < lastNow || !lease.sameIdentity(current) || !current.validAt(now)
                || current.state() != ShardLifecycleState.ACQUIRING && current.state() != ShardLifecycleState.RESTORING
                        && current.state() != ShardLifecycleState.CATCHING_UP
                        && !(allowActive && current.state() == ShardLifecycleState.ACTIVE_FOR_COMMANDS)) {
            throw new IllegalStateException("Target recovery Owner/session/time changed");
        }
        lastNow = now;
        lease = current;
        return current;
    }

    private void requireCatalog() {
        protection.run();
        catalog.validatePublishedRestoreCandidate(manifest);
        if (!catalog.activeRecoveryPin()
                .orElseThrow(() -> new IllegalStateException("Target recovery Pin is no longer active")).equals(pin)) {
            throw new IllegalStateException("Target recovery exact RecoveryPin changed");
        }
        protection.run();
    }

    private long elapsed(long started) {
        final long now = monotonic.getAsLong();
        if (now < started) { throw new IllegalStateException("Target recovery monotonic clock regressed"); }
        return Math.subtractExact(now, started);
    }

    public synchronized OwnerLease activeLease() {
        if (!complete) { throw new IllegalStateException("Target recovery has not completed"); }
        return requireLease(true);
    }

    private static boolean sameEntry(SourceReplayEntry first, SourceReplayEntry second) {
        return second != null && Arrays.equals(first.position().canonicalBytes(), second.position().canonicalBytes())
                && Arrays.equals(frame(first), frame(second))
                && Arrays.equals(first.guardAttestationDigest(), second.guardAttestationDigest())
                && Objects.equals(first.sourceConnectionGeneration(), second.sourceConnectionGeneration());
    }

    private static byte[] frame(SourceReplayEntry entry) {
        return entry instanceof SourceReplayRecord command ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException runtime) { throw runtime; }
        if (failure instanceof Error error) { throw error; }
        throw new IllegalStateException("Target recovery failed", failure);
    }
}
