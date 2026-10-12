package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetClaimStore;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Startup-only one-Claim turns after authenticated Source barrier replay, before active Worker exposure. */
final class TargetClaimTakeoverCoordinator {
    private final ShardStore store;
    private final TargetStoreBootstrap.Reopened initial;
    private final SourceAssignment assignment;
    private final OwnerIdentity owner;
    private OwnerLease lease;
    private final byte[] storeIncarnation;
    private final OxiaOwnerLeaseStore leases;
    private final Runnable protection;
    private final PulsarAttemptJournal journal;
    private final Runnable journalProtection;
    private final Supplier<BoundedReadBudget> reads;
    private final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits;
    private final TargetSourceApplyRuntime.Limits limits;
    private final LongSupplier clock;
    private final TargetClaimStore claims;
    private TargetClaimStore.TakeoverCandidate pending;
    private byte[] after;
    private TargetStoreBootstrap.Reopened completed;
    private long lastNow = -1;
    private long expectedNativeSequence;

    TargetClaimTakeoverCoordinator(ShardStore store, TargetStoreBootstrap.Reopened audited,
            SourceAssignment assignment, OwnerIdentity owner, OwnerLease lease, OxiaOwnerLeaseStore leases,
            Runnable protection, PulsarAttemptJournal journal, Runnable journalProtection,
            Supplier<BoundedReadBudget> reads, TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            TargetSourceApplyRuntime.Limits limits, LongSupplier clock) {
        this.store = Objects.requireNonNull(store, "store");
        storeIncarnation = store.metadata().storeIncarnation();
        initial = Objects.requireNonNull(audited, "audited");
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.owner = owner;
        this.lease = Objects.requireNonNull(lease, "lease");
        this.leases = Objects.requireNonNull(leases, "leases");
        this.protection = Objects.requireNonNull(protection, "Source/fencing/lineage protection");
        this.journal = journal;
        this.journalProtection = journalProtection;
        this.reads = Objects.requireNonNull(reads, "reads");
        this.ledgerLimits = Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.clock = Objects.requireNonNull(clock, "clock");
        initial.backend().requireStore(store);
        store.withReadView(initial.view(), () -> null);
        expectedNativeSequence = initial.backend().guardedRead(readBudget(), reader -> reader.nativeSequence(),
                (metadata, scope) -> heldGuard());
        final TargetQuotaDelta.LocalClaimAuthority quota = (kind, delta) -> {
            if (kind != TargetQuotaDelta.LocalClaimKind.REVOKE) {
                throw new IllegalStateException("takeover authority cannot create Claims");
            }
            requireHeld();
        };
        claims = new TargetClaimStore(initial.backend(), initial.root().scope(), initial.root().recoveryLineage(),
                limits.domains(), quota);
    }

    /** False includes a yielded/unfinished scan; only the guarded range end and final audit complete startup. */
    synchronized boolean runTurn() {
        if (completed != null) { requireExternal(); return true; }
        requireExternal();
        try {
            if (pending == null) {
                pending = claims.nextTakeoverCandidate(readBudget(), after, (metadata, scope) -> heldGuard()).orElse(null);
                if (pending == null) {
                    if (store.latestSequenceNumber() != expectedNativeSequence) {
                        throw new IllegalStateException("takeover range end changed its protected Store cut");
                    }
                    completed = after == null ? store.withReadView(initial.view(), () -> initial)
                            : TargetStoreBootstrap.reopen(store, initial.root().scope(),
                                    new TargetStoreBackend.WriteLimits(limits.records(), limits.bytes()), readBudget(),
                                    (metadata, scope) -> heldGuard(), ledgerLimits);
                    requireExternal();
                    return true;
                }
            }
            if (owner == null || owner.ownerEpoch() != lease.ownerEpoch()
                    || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), lease.leaseToken())) {
                throw new IllegalStateException("unadmitted Claims require configured authenticated recovery Owner identity");
            }
            final var candidate = pending;
            final var claim = candidate.claim();
            if (candidate.nativeSequence() != expectedNativeSequence) {
                throw new IllegalStateException("takeover scan Store changed outside its protected local batches");
            }
            final PulsarAttemptJournal.TargetAttemptAbsence absence;
            if (candidate.physical().resource().kind() == BrokerResourceIdentity.Kind.PULSAR) {
                Objects.requireNonNull(journalProtection, "retained Journal protection").run();
                final byte[] attempt = SystemMutation.computePublishAttemptLogicalIdentity(claim.claimId(),
                        claim.work().locator().messageId(), Integer.toUnsignedLong(claim.work().locator().generation()),
                        Integer.toUnsignedLong(claim.work().candidateAttemptNo()));
                absence = Objects.requireNonNull(journal, "fully replayed Target Journal")
                        .readTargetAttemptAbsence(store.shardId(), attempt);
                journalProtection.run();
            } else { absence = null; }
            final byte[] digest = candidate.recoveryDigest(owner, initial.root().recoveryLineage(),
                    Bytes.concat(assignment.canonicalBytes(), Bytes.utf8(lease.ownerId()), lease.sessionIdentity()));
            synchronized (store) {
                requireHeld();
                if (store.latestSequenceNumber() != expectedNativeSequence) {
                    throw new IllegalStateException("takeover prepared input Store cut changed");
                }
                final var prepared = claims.prepareTakeoverRevoke(readBudget(), claim, owner, digest,
                        (reader, actualClaim, charge, message, operation) -> {
                            requireHeld();
                            if (reader.nativeSequence() != expectedNativeSequence
                                    || !Arrays.equals(reader.metadata().storeIncarnation(), store.metadata().storeIncarnation())
                                    || !Arrays.equals(charge.recoveryLineage(), initial.root().recoveryLineage())
                                    || !assignment.activationBarrier().reachedBy(reader.source())) {
                                throw new IllegalStateException("takeover Claim lost its exact Store/source/lineage cut");
                            }
                            final var base = store.recoveryMetadata().lineageBase();
                            if (!Arrays.equals(claim.storeIncarnation(), reader.metadata().storeIncarnation())
                                    && (base == null || !Arrays.equals(base.recoveryLineageId(), charge.recoveryLineage()))) {
                                throw new IllegalStateException("cross-Store Claim lacks authenticated installed lineage");
                            }
                            if (!Arrays.equals(actualClaim.canonicalBytes(), candidate.claim().canonicalBytes())
                                    || !Arrays.equals(charge.canonicalBytes(), candidate.charge().canonicalBytes())
                                    || !Arrays.equals(message.canonicalBytes(), candidate.message().canonicalBytes())) {
                                throw new IllegalStateException("takeover changed its complete Claim/charge/Message before facts");
                            }
                            if (absence != null) { absence.requireCurrent(); }
                            operation.requireStoreSuccessorOf(charge.creation());
                        });
                claims.commit(prepared, (metadata, scope, mutation) -> new TargetStoreBackend.CommitGuard() {
                    @Override public void requireCurrent() {
                        requireHeld();
                        if (absence != null) { absence.requireCurrent(); }
                    }
                    @Override public void close() {}
                });
                expectedNativeSequence = store.latestSequenceNumber();
                after = claim.key();
                pending = null;
            }
            return false;
        } catch (ReadIncompleteException incomplete) {
            return false;
        }
    }

    private void requireExternal() {
        protection.run();
        if (journalProtection != null) { journalProtection.run(); }
        final long now = clock.getAsLong();
        final var current = leases.current(store.shardId())
                .orElseThrow(() -> new IllegalStateException("takeover Owner disappeared"));
        if (now < 0 || now < lastNow || !lease.sameIdentity(current) || !current.validAt(now)
                || current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("takeover Owner/session/time changed");
        }
        lastNow = now;
        lease = current;
        requireHeld();
    }

    /** Called under Store monitors; the held providers must expose current fencing snapshots without I/O. */
    private void requireHeld() {
        protection.run();
        if (journalProtection != null) { journalProtection.run(); }
        if (store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()
                || !Arrays.equals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()
                || !assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())
                || !Arrays.equals(store.metadata().storeIncarnation(), storeIncarnation)) {
            throw new IllegalStateException("takeover lacks active persisted Owner/Store/Source barrier");
        }
        final long now = clock.getAsLong();
        if (now < lastNow || !lease.validAt(now)) { throw new IllegalStateException("takeover lease expired/regressed"); }
    }

    private TargetStoreBackend.CommitGuard heldGuard() {
        requireHeld();
        return new TargetStoreBackend.CommitGuard() {
            @Override public void requireCurrent() { requireHeld(); }
            @Override public void close() {}
        };
    }

    synchronized TargetStoreBootstrap.Reopened completedRoot() {
        if (completed == null) { throw new IllegalStateException("Claim takeover has not completed its audit"); }
        requireExternal();
        store.withReadView(completed.view(), () -> null);
        return completed;
    }

    private BoundedReadBudget readBudget() { return Objects.requireNonNull(reads.get(), "recovery read budget"); }
}
