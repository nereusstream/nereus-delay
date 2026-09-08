package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetCommandReplayStore;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetQuotaGrantStore;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.runtime.TargetSystemReplayStore;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Active Target source execution over the existing bounded poll/apply/ACK loop. Grant controls are wired first. */
public final class TargetSourceApplyRuntime extends SourceApplyTarget {
    public record Limits(int records, long bytes, long elapsedNanos, int counters, int domains) {
        public Limits {
            if (records <= 0 || bytes <= 0 || elapsedNanos <= 0 || counters < 2 || domains < 1 || domains > 64) {
                throw new IllegalArgumentException("Target source runtime requires finite positive limits");
            }
        }
    }

    /** First-application authority only. Immutable duplicates do not resolve current registrations or capacity. */
    public record GrantControl(
            PreparedControlOperation prepared,
            TargetQuotaGrantControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public GrantControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface GrantControls {
        GrantControl resolve(SourceReplayMutation entry);
    }

    /** External resource/Control snapshots remain mandatory; local ownership checks cannot substitute for them. */
    public record Authorities(
            OxiaOwnerLeaseStore leases,
            SourceReplaySuccessor successor,
            GrantControls grants,
            TargetStoreBackend.CommitAuthority duplicateWrites,
            TargetStoreBackend.ReadAuthority reads) {
        public Authorities {
            Objects.requireNonNull(leases, "leases");
            Objects.requireNonNull(successor, "successor");
            Objects.requireNonNull(grants, "grants");
            Objects.requireNonNull(duplicateWrites, "duplicateWrites");
            Objects.requireNonNull(reads, "reads");
        }
    }

    private final ShardStore store;
    private final TargetStoreBackend backend;
    private final StoreMetadata metadata;
    private final TargetQuotaScope scope;
    private final TargetQuotaGrantStore grants;
    private final TargetSystemReplayStore replay;
    private final TargetCommandReplayStore commandReplay;
    private final SourceAssignment assignment;
    private final Authorities authorities;
    private final Limits limits;
    private final LongSupplier monotonicClock;
    private OwnerLease lease;
    private boolean fenced;
    private long lastOwnerTime = -1;

    /** Requires prior explicit activation, including persisted Owner epoch and the reached source barrier. */
    public TargetSourceApplyRuntime(
            final TargetStoreBootstrap.Initialized initialized,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock) {
        this.store = Objects.requireNonNull(store, "store");
        backend = Objects.requireNonNull(initialized, "initialized").backend();
        backend.requireStore(store);
        metadata = store.metadata();
        scope = initialized.root().scope();
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.authorities = Objects.requireNonNull(authorities, "authorities");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        if (!scope.shard().equals(lease.shardId())
                || !scope.shard().equals(assignment.shardId())
                || lease.context() == null
                || !Arrays.equals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()
                || lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()
                || !assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())) {
            throw new IllegalArgumentException("Target source runtime lacks exact active Owner/assignment/Store state");
        }
        final byte[] lineage = initialized.root().recoveryLineage();
        grants = new TargetQuotaGrantStore(backend, scope, lineage, limits.counters(), limits.domains());
        replay = new TargetSystemReplayStore(backend, scope, lineage, limits.counters(), limits.domains());
        commandReplay = new TargetCommandReplayStore(backend, scope, lineage, limits.counters(), limits.domains());
    }

    @Override
    void bind(final WorkClassExecutionRegistry registry) {
        store.sharedResources().bindWorkClassExecutionRegistry(registry);
    }

    @Override
    synchronized void requireSubmission(final SourceReplayEntry entry, final boolean recovery) {
        if (fenced || recovery) {
            throw new IllegalStateException("Target active source is fenced or was used as a recovery executor");
        }
        validateEntry(entry);
    }

    @Override
    synchronized SourceReplayOutcome apply(
            final SourceReplayEntry entry, final LongSupplier clock, final boolean recovery) {
        try {
            requireSubmission(entry, recovery);
            requireOwner(clock);
            if (entry instanceof SourceReplayRecord command) {
                return replayCommand(command, clock);
            }
            final var mutation = (SourceReplayMutation) entry;
            final var budget =
                    new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
            final TargetSystemReplayStore.Prepared duplicate;
            final TargetQuotaGrantStore.Prepared first;
            final GrantControl control;
            // Only backend preparation yields are retryable. External resolution and commit failures must fence.
            try {
                duplicate = replay.prepareIfPresent(budget, mutation.mutation(), entry.position())
                        .orElse(null);
            } catch (ReadIncompleteException incomplete) {
                throw new ReadYield(incomplete);
            }
            if (duplicate == null) {
                control = Objects.requireNonNull(authorities.grants().resolve(mutation), "grant control");
                try {
                    first = grants.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
            } else {
                control = null;
                first = null;
            }
            final SystemMutationResult result = duplicate == null
                    ? grants.commit(first, writes(control.commit(), entry, clock))
                    : replay.commit(
                            duplicate, writes(authorities.duplicateWrites(), entry, clock), reads(entry, clock));
            // Broker ACK carries this physical record's anchor, while the Store retains the first logical result.
            return SourceReplayOutcome.systemMutation(
                    entry.position(),
                    new SystemMutationResult(
                            result.mutationId(),
                            result.mutationHash(),
                            result.mutationType(),
                            result.retryUntilEpochMs(),
                            result.authorIdentity(),
                            result.applyStatus(),
                            result.stableCode(),
                            entry.position().canonicalBytes()));
        } catch (ReadYield incomplete) {
            throw incomplete;
        } catch (RuntimeException | Error failure) {
            fenced = true;
            throw failure;
        }
    }

    private SourceReplayOutcome replayCommand(final SourceReplayRecord entry, final LongSupplier clock) {
        final var budget =
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
        final TargetCommandReplayStore.Prepared prepared;
        try {
            prepared = commandReplay
                    .prepareIfPresent(budget, entry.command(), entry.position())
                    .orElseThrow(() ->
                            new IllegalStateException("first Target Command business is not wired yet; retain source"));
        } catch (ReadIncompleteException incomplete) {
            throw new ReadYield(incomplete);
        }
        final var result = commandReplay.commit(
                prepared, writes(authorities.duplicateWrites(), entry, clock), reads(entry, clock));
        return SourceReplayOutcome.command(
                entry.position(),
                new CommandResult(
                        result.applyStatus(),
                        result.stableCode(),
                        result.generation(),
                        result.stateVersion(),
                        result.messageStatus(),
                        entry.position().canonicalBytes()));
    }

    private static byte[] sourceDigest(final SourceReplayEntry entry) {
        return Bytes.sha256(
                entry instanceof SourceReplayRecord command
                        ? CommandCodec.encodeFrame(command.command())
                        : ((SourceReplayMutation) entry).mutation().canonicalEnvelope());
    }

    private TargetStoreBackend.CommitAuthority writes(
            final TargetStoreBackend.CommitAuthority external,
            final SourceReplayEntry entry,
            final LongSupplier clock) {
        return (actual, actualScope, mutation) -> {
            final var stamp = mutation.quota().counters().mutation();
            if (stamp.localClaimOrdinal() != 0
                    || !Arrays.equals(
                            stamp.source().canonicalBytes(), entry.position().canonicalBytes())
                    || !Arrays.equals(stamp.mutationDigest(), sourceDigest(entry))) {
                throw new IllegalStateException("Target source commit changed its exact source/envelope stamp");
            }
            requireOwner(clock);
            return guard(external.acquire(actual, actualScope, mutation), actual, actualScope, entry, clock);
        };
    }

    private TargetStoreBackend.ReadAuthority reads(final SourceReplayEntry entry, final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireOwner(clock);
            return guard(authorities.reads().acquire(actual, actualScope), actual, actualScope, entry, clock);
        };
    }

    private TargetStoreBackend.CommitGuard guard(
            final TargetStoreBackend.CommitGuard external,
            final StoreMetadata actual,
            final TargetQuotaScope actualScope,
            final SourceReplayEntry entry,
            final LongSupplier clock) {
        Objects.requireNonNull(external, "external guard");
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                    throw new IllegalStateException("Target source guard belongs to another Store/scope");
                }
                validateEntry(entry);
                final SourcePosition prior =
                        Objects.requireNonNull(store.appliedShardLogPosition(), "initialized source");
                authorities.successor().validate(prior, entry.position());
                external.requireCurrent();
                requireOwner(clock);
            }

            @Override
            public void close() {
                external.close();
            }
        };
    }

    @Override
    synchronized void beforeAcknowledgement(
            final SourceReplayEntry entry, final SourceReplayOutcome outcome, final LongSupplier clock) {
        try (var guard = reads(entry, clock).acquire(store.metadata(), scope)) {
            guard.requireCurrent();
            if (!Arrays.equals(
                    store.appliedShardLogPosition().canonicalBytes(),
                    entry.position().canonicalBytes())) {
                throw new IllegalStateException("Target ACK no longer names the current durable source");
            }
        } catch (RuntimeException | Error failure) {
            fenced = true;
            throw failure;
        }
    }

    private void requireOwner(final LongSupplier clock) {
        if (fenced || store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()) {
            throw new IllegalStateException("Target source Owner/Store is fenced");
        }
        final OwnerLease current = authorities.leases().current(scope.shard()).orElse(null);
        final long now = Objects.requireNonNull(clock, "ownerClock").getAsLong();
        if (now < 0
                || now < lastOwnerTime
                || current == null
                || !lease.sameIdentity(current)
                || current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || !current.validAt(now)
                || current.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
            throw new IllegalStateException("Target source authoritative lease/time changed");
        }
        lastOwnerTime = now;
        lease = current;
    }

    private void validateEntry(final SourceReplayEntry entry) {
        Objects.requireNonNull(entry, "entry");
        assignment.activationBarrier().validatePosition(entry.position());
        if (entry.position() instanceof PulsarSourcePosition) {
            if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                    || entry.sourceConnectionGeneration() == null
                    || entry.guardAttestationDigest() == null) {
                throw new IllegalArgumentException("Target source lacks a Pulsar connection proof");
            }
            barrier.validateSourceConnection(entry.sourceConnectionGeneration(), entry.guardAttestationDigest());
        } else if (entry.sourceConnectionGeneration() != null || entry.guardAttestationDigest() != null) {
            throw new IllegalArgumentException("Kafka source cannot carry Pulsar connection proof");
        }
        if (entry instanceof SourceReplayRecord) {
            return;
        }
        if (!(entry instanceof SourceReplayMutation mutation)
                || mutation.mutation().type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException(
                    "Target source business branch is not wired yet; source must remain pending");
        }
        TargetQuotaGrantControlBody.decode(mutation.mutation().canonicalBody());
    }

    @Override
    public synchronized void fence() {
        fenced = true;
    }

    public synchronized boolean fenced() {
        return fenced;
    }

    @Override
    boolean isReadIncomplete(final Throwable failure) {
        return failure instanceof ReadYield;
    }

    private static final class ReadYield extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ReadYield(final ReadIncompleteException cause) {
            super("Target source preparation yielded before commit", cause);
        }
    }
}
