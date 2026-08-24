package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ActiveLaneStateV1;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ClaimMaterializationV1;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.OwnerIdentityV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import com.nereusstream.delay.protocol.ResourceRetireIntentBody;
import com.nereusstream.delay.protocol.SourceActivationBarrier;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationBodyCodec;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.PersistentLaneScheduler;
import com.nereusstream.delay.scheduler.ScheduleWorkItem;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Fenced owner view; lease loss closes all new local authority gates. */
public final class OwnedDelayShard {
    private final DelayShard delegate;
    private final OwnerIdentityV1 ownerIdentity;
    private OwnerLease lease;
    private ShardLifecycleState state;
    private SourceActivationBarrier activationBarrier;
    private SourceAssignment sourceAssignment;
    /**
     * Authority bound by the strict catch-up entrypoint.  Compatibility
     * assignment-only paths intentionally leave this unset; production
     * replay must reread the same Oxia lease before each bounded turn and
     * record so a local clock cannot outlive an ownership change.
     */
    private OxiaOwnerLeaseStore replayAuthority;

    private SourceReplaySuccessor replaySuccessor = SourceReplaySuccessor.monotonic();
    private SourcePosition lastCatchupPosition;
    private ShardFailureReason failureReason = ShardFailureReason.NONE;
    /**
     * Guards the complete local owner-drain attempt for this shard.  The
     * Worker-level drain semaphore limits aggregate concurrency, but it cannot
     * distinguish two coordinators accidentally targeting the same shard.
     */
    private boolean drainAttemptInProgress;

    OwnedDelayShard(final DelayShard delegate, final OwnerLease lease) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.ownerIdentity = null;
        validateShardIdentity(delegate, lease);
        this.state = ShardLifecycleState.RESTORING;
    }

    /** Binds the complete protocol Owner identity used by new live Owner-authored actions. */
    public OwnedDelayShard(final DelayShard delegate, final OwnerLease lease, final OwnerIdentityV1 ownerIdentity) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.ownerIdentity = Objects.requireNonNull(ownerIdentity, "ownerIdentity");
        validateShardIdentity(delegate, lease);
        if (ownerIdentity.ownerEpoch() != lease.ownerEpoch()) {
            throw new IllegalArgumentException("protocol Owner epoch does not match lease");
        }
        this.state = ShardLifecycleState.RESTORING;
    }

    private static void validateShardIdentity(final DelayShard delegate, final OwnerLease lease) {
        if (!delegate.shardId().equals(lease.shardId())) {
            throw new IllegalArgumentException("owned shard and lease identity mismatch");
        }
    }

    synchronized CommandResult apply(
            final PreparedCommand command, final SourcePosition position, final long nowEpochMs) {
        return apply(command, position, nowEpochMs, null, null);
    }

    /**
     * Applies a record from a guarded Pulsar source connection.  The
     * connection proof is required for Pulsar because a replacement consumer
     * can otherwise emit a position with the same physical topic identity.
     * Kafka has no connection-generation field and passes {@code null} proof.
     */
    synchronized CommandResult apply(
            final PreparedCommand command,
            final SourcePosition position,
            final long nowEpochMs,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        ensureActive(nowEpochMs);
        if (activationBarrier != null) {
            activationBarrier.validatePosition(position);
            validateSourceConnection(position, sourceConnectionGeneration, guardAttestationDigest);
        }
        try {
            return delegate.apply(command, position);
        } catch (ShardStore.RocksDbWriteFailure failure) {
            // A native batch failure can leave commit status unknown.  Close
            // the owner gate immediately; source replay must retain the
            // physical record until a fresh Store incarnation is opened.
            state = ShardLifecycleState.FENCED;
            throw failure;
        } catch (RuntimeException | Error failure) {
            // Any unexpected runtime/native failure leaves the local
            // projection or its commit boundary unproven. Close the owner
            // gate before rethrowing so a caller cannot continue from an
            // uncertain image. Deterministic command rejections are returned
            // by DelayShard as CommandResult and do not reach this branch.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /**
     * Applies one command only after rereading the authoritative Oxia lease.
     * The local-only overload remains useful for the embedded conformance
     * service; source writers with a live Owner Lease should use this boundary
     * so a same-process stale lease cannot outlive an Oxia owner change.
     */
    synchronized CommandResult applyAuthoritatively(
            final OxiaOwnerLeaseStore authority,
            final PreparedCommand command,
            final SourcePosition position,
            final long nowEpochMs) {
        return applyAuthoritatively(authority, command, position, nowEpochMs, null, null);
    }

    /**
     * Strict V1 command mutation entrypoint.  The ordinary authoritative
     * overload remains an embedded compatibility seam; production source
     * writers must prove that the active lease was established by the
     * context-bound catch-up path before applying a command.
     */
    synchronized CommandResult applyAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final PreparedCommand command,
            final SourcePosition position,
            final long nowEpochMs) {
        return applyAuthoritativelyStrict(authority, command, position, nowEpochMs, null, null);
    }

    /** Applies a command with the strict assignment/session-bound authority fence. */
    synchronized CommandResult applyAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final PreparedCommand command,
            final SourcePosition position,
            final long nowEpochMs,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        requireStrictActiveAuthority(authority);
        return applyAuthoritatively(
                authority, command, position, nowEpochMs, sourceConnectionGeneration, guardAttestationDigest);
    }

    /** Applies one guarded command after an authoritative lease reread. */
    synchronized CommandResult applyAuthoritatively(
            final OxiaOwnerLeaseStore authority,
            final PreparedCommand command,
            final SourcePosition position,
            final long nowEpochMs,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        ensureAuthoritativeActive(authority, nowEpochMs);
        return apply(command, position, nowEpochMs, sourceConnectionGeneration, guardAttestationDigest);
    }

    /**
     * Pure local preflight used before a source record enters the bounded
     * {@code SOURCE_APPLY} queue.  It validates the strict lifecycle and
     * record/assignment identity without reading Oxia or mutating the Store;
     * execution repeats every check after the queue wait.
     */
    synchronized void requireSourceApplySubmission(
            final OxiaOwnerLeaseStore authority, final SourceReplayEntry entry, final PublicKey verificationKey) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("source apply requires an active shard");
        }
        validateActiveSourceEntry(entry, verificationKey);
    }

    /**
     * Applies one active mixed Shard Log entry behind the WorkClass boundary.
     * Ordinary/fatal failures fence this Owner before they escape; the caller
     * retains the physical source record until the returned outcome proves a
     * successful synchronous WriteBatch.
     */
    synchronized SourceReplayOutcome applySourceEntryAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final SourceReplayEntry entry,
            final PublicKey verificationKey,
            final LongSupplier clock) {
        requireSourceApplySubmission(authority, entry, verificationKey);
        final long nowEpochMs = readActiveSourceClock(clock);
        ensureAuthoritativeActive(authority, nowEpochMs);
        // Queue wait, lease reread and renewal may have changed local state;
        // repeat the pure source/guard fence immediately before the WriteBatch.
        validateActiveSourceEntry(entry, verificationKey);
        final SourcePosition position = entry.position();
        if (entry instanceof SourceReplayRecord commandRecord) {
            final CommandResult applied = apply(
                    commandRecord.command(),
                    position,
                    nowEpochMs,
                    commandRecord.sourceConnectionGeneration(),
                    commandRecord.guardAttestationDigest());
            try {
                return SourceReplayOutcome.command(position, replayCommandResultAt(position, applied));
            } catch (RuntimeException | Error failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            }
        }
        final SourceReplayMutation mutationRecord = (SourceReplayMutation) entry;
        final SystemMutationResult applied;
        try {
            applied = delegate.applySystemMutation(mutationRecord.mutation(), position, verificationKey);
        } catch (ShardStore.RocksDbWriteFailure failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        try {
            return SourceReplayOutcome.systemMutation(position, replaySystemMutationResultAt(position, applied));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /**
     * Pure local preflight for one recovery replay action.  Recovery uses the
     * same {@link WorkClass#SOURCE_APPLY} queue as the active source reader,
     * but its lifecycle is still {@code CATCHING_UP}; accepting that state in
     * the active method would accidentally open command-time apply semantics
     * during takeover.
     */
    synchronized void requireRecoverySourceApplySubmission(
            final OxiaOwnerLeaseStore authority, final SourceReplayEntry entry, final PublicKey verificationKey) {
        requireStrictLifecycleAuthority(authority);
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("recovery source apply requires a catching-up shard");
        }
        validateActiveSourceEntry(entry, verificationKey);
    }

    /**
     * Revalidates the strict catch-up lease and applies exactly one replay
     * entry behind the bounded SOURCE_APPLY action.  The caller-owned cursor
     * is deliberately not advanced here; the coordinator advances it only
     * after this method returns a valid physical-position outcome.
     */
    synchronized SourceReplayOutcome applyRecoverySourceEntryAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final SourceReplayEntry entry,
            final PublicKey verificationKey,
            final LongSupplier clock) {
        requireRecoverySourceApplySubmission(authority, entry, verificationKey);
        final long nowEpochMs = readClock(clock);
        ensureReplayWindow(nowEpochMs);
        ensureAuthoritativeCatchup(authority, nowEpochMs);
        // Queue wait may have changed the assignment, lease expiry or source
        // connection proof.  Repeat the exact entry fence immediately before
        // the recovery WriteBatch.
        validateActiveSourceEntry(entry, verificationKey);
        final SourcePosition position = entry.position();
        validateReplayPosition(position, entry.sourceConnectionGeneration(), entry.guardAttestationDigest());
        try {
            if (entry instanceof SourceReplayRecord commandRecord) {
                final CommandResult applied = delegate.apply(commandRecord.command(), position);
                final CommandResult projected = replayCommandResultAt(position, applied);
                return SourceReplayOutcome.command(position, projected);
            }
            final SourceReplayMutation mutationRecord = (SourceReplayMutation) entry;
            final SystemMutationResult applied =
                    delegate.applySystemMutation(mutationRecord.mutation(), position, verificationKey);
            final SystemMutationResult projected = replaySystemMutationResultAt(position, applied);
            return SourceReplayOutcome.systemMutation(position, projected);
        } catch (ShardStore.RocksDbWriteFailure failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        } catch (RuntimeException | Error failure) {
            // A durable write or its result projection is not proven.  Keep
            // the source cursor at the exact entry and close local authority.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /** Rechecks the strict replay window before a recovery turn can yield. */
    synchronized void requireRecoveryTurn(final OxiaOwnerLeaseStore authority, final LongSupplier clock) {
        requireStrictLifecycleAuthority(authority);
        final long nowEpochMs = readClock(clock);
        ensureReplayWindow(nowEpochMs);
        ensureAuthoritativeCatchup(authority, nowEpochMs);
    }

    /**
     * Publishes the in-memory catch-up position only after the caller-owned
     * source cursor has advanced.  If cursor advancement fails, the durable
     * WriteBatch remains retryable from the previous position in a fresh
     * Store incarnation rather than creating a position-ahead-of-cursor
     * continuity claim.
     */
    synchronized void recordRecoverySourceCursorAdvanced(final SourceReplayEntry entry) {
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("recovery source cursor requires a catching-up shard");
        }
        final SourceReplayEntry exact = Objects.requireNonNull(entry, "recovery source entry");
        lastCatchupPosition = exact.position();
    }

    /**
     * Pure local preflight for one bounded READY-discovery action.  It reads
     * neither Oxia nor RocksDB, so a rejected {@code DUE_SCHEDULER} queue
     * admission cannot advance the durable discovery cursor or the Lane ring.
     */
    synchronized void requireDueSchedulerSubmission(
            final OxiaOwnerLeaseStore authority, final PersistentLaneScheduler scheduler) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("due scheduler discovery requires an active shard");
        }
        if (!lease.shardId()
                .equals(Objects.requireNonNull(scheduler, "scheduler").shardId())) {
            throw new IllegalArgumentException("due scheduler belongs to a different shard");
        }
        if (!Bytes.constantTimeEquals(scheduler.storeIncarnation(), delegate.storeIncarnation())) {
            throw new IllegalArgumentException("due scheduler belongs to a different Store Incarnation");
        }
        if (!scheduler.ownerIdentity().equals(requireBoundOwnerIdentity())) {
            throw new IllegalArgumentException("due scheduler belongs to a different Owner identity");
        }
    }

    /**
     * Runs one bounded persistent READY discovery after an execution-time
     * Owner Lease/session reread.  Any malformed READY projection, scheduler
     * persistence failure or authority failure stops this owner so recovery
     * can rebuild the index while fenced.
     */
    synchronized List<ScheduleWorkItem> discoverReadyAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler,
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final LongSupplier clock) {
        requireDueSchedulerSubmission(authority, scheduler);
        final long nowEpochMs = readActiveWorkClock(clock, "due scheduler");
        ensureAuthoritativeActive(authority, nowEpochMs, "due scheduler discovery");
        try {
            return scheduler.discoverReady(
                    Objects.requireNonNull(evidence, "trusted UTC evidence"),
                    Objects.requireNonNull(budget, "scheduler budget"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /**
     * Polls the exact READY projection only after the execution-time Owner
     * Lease/session reread.  The scheduler result is still a local Claim
     * candidate and must cross the Claim work-class boundary before any
     * Claim WriteBatch is allowed.
     */
    synchronized List<ScheduleWorkItem> pollReadyAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler,
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final LongSupplier clock) {
        requireDueSchedulerSubmission(authority, scheduler);
        final long nowEpochMs = readActiveWorkClock(clock, "READY poll");
        ensureAuthoritativeActive(authority, nowEpochMs, "READY poll");
        try {
            return scheduler.poll(
                    Objects.requireNonNull(evidence, "trusted UTC evidence").earliestEpochMs(),
                    Objects.requireNonNull(budget, "scheduler budget"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /**
     * Side-effect-free preflight for one exact already-polled Claim handoff.
     * It proves strict local ownership and revalidates the durable READY/
     * certificate projection, but performs no Claim WriteBatch.
     */
    synchronized PersistentLaneScheduler.ClaimCandidate requireClaimSubmission(
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler,
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence) {
        requireDueSchedulerSubmission(authority, scheduler);
        return scheduler.requireClaimCandidate(
                Objects.requireNonNull(item, "Claim work item"),
                Objects.requireNonNull(evidence, "trusted UTC evidence"));
    }

    /**
     * Derives the local durable V1 Claim projection behind the same strict
     * Owner/READY fence used by Claim submission.  External Profile,
     * serialization, credential/channel, and charge prerequisites remain
     * outside this read-only materialization boundary.
     */
    synchronized ClaimMaterializationV1 resolveClaimMaterializationAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler,
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence,
            final LongSupplier clock) {
        requireClaimSubmission(authority, scheduler, item, evidence);
        final long nowEpochMs = readActiveWorkClock(
                Objects.requireNonNull(clock, "Claim materialization clock"), "Claim materialization");
        ensureAuthoritativeActive(authority, nowEpochMs, "Claim materialization");
        final ClaimMaterializationV1 materialization = delegate.resolveClaimMaterializationV1(item.messageId());
        if (!materialization.messageId().equals(item.messageId())
                || materialization.generation() != Integer.toUnsignedLong(item.generation())) {
            throw new IllegalStateException("derived Claim materialization differs from READY work identity");
        }
        return materialization;
    }

    /**
     * Side-effect-free preflight for a prepared Publish Admission.  The
     * Claim must still be the exact local durable Claim; this method does not
     * append a mutation or synthesize a Source Position.
     */
    synchronized void requirePublishAdmissionSubmission(
            final OxiaOwnerLeaseStore authority, final ClaimRecord expectedClaim) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Publish Admission requires an active shard");
        }
        final ClaimRecord expected = validateAdmissionClaimIdentity(expectedClaim);
        final ClaimRecord current = delegate.getClaim(expected.claimId(), expected.ownerEpoch());
        if (current == null || !current.equals(expected)) {
            throw new IllegalStateException("Publish Admission Claim is no longer the exact durable Claim");
        }
    }

    /**
     * Side-effect-free preflight for one exact expiry candidate.  The
     * candidate was discovered from the durable EXPIRY index by the caller;
     * this boundary validates only the shard, trusted-time and owner identity
     * before the bounded EXPIRY queue accepts the action.
     */
    synchronized void requireExpirySubmission(
            final OxiaOwnerLeaseStore authority,
            final com.nereusstream.delay.runtime.DelayShard.ExpiryWork candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentityV1 owner) {
        requireStrictActiveAuthority(authority);
        validateExpirySubmission(candidate, evidence, owner);
    }

    /** Pure local preflight for one bounded EXPIRY-index discovery action. */
    synchronized void requireExpiryDiscoverySubmission(final OxiaOwnerLeaseStore authority) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("expiry discovery requires an active shard");
        }
    }

    /** Rereads Owner authority before a record/byte/time-bounded EXPIRY scan. */
    synchronized List<com.nereusstream.delay.runtime.DelayShard.ExpiryWork> discoverExpiryAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final LongSupplier ownerClock,
            final LongSupplier scanClockNanos) {
        requireExpiryDiscoverySubmission(authority);
        final long nowEpochMs = readActiveWorkClock(ownerClock, "expiry discovery");
        ensureAuthoritativeActive(authority, nowEpochMs, "expiry discovery");
        try {
            return delegate.discoverExpiry(
                    Objects.requireNonNull(evidence, "trusted UTC evidence").earliestEpochMs(),
                    Objects.requireNonNull(budget, "expiry discovery budget"),
                    Objects.requireNonNull(scanClockNanos, "expiry discovery scan clock"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /** Rereads the authoritative Owner Lease immediately before appending expiry. */
    synchronized void requireExpiryAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final com.nereusstream.delay.runtime.DelayShard.ExpiryWork candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentityV1 owner,
            final LongSupplier clock) {
        requireExpirySubmission(authority, candidate, evidence, owner);
        final long nowEpochMs = readActiveWorkClock(clock, "expiry handoff");
        ensureAuthoritativeActive(authority, nowEpochMs, "expiry handoff");
        validateExpirySubmission(candidate, evidence, owner);
    }

    private void validateExpirySubmission(
            final com.nereusstream.delay.runtime.DelayShard.ExpiryWork candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentityV1 owner) {
        final com.nereusstream.delay.runtime.DelayShard.ExpiryWork work =
                Objects.requireNonNull(candidate, "expiry candidate");
        final TrustedUtcIntervalEvidence trusted = Objects.requireNonNull(evidence, "expiry evidence");
        final OwnerIdentityV1 author = Objects.requireNonNull(owner, "expiry owner");
        if (!delegate.shardId().equals(work.messageId().routingId().shardId())
                || !delegate.shardId().equals(lease.shardId())) {
            throw new IllegalArgumentException("expiry candidate does not belong to this shard");
        }
        trusted.requireEarliestAtLeast(work.expireAtEpochMs());
        if (!author.equals(requireBoundOwnerIdentity())) {
            throw new IllegalArgumentException("expiry Owner identity does not match the active owner");
        }
    }

    /** Side-effect-free preflight for one durable reservation-expiry candidate. */
    synchronized void requireReservationExpirySubmission(
            final OxiaOwnerLeaseStore authority,
            final com.nereusstream.delay.runtime.DelayShard.ReservationExpiryWork candidate) {
        requireStrictActiveAuthority(authority);
        validateReservationExpiryCandidate(candidate);
    }

    /** Pure local preflight for one bounded RESERVATION_EXPIRY discovery action. */
    synchronized void requireReservationExpiryDiscoverySubmission(final OxiaOwnerLeaseStore authority) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("reservation expiry discovery requires an active shard");
        }
    }

    /** Rereads Owner authority before scanning through the persisted TIME_FENCE watermark. */
    synchronized List<com.nereusstream.delay.runtime.DelayShard.ReservationExpiryWork>
            discoverReservationExpiryAuthoritativelyStrict(
                    final OxiaOwnerLeaseStore authority,
                    final SchedulerBudget budget,
                    final LongSupplier ownerClock,
                    final LongSupplier scanClockNanos) {
        requireReservationExpiryDiscoverySubmission(authority);
        final long nowEpochMs = readActiveWorkClock(ownerClock, "reservation expiry discovery");
        ensureAuthoritativeActive(authority, nowEpochMs, "reservation expiry discovery");
        try {
            return delegate.discoverReservationExpiry(
                    Objects.requireNonNull(budget, "reservation expiry discovery budget"),
                    Objects.requireNonNull(scanClockNanos, "reservation expiry discovery scan clock"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /** Rereads the Owner Lease before the GC-class materializer touches RocksDB. */
    synchronized com.nereusstream.delay.runtime.DelayShard.ReservationExpiryMaterializationResult
            materializeReservationExpiryAuthoritativelyStrict(
                    final OxiaOwnerLeaseStore authority,
                    final com.nereusstream.delay.runtime.DelayShard.ReservationExpiryWork candidate,
                    final LongSupplier clock) {
        requireReservationExpirySubmission(authority, candidate);
        final long nowEpochMs = readActiveWorkClock(clock, "reservation expiry materialization");
        ensureAuthoritativeActive(authority, nowEpochMs, "reservation expiry materialization");
        validateReservationExpiryCandidate(candidate);
        try {
            return delegate.materializeReservationExpiry(candidate);
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private void validateReservationExpiryCandidate(
            final com.nereusstream.delay.runtime.DelayShard.ReservationExpiryWork candidate) {
        final com.nereusstream.delay.runtime.DelayShard.ReservationExpiryWork work =
                Objects.requireNonNull(candidate, "reservation expiry candidate");
        if (!delegate.shardId().equals(work.messageId().routingId().shardId())
                || !delegate.shardId().equals(lease.shardId())) {
            throw new IllegalArgumentException("reservation expiry candidate does not belong to this shard");
        }
    }

    /** Side-effect-free preflight for one exact source-ordered Lane-close cursor. */
    synchronized void requireLaneCloseMaterializationSubmission(
            final OxiaOwnerLeaseStore authority,
            final com.nereusstream.delay.runtime.DelayShard.LaneCloseMaterializationWork candidate,
            final int maxRecords) {
        requireStrictActiveAuthority(authority);
        validateLaneCloseMaterializationCandidate(candidate, maxRecords);
    }

    /** Pure local preflight for one bounded Lane-close cursor discovery action. */
    synchronized void requireLaneCloseDiscoverySubmission(final OxiaOwnerLeaseStore authority) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Lane close discovery requires an active shard");
        }
    }

    /** Rereads Owner authority before a record/byte/time-bounded cursor scan. */
    synchronized List<com.nereusstream.delay.runtime.DelayShard.LaneCloseMaterializationWork>
            discoverLaneCloseAuthoritativelyStrict(
                    final OxiaOwnerLeaseStore authority,
                    final SchedulerBudget budget,
                    final LongSupplier ownerClock,
                    final LongSupplier scanClockNanos) {
        requireLaneCloseDiscoverySubmission(authority);
        final long nowEpochMs = readActiveWorkClock(ownerClock, "Lane close discovery");
        ensureAuthoritativeActive(authority, nowEpochMs, "Lane close discovery");
        try {
            return delegate.discoverLaneCloseMaterialization(
                    Objects.requireNonNull(budget, "Lane close discovery budget"),
                    Objects.requireNonNull(scanClockNanos, "Lane close discovery scan clock"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /** Rereads the Owner Lease before a bounded Lane-close cursor batch. */
    synchronized com.nereusstream.delay.runtime.DelayShard.LaneCloseMaterializationExecutionResult
            materializeLaneCloseAuthoritativelyStrict(
                    final OxiaOwnerLeaseStore authority,
                    final com.nereusstream.delay.runtime.DelayShard.LaneCloseMaterializationWork candidate,
                    final int maxRecords,
                    final LongSupplier clock) {
        requireLaneCloseMaterializationSubmission(authority, candidate, maxRecords);
        final long nowEpochMs = readActiveWorkClock(clock, "Lane close materialization");
        ensureAuthoritativeActive(authority, nowEpochMs, "Lane close materialization");
        validateLaneCloseMaterializationCandidate(candidate, maxRecords);
        try {
            return delegate.materializeClosedLane(candidate, maxRecords);
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private void validateLaneCloseMaterializationCandidate(
            final com.nereusstream.delay.runtime.DelayShard.LaneCloseMaterializationWork candidate,
            final int maxRecords) {
        final com.nereusstream.delay.runtime.DelayShard.LaneCloseMaterializationWork work =
                Objects.requireNonNull(candidate, "Lane close materialization candidate");
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive");
        }
        final SourcePosition closePosition =
                SourcePositionCodec.decode(work.cursor().closeSourcePosition());
        if (!delegate.shardId().equals(closePosition.shardId())
                || !delegate.shardId().equals(lease.shardId())) {
            throw new IllegalArgumentException("Lane close cursor does not belong to this shard");
        }
    }

    /**
     * Rechecks the exact Claim and authoritative Oxia Owner Lease immediately
     * before calling the external Shard Log writer.
     */
    synchronized void requirePublishAdmissionAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority, final ClaimRecord expectedClaim, final LongSupplier clock) {
        requirePublishAdmissionSubmission(authority, expectedClaim);
        final long nowEpochMs = readActiveWorkClock(clock, "Publish Admission handoff");
        ensureAuthoritativeActive(authority, nowEpochMs, "Publish Admission handoff");
        final ClaimRecord current = delegate.getClaim(expectedClaim.claimId(), expectedClaim.ownerEpoch());
        if (current == null || !current.equals(expectedClaim)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("Publish Admission Claim changed before Shard Log append");
        }
    }

    /**
     * Side-effect-free preflight for a prepared result/control mutation.  The
     * mutation is already signed and semantically encoded by its producer;
     * this owner boundary only admits the four result types whose physical
     * append must be bounded by {@link WorkClass#OUTCOME_AND_CONTROL}.
     */
    synchronized void requireOutcomeMutationSubmission(
            final OxiaOwnerLeaseStore authority, final SystemMutation mutation) {
        requireStrictActiveAuthority(authority);
        validateOutcomeMutation(mutation);
    }

    /** Rereads the authoritative Owner Lease immediately before the external append. */
    synchronized void requireOutcomeMutationAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority, final SystemMutation mutation, final LongSupplier clock) {
        requireOutcomeMutationSubmission(authority, mutation);
        final long nowEpochMs = readActiveWorkClock(clock, "outcome mutation handoff");
        ensureAuthoritativeActive(authority, nowEpochMs, "outcome mutation handoff");
        // Queue wait and lease renewal may have changed the local epoch.  The
        // exact signed bytes are rechecked immediately before append.
        validateOutcomeMutation(mutation);
    }

    private void validateOutcomeMutation(final SystemMutation mutation) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "outcome mutation");
        switch (exact.type()) {
            case PUBLISH_OUTCOME, EVIDENCE_RESOLUTION, CLAIM_RESULT, DLQ_EXPORT_RESULT -> {
                // Allowed result mutations.
            }
            default ->
                throw new IllegalArgumentException(
                        "mutation type is not an OUTCOME_AND_CONTROL result: " + exact.type());
        }
        if (!delegate.shardId().equals(exact.shardId()) || !lease.shardId().equals(exact.shardId())) {
            throw new IllegalArgumentException("outcome mutation does not belong to this shard");
        }
        final AuthorIdentity author = AuthorIdentity.decode(exact.authorIdentity());
        author.requireFor(exact.type());
        if (author.kind() == AuthorIdentity.Kind.OWNER
                && !author.asOwnerIdentity().equals(requireBoundOwnerIdentity())) {
            throw new IllegalArgumentException("outcome mutation Owner identity does not match the active owner");
        }
    }

    /** Side-effect-free preflight for an exact resource-GC System Mutation. */
    synchronized void requireGcMutationSubmission(final OxiaOwnerLeaseStore authority, final SystemMutation mutation) {
        requireStrictActiveAuthority(authority);
        validateGcMutation(mutation);
    }

    /** Rereads the authoritative Owner Lease immediately before a GC append. */
    synchronized void requireGcMutationAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority, final SystemMutation mutation, final LongSupplier clock) {
        requireGcMutationSubmission(authority, mutation);
        final long nowEpochMs = readActiveWorkClock(clock, "GC mutation handoff");
        ensureAuthoritativeActive(authority, nowEpochMs, "GC mutation handoff");
        validateGcMutation(mutation);
    }

    private void validateGcMutation(final SystemMutation mutation) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "GC mutation");
        switch (exact.type()) {
            case RESOURCE_RETIRE_INTENT -> {
                final ResourceRetireIntentBody body = ResourceRetireIntentBody.decode(exact.canonicalBody());
                body.validateProtectionSourceShard(delegate.shardId());
                final byte[] expected = SystemMutation.computeResourceRetireLogicalIdentity(
                        body.resourceKind(), body.resource().identityHash(), body.expectedResourceStateVersion());
                if (!Bytes.constantTimeEquals(exact.logicalOperationIdentity(), expected)) {
                    throw new IllegalArgumentException("GC retire mutation logical identity mismatch");
                }
            }
            case RESOURCE_DELETE_CONFIRMED -> {
                final ResourceDeleteConfirmedBody body = ResourceDeleteConfirmedBody.decode(exact.canonicalBody());
                if (!Bytes.constantTimeEquals(
                        exact.logicalOperationIdentity(), body.intent().mutationId())) {
                    throw new IllegalArgumentException("GC delete confirmation logical identity mismatch");
                }
            }
            default -> throw new IllegalArgumentException("mutation type is not a GC result: " + exact.type());
        }
        if (!delegate.shardId().equals(exact.shardId()) || !lease.shardId().equals(exact.shardId())) {
            throw new IllegalArgumentException("GC mutation does not belong to this shard");
        }
        final AuthorIdentity author = AuthorIdentity.decode(exact.authorIdentity());
        author.requireFor(exact.type());
    }

    /** Side-effect-free preflight for an exact control-plane mutation. */
    synchronized void requireControlMutationSubmission(
            final OxiaOwnerLeaseStore authority, final SystemMutation mutation) {
        requireStrictActiveAuthority(authority);
        validateControlMutation(mutation);
    }

    /** Rereads the authoritative Owner Lease immediately before a control append. */
    synchronized void requireControlMutationAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority, final SystemMutation mutation, final LongSupplier clock) {
        requireControlMutationSubmission(authority, mutation);
        final long nowEpochMs = readActiveWorkClock(clock, "control mutation handoff");
        ensureAuthoritativeActive(authority, nowEpochMs, "control mutation handoff");
        validateControlMutation(mutation);
    }

    private void validateControlMutation(final SystemMutation mutation) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "control mutation");
        switch (exact.type()) {
            case APPLY_SHARD_CONTROL, REPLAY_DEAD_LETTER, RESOLVE_UNCERTAIN -> {
                // Allowed control mutations.
            }
            case TIME_FENCE -> {
                final var fields = SystemMutationBodyCodec.fields(SystemMutationType.TIME_FENCE, exact.canonicalBody());
                if (fields.isEmpty() || fields.get(0).number() != 1) {
                    throw new IllegalArgumentException("TIME_FENCE body subject is missing");
                }
                final long closeThrough = fields.get(3).unsignedValue();
                final int keyVersion = Math.toIntExact(fields.get(4).unsignedValue());
                final byte[] proofId = fields.get(5).rawValue();
                final TrustedUtcIntervalEvidence proof =
                        TrustedUtcIntervalEvidence.decode(fields.get(6).rawValue());
                final byte[] expectedProofId = Bytes.sha256(
                        Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                        delegate.shardId().routeIncarnation().bytes(),
                        Bytes.u32beBits(delegate.shardId().partition()),
                        Bytes.i64be(closeThrough),
                        Bytes.u32beBits(keyVersion),
                        Bytes.lp32(proof.canonicalBytes()));
                if (keyVersion != exact.signingKeyVersion()
                        || !Bytes.constantTimeEquals(proofId, expectedProofId)
                        || !Bytes.constantTimeEquals(exact.logicalOperationIdentity(), proofId)) {
                    throw new IllegalArgumentException("TIME_FENCE identity/proof does not match mutation");
                }
            }
            default -> throw new IllegalArgumentException("mutation type is not a control mutation: " + exact.type());
        }
        if (!delegate.shardId().equals(exact.shardId()) || !lease.shardId().equals(exact.shardId())) {
            throw new IllegalArgumentException("control mutation does not belong to this shard");
        }
        final AuthorIdentity author = AuthorIdentity.decode(exact.authorIdentity());
        author.requireFor(exact.type());
    }

    /**
     * Validates a Source Position returned by the external Shard Log writer.
     * A ShardId match alone is insufficient because a replacement Kafka topic
     * or Pulsar resource can retain the same logical route and partition.
     */
    synchronized void requireCurrentShardLogPosition(
            final SourcePosition position,
            final com.nereusstream.delay.protocol.ShardId expectedShard,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        final SourcePosition persisted = Objects.requireNonNull(position, "persisted Source Position");
        if (!Objects.requireNonNull(expectedShard, "expectedShard").equals(persisted.shardId())
                || !delegate.shardId().equals(persisted.shardId())) {
            throw new IllegalStateException("Shard Log writer returned a foreign Source Position");
        }
        if (activationBarrier == null) {
            throw new IllegalStateException("Publish Admission requires an active source assignment");
        }
        activationBarrier.validatePosition(persisted);
        validateSourceConnection(persisted, sourceConnectionGeneration, guardAttestationDigest);
    }

    private ClaimRecord validateAdmissionClaimIdentity(final ClaimRecord claim) {
        final ClaimRecord expected = Objects.requireNonNull(claim, "Claim");
        if (!delegate.shardId().equals(expected.delayMessageId().routingId().shardId())
                || expected.ownerEpoch() != lease.ownerEpoch()
                || !java.util.Arrays.equals(expected.storeIncarnation(), delegate.storeIncarnation())) {
            throw new IllegalArgumentException("Publish Admission Claim identity does not belong to this owner/store");
        }
        final OwnerIdentityV1 owner = OwnerIdentityV1.decode(expected.ownerIdentity());
        if (owner.ownerEpoch() != expected.ownerEpoch() || !owner.equals(requireBoundOwnerIdentity())) {
            throw new IllegalArgumentException("Publish Admission Claim Owner identity mismatch");
        }
        if (!expected.hasMaterialization()) {
            throw new IllegalArgumentException("Publish Admission requires Claim materialization");
        }
        return expected;
    }

    /**
     * Creates one reversible typed Claim after the queue wait and an exact
     * execution-time Owner Lease/session reread.
     *
     * <p>The caller must already hold the matching logical Claim permit and
     * prove any external Profile/Object-Store/Adapter/channel prerequisites.
     * This method owns the final local authority window: it repeats READY and
     * certificate validation immediately before the Claim WriteBatch, then
     * completes the scheduler handoff only after that batch consumed READY.</p>
     */
    synchronized ClaimRecord claimAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler,
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence,
            final long claimDeadlineEpochMs,
            final ClaimMaterializationV1 materialization,
            final byte[] claimedCharge,
            final LongSupplier clock) {
        requireClaimSubmission(authority, scheduler, item, evidence);
        if (evidence.latestEpochMs() >= claimDeadlineEpochMs) {
            throw new IllegalArgumentException("Claim deadline is not live through trusted UTC evidence");
        }
        final long nowEpochMs = readActiveWorkClock(clock, "Claim handoff");
        ensureAuthoritativeActive(authority, nowEpochMs, "Claim handoff");
        final OwnerIdentityV1 owner = requireBoundOwnerIdentity();
        final AuthorIdentity author = AuthorIdentity.owner(
                owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(), owner.leaseFencingDigest());
        try {
            final ClaimRecord claim = delegate.claimForPublishV1(
                    item.messageId(),
                    author,
                    claimDeadlineEpochMs,
                    Objects.requireNonNull(materialization, "materialization"),
                    Objects.requireNonNull(claimedCharge, "claimedCharge"));
            scheduler.completeClaim(item);
            return claim;
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    synchronized void updateLease(final OwnerLease renewed) {
        Objects.requireNonNull(renewed, "renewed");
        if (!lease.sameIdentity(renewed)
                || renewed.state() != lease.state()
                || renewed.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
            throw new IllegalArgumentException("lease renewal changed owner identity/epoch");
        }
        lease = renewed;
    }

    private OwnerIdentityV1 requireBoundOwnerIdentity() {
        if (ownerIdentity == null) {
            throw new IllegalStateException("strict live Owner action requires an explicit OwnerIdentityV1");
        }
        return ownerIdentity;
    }

    public synchronized void fence() {
        state = ShardLifecycleState.FENCED;
        failureReason = ShardFailureReason.NONE;
    }

    /**
     * Fences this local owner only when the queued fence still names the
     * current local fencing identity.  A delayed {@code LEASE_FENCE} action
     * must not fence a replacement Owner that has already been installed in
     * the same process.
     */
    synchronized boolean fenceIfLeaseMatches(final OwnerLease expected) {
        Objects.requireNonNull(expected, "expected lease");
        if (!lease.sameIdentity(expected)) {
            return false;
        }
        state = ShardLifecycleState.FENCED;
        failureReason = ShardFailureReason.NONE;
        return true;
    }

    /** Validates a queued lease-fence task without consulting external authority. */
    synchronized void requireLeaseFenceSubmission(final OwnerLease expected) {
        Objects.requireNonNull(expected, "expected lease");
        if (!lease.sameIdentity(expected)) {
            throw new IllegalStateException("lease-fence task belongs to a different local owner");
        }
        if (!delegate.shardId().equals(expected.shardId())) {
            throw new IllegalArgumentException("lease-fence task belongs to another shard");
        }
    }

    /** Validates a read-only query admission without reading Oxia or RocksDB. */
    synchronized void requireQuerySubmission(
            final OxiaOwnerLeaseStore authority, final com.nereusstream.delay.protocol.ShardId queryShard) {
        requireStrictActiveAuthority(authority);
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("query requires an active shard");
        }
        if (!delegate.shardId().equals(Objects.requireNonNull(queryShard, "queryShard"))
                || !lease.shardId().equals(queryShard)) {
            throw new IllegalArgumentException("query belongs to another shard");
        }
    }

    /**
     * Captures the exact typed Lane identity that an external activator must
     * resolve.  This preflight does not read Oxia or mutate the Store.
     */
    synchronized LaneActivationCoordinator.ActivationRequest requireLaneActivationRequest(
            final DestinationLaneId laneId, final long nowEpochMs) {
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("Lane activation requires a catching-up shard");
        }
        if (ownerIdentity == null) {
            throw new IllegalStateException("Lane activation requires a bound protocol Owner identity");
        }
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("owner clock returned a negative time");
        }
        final ActiveLaneStateV1 lane = delegate.getActiveLaneStateV1(Objects.requireNonNull(laneId, "laneId"));
        if (lane == null) {
            throw new IllegalStateException("Lane activation requires a typed active Lane");
        }
        return new LaneActivationCoordinator.ActivationRequest(
                lane.laneId(), lane.laneIncarnation(), ownerIdentity, delegate.storeIncarnation(), lane, nowEpochMs);
    }

    /**
     * Commits a certificate-backed READY projection after rereading the same
     * context-bound Owner Lease in CATCHING_UP.  A response-loss retry is
     * idempotent only for the exact certificate already stored on the Lane.
     */
    synchronized LaneRecord activateLaneAuthoritatively(
            final OxiaOwnerLeaseStore authority,
            final DestinationLaneId laneId,
            final LaneActivationPrerequisites prerequisites,
            final long nowEpochMs) {
        requireStrictActivationAuthority(authority);
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("Lane activation requires a catching-up shard");
        }
        if (ownerIdentity == null) {
            throw new IllegalStateException("Lane activation requires a bound protocol Owner identity");
        }
        final LaneActivationPrerequisites proof = Objects.requireNonNull(prerequisites, "prerequisites");
        proof.requireCurrentAt(nowEpochMs);
        final ActiveLaneStateV1 lane = delegate.getActiveLaneStateV1(Objects.requireNonNull(laneId, "laneId"));
        if (lane == null
                || !lane.laneId().equals(laneId)
                || !Bytes.constantTimeEquals(
                        lane.laneIncarnation(), proof.readyCertificate().laneIncarnation())
                || !Bytes.constantTimeEquals(
                        delegate.storeIncarnation(), proof.readyCertificate().storeIncarnation())) {
            throw new IllegalArgumentException("Lane activation proof does not match the current Store Lane");
        }
        final OwnerIdentityV1 proofOwner =
                OwnerIdentityV1.decode(proof.readyCertificate().ownerIdentity());
        if (!ownerIdentity.equals(proofOwner)) {
            throw new IllegalArgumentException("Lane activation certificate belongs to another Owner");
        }
        ensureAuthoritativeCatchup(authority, nowEpochMs);
        return delegate.activateLaneReadiness(
                laneId, lane.laneIncarnation(), proof.channel(), proof.readyCertificate(), proof.evidenceCursors());
    }

    /**
     * Rereads the authoritative Owner Lease immediately around one local
     * read.  The returned timestamp is the execution-time owner clock passed
     * to the caller's read-only projection; no Store mutation is performed.
     */
    synchronized long requireQueryAuthoritativelyStrict(
            final OxiaOwnerLeaseStore authority,
            final com.nereusstream.delay.protocol.ShardId queryShard,
            final LongSupplier clock) {
        requireQuerySubmission(authority, queryShard);
        final long nowEpochMs = readActiveWorkClock(clock, "query");
        ensureAuthoritativeActive(authority, nowEpochMs, "query");
        return nowEpochMs;
    }

    /**
     * @deprecated V1 requires an explicit source assignment; use
     * {@link #markCatchingUp(SourceAssignment)}.
     */
    @Deprecated
    synchronized void markCatchingUp() {
        markCatchingUp((SourceActivationBarrier) null);
    }

    /**
     * Compatibility check for an assignment that has already been accepted.
     * This overload cannot establish source identity and therefore cannot
     * replace {@link #markCatchingUp(SourceAssignment)}.
     *
     * @deprecated use {@link #markCatchingUp(SourceAssignment)}.
     */
    @Deprecated
    synchronized void markCatchingUp(final SourceActivationBarrier barrier) {
        if (sourceAssignment == null) {
            throw new IllegalStateException("source assignment must be accepted before catch-up");
        }
        if (!Objects.equals(sourceAssignment.activationBarrier(), barrier)) {
            throw new IllegalArgumentException("catch-up barrier is not the accepted source assignment barrier");
        }
        markCatchingUp(sourceAssignment);
    }

    /**
     * Accepts the exact assignment/barrier pair supplied by the source
     * adapter, using the legacy monotonic-only compatibility seam.
     *
     * <p>V1 source adapters must use
     * {@link #markCatchingUp(SourceAssignment, SourceReplaySuccessor)} so a
     * source gap cannot be mistaken for a caught-up shard.</p>
     */
    synchronized void markCatchingUp(final SourceAssignment assignment) {
        markCatchingUp(assignment, SourceReplaySuccessor.monotonic());
    }

    /**
     * Accepts an assignment and pins its adapter-defined replay successor for
     * the complete catch-up window.  The successor cannot be changed halfway
     * through replay, which prevents a caller from weakening a gap proof after
     * the first record has been applied.
     */
    synchronized void markCatchingUp(final SourceAssignment assignment, final SourceReplaySuccessor successor) {
        if (state != ShardLifecycleState.RESTORING) {
            throw new IllegalStateException("shard is not restoring");
        }
        Objects.requireNonNull(assignment, "assignment");
        Objects.requireNonNull(successor, "successor");
        if (!delegate.shardId().equals(assignment.shardId())) {
            throw new IllegalArgumentException("source assignment does not belong to shard");
        }
        if (lease.context() != null && lease.sourceAssignmentEpoch() <= 0) {
            throw new IllegalArgumentException("owner lease context has no positive assignment epoch");
        }
        if (lease.sourceAssignmentId() != null
                && !Bytes.constantTimeEquals(lease.sourceAssignmentId(), assignment.assignmentId())) {
            throw new IllegalArgumentException("source assignment does not match owner lease context");
        }
        if (lease.sourceAssignmentEpoch() > 0 && lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()) {
            throw new IllegalArgumentException("source assignment epoch does not match owner lease context");
        }
        sourceAssignment = assignment;
        activationBarrier = assignment.activationBarrier();
        replayAuthority = null;
        replaySuccessor = successor;
        lastCatchupPosition = delegate.lastAppliedSourcePosition();
        failureReason = ShardFailureReason.NONE;
        state = ShardLifecycleState.CATCHING_UP;
    }

    /**
     * Strict V1 catch-up admission.  The local replay gate is not opened until
     * the same owner lease is CASed to {@code CATCHING_UP}; a response-loss
     * reread is accepted only for that exact lease identity and lifecycle
     * successor.  The context-bound overload is the production boundary;
     * assignment-only overloads remain embedded compatibility seams.
     */
    synchronized void markCatchingUp(
            final OxiaOwnerLeaseStore authority,
            final SourceAssignment assignment,
            final SourceReplaySuccessor successor,
            final long nowEpochMs) {
        Objects.requireNonNull(authority, "authority");
        if (state != ShardLifecycleState.RESTORING) {
            throw new IllegalStateException("shard is not restoring");
        }
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("owner clock returned a negative time");
        }
        Objects.requireNonNull(assignment, "assignment");
        Objects.requireNonNull(successor, "successor");
        if (lease.context() == null) {
            throw new IllegalStateException("strict catch-up requires a context-bound owner lease");
        }
        validateCatchupAssignment(assignment);
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired before catch-up CAS");
        }

        final OwnerLease transitioned;
        try {
            transitioned = authority
                    .transitionOrRead(lease, ShardLifecycleState.CATCHING_UP)
                    .orElseThrow(() -> new IllegalStateException("owner lease catch-up CAS was lost"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        if (!lease.sameIdentity(transitioned)
                || transitioned.state() != ShardLifecycleState.CATCHING_UP
                || transitioned.expiresAtEpochMs() < lease.expiresAtEpochMs()
                || !transitioned.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease catch-up CAS changed fencing identity");
        }
        try {
            sourceAssignment = assignment;
            activationBarrier = assignment.activationBarrier();
            replayAuthority = authority;
            replaySuccessor = successor;
            lastCatchupPosition = delegate.lastAppliedSourcePosition();
            failureReason = ShardFailureReason.NONE;
            lease = transitioned;
            state = ShardLifecycleState.CATCHING_UP;
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /** Public adapter boundary for an externally coordinated source successor. */
    public synchronized void markCatchingUpForReactivation(
            final OxiaOwnerLeaseStore authority,
            final SourceAssignment assignment,
            final SourceReplaySuccessor successor,
            final long nowEpochMs) {
        markCatchingUp(authority, assignment, successor, nowEpochMs);
    }

    private void validateCatchupAssignment(final SourceAssignment assignment) {
        if (!delegate.shardId().equals(assignment.shardId())) {
            throw new IllegalArgumentException("source assignment does not belong to shard");
        }
        if (lease.context() != null && lease.sourceAssignmentEpoch() <= 0) {
            throw new IllegalArgumentException("owner lease context has no positive assignment epoch");
        }
        if (lease.sourceAssignmentId() != null
                && !Bytes.constantTimeEquals(lease.sourceAssignmentId(), assignment.assignmentId())) {
            throw new IllegalArgumentException("source assignment does not match owner lease context");
        }
        if (lease.sourceAssignmentEpoch() > 0 && lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()) {
            throw new IllegalArgumentException("source assignment epoch does not match owner lease context");
        }
    }

    synchronized void recordCatchup(final SourcePosition position) {
        recordCatchup(position, null, null);
    }

    /** Records catch-up from the exact guarded source connection generation. */
    synchronized void recordCatchup(
            final SourcePosition position, final Long sourceConnectionGeneration, final byte[] guardAttestationDigest) {
        Objects.requireNonNull(position, "position");
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!delegate.shardId().equals(position.shardId())) {
            throw new IllegalArgumentException("catch-up position does not belong to shard");
        }
        if (activationBarrier != null) {
            activationBarrier.validatePosition(position);
            validateSourceConnection(position, sourceConnectionGeneration, guardAttestationDigest);
        }
        validateCatchupOrder(position);
        lastCatchupPosition = position;
    }

    /**
     * Compatibility whole-iterable replay. Production source consumers must
     * use {@link #replayCatchupTurn(SourceReplayCursor, LongSupplier,
     * ReplayTurnBudget)} so a source turn cannot grow without a bound.
     */
    synchronized List<CommandResult> replayCatchup(final Iterable<SourceReplayRecord> records, final long nowEpochMs) {
        return replayCatchup(records, () -> nowEpochMs);
    }

    /**
     * Replays catch-up records while rereading the owner clock before every
     * record. The fixed-time overload remains a compatibility seam for
     * deterministic callers; source consumers should provide a live clock so
     * a long replay cannot continue after the lease expires.
     */
    synchronized List<CommandResult> replayCatchup(
            final Iterable<SourceReplayRecord> records, final LongSupplier clock) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(clock, "clock");
        return replayCatchupTurn(SourceReplayCursor.of(records.iterator()), clock, ReplayTurnBudget.unbounded())
                .results();
    }

    /** Replays at most one bounded catch-up turn using a fixed owner clock. */
    synchronized SourceReplayTurn<CommandResult> replayCatchupTurn(
            final SourceReplayCursor<? extends SourceReplayRecord> records,
            final long nowEpochMs,
            final ReplayTurnBudget budget) {
        return replayCatchupTurn(records, () -> nowEpochMs, budget);
    }

    /**
     * Replays one bounded catch-up turn. The caller retains the cursor and
     * invokes this method again when {@link SourceReplayTurn#hasMore()} is
     * true. The next record is looked up before applying it so the canonical
     * byte cap never consumes a record that belongs to a later turn.
     */
    synchronized SourceReplayTurn<CommandResult> replayCatchupTurn(
            final SourceReplayCursor<? extends SourceReplayRecord> records,
            final LongSupplier clock,
            final ReplayTurnBudget budget) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(budget, "budget");
        ensureReplayWindow(readClock(clock));
        final long startedNanos = System.nanoTime();
        int recordCount = 0;
        long canonicalBytes = 0;
        final List<CommandResult> results = new ArrayList<>();
        while (true) {
            ensureReplayWindow(readClock(clock));
            if (!sourceHasNext(records)) {
                return new SourceReplayTurn<>(results, true);
            }
            if (turnCapReached(recordCount, canonicalBytes, startedNanos, budget)) {
                return new SourceReplayTurn<>(results, false);
            }
            final SourceReplayRecord candidate = sourcePeek(records);
            final long recordBytes = canonicalReplayBytesSafely(candidate);
            if (recordBytes > budget.maxCanonicalBytes()) {
                throw new IllegalArgumentException("single source replay record exceeds canonical-byte turn budget");
            }
            if (canonicalBytes > budget.maxCanonicalBytes() - recordBytes) {
                return new SourceReplayTurn<>(results, false);
            }
            final SourceReplayRecord record = sourcePeek(records);
            final SourcePosition position = record.position();
            validateReplayPosition(position, record.sourceConnectionGeneration(), record.guardAttestationDigest());
            final CommandResult appliedResult;
            try {
                appliedResult = delegate.apply(record.command(), position);
            } catch (ShardStore.RocksDbWriteFailure failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            } catch (RuntimeException | Error failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            }
            final CommandResult result;
            try {
                // A logical duplicate keeps its durable result anchored at
                // the first Source Position, but this type-specific replay
                // result still describes the physical record just consumed.
                // Validate that projection before advancing the cursor so a
                // malformed result cannot create a position-ahead-of-cursor
                // continuity claim.
                result = replayCommandResultAt(position, appliedResult);
            } catch (RuntimeException | Error failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            }
            // Advance the caller-owned cursor only after the shard WriteBatch
            // has returned successfully.  A validation or storage failure
            // must leave the exact source record available for retry.
            sourceNext(records);
            lastCatchupPosition = position;
            results.add(result);
            recordCount++;
            canonicalBytes = Math.addExact(canonicalBytes, recordBytes);
        }
    }

    /** Returns the last position applied or observed during this catch-up. */
    public synchronized SourcePosition lastCatchupPosition() {
        return lastCatchupPosition;
    }

    /** Compatibility whole-iterable System Mutation replay. */
    synchronized List<SystemMutationResult> replaySystemMutations(
            final Iterable<SourceReplayMutation> records, final PublicKey verificationKey, final long nowEpochMs) {
        return replaySystemMutations(records, verificationKey, () -> nowEpochMs);
    }

    /** Replays signed System Mutations with a live per-record lease check. */
    synchronized List<SystemMutationResult> replaySystemMutations(
            final Iterable<SourceReplayMutation> records, final PublicKey verificationKey, final LongSupplier clock) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        Objects.requireNonNull(clock, "clock");
        return replaySystemMutationsTurn(
                        SourceReplayCursor.of(records.iterator()), verificationKey, clock, ReplayTurnBudget.unbounded())
                .results();
    }

    /** Replays at most one bounded System Mutation turn using a fixed clock. */
    synchronized SourceReplayTurn<SystemMutationResult> replaySystemMutationsTurn(
            final SourceReplayCursor<? extends SourceReplayMutation> records,
            final PublicKey verificationKey,
            final long nowEpochMs,
            final ReplayTurnBudget budget) {
        return replaySystemMutationsTurn(records, verificationKey, () -> nowEpochMs, budget);
    }

    /** Replays one bounded signed System Mutation turn. */
    synchronized SourceReplayTurn<SystemMutationResult> replaySystemMutationsTurn(
            final SourceReplayCursor<? extends SourceReplayMutation> records,
            final PublicKey verificationKey,
            final LongSupplier clock,
            final ReplayTurnBudget budget) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(budget, "budget");
        ensureReplayWindow(readClock(clock));
        final long startedNanos = System.nanoTime();
        int recordCount = 0;
        long canonicalBytes = 0;
        final List<SystemMutationResult> results = new ArrayList<>();
        while (true) {
            ensureReplayWindow(readClock(clock));
            if (!sourceHasNext(records)) {
                return new SourceReplayTurn<>(results, true);
            }
            if (turnCapReached(recordCount, canonicalBytes, startedNanos, budget)) {
                return new SourceReplayTurn<>(results, false);
            }
            final SourceReplayMutation candidate = sourcePeek(records);
            final long recordBytes = canonicalReplayBytesSafely(candidate);
            if (recordBytes > budget.maxCanonicalBytes()) {
                throw new IllegalArgumentException("single source replay record exceeds canonical-byte turn budget");
            }
            if (canonicalBytes > budget.maxCanonicalBytes() - recordBytes) {
                return new SourceReplayTurn<>(results, false);
            }
            final SourceReplayMutation record = sourcePeek(records);
            final SourcePosition position = record.position();
            validateReplayPosition(position, record.sourceConnectionGeneration(), record.guardAttestationDigest());
            final SystemMutationResult appliedResult;
            try {
                appliedResult = delegate.applySystemMutation(record.mutation(), position, verificationKey);
            } catch (ShardStore.RocksDbWriteFailure failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            } catch (RuntimeException | Error failure) {
                // A fatal dependency/JNI failure has the same owner-authority
                // consequence as an uncertain native WriteBatch: the source
                // record must remain available for a fresh Store incarnation,
                // and this Owner must not continue from an unproven image.
                state = ShardLifecycleState.FENCED;
                throw failure;
            }
            final SystemMutationResult result;
            try {
                // Keep the returned result aligned with the physical source
                // entry while leaving the durable logical result anchored at
                // its first application position.
                result = replaySystemMutationResultAt(position, appliedResult);
            } catch (RuntimeException | Error failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            }
            sourceNext(records);
            lastCatchupPosition = position;
            results.add(result);
            recordCount++;
            canonicalBytes = Math.addExact(canonicalBytes, recordBytes);
        }
    }

    /** Compatibility whole-iterable mixed replay. */
    synchronized List<SourceReplayOutcome> replay(
            final Iterable<? extends SourceReplayEntry> records,
            final PublicKey verificationKey,
            final long nowEpochMs) {
        return replay(records, verificationKey, () -> nowEpochMs);
    }

    /** Replays mixed source entries with a live per-record lease check. */
    synchronized List<SourceReplayOutcome> replay(
            final Iterable<? extends SourceReplayEntry> records,
            final PublicKey verificationKey,
            final LongSupplier clock) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        Objects.requireNonNull(clock, "clock");
        return replayTurn(
                        SourceReplayCursor.of(records.iterator()), verificationKey, clock, ReplayTurnBudget.unbounded())
                .results();
    }

    /** Replays at most one bounded mixed source turn using a fixed clock. */
    synchronized SourceReplayTurn<SourceReplayOutcome> replayTurn(
            final SourceReplayCursor<? extends SourceReplayEntry> records,
            final PublicKey verificationKey,
            final long nowEpochMs,
            final ReplayTurnBudget budget) {
        return replayTurn(records, verificationKey, () -> nowEpochMs, budget);
    }

    /**
     * Replays one bounded mixed Command/System Mutation source turn. Commands
     * and mutations retain one source cursor, so a turn cap cannot reorder the
     * two branches or advance the cursor before the selected WriteBatch commits.
     */
    synchronized SourceReplayTurn<SourceReplayOutcome> replayTurn(
            final SourceReplayCursor<? extends SourceReplayEntry> records,
            final PublicKey verificationKey,
            final LongSupplier clock,
            final ReplayTurnBudget budget) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(budget, "budget");
        ensureReplayWindow(readClock(clock));
        final long startedNanos = System.nanoTime();
        int recordCount = 0;
        long canonicalBytes = 0;
        final List<SourceReplayOutcome> results = new ArrayList<>();
        while (true) {
            ensureReplayWindow(readClock(clock));
            if (!sourceHasNext(records)) {
                return new SourceReplayTurn<>(results, true);
            }
            if (turnCapReached(recordCount, canonicalBytes, startedNanos, budget)) {
                return new SourceReplayTurn<>(results, false);
            }
            final SourceReplayEntry candidate = sourcePeek(records);
            final long recordBytes = canonicalReplayBytesSafely(candidate);
            if (recordBytes > budget.maxCanonicalBytes()) {
                throw new IllegalArgumentException("single source replay record exceeds canonical-byte turn budget");
            }
            if (canonicalBytes > budget.maxCanonicalBytes() - recordBytes) {
                return new SourceReplayTurn<>(results, false);
            }
            final SourceReplayEntry record = sourcePeek(records);
            final SourcePosition position = record.position();
            validateReplayPosition(position, record.sourceConnectionGeneration(), record.guardAttestationDigest());
            final SourceReplayOutcome outcome;
            if (record instanceof SourceReplayRecord commandRecord) {
                final CommandResult result;
                try {
                    result = delegate.apply(commandRecord.command(), position);
                } catch (ShardStore.RocksDbWriteFailure failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                } catch (RuntimeException | Error failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                }
                try {
                    outcome = SourceReplayOutcome.command(position, replayCommandResultAt(position, result));
                } catch (RuntimeException | Error failure) {
                    // The WriteBatch may already be durable, but a malformed
                    // result projection is not a continuity proof.  Do not
                    // advance the in-memory source position before the
                    // outcome has passed its exact canonical-position fence.
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                }
            } else if (record instanceof SourceReplayMutation mutationRecord) {
                final SystemMutationResult result;
                try {
                    result = delegate.applySystemMutation(mutationRecord.mutation(), position, verificationKey);
                } catch (ShardStore.RocksDbWriteFailure failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                } catch (RuntimeException | Error failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                }
                try {
                    outcome = SourceReplayOutcome.systemMutation(
                            position, replaySystemMutationResultAt(position, result));
                } catch (RuntimeException | Error failure) {
                    // Keep the local source cursor/position pair untouched
                    // when a post-WriteBatch result projection is malformed.
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                }
            } else {
                throw new IllegalArgumentException("unsupported source replay entry: " + record.getClass());
            }
            // The cursor is part of the continuity proof.  Only after the
            // exact outcome projection is valid and the caller-owned cursor
            // advances may the in-memory last position move forward.
            sourceNext(records);
            lastCatchupPosition = position;
            results.add(outcome);
            recordCount++;
            canonicalBytes = Math.addExact(canonicalBytes, recordBytes);
        }
    }

    private static boolean turnCapReached(
            final int recordCount, final long canonicalBytes, final long startedNanos, final ReplayTurnBudget budget) {
        if (recordCount >= budget.maxRecords() || canonicalBytes >= budget.maxCanonicalBytes()) {
            return true;
        }
        final long elapsedNanos = System.nanoTime() - startedNanos;
        return elapsedNanos >= budget.maxElapsedNanos();
    }

    /**
     * A source cursor is part of the replay continuity proof.  If its backing
     * iterator fails while loading, the Owner cannot prove which physical
     * record is next; keep the cursor untouched and close the local authority
     * gate before allowing the failure to escape.
     */
    private boolean sourceHasNext(final SourceReplayCursor<?> records) {
        try {
            return records.hasNext();
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private <T> T sourcePeek(final SourceReplayCursor<? extends T> records) {
        try {
            return records.peek();
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private <T> T sourceNext(final SourceReplayCursor<? extends T> records) {
        try {
            return records.next();
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private static long canonicalReplayBytes(final SourceReplayEntry record) {
        Objects.requireNonNull(record, "source replay entry");
        final int positionBytes = record.position().canonicalBytes().length;
        final int frameBytes;
        if (record instanceof SourceReplayRecord commandRecord) {
            frameBytes = CommandCodec.encodeFrame(commandRecord.command()).length;
        } else if (record instanceof SourceReplayMutation mutationRecord) {
            frameBytes = mutationRecord.mutation().encodeFrame().length;
        } else {
            throw new IllegalArgumentException("unsupported source replay entry: " + record.getClass());
        }
        return Math.addExact(positionBytes, frameBytes);
    }

    private long canonicalReplayBytesSafely(final SourceReplayEntry record) {
        try {
            return canonicalReplayBytes(record);
        } catch (RuntimeException | Error failure) {
            // A source record that cannot be canonically bounded is not a
            // proven business rejection. Close the local replay authority and
            // retain the cursor for a fresh source/store proof.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    /**
     * A logical duplicate keeps its durable result anchored at the first
     * Source Position.  Mixed replay outcomes describe the current physical
     * record, so project that result's anchor only in the returned value.
     */
    private static CommandResult replayCommandResultAt(final SourcePosition position, final CommandResult result) {
        final byte[] sourceBytes = position.canonicalBytes();
        if (Bytes.constantTimeEquals(sourceBytes, result.appliedSourcePosition())) {
            return result;
        }
        return new CommandResult(
                result.applyStatus(),
                result.stableCode(),
                result.generation(),
                result.stateVersion(),
                result.messageStatus(),
                sourceBytes);
    }

    private static SystemMutationResult replaySystemMutationResultAt(
            final SourcePosition position, final SystemMutationResult result) {
        final byte[] sourceBytes = position.canonicalBytes();
        if (Bytes.constantTimeEquals(sourceBytes, result.appliedSourcePosition())) {
            return result;
        }
        return new SystemMutationResult(
                result.mutationId(),
                result.mutationHash(),
                result.mutationType(),
                result.retryUntilEpochMs(),
                result.authorIdentity(),
                result.applyStatus(),
                result.stableCode(),
                sourceBytes);
    }

    private void ensureReplayWindow(final long nowEpochMs) {
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard is not catching up");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired during source catch-up");
        }
        if (replayAuthority == null) {
            return;
        }
        try {
            final OwnerLease observed = replayAuthority.current(lease.shardId()).orElse(null);
            if (observed == null
                    || !lease.sameIdentity(observed)
                    || observed.state() != ShardLifecycleState.CATCHING_UP
                    || observed.expiresAtEpochMs() < lease.expiresAtEpochMs()
                    || !observed.validAt(nowEpochMs)) {
                state = ShardLifecycleState.FENCED;
                throw new IllegalStateException("authoritative owner lease changed during source catch-up");
            }
            if (observed.expiresAtEpochMs() > lease.expiresAtEpochMs()) {
                lease = observed;
            }
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private long readClock(final LongSupplier clock) {
        try {
            final long nowEpochMs = clock.getAsLong();
            if (nowEpochMs < 0) {
                throw new IllegalArgumentException("owner clock returned a negative time");
            }
            return nowEpochMs;
        } catch (RuntimeException | Error failure) {
            // A replay clock is part of the lease-validity proof.  If it is
            // unavailable or malformed, the Owner cannot establish that the
            // lease is still valid; keep the source cursor untouched and close
            // the local mutation gate before the failure escapes.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private long readActiveSourceClock(final LongSupplier clock) {
        return readActiveWorkClock(clock, "source apply");
    }

    private long readActiveWorkClock(final LongSupplier clock, final String operation) {
        try {
            final long nowEpochMs =
                    Objects.requireNonNull(clock, operation + " clock").getAsLong();
            if (nowEpochMs < 0) {
                throw new IllegalArgumentException(operation + " clock returned a negative time");
            }
            return nowEpochMs;
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private void validateActiveSourceEntry(final SourceReplayEntry entry, final PublicKey verificationKey) {
        final SourceReplayEntry candidate = Objects.requireNonNull(entry, "source apply entry");
        Objects.requireNonNull(verificationKey, "system mutation verification key");
        final SourcePosition position = candidate.position();
        if (!delegate.shardId().equals(position.shardId())) {
            throw new IllegalArgumentException("source apply position does not belong to shard");
        }
        if (candidate instanceof SourceReplayRecord commandRecord
                && !commandRecord.command().shardId().equals(delegate.shardId())) {
            throw new IllegalArgumentException("source apply command does not belong to shard");
        }
        if (candidate instanceof SourceReplayMutation mutationRecord
                && !mutationRecord.mutation().shardId().equals(delegate.shardId())) {
            throw new IllegalArgumentException("source apply mutation does not belong to shard");
        }
        if (activationBarrier == null) {
            throw new IllegalStateException("strict source apply requires an activation barrier");
        }
        activationBarrier.validatePosition(position);
        validateSourceConnection(position, candidate.sourceConnectionGeneration(), candidate.guardAttestationDigest());
    }

    private void validateReplayPosition(
            final SourcePosition position, final Long sourceConnectionGeneration, final byte[] guardAttestationDigest) {
        try {
            Objects.requireNonNull(position, "source replay position");
            if (!delegate.shardId().equals(position.shardId())) {
                throw new IllegalArgumentException("source replay position does not belong to shard");
            }
            if (activationBarrier != null) {
                activationBarrier.validatePosition(position);
                validateSourceConnection(position, sourceConnectionGeneration, guardAttestationDigest);
            }
            validateCatchupOrder(position);
        } catch (SourceReplayGapException failure) {
            fail(ShardFailureReason.SOURCE_GAP);
            throw failure;
        } catch (RuntimeException | Error failure) {
            // The source/guard proof is unavailable or malformed, but this
            // path has not proven a gap. Fence rather than leaving a replay
            // owner in CATCHING_UP with an unproven continuity claim.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private void validateCatchupOrder(final SourcePosition position) {
        if (lastCatchupPosition == null) {
            return;
        }
        try {
            replaySuccessor.validate(lastCatchupPosition, position);
        } catch (SourceReplayGapException failure) {
            fail(ShardFailureReason.SOURCE_GAP);
            throw failure;
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private void validateSourceConnection(
            final SourcePosition position, final Long connectionGeneration, final byte[] guardAttestationDigest) {
        if (!(position instanceof com.nereusstream.delay.protocol.PulsarSourcePosition)) {
            if (connectionGeneration != null || guardAttestationDigest != null) {
                throw new IllegalArgumentException("source connection proof is only valid for Pulsar");
            }
            return;
        }
        if (!(activationBarrier instanceof PulsarActivationBarrier pulsarBarrier)) {
            return;
        }
        if (connectionGeneration == null || connectionGeneration == 0 || guardAttestationDigest == null) {
            throw new IllegalArgumentException("Pulsar source connection proof is required");
        }
        pulsarBarrier.validateSourceConnection(connectionGeneration, guardAttestationDigest);
    }

    synchronized void activateForCommands(final long nowEpochMs) {
        ensureActivationPreconditions(nowEpochMs);
        // A restored CLAIMED record is only a reversible pre-Producer
        // reservation.  Requeue it before opening the command gate so a new
        // Owner cannot inherit an old Owner Epoch's local send authority.
        try {
            // Persist the owner-open marker before exposing ACTIVE_FOR_COMMANDS.
            // This is Store metadata, not a source mutation; a failed write
            // leaves the owner fenced and the source cursor unchanged.
            delegate.recordOpenedOwnerEpoch(lease.ownerEpoch());
            delegate.requeueClaimsForRecovery();
        } catch (ShardStore.RocksDbWriteFailure failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        } catch (Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        } catch (RuntimeException failure) {
            // Metadata/recovery projection failures are not activation
            // rejections. Keep the command gate closed and fence this Owner
            // before the failure escapes, including validation failures that
            // are not wrapped as RocksDbWriteFailure.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        state = ShardLifecycleState.ACTIVE_FOR_COMMANDS;
    }

    /**
     * Strict V1 activation that proves the complete shard-bound control
     * snapshot before opening the local command gate. The legacy overload
     * remains an embedded compatibility seam; production activation should
     * pass the exact snapshot obtained from the authoritative control path.
     */
    synchronized void activateForCommandsWithControlSnapshot(
            final CompatibleControlSnapshotV1 expected, final long nowEpochMs) {
        requireControlSnapshot(expected);
        activateForCommands(nowEpochMs);
    }

    /** Completes activation only after the authority CASes the same lease to ACTIVE_FOR_COMMANDS. */
    synchronized void activateForCommands(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        activateForCommands(authority, nowEpochMs, false);
    }

    private void activateForCommands(
            final OxiaOwnerLeaseStore authority, final long nowEpochMs, final boolean requireAuthoritativeCatchup) {
        Objects.requireNonNull(authority, "authority");
        ensureActivationPreconditions(nowEpochMs);
        if (requireAuthoritativeCatchup) {
            ensureAuthoritativeCatchup(authority, nowEpochMs);
        }
        // Keep the local recovery boundary identical for the authoritative
        // and embedded activation paths.  A failed lease CAS leaves the
        // requeue durable and harmless; it never grants publish authority.
        try {
            // Write the marker while the local gate is still CATCHING_UP. If
            // the authority CAS is lost afterwards, the conservative higher
            // observed epoch remains durable for the next owner.
            delegate.recordOpenedOwnerEpoch(lease.ownerEpoch());
            delegate.requeueClaimsForRecovery();
        } catch (ShardStore.RocksDbWriteFailure failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        } catch (RuntimeException | Error failure) {
            // Metadata/recovery projection failures are not activation
            // rejections.  Keep the local command gate closed and fence the
            // Owner before the failure escapes, regardless of whether the
            // failure was typed by RocksDB or surfaced during validation.
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        final OwnerLease transitioned;
        try {
            transitioned = authority
                    .transitionOrRead(lease, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow(() -> new IllegalStateException("owner lease activation CAS was lost"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        if (!transitioned.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired during activation CAS");
        }
        lease = transitioned;
        state = ShardLifecycleState.ACTIVE_FOR_COMMANDS;
    }

    /** Strict V1 activation with both control-snapshot and Owner Lease CAS fences. */
    synchronized void activateForCommandsWithControlSnapshot(
            final OxiaOwnerLeaseStore authority, final CompatibleControlSnapshotV1 expected, final long nowEpochMs) {
        requireStrictActivationAuthority(authority);
        requireControlSnapshot(expected);
        activateForCommands(authority, nowEpochMs, true);
    }

    /** Public strict activation boundary for an externally coordinated source successor. */
    public synchronized void activateForReactivation(
            final OxiaOwnerLeaseStore authority, final CompatibleControlSnapshotV1 expected, final long nowEpochMs) {
        activateForCommandsWithControlSnapshot(authority, expected, nowEpochMs);
    }

    private void requireStrictActivationAuthority(final OxiaOwnerLeaseStore authority) {
        Objects.requireNonNull(authority, "authority");
        if (lease.context() == null || sourceAssignment == null || replayAuthority == null) {
            throw new IllegalStateException("strict activation requires a context-bound strict catch-up lease");
        }
        validateCatchupAssignment(sourceAssignment);
    }

    private void ensureAuthoritativeCatchup(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        try {
            final OwnerLease observed = authority.current(lease.shardId()).orElse(null);
            if (observed == null
                    || !lease.sameIdentity(observed)
                    || observed.state() != ShardLifecycleState.CATCHING_UP
                    || observed.expiresAtEpochMs() < lease.expiresAtEpochMs()
                    || !observed.validAt(nowEpochMs)) {
                state = ShardLifecycleState.FENCED;
                throw new IllegalStateException("authoritative owner lease changed before strict activation");
            }
            if (observed.expiresAtEpochMs() > lease.expiresAtEpochMs()) {
                lease = observed;
            }
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }

    private void requireControlSnapshot(final CompatibleControlSnapshotV1 expected) {
        Objects.requireNonNull(expected, "expected control snapshot");
        if (!delegate.shardId().equals(expected.shard().shardId())) {
            throw new IllegalArgumentException("control snapshot belongs to another shard");
        }
        final CompatibleControlSnapshotV1 persisted = delegate.controlSnapshot();
        if (persisted == null || !persisted.equals(expected)) {
            throw new IllegalStateException("shard control snapshot is missing or does not match activation input");
        }
    }

    private void ensureActivationPreconditions(final long nowEpochMs) {
        if (state != ShardLifecycleState.CATCHING_UP) {
            throw new IllegalStateException("shard has not completed source catch-up");
        }
        if (activationBarrier == null || !activationBarrier.reachedBy(lastCatchupPosition)) {
            throw new IllegalStateException("source activation barrier has not been reached");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired before activation");
        }
    }

    synchronized void beginDrain() {
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("only an active shard can drain");
        }
        state = ShardLifecycleState.DRAINING;
    }

    /**
     * Begins the owner drain only after the authoritative lease performs the
     * same-identity lifecycle CAS. A lost transition response is accepted
     * only when {@link OxiaOwnerLeaseStore#transitionOrRead(OwnerLease,
     * ShardLifecycleState)} rereads that exact successor; a different owner,
     * epoch, token, assignment or session never opens the local drain gate.
     *
     * <p>The local transition only closes new command admission. Claim
     * revocation, in-flight publish quiescence, the final checkpoint and lease
     * release remain explicit drain steps owned by the surrounding worker
     * orchestration.</p>
     */
    synchronized void beginDrain(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        beginDrain(authority, nowEpochMs, false);
    }

    /**
     * Strict V1 planned-drain entrypoint.  A production owner that opened its
     * replay window through the context-bound catch-up path must retain that
     * assignment/session fence through the ACTIVE -> DRAINING CAS.  The
     * authority object may be a separate validating wrapper, but the local
     * lease must still carry the exact context and the shard must have an
     * accepted assignment.  The authority-less/assignment-only lifecycle
     * methods remain embedded compatibility seams.
     */
    synchronized void beginDrainStrict(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        requireStrictLifecycleAuthority(authority);
        beginDrain(authority, nowEpochMs, true);
    }

    /** Returns whether this owner has the context-bound state required by strict drain. */
    synchronized boolean hasStrictLifecycleAuthority() {
        return lease.context() != null && sourceAssignment != null && replayAuthority != null;
    }

    private void beginDrain(final OxiaOwnerLeaseStore authority, final long nowEpochMs, final boolean strictLifecycle) {
        Objects.requireNonNull(authority, "authority");
        if (strictLifecycle) {
            requireStrictLifecycleAuthority(authority);
        }
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("only an active shard can drain");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired before drain CAS");
        }
        final OwnerLease transitioned;
        try {
            transitioned = authority
                    .transitionOrRead(lease, ShardLifecycleState.DRAINING)
                    .orElseThrow(() -> new IllegalStateException("owner lease drain CAS was lost"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        if (!lease.sameIdentity(transitioned)
                || transitioned.state() != ShardLifecycleState.DRAINING
                || !transitioned.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease drain CAS changed fencing identity");
        }
        lease = transitioned;
        state = ShardLifecycleState.DRAINING;
    }

    private void requireStrictLifecycleAuthority(final OxiaOwnerLeaseStore authority) {
        Objects.requireNonNull(authority, "authority");
        if (!hasStrictLifecycleAuthority()) {
            throw new IllegalStateException("strict drain requires a context-bound strict catch-up lease");
        }
        validateCatchupAssignment(sourceAssignment);
    }

    private void requireStrictActiveAuthority(final OxiaOwnerLeaseStore authority) {
        Objects.requireNonNull(authority, "authority");
        if (!hasStrictLifecycleAuthority()) {
            throw new IllegalStateException("strict apply requires a context-bound strict catch-up lease");
        }
        validateCatchupAssignment(sourceAssignment);
    }

    /**
     * Acquires the shard-local drain-attempt gate without changing lifecycle
     * state.  A failed attempt releases the gate so a caller can retry while
     * the authoritative lease remains in {@code DRAINING}.
     */
    synchronized boolean tryAcquireDrainAttempt() {
        if (drainAttemptInProgress) {
            return false;
        }
        drainAttemptInProgress = true;
        return true;
    }

    /** Releases the shard-local drain-attempt gate after the coordinator exits. */
    synchronized void releaseDrainAttempt() {
        if (!drainAttemptInProgress) {
            throw new IllegalStateException("owner drain attempt is not active");
        }
        drainAttemptInProgress = false;
    }

    public synchronized OwnerLease lease() {
        return lease;
    }

    /**
     * Returns the delegate for ownership-package drain/inspection code only.
     * Public callers must use the fenced apply/replay operations above; a raw
     * delegate would bypass the owner lifecycle and lease checks.
     */
    synchronized DelayShard shard() {
        return delegate;
    }

    /** Binds an ownership-package executor to this shard's exact Worker resource graph. */
    synchronized void bindWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        delegate.bindWorkClassExecutionRegistry(registry);
    }

    public synchronized SourceAssignment sourceAssignment() {
        return sourceAssignment;
    }

    /** Returns the exact durable typed Lane projection for restart-time proof reuse. */
    public synchronized ActiveLaneStateV1 getActiveLaneStateV1(final DestinationLaneId laneId) {
        return delegate.getActiveLaneStateV1(Objects.requireNonNull(laneId, "laneId"));
    }

    public synchronized ShardLifecycleState state() {
        return state;
    }

    /** Returns the closed failure reason when this local Owner is FAILED. */
    public synchronized ShardFailureReason failureReason() {
        return failureReason;
    }

    private void fail(final ShardFailureReason reason) {
        failureReason = Objects.requireNonNull(reason, "reason");
        state = ShardLifecycleState.FAILED;
    }

    private void ensureActive(final long nowEpochMs) {
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("shard owner lease is not active");
        }
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("shard lifecycle is not active for commands: " + state);
        }
    }

    private void ensureAuthoritativeActive(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        ensureAuthoritativeActive(authority, nowEpochMs, "command apply");
    }

    private void ensureAuthoritativeActive(
            final OxiaOwnerLeaseStore authority, final long nowEpochMs, final String operation) {
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(operation, "operation");
        ensureActive(nowEpochMs);
        try {
            final OwnerLease observed = authority.current(lease.shardId()).orElse(null);
            if (observed == null
                    || !lease.sameIdentity(observed)
                    || observed.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                    || !observed.validAt(nowEpochMs)) {
                state = ShardLifecycleState.FENCED;
                throw new IllegalStateException("authoritative owner lease changed before " + operation);
            }
            if (observed.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
                state = ShardLifecycleState.FENCED;
                throw new IllegalStateException("authoritative owner lease expiry regressed before " + operation);
            }
            if (observed.expiresAtEpochMs() > lease.expiresAtEpochMs()) {
                lease = observed;
            }
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
    }
}
