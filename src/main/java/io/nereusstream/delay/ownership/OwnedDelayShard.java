package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.SourceActivationBarrier;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.runtime.SystemMutationResult;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.store.ShardStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.security.PublicKey;
import java.util.function.LongSupplier;

/** Fenced owner view; lease loss closes all new local authority gates. */
public final class OwnedDelayShard {
    private final DelayShard delegate;
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

    public OwnedDelayShard(final DelayShard delegate, final OwnerLease lease) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.state = ShardLifecycleState.RESTORING;
    }

    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition position,
                                            final long nowEpochMs) {
        return apply(command, position, nowEpochMs, null, null);
    }

    /**
     * Applies a record from a guarded Pulsar source connection.  The
     * connection proof is required for Pulsar because a replacement consumer
     * can otherwise emit a position with the same physical topic identity.
     * Kafka has no connection-generation field and passes {@code null} proof.
     */
    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition position,
                                            final long nowEpochMs, final Long sourceConnectionGeneration,
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
        } catch (Error failure) {
            // JNI/native fatal failures have the same owner-authority
            // consequence as a typed storage failure.  The Store fences its
            // write outcome; close this local gate before rethrowing so a
            // caller cannot admit another command on an uncertain image.
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
    public synchronized CommandResult applyAuthoritatively(final OxiaOwnerLeaseStore authority,
                                                            final PreparedCommand command,
                                                            final SourcePosition position, final long nowEpochMs) {
        return applyAuthoritatively(authority, command, position, nowEpochMs, null, null);
    }

    /** Applies one guarded command after an authoritative lease reread. */
    public synchronized CommandResult applyAuthoritatively(final OxiaOwnerLeaseStore authority,
                                                            final PreparedCommand command,
                                                            final SourcePosition position, final long nowEpochMs,
                                                            final Long sourceConnectionGeneration,
                                                            final byte[] guardAttestationDigest) {
        ensureAuthoritativeActive(authority, nowEpochMs);
        return apply(command, position, nowEpochMs, sourceConnectionGeneration, guardAttestationDigest);
    }

    public synchronized void updateLease(final OwnerLease renewed) {
        Objects.requireNonNull(renewed, "renewed");
        if (!lease.sameIdentity(renewed) || renewed.state() != lease.state()
                || renewed.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
            throw new IllegalArgumentException("lease renewal changed owner identity/epoch");
        }
        lease = renewed;
    }

    public synchronized void fence() {
        state = ShardLifecycleState.FENCED;
        failureReason = ShardFailureReason.NONE;
    }

    /**
     * @deprecated V1 requires an explicit source assignment; use
     * {@link #markCatchingUp(SourceAssignment)}.
     */
    @Deprecated
    public synchronized void markCatchingUp() {
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
    public synchronized void markCatchingUp(final SourceActivationBarrier barrier) {
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
    public synchronized void markCatchingUp(final SourceAssignment assignment) {
        markCatchingUp(assignment, SourceReplaySuccessor.monotonic());
    }

    /**
     * Accepts an assignment and pins its adapter-defined replay successor for
     * the complete catch-up window.  The successor cannot be changed halfway
     * through replay, which prevents a caller from weakening a gap proof after
     * the first record has been applied.
     */
    public synchronized void markCatchingUp(final SourceAssignment assignment,
                                             final SourceReplaySuccessor successor) {
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
        if (lease.sourceAssignmentEpoch() > 0
                && lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()) {
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
    public synchronized void markCatchingUp(final OxiaOwnerLeaseStore authority,
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
            transitioned = authority.transitionOrRead(lease, ShardLifecycleState.CATCHING_UP)
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
        if (lease.sourceAssignmentEpoch() > 0
                && lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()) {
            throw new IllegalArgumentException("source assignment epoch does not match owner lease context");
        }
    }

    public synchronized void recordCatchup(final SourcePosition position) {
        recordCatchup(position, null, null);
    }

    /** Records catch-up from the exact guarded source connection generation. */
    public synchronized void recordCatchup(final SourcePosition position, final Long sourceConnectionGeneration,
                                           final byte[] guardAttestationDigest) {
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
    public synchronized List<CommandResult> replayCatchup(final Iterable<SourceReplayRecord> records,
                                                           final long nowEpochMs) {
        return replayCatchup(records, () -> nowEpochMs);
    }

    /**
     * Replays catch-up records while rereading the owner clock before every
     * record. The fixed-time overload remains a compatibility seam for
     * deterministic callers; source consumers should provide a live clock so
     * a long replay cannot continue after the lease expires.
     */
    public synchronized List<CommandResult> replayCatchup(final Iterable<SourceReplayRecord> records,
                                                           final LongSupplier clock) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(clock, "clock");
        return replayCatchupTurn(SourceReplayCursor.of(records.iterator()), clock,
                ReplayTurnBudget.unbounded()).results();
    }

    /** Replays at most one bounded catch-up turn using a fixed owner clock. */
    public synchronized SourceReplayTurn<CommandResult> replayCatchupTurn(
            final SourceReplayCursor<? extends SourceReplayRecord> records, final long nowEpochMs,
            final ReplayTurnBudget budget) {
        return replayCatchupTurn(records, () -> nowEpochMs, budget);
    }

    /**
     * Replays one bounded catch-up turn. The caller retains the cursor and
     * invokes this method again when {@link SourceReplayTurn#hasMore()} is
     * true. The next record is looked up before applying it so the canonical
     * byte cap never consumes a record that belongs to a later turn.
     */
    public synchronized SourceReplayTurn<CommandResult> replayCatchupTurn(
            final SourceReplayCursor<? extends SourceReplayRecord> records, final LongSupplier clock,
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
            final CommandResult result;
            try {
                result = delegate.apply(record.command(), position);
            } catch (ShardStore.RocksDbWriteFailure failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            } catch (Error failure) {
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
    public synchronized List<SystemMutationResult> replaySystemMutations(
            final Iterable<SourceReplayMutation> records, final PublicKey verificationKey,
            final long nowEpochMs) {
        return replaySystemMutations(records, verificationKey, () -> nowEpochMs);
    }

    /** Replays signed System Mutations with a live per-record lease check. */
    public synchronized List<SystemMutationResult> replaySystemMutations(
            final Iterable<SourceReplayMutation> records, final PublicKey verificationKey,
            final LongSupplier clock) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        Objects.requireNonNull(clock, "clock");
        return replaySystemMutationsTurn(SourceReplayCursor.of(records.iterator()), verificationKey, clock,
                ReplayTurnBudget.unbounded()).results();
    }

    /** Replays at most one bounded System Mutation turn using a fixed clock. */
    public synchronized SourceReplayTurn<SystemMutationResult> replaySystemMutationsTurn(
            final SourceReplayCursor<? extends SourceReplayMutation> records, final PublicKey verificationKey,
            final long nowEpochMs, final ReplayTurnBudget budget) {
        return replaySystemMutationsTurn(records, verificationKey, () -> nowEpochMs, budget);
    }

    /** Replays one bounded signed System Mutation turn. */
    public synchronized SourceReplayTurn<SystemMutationResult> replaySystemMutationsTurn(
            final SourceReplayCursor<? extends SourceReplayMutation> records, final PublicKey verificationKey,
            final LongSupplier clock, final ReplayTurnBudget budget) {
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
            final SystemMutationResult result;
            try {
                result = delegate.applySystemMutation(record.mutation(), position, verificationKey);
            } catch (ShardStore.RocksDbWriteFailure failure) {
                state = ShardLifecycleState.FENCED;
                throw failure;
            } catch (Error failure) {
                // A fatal dependency/JNI failure has the same owner-authority
                // consequence as an uncertain native WriteBatch: the source
                // record must remain available for a fresh Store incarnation,
                // and this Owner must not continue from an unproven image.
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
    public synchronized List<SourceReplayOutcome> replay(
            final Iterable<? extends SourceReplayEntry> records, final PublicKey verificationKey,
            final long nowEpochMs) {
        return replay(records, verificationKey, () -> nowEpochMs);
    }

    /** Replays mixed source entries with a live per-record lease check. */
    public synchronized List<SourceReplayOutcome> replay(
            final Iterable<? extends SourceReplayEntry> records, final PublicKey verificationKey,
            final LongSupplier clock) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(verificationKey, "verificationKey");
        Objects.requireNonNull(clock, "clock");
        return replayTurn(SourceReplayCursor.of(records.iterator()), verificationKey, clock,
                ReplayTurnBudget.unbounded()).results();
    }

    /** Replays at most one bounded mixed source turn using a fixed clock. */
    public synchronized SourceReplayTurn<SourceReplayOutcome> replayTurn(
            final SourceReplayCursor<? extends SourceReplayEntry> records, final PublicKey verificationKey,
            final long nowEpochMs, final ReplayTurnBudget budget) {
        return replayTurn(records, verificationKey, () -> nowEpochMs, budget);
    }

    /**
     * Replays one bounded mixed Command/System Mutation source turn. Commands
     * and mutations retain one source cursor, so a turn cap cannot reorder the
     * two branches or advance the cursor before the selected WriteBatch commits.
     */
    public synchronized SourceReplayTurn<SourceReplayOutcome> replayTurn(
            final SourceReplayCursor<? extends SourceReplayEntry> records, final PublicKey verificationKey,
            final LongSupplier clock, final ReplayTurnBudget budget) {
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
            if (record instanceof SourceReplayRecord commandRecord) {
                final CommandResult result;
                try {
                    result = delegate.apply(commandRecord.command(), position);
                } catch (ShardStore.RocksDbWriteFailure failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                } catch (Error failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                }
                lastCatchupPosition = position;
                results.add(SourceReplayOutcome.command(position, replayCommandResultAt(position, result)));
            } else if (record instanceof SourceReplayMutation mutationRecord) {
                final SystemMutationResult result;
                try {
                    result = delegate.applySystemMutation(mutationRecord.mutation(), position, verificationKey);
                } catch (ShardStore.RocksDbWriteFailure failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                } catch (Error failure) {
                    state = ShardLifecycleState.FENCED;
                    throw failure;
                }
                lastCatchupPosition = position;
                results.add(SourceReplayOutcome.systemMutation(position, replaySystemMutationResultAt(position, result)));
            } else {
                throw new IllegalArgumentException("unsupported source replay entry: " + record.getClass());
            }
            sourceNext(records);
            recordCount++;
            canonicalBytes = Math.addExact(canonicalBytes, recordBytes);
        }
    }

    private static boolean turnCapReached(final int recordCount, final long canonicalBytes,
                                          final long startedNanos, final ReplayTurnBudget budget) {
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
    private static CommandResult replayCommandResultAt(final SourcePosition position,
                                                        final CommandResult result) {
        final byte[] sourceBytes = position.canonicalBytes();
        if (Bytes.constantTimeEquals(sourceBytes, result.appliedSourcePosition())) {
            return result;
        }
        return new CommandResult(result.applyStatus(), result.stableCode(), result.generation(),
                result.stateVersion(), result.messageStatus(), sourceBytes);
    }

    private static SystemMutationResult replaySystemMutationResultAt(final SourcePosition position,
                                                                      final SystemMutationResult result) {
        final byte[] sourceBytes = position.canonicalBytes();
        if (Bytes.constantTimeEquals(sourceBytes, result.appliedSourcePosition())) {
            return result;
        }
        return new SystemMutationResult(result.mutationId(), result.mutationHash(), result.mutationType(),
                result.retryUntilEpochMs(), result.authorIdentity(), result.applyStatus(), result.stableCode(),
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
            if (observed == null || !lease.sameIdentity(observed)
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

    private void validateReplayPosition(final SourcePosition position, final Long sourceConnectionGeneration,
                                        final byte[] guardAttestationDigest) {
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

    private void validateSourceConnection(final SourcePosition position, final Long connectionGeneration,
                                          final byte[] guardAttestationDigest) {
        if (!(position instanceof io.nereusstream.delay.protocol.PulsarSourcePosition)) {
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

    public synchronized void activateForCommands(final long nowEpochMs) {
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
    public synchronized void activateForCommandsWithControlSnapshot(
            final CompatibleControlSnapshotV1 expected, final long nowEpochMs) {
        requireControlSnapshot(expected);
        activateForCommands(nowEpochMs);
    }

    /** Completes activation only after the authority CASes the same lease to ACTIVE_FOR_COMMANDS. */
    public synchronized void activateForCommands(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        activateForCommands(authority, nowEpochMs, false);
    }

    private void activateForCommands(final OxiaOwnerLeaseStore authority, final long nowEpochMs,
                                     final boolean requireAuthoritativeCatchup) {
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
            transitioned = authority.transitionOrRead(lease, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
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
    public synchronized void activateForCommandsWithControlSnapshot(final OxiaOwnerLeaseStore authority,
                                                                      final CompatibleControlSnapshotV1 expected,
                                                                      final long nowEpochMs) {
        requireStrictActivationAuthority(authority);
        requireControlSnapshot(expected);
        activateForCommands(authority, nowEpochMs, true);
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
            if (observed == null || !lease.sameIdentity(observed)
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

    public synchronized void beginDrain() {
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
    public synchronized void beginDrain(final OxiaOwnerLeaseStore authority, final long nowEpochMs) {
        Objects.requireNonNull(authority, "authority");
        if (state != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("only an active shard can drain");
        }
        if (!lease.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease expired before drain CAS");
        }
        final OwnerLease transitioned;
        try {
            transitioned = authority.transitionOrRead(lease, ShardLifecycleState.DRAINING)
                    .orElseThrow(() -> new IllegalStateException("owner lease drain CAS was lost"));
        } catch (RuntimeException | Error failure) {
            state = ShardLifecycleState.FENCED;
            throw failure;
        }
        if (!lease.sameIdentity(transitioned) || transitioned.state() != ShardLifecycleState.DRAINING
                || !transitioned.validAt(nowEpochMs)) {
            state = ShardLifecycleState.FENCED;
            throw new IllegalStateException("owner lease drain CAS changed fencing identity");
        }
        lease = transitioned;
        state = ShardLifecycleState.DRAINING;
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

    public synchronized SourceAssignment sourceAssignment() {
        return sourceAssignment;
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
        Objects.requireNonNull(authority, "authority");
        ensureActive(nowEpochMs);
        try {
            final OwnerLease observed = authority.current(lease.shardId()).orElse(null);
            if (observed == null || !lease.sameIdentity(observed)
                    || observed.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                    || !observed.validAt(nowEpochMs)) {
                state = ShardLifecycleState.FENCED;
                throw new IllegalStateException("authoritative owner lease changed before command apply");
            }
            if (observed.expiresAtEpochMs() < lease.expiresAtEpochMs()) {
                state = ShardLifecycleState.FENCED;
                throw new IllegalStateException("authoritative owner lease expiry regressed before command apply");
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
