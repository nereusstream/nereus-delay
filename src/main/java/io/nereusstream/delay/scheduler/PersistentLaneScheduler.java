package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.ActiveLaneStateV1;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.SchedulerProjectionsV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.ReadyIndexValue;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.runtime.TimelineEntry;
import io.nereusstream.delay.runtime.TimelineWorkRef;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ValueEnvelope;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Durable fairness wrapper for a shard-local {@link LaneScheduler}.
 *
 * <p>The five closed Registry scheduler projections are written together. Lane
 * records and timeline work remain authoritative in their own keys; these
 * values only retain the bounded successor order and fairness counters needed
 * to resume without resetting a Lane's service gap.</p>
 */
public final class PersistentLaneScheduler {
    private static final int VALUE_TYPE = 5;
    private final ShardStore store;
    private final LaneScheduler delegate;
    private final OwnerIdentityV1 owner;
    private final LongSupplier clockNanos;
    private long lastClockNanos;
    private boolean clockInitialized;
    private final Map<DestinationLaneId, LaneRecord> registered = new HashMap<>();
    private final PersistedState persisted;
    private final Set<DestinationLaneId> recoveryServed = new HashSet<>();
    /** Exact READY head last admitted to this process, including a polled head awaiting Claim. */
    private final Map<DestinationLaneId, DiscoveredHead> discoveredHeads = new HashMap<>();
    private long ringGeneration;
    private byte[] lastScannedReadyKey;
    private long wrapGeneration;
    private boolean recoveryFirstPass = true;
    private boolean persistedRestored;

    public PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate) {
        this(store, delegate, defaultOwner(store), System::nanoTime);
    }

    public PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate,
                                   final OwnerIdentityV1 owner) {
        this(store, delegate, owner, System::nanoTime);
    }

    PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate,
                             final OwnerIdentityV1 owner, final LongSupplier clockNanos) {
        this.store = Objects.requireNonNull(store, "store");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.clockNanos = Objects.requireNonNull(clockNanos, "clockNanos");
        this.persisted = load(store);
        this.ringGeneration = persisted == null ? 0 : persisted.activeRing().ringGeneration();
        this.lastScannedReadyKey = persisted == null ? null : persisted.discovery().lastScannedReadyKey();
        this.wrapGeneration = persisted == null ? 0 : persisted.discovery().wrapGeneration();
    }

    public static PersistentLaneScheduler defaults(final ShardStore store) {
        return new PersistentLaneScheduler(store, LaneScheduler.defaults());
    }

    /** Returns the physical shard whose READY and fairness projections this scheduler owns. */
    public ShardId shardId() {
        return store.shardId();
    }

    /** Returns the immutable Owner identity persisted in SchedulerRoundV1. */
    public OwnerIdentityV1 ownerIdentity() {
        return owner;
    }

    public synchronized void register(final LaneRecord lane) {
        Objects.requireNonNull(lane, "lane");
        delegate.register(lane);
        // Keep the registry update after the delegate's identity fence. A
        // rejected incarnation must not replace the state used to persist
        // scheduler projections.
        registered.put(lane.laneId(), lane);
    }

    /** Applies the saved projections after all currently active lanes are registered. */
    public synchronized void restorePersistedState() {
        if (persisted == null) {
            discoveredHeads.clear();
            persistedRestored = true;
            return;
        }
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            final List<DestinationLaneId> order = persisted.activeRing().entries().stream()
                    .filter(this::matchesRegisteredLane)
                    .map(SchedulerProjectionsV1.RingEntry::laneId)
                    .toList();
            final Map<LaneKey, SchedulerProjectionsV1.DeficitEntry> deficits = new HashMap<>();
            for (SchedulerProjectionsV1.DeficitEntry entry : persisted.deficitMap().entries()) {
                // Deficit is a physical Lane-version projection. A same-key
                // Lane that has advanced its runtime version must not inherit
                // the old cap/credit state after a restart.
                if (matchesRegisteredLane(entry.laneId(), entry.laneIncarnation(), entry.observedLaneVersion())) {
                    deficits.put(new LaneKey(entry.laneId(), entry.laneIncarnation()), entry);
                }
            }
            final Map<LaneKey, SchedulerProjectionsV1.LastServedEntry> lastServed = new HashMap<>();
            for (SchedulerProjectionsV1.LastServedEntry entry : persisted.lastServedMap().entries()) {
                lastServed.put(new LaneKey(entry.laneId(), entry.laneIncarnation()), entry);
            }
            // Fairness counters are persisted for every registered Lane, not
            // only the currently active ring.  A blocked/paused Lane may be
            // absent from the ring but must retain its service-gap state when it
            // becomes READY again after restart.
            final List<LaneScheduler.LaneSnapshot> snapshots = delegate.snapshot().lanes().stream()
                    .map(snapshot -> {
                        final LaneRecord lane = registered.get(snapshot.laneId());
                        final byte[] incarnation = lane == null ? new byte[16] : lane.laneIncarnation();
                        final LaneKey key = new LaneKey(snapshot.laneId(), incarnation);
                        final SchedulerProjectionsV1.DeficitEntry deficit = deficits.get(key);
                        final SchedulerProjectionsV1.LastServedEntry served = lastServed.get(key);
                        return new LaneScheduler.LaneSnapshot(snapshot.laneId(), snapshot.weight(),
                                deficit == null ? 0 : deficit.deficitBytes(),
                                served == null ? 0 : served.lastServedRound(), snapshot.pendingItems(),
                                snapshot.schedulable());
                    }).toList();
            // Validate and apply the complete counter projection before changing
            // the active ring. A malformed persisted generation must not leave a
            // newly registered scheduler with a partially restored ring.
            delegate.restore(new LaneScheduler.SchedulerSnapshot(persisted.activeRing().nextIndex(),
                    persisted.round().roundGeneration(), snapshots));
            delegate.rebuildActiveRing(order);
            final boolean ownerChanged = !persisted.round().owner().equals(owner);
            recoveryFirstPass = ownerChanged || persisted.round().recoveryFirstPass();
            recoveryServed.clear();
            discoveredHeads.clear();
            persistedRestored = true;
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), null, failure, null);
            throw failure;
        }
    }

    /**
     * Rebuilds the scheduler from the authoritative Lane and READY indexes.
     * This method is intended for a fenced owner only: a stale or orphaned
     * projection fails closed instead of being silently dropped.  The bound
     * must be at least the certified maximum number of READY Lanes; one extra
     * entry is read internally to detect overflow.
     *
     * @return the number of READY heads installed in the in-memory scheduler
     */
    public synchronized int rebuildFromAuthoritativeReady(final int maxReadyEntries) {
        if (maxReadyEntries <= 0) {
            throw new IllegalArgumentException("maxReadyEntries must be positive");
        }
        if (!persistedRestored) {
            restorePersistedState();
        }
        final RuntimeSnapshot before = runtimeSnapshot();
        final Map<DestinationLaneId, List<ScheduleWorkItem>> queuesBefore = delegate.queueSnapshot();
        try {
            final int scanLimit = maxReadyEntries == Integer.MAX_VALUE
                    ? Integer.MAX_VALUE : Math.addExact(maxReadyEntries, 1);
            // Recovery is a complete bounded pass over the authoritative READY
            // namespace.  The rotating discovery cursor is for steady-state
            // promotion only; using it here could consume the cursor entry as
            // look-ahead, fill the remaining bound from the wrapped prefix, and
            // silently omit a READY head without detecting overflow.
            final List<ShardStore.KeyValue> entries = scanAllReadyEntries(scanLimit);
            if (entries.size() > maxReadyEntries) {
                throw new IllegalStateException("READY index exceeds scheduler recovery bound");
            }
            final Map<DestinationLaneId, ScheduleWorkItem> byLane = new HashMap<>();
            final Map<DestinationLaneId, DiscoveredHead> discovered = new HashMap<>();
            final List<DestinationLaneId> activeOrder = new ArrayList<>();
            for (ShardStore.KeyValue entry : entries) {
                final ReadyProjection projection = decodeReadyProjection(entry);
                if (byLane.put(projection.lane().laneId(), projection.item()) != null) {
                    throw new IllegalStateException("multiple READY heads for Lane: "
                            + projection.lane().laneId());
                }
                discovered.put(projection.lane().laneId(),
                        new DiscoveredHead(projection.item(), projection.readyKey()));
                activeOrder.add(projection.lane().laneId());
            }
            delegate.replacePending(new ArrayList<>(byLane.values()));
            delegate.rebuildActiveRing(activeOrder);
            discoveredHeads.clear();
            discoveredHeads.putAll(discovered);
            recoveryFirstPass = true;
            recoveryServed.clear();
            if (entries.isEmpty()) {
                lastScannedReadyKey = null;
            } else {
                lastScannedReadyKey = entries.get(entries.size() - 1).key();
            }
            persist();
            return byLane.size();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), null, failure, queuesBefore);
            throw failure;
        }
    }

    /**
     * Promotes a bounded rotating READY-index slice into the active Lane ring.
     *
     * <p>The READY index is authoritative; a head already present in the
     * in-memory queue, or a head that was polled but is still awaiting its
     * Claim result, is not offered a second time.  A changed head for the same
     * Lane replaces that process-local discovery record after the caller has
     * removed the old READY projection from the Store.</p>
     *
     * @return newly promoted work items in discovery order
     */
    public synchronized List<ScheduleWorkItem> discoverReady(final SchedulerBudget budget) {
        // The legacy overload intentionally keeps its historical unbounded
        // discovery behavior.  Production callers must provide the trusted
        // due-through timestamp below.
        return discoverReady(Long.MAX_VALUE, budget);
    }

    /**
     * Promotes READY heads from the authoritative index while returning only
     * heads whose absolute eligibility is at or before the trusted due-through
     * time. Future heads may be retained in the in-memory queue so a later
     * turn can serve them without relying on a second discovery pass; the
     * downstream time-aware poll still fences them. Equality is due and
     * therefore allowed.
     */
    public synchronized List<ScheduleWorkItem> discoverReady(final long dueThroughEpochMs,
                                                              final SchedulerBudget budget) {
        return discoverReady(dueThroughEpochMs, null, budget);
    }

    /**
     * Production READY discovery with the complete trusted-time interval.
     * Besides using its earliest bound for due eligibility, this path binds
     * every typed READY certificate to the current scheduler Owner, Store
     * Incarnation and latest pre-expiry bound before promoting a head.
     */
    public synchronized List<ScheduleWorkItem> discoverReady(final TrustedUtcIntervalEvidence evidence,
                                                              final SchedulerBudget budget) {
        final TrustedUtcIntervalEvidence trusted = Objects.requireNonNull(evidence, "evidence");
        return discoverReady(trusted.earliestEpochMs(), trusted, budget);
    }

    private List<ScheduleWorkItem> discoverReady(final long dueThroughEpochMs,
                                                  final TrustedUtcIntervalEvidence evidence,
                                                  final SchedulerBudget budget) {
        requireDueThrough(dueThroughEpochMs);
        Objects.requireNonNull(budget, "budget");
        if (!persistedRestored) {
            restorePersistedState();
        }
        final RuntimeSnapshot before = runtimeSnapshot();
        final List<ScheduleWorkItem> offered = new ArrayList<>();
        try {
            final long startedNanos = readClock();
            final ReadyScan readyScan = scanReadyEntries(budget.maxMessages());
            final List<ShardStore.KeyValue> entries = readyScan.entries();
            final List<ReadyProjection> projections = new ArrayList<>();
            final Set<DestinationLaneId> scannedLanes = new HashSet<>();
            long scannedBytes = 0;
            byte[] lastEligibleReadyKey = null;
            for (ShardStore.KeyValue entry : entries) {
                if (elapsedSince(startedNanos, readClock()) >= budget.maxElapsedNanos()) {
                    break;
                }
                final long entryBytes = Math.addExact(entry.key().length, entry.value().length);
                // A valid READY projection must fit every certified scheduler
                // byte cap.  Do not make a first-entry exception that bypasses
                // the cap; activation/configuration is responsible for proving
                // that admitted work and its durable projection fit.
                if (entryBytes > budget.maxBytes()) {
                    throw new IllegalStateException("READY discovery entry exceeds byte budget");
                }
                if (entryBytes > budget.maxBytes() - scannedBytes) {
                    break;
                }
                scannedBytes = Math.addExact(scannedBytes, entryBytes);
                final ReadyProjection projection = decodeReadyProjection(entry, evidence);
                if (!scannedLanes.add(projection.lane().laneId())) {
                    throw new IllegalStateException("multiple READY heads discovered for Lane: "
                            + projection.lane().laneId());
                }
                projections.add(projection);
                if (projection.item().eligibleAtEpochMs() <= dueThroughEpochMs) {
                    lastEligibleReadyKey = entry.key();
                }
            }

            final List<ScheduleWorkItem> toOffer = new ArrayList<>();
            final List<ScheduleWorkItem> newlyPromoted = new ArrayList<>();
            final Map<DestinationLaneId, DiscoveredHead> nextHeads = new HashMap<>();
            for (ReadyProjection projection : projections) {
                final DestinationLaneId laneId = projection.lane().laneId();
                final ScheduleWorkItem item = projection.item();
                final ScheduleWorkItem queued = delegate.pendingHead(laneId);
                final DiscoveredHead known = discoveredHeads.get(laneId);
                if (queued != null) {
                    if (!sameWork(queued, item)) {
                        throw new IllegalStateException("in-memory READY head differs from authoritative READY: "
                                + laneId);
                    }
                    if (known != null && !sameHead(known, projection)) {
                        throw new IllegalStateException("in-memory READY key differs from authoritative READY: "
                                + laneId);
                    }
                    nextHeads.put(laneId, new DiscoveredHead(queued, projection.readyKey()));
                    continue;
                }
                if (known != null && sameHead(known, projection)) {
                    nextHeads.put(laneId, known);
                    continue;
                }
                nextHeads.put(laneId, new DiscoveredHead(item, projection.readyKey()));
                newlyPromoted.add(item);
                if (item.eligibleAtEpochMs() <= dueThroughEpochMs) {
                    toOffer.add(item);
                }
            }
            for (ScheduleWorkItem item : newlyPromoted) {
                delegate.activateLane(item.laneId());
                delegate.offer(item);
                offered.add(item);
            }
            // Do not consume a future READY key in the durable cursor.  The
            // future item may be retained in this process-local queue, but a
            // restart must be able to rediscover it before its due turn.  READY
            // keys are ordered by eligibility, so the last eligible key is the
            // safe cursor boundary for this time-bounded discovery turn.
            if (lastEligibleReadyKey != null) {
                lastScannedReadyKey = lastEligibleReadyKey;
            }
            discoveredHeads.putAll(nextHeads);
            if (lastEligibleReadyKey != null) {
                final long nextWrapGeneration = readyScan.wrapped()
                        ? incrementWrapGeneration(wrapGeneration) : wrapGeneration;
                persist(nextWrapGeneration);
            }
            return List.copyOf(toOffer);
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), offered, null, failure, null);
            throw failure;
        }
    }

    /** Returns the current durable discovery cursor projection. */
    public synchronized SchedulerProjectionsV1.ReadyDiscoveryCursor discoveryCursor() {
        return new SchedulerProjectionsV1.ReadyDiscoveryCursor(lastScannedReadyKey, wrapGeneration,
                Math.max(1, ringGeneration));
    }

    public synchronized void offer(final ScheduleWorkItem item) {
        delegate.offer(item);
    }

    public synchronized List<ScheduleWorkItem> poll(final SchedulerBudget budget) {
        // Compatibility overload; production scheduling must pass the
        // trusted due-through timestamp below.
        return poll(Long.MAX_VALUE, budget);
    }

    /** Polls only work that is due through the supplied trusted time. */
    public synchronized List<ScheduleWorkItem> poll(final long dueThroughEpochMs,
                                                     final SchedulerBudget budget) {
        requireDueThrough(dueThroughEpochMs);
        Objects.requireNonNull(budget, "budget");
        final RuntimeSnapshot before = runtimeSnapshot();
        List<ScheduleWorkItem> result = List.of();
        try {
            if (recoveryFirstPass) {
                final Set<DestinationLaneId> eligible = dueSchedulableLanesWithinBudget(
                        dueThroughEpochMs, budget.maxBytes());
                recoveryServed.retainAll(eligible);
                result = delegate.pollRecoveryFirstPass(dueThroughEpochMs, budget, recoveryServed);
                result.forEach(item -> recoveryServed.add(item.laneId()));
                if (!eligible.isEmpty() && recoveryServed.containsAll(eligible)) {
                    recoveryFirstPass = false;
                    recoveryServed.clear();
                }
            } else {
                result = delegate.poll(dueThroughEpochMs, budget);
            }
            persist();
            return result;
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, result, List.of(), null, failure, null);
            throw failure;
        }
    }

    /**
     * A due head that is larger than this turn's global byte budget cannot be
     * claimed in this turn.  It must not keep the recovery first pass open and
     * thereby prevent smaller healthy lanes from receiving later turns.
     */
    private Set<DestinationLaneId> dueSchedulableLanesWithinBudget(final long dueThroughEpochMs,
                                                                    final long maximumHeadBytes) {
        if (maximumHeadBytes <= 0) {
            throw new IllegalArgumentException("maximum recovery head bytes must be positive");
        }
        return delegate.dueSchedulableLanes(dueThroughEpochMs).stream()
                .filter(laneId -> {
                    final ScheduleWorkItem head = delegate.pendingHead(laneId);
                    return head != null && head.accountedBytes() <= maximumHeadBytes;
                })
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private void requireRegisteredLane(final DestinationLaneId laneId) {
        final LaneRecord lane = registered.get(Objects.requireNonNull(laneId, "laneId"));
        if (lane == null) {
            throw new IllegalArgumentException("lane is not registered: " + laneId);
        }
    }

    private RuntimeSnapshot runtimeSnapshot() {
        return new RuntimeSnapshot(delegate.snapshot(), delegate.ringOrder(), new HashMap<>(discoveredHeads),
                lastScannedReadyKey == null ? null : Bytes.copy(lastScannedReadyKey), ringGeneration,
                wrapGeneration, recoveryFirstPass, new HashSet<>(recoveryServed), delegate.readinessSnapshot());
    }

    /**
     * Restores process state after a durable scheduler projection failed.  The
     * original failure remains the primary error; an inability to roll back is
     * attached so the caller never mistakes a partially restored registry for
     * a successful scheduler turn.
     */
    private void rollbackRuntime(final RuntimeSnapshot snapshot,
                                 final List<ScheduleWorkItem> polled,
                                 final List<ScheduleWorkItem> offered,
                                 final DestinationLaneId restoreLaneId,
                                 final Throwable original,
                                 final Map<DestinationLaneId, List<ScheduleWorkItem>> queueSnapshot) {
        try {
            if (!offered.isEmpty()) {
                delegate.rollbackOffers(offered);
            }
            for (int index = polled.size() - 1; index >= 0; index--) {
                delegate.requeueFirst(polled.get(index));
            }
            if (queueSnapshot != null) {
                delegate.restoreQueues(queueSnapshot);
            }
            delegate.rebuildActiveRing(snapshot.ringOrder());
            delegate.restore(snapshot.schedulerSnapshot());
            discoveredHeads.clear();
            discoveredHeads.putAll(snapshot.discoveredHeads());
            lastScannedReadyKey = snapshot.lastScannedReadyKey() == null
                    ? null : Bytes.copy(snapshot.lastScannedReadyKey());
            ringGeneration = snapshot.ringGeneration();
            wrapGeneration = snapshot.wrapGeneration();
            recoveryFirstPass = snapshot.recoveryFirstPass();
            recoveryServed.clear();
            recoveryServed.addAll(snapshot.recoveryServed());
            delegate.restoreReadiness(snapshot.readiness());
        } catch (RuntimeException | Error rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }

    private static void requireDueThrough(final long dueThroughEpochMs) {
        if (dueThroughEpochMs < 0) {
            throw new IllegalArgumentException("scheduler due-through time must be non-negative");
        }
    }

    private long readClock() {
        final long now = clockNanos.getAsLong();
        if (now < 0 || (clockInitialized && now < lastClockNanos)) {
            throw new IllegalStateException("persistent scheduler clock must be monotonic and non-negative");
        }
        lastClockNanos = now;
        clockInitialized = true;
        return now;
    }

    private static long elapsedSince(final long start, final long end) {
        try {
            return Math.subtractExact(end, start);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    public synchronized void markBlocked(final DestinationLaneId laneId) {
        requireRegisteredLane(laneId);
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            delegate.markBlocked(laneId);
            delegate.deactivateLane(laneId);
            recoveryFirstPass = true;
            recoveryServed.clear();
            persist();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), laneId, failure, null);
            throw failure;
        }
    }

    /** Returns a registered Lane to evidence recovery before it can become READY. */
    public synchronized void markRecoveringEvidence(final DestinationLaneId laneId) {
        requireRegisteredLane(laneId);
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            delegate.markRecoveringEvidence(laneId);
            recoveryFirstPass = true;
            recoveryServed.clear();
            persist();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), laneId, failure, null);
            throw failure;
        }
    }

    public synchronized void markReady(final DestinationLaneId laneId) {
        requireRegisteredLane(laneId);
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            delegate.markReady(laneId);
            delegate.activateLane(laneId);
            recoveryFirstPass = true;
            recoveryServed.clear();
            persist();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), laneId, failure, null);
            throw failure;
        }
    }

    /**
     * Removes a source-ordered terminal Lane from memory and its persisted
     * fairness projections.  The exact-incarnation and terminal/empty-queue
     * checks are owned by the shard-local scheduler; this wrapper only
     * removes the corresponding registry and discovery entries after that
     * check succeeds.
     */
    public synchronized void unregister(final DestinationLaneId laneId,
                                        final byte[] laneIncarnation) {
        Objects.requireNonNull(laneId, "laneId");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        final LaneRecord lane = registered.get(laneId);
        if (lane == null) {
            throw new IllegalArgumentException("lane is not registered: " + laneId);
        }
        if (!Arrays.equals(lane.laneIncarnation(), laneIncarnation)) {
            throw new IllegalArgumentException("lane incarnation mismatch");
        }
        final LaneScheduler.SchedulerSnapshot before = delegate.snapshot();
        final List<DestinationLaneId> beforeRing = delegate.ringOrder();
        final Set<DestinationLaneId> beforeRecoveryServed = new HashSet<>(recoveryServed);
        final DiscoveredHead beforeHead = discoveredHeads.get(laneId);
        try {
            delegate.unregister(laneId, laneIncarnation);
            registered.remove(laneId);
            recoveryServed.remove(laneId);
            discoveredHeads.remove(laneId);
            persist();
        } catch (RuntimeException | Error failure) {
            // A failed WriteBatch must not leave this in-memory registry
            // ahead of the durable scheduler projection.  The terminal Lane
            // has no pending queue by contract, so its registration and
            // fairness snapshot can be restored without losing work.
            registered.put(laneId, lane);
            delegate.register(lane);
            delegate.restore(before);
            // The failed unregister may have re-registered a Lane that was
            // intentionally outside the active ring (for example a blocked
            // terminal Lane).  restoreRing() merges any currently registered
            // Lane not present in the saved order, which would silently
            // reactivate that Lane after a failed WriteBatch.  Rebuild the
            // exact prior active projection instead.
            delegate.rebuildActiveRing(beforeRing);
            recoveryServed.clear();
            recoveryServed.addAll(beforeRecoveryServed);
            if (beforeHead == null) {
                discoveredHeads.remove(laneId);
            } else {
                discoveredHeads.put(laneId, beforeHead);
            }
            throw failure;
        }
    }

    public synchronized void requeueFirst(final ScheduleWorkItem item) {
        delegate.requeueFirst(item);
    }

    public synchronized LaneScheduler.SchedulerSnapshot snapshot() {
        return delegate.snapshot();
    }

    public synchronized void persist() {
        persist(wrapGeneration);
    }

    private void persist(final long persistedWrapGeneration) {
        final LaneScheduler.SchedulerSnapshot snapshot = delegate.snapshot();
        final long nextRingGeneration = ringGeneration == Long.MAX_VALUE ? Long.MAX_VALUE : ringGeneration + 1;
        final long persistedRingGeneration = Math.max(1, nextRingGeneration);
        final List<SchedulerProjectionsV1.RingEntry> ringEntries = delegate.orderedSnapshot().stream()
                .map(state -> {
                    final LaneRecord lane = registered.get(state.laneId());
                    if (lane == null) {
                        throw new IllegalStateException("scheduler lane is not registered: " + state.laneId());
                    }
                    return new SchedulerProjectionsV1.RingEntry(state.laneId(), lane.laneIncarnation(),
                            observedVersion(lane));
                }).toList();
        final List<SchedulerProjectionsV1.DeficitEntry> deficits = snapshot.lanes().stream()
                .map(state -> {
                    final LaneRecord lane = registered.get(state.laneId());
                    if (lane == null) {
                        throw new IllegalStateException("scheduler lane is not registered: " + state.laneId());
                    }
                    return new SchedulerProjectionsV1.DeficitEntry(state.laneId(), lane.laneIncarnation(),
                            state.deficit(), observedVersion(lane));
                }).toList();
        final List<SchedulerProjectionsV1.LastServedEntry> lastServed = snapshot.lanes().stream()
                .map(state -> {
                    final LaneRecord lane = registered.get(state.laneId());
                    if (lane == null) {
                        throw new IllegalStateException("scheduler lane is not registered: " + state.laneId());
                    }
                    final long gap = snapshot.roundGeneration() >= state.lastServedRound()
                            ? snapshot.roundGeneration() - state.lastServedRound() : 0;
                    return new SchedulerProjectionsV1.LastServedEntry(state.laneId(), lane.laneIncarnation(),
                            state.lastServedRound(), gap);
                }).toList();
        final int nextIndex = ringEntries.isEmpty() ? 0 : snapshot.cursor() % ringEntries.size();
        final SchedulerProjectionsV1.ActiveRing activeRing = new SchedulerProjectionsV1.ActiveRing(
                persistedRingGeneration,
                snapshot.roundGeneration(), nextIndex, ringEntries);
        final SchedulerProjectionsV1.ReadyDiscoveryCursor discovery =
                new SchedulerProjectionsV1.ReadyDiscoveryCursor(lastScannedReadyKey, persistedWrapGeneration,
                        persistedRingGeneration);
        final SchedulerProjectionsV1.DeficitMap deficitMap = new SchedulerProjectionsV1.DeficitMap(deficits);
        final SchedulerProjectionsV1.Round round = new SchedulerProjectionsV1.Round(snapshot.roundGeneration(), owner,
                recoveryFirstPass);
        final SchedulerProjectionsV1.LastServedMap lastServedMap =
                new SchedulerProjectionsV1.LastServedMap(lastServed);
        store.write(batch -> {
            batch.putValue(ColumnFamily.META, VALUE_TYPE, KeyCodec.metaScheduler(1), discovery.canonicalBytes());
            batch.putValue(ColumnFamily.META, VALUE_TYPE, KeyCodec.metaScheduler(2), activeRing.canonicalBytes());
            batch.putValue(ColumnFamily.META, VALUE_TYPE, KeyCodec.metaScheduler(3), deficitMap.canonicalBytes());
            batch.putValue(ColumnFamily.META, VALUE_TYPE, KeyCodec.metaScheduler(4), round.canonicalBytes());
            batch.putValue(ColumnFamily.META, VALUE_TYPE, KeyCodec.metaScheduler(5), lastServedMap.canonicalBytes());
        });
        // Keep the in-memory generation projection behind the same successful
        // WriteBatch boundary as its durable counterpart. A failed write must
        // not make a retry describe a generation that never reached RocksDB.
        ringGeneration = persistedRingGeneration;
        wrapGeneration = persistedWrapGeneration;
    }

    private ReadyScan scanReadyEntries(final int limit) {
        final byte[] prefix = new byte[]{3, 1};
        final byte[] upper = new byte[]{4, 1};
        if (lastScannedReadyKey == null) {
            return new ReadyScan(store.scan(ColumnFamily.TIMELINE, prefix, upper, limit), false);
        }
        if (!hasPrefix(lastScannedReadyKey, prefix)) {
            throw new IllegalStateException("persisted READY discovery cursor is outside READY namespace");
        }
        final List<ShardStore.KeyValue> result = new ArrayList<>();
        // The lower bound is inclusive.  Read one extra entry so the cursor
        // itself does not consume the whole bounded slice when it is the
        // first key and the caller asks for a one-entry discovery turn.
        final int tailLimit = limit == Integer.MAX_VALUE ? limit : Math.addExact(limit, 1);
        final List<ShardStore.KeyValue> tail = store.scan(ColumnFamily.TIMELINE, lastScannedReadyKey, upper,
                tailLimit);
        for (ShardStore.KeyValue entry : tail) {
            if (!Arrays.equals(entry.key(), lastScannedReadyKey)) {
                result.add(entry);
            }
            if (result.size() == limit) {
                return new ReadyScan(List.copyOf(result), false);
            }
        }
        final int remaining = limit - result.size();
        if (remaining > 0) {
            final List<ShardStore.KeyValue> head = store.scan(ColumnFamily.TIMELINE, prefix,
                    lastScannedReadyKey, remaining);
            if (!head.isEmpty()) {
                result.addAll(head);
                return new ReadyScan(List.copyOf(result), true);
            }
        }
        return new ReadyScan(List.copyOf(result), false);
    }

    private static long incrementWrapGeneration(final long current) {
        return current == Long.MAX_VALUE ? Long.MAX_VALUE : current + 1;
    }

    private List<ShardStore.KeyValue> scanAllReadyEntries(final int limit) {
        final byte[] prefix = new byte[]{3, 1};
        final byte[] upper = new byte[]{4, 1};
        return store.scan(ColumnFamily.TIMELINE, prefix, upper, limit);
    }

    private ReadyProjection decodeReadyProjection(final ShardStore.KeyValue entry) {
        return decodeReadyProjection(entry, null);
    }

    private ReadyProjection decodeReadyProjection(final ShardStore.KeyValue entry,
                                                   final TrustedUtcIntervalEvidence evidence) {
        final ReadyKey key = decodeReadyKey(entry.key());
        final ReadyIndexValue value = ReadyIndexValue.decode(
                ValueEnvelope.decode(entry.value(), 3).payload());
        if (!key.laneId().equals(value.laneId()) || key.nextEligibleAtEpochMs() != value.nextEligibleAtEpochMs()
                || key.laneVersion() != value.laneVersion()) {
            throw new IllegalStateException("READY key/value identity mismatch during scheduler rebuild");
        }
        final LaneRecord lane = registered.get(key.laneId());
        if (lane == null) {
            throw new IllegalStateException("READY Lane is not registered: " + key.laneId());
        }
        validateStoredLane(lane);
        if (!lane.schedulable() || lane.laneVersion() != key.laneVersion()
                || lane.nextEligibleAtEpochMs() != key.nextEligibleAtEpochMs()) {
            throw new IllegalStateException("stale or non-schedulable READY Lane: " + key.laneId());
        }
        final ValueEnvelope.Decoded messageValue = store.getValue(ColumnFamily.ID,
                KeyCodec.idMessage(value.messageId()), 1);
        if (messageValue == null) {
            throw new IllegalStateException("READY points to a missing message: " + value.messageId());
        }
        final MessageRecord message = MessageRecord.decode(messageValue.payload());
        if (!store.shardId().equals(value.messageId().routingId().shardId())) {
            throw new IllegalStateException("READY message key belongs to another Shard: " + value.messageId());
        }
        final var scheduleSourcePosition = SourcePositionCodec.decode(message.scheduleSourcePosition());
        if (!store.shardId().equals(scheduleSourcePosition.shardId())) {
            throw new IllegalStateException("READY message source position belongs to another Shard: "
                    + value.messageId());
        }
        if (message.status() != MessageStatus.SCHEDULED || message.generation() != value.generation()
                || !message.laneId().equals(key.laneId())) {
            throw new IllegalStateException("READY points to a non-current scheduled message: " + value.messageId());
        }
        final TimelineWorkRef currentWork = message.runtimeIndex().timeline();
        final long timelineEligibleAt = message.orderingMode()
                == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? message.deliverAtEpochMs()
                : Math.max(currentWork == null ? message.deliverAtEpochMs() : currentWork.actionAtEpochMs(),
                message.retryEligibilityAtEpochMs());
        final byte[] timelineKey = message.orderingMode()
                == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? KeyCodec.timelineOrdered(message.laneId(), timelineEligibleAt,
                scheduleSourcePosition.sourceOrderToken(), value.messageId(), message.generation())
                : KeyCodec.timelineDue(message.laneId(), timelineEligibleAt,
                scheduleSourcePosition.sourceOrderToken(), value.messageId(), message.generation());
        if (!Bytes.constantTimeEquals(value.timelineKeySha256(), Bytes.sha256(timelineKey))) {
            throw new IllegalStateException("READY timeline digest mismatch: " + value.messageId());
        }
        final byte[] timelineBytes = store.get(ColumnFamily.TIMELINE, timelineKey);
        if (timelineBytes == null) {
            throw new IllegalStateException("READY points to a missing timeline entry: " + value.messageId());
        }
        final TimelineWorkRef timeline = validateTimelineValue(ValueEnvelope.decode(timelineBytes, 1).payload(),
                value.messageId(), message, timelineKey);
        if (timeline != null && key.nextEligibleAtEpochMs() != Math.max(timeline.actionAtEpochMs(),
                timeline.retryEligibilityAtEpochMs())) {
            throw new IllegalStateException("READY eligibility disagrees with TimelineWorkRef: " + value.messageId());
        }
        final ActiveLaneStateV1 typedLane = readTypedLane(lane);
        if (typedLane != null) {
            validateTypedReadyProjection(typedLane, entry.key(), key, evidence);
            final long actionAt = timeline == null
                    ? (currentWork == null ? message.deliverAtEpochMs() : currentWork.actionAtEpochMs())
                    : timeline.actionAtEpochMs();
            if (typedLane.earliestActionAtEpochMs() == null
                    || typedLane.earliestActionAtEpochMs() != actionAt
                    || typedLane.nextEligibleAtEpochMs() == null
                    || typedLane.nextEligibleAtEpochMs() != key.nextEligibleAtEpochMs()) {
                throw new IllegalStateException("typed READY action/eligibility projection disagrees with current head: "
                        + value.messageId());
            }
        } else if (evidence != null) {
            throw new IllegalStateException("strict READY discovery requires a typed ACTIVE Lane projection");
        }
        final long accountedBytes = Math.max(1, message.payloadLength());
        return new ReadyProjection(lane, new ScheduleWorkItem(key.laneId(), value.messageId(), value.generation(),
                key.nextEligibleAtEpochMs(), accountedBytes), entry.key());
    }

    /**
     * Fences the physical READY index against the complete typed ACTIVE
     * projection.  The typed value is the durable witness that the Registry
     * Lane state and the scheduler index were advanced together; checking only
     * Lane/version/time fields would allow a future codec revision to omit the
     * key or certificate while still rebuilding a claimable head.
     */
    private void validateTypedReadyProjection(final ActiveLaneStateV1 state,
                                              final byte[] physicalReadyKey,
                                              final ReadyKey decodedReadyKey,
                                              final TrustedUtcIntervalEvidence evidence) {
        if (state.runtimeReadiness() != RuntimeReadiness.READY || state.admissionGate() != AdmissionGate.OPEN) {
            throw new IllegalStateException("typed READY projection belongs to a non-schedulable Lane");
        }
        final byte[] encodedReadyKey = state.encodedReadyKey();
        final byte[] readyCertificate = state.readyCertificate();
        if (encodedReadyKey == null || readyCertificate == null) {
            throw new IllegalStateException("typed READY projection is missing key or certificate");
        }
        if (!Arrays.equals(encodedReadyKey, physicalReadyKey)) {
            throw new IllegalStateException("typed READY key disagrees with physical READY index");
        }
        if (decodedReadyKey.nextEligibleAtEpochMs() != state.nextEligibleAtEpochMs()
                || decodedReadyKey.laneVersion() != state.laneVersion()
                || !decodedReadyKey.laneId().equals(state.laneId())) {
            throw new IllegalStateException("typed READY key fields disagree with Lane state");
        }
        try {
            final ReadyCertificateV1 certificate = ReadyCertificateV1.decode(readyCertificate);
            if (evidence != null) {
                validateLiveReadyCertificate(certificate, evidence);
            }
        } catch (IllegalArgumentException malformedCertificate) {
            throw new IllegalStateException("typed READY projection carries an invalid certificate", malformedCertificate);
        }
    }

    private void validateLiveReadyCertificate(final ReadyCertificateV1 certificate,
                                               final TrustedUtcIntervalEvidence evidence) {
        final byte[] expectedOwner = owner.canonicalBytes();
        if (!Arrays.equals(certificate.ownerIdentity(), expectedOwner)) {
            throw new IllegalArgumentException("READY certificate belongs to a different scheduler Owner");
        }
        if (!Arrays.equals(certificate.storeIncarnation(), store.metadata().storeIncarnation())) {
            throw new IllegalArgumentException("READY certificate belongs to a different Store Incarnation");
        }
        if (evidence.earliestEpochMs() < certificate.issuedAt().latestEpochMs()) {
            throw new IllegalArgumentException("READY discovery evidence predates certificate issuance");
        }
        if (evidence.latestEpochMs() >= certificate.validUntilEpochMs()) {
            throw new IllegalArgumentException("READY certificate is not live through the trusted UTC interval");
        }
    }

    private ActiveLaneStateV1 readTypedLane(final LaneRecord expected) {
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.META,
                KeyCodec.metaLane(expected.laneId()), 2);
        if (value == null) {
            throw new IllegalStateException("registered Lane disappeared during READY validation: " + expected.laneId());
        }
        final LaneRecordEnvelopeV1 envelope = LaneRecordEnvelopeV1.decode(value.payload());
        return envelope.isActive() ? envelope.typedActiveState().orElse(null) : null;
    }

    private static TimelineWorkRef validateTimelineValue(final byte[] encodedValue, final DelayMessageId messageId,
                                                         final MessageRecord message, final byte[] expectedTimelineKey) {
        if (encodedValue.length >= Integer.BYTES
                && java.nio.ByteBuffer.wrap(encodedValue, 0, Integer.BYTES).getInt() == 1) {
            final TimelineEntry legacy = TimelineEntry.decode(encodedValue);
            if (!legacy.messageId().equals(messageId) || legacy.generation() != message.generation()) {
                throw new IllegalStateException("legacy READY timeline identity mismatch: " + messageId);
            }
            return null;
        }
        final TimelineWorkRef work = TimelineWorkRef.decode(encodedValue);
        if (!Arrays.equals(work.encodedTimelineKey(), expectedTimelineKey)) {
            throw new IllegalStateException("READY TimelineWorkRef key mismatch: " + messageId);
        }
        final TimelineWorkRef current = message.runtimeIndex().timeline();
        if (current != null && !Arrays.equals(current.canonicalBytes(), work.canonicalBytes())) {
            throw new IllegalStateException("READY TimelineWorkRef disagrees with Message runtime: " + messageId);
        }
        if (current == null && (work.retryEligibilityAtEpochMs() != message.retryEligibilityAtEpochMs()
                || work.orderedHeadBlocking()
                != (message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO)
                || work.actionAtEpochMs() > message.deliverAtEpochMs())) {
            throw new IllegalStateException("READY TimelineWorkRef disagrees with legacy Message: " + messageId);
        }
        return work;
    }

    private static boolean sameWork(final ScheduleWorkItem left, final ScheduleWorkItem right) {
        return left.laneId().equals(right.laneId()) && left.messageId().equals(right.messageId())
                && left.generation() == right.generation() && left.eligibleAtEpochMs() == right.eligibleAtEpochMs()
                && left.accountedBytes() == right.accountedBytes();
    }

    private static boolean sameHead(final DiscoveredHead known, final ReadyProjection projection) {
        return sameWork(known.item(), projection.item())
                && Arrays.equals(known.readyKey(), projection.readyKey());
    }

    private void validateStoredLane(final LaneRecord expected) {
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.META, KeyCodec.metaLane(expected.laneId()), 2);
        if (value == null) {
            throw new IllegalStateException("registered Lane is missing from meta_cf: " + expected.laneId());
        }
        final byte[] payload = value.payload();
        if (payload.length >= 4 && payload[0] == 0) {
            assertLaneMatches(expected, LaneRecord.decode(payload));
            return;
        }
        final LaneRecordEnvelopeV1 envelope = LaneRecordEnvelopeV1.decode(payload);
        if (!envelope.isActive()) {
            throw new IllegalStateException("READY Lane is terminal: " + expected.laneId());
        }
        final java.util.Optional<ActiveLaneStateV1> typed = envelope.typedActiveState();
        if (typed.isPresent()) {
            final ActiveLaneStateV1 state = typed.orElseThrow();
            if (!state.laneId().equals(expected.laneId())
                    || !Arrays.equals(state.laneIncarnation(), expected.laneIncarnation())
                    || state.laneControlVersion() != expected.laneControlVersion()
                    || state.laneVersion() != expected.laneVersion()
                    || state.admissionGate() != expected.admissionGate()
                    || state.runtimeReadiness() != expected.runtimeReadiness()
                    || state.schedulerWeight() != expected.weight()
                    || state.nextEligibleAtEpochMs() == null
                    || state.nextEligibleAtEpochMs() != expected.nextEligibleAtEpochMs()) {
                throw new IllegalStateException("registered Lane differs from typed meta_cf state: " + expected.laneId());
            }
            return;
        }
        assertLaneMatches(expected, LaneRecord.decode(envelope.activeStateBytes()));
    }

    private static void assertLaneMatches(final LaneRecord expected, final LaneRecord actual) {
        if (!actual.laneId().equals(expected.laneId())
                || !Arrays.equals(actual.laneIncarnation(), expected.laneIncarnation())
                || actual.laneControlVersion() != expected.laneControlVersion()
                || actual.laneVersion() != expected.laneVersion()
                || actual.admissionGate() != expected.admissionGate()
                || actual.runtimeReadiness() != expected.runtimeReadiness()
                || actual.weight() != expected.weight()
                || actual.nextEligibleAtEpochMs() != expected.nextEligibleAtEpochMs()) {
            throw new IllegalStateException("registered Lane differs from meta_cf state: " + expected.laneId());
        }
    }

    private static ReadyKey decodeReadyKey(final byte[] key) {
        if (key.length != 2 + 8 + DestinationLaneId.LENGTH + 8 || key[0] != 3 || key[1] != 1) {
            throw new IllegalStateException("invalid READY key during scheduler rebuild");
        }
        final java.nio.ByteBuffer input = java.nio.ByteBuffer.wrap(key);
        input.position(2);
        final long eligibleAt = input.getLong();
        final byte[] lane = new byte[DestinationLaneId.LENGTH];
        input.get(lane);
        return new ReadyKey(new DestinationLaneId(lane), eligibleAt, input.getLong());
    }

    private static boolean hasPrefix(final byte[] value, final byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int index = 0; index < prefix.length; index++) {
            if (value[index] != prefix[index]) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesRegisteredLane(final SchedulerProjectionsV1.RingEntry entry) {
        return matchesRegisteredLane(entry.laneId(), entry.laneIncarnation(), entry.observedLaneVersion());
    }

    private boolean matchesRegisteredLane(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                          final long observedLaneVersion) {
        final LaneRecord lane = registered.get(laneId);
        return lane != null && Arrays.equals(lane.laneIncarnation(), laneIncarnation)
                && observedVersion(lane) == observedLaneVersion;
    }

    private static long observedVersion(final LaneRecord lane) {
        return Math.max(1, lane.laneVersion());
    }

    private static PersistedState load(final ShardStore store) {
        final var discovery = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(1), VALUE_TYPE);
        final var activeRing = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(2), VALUE_TYPE);
        final var deficits = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(3), VALUE_TYPE);
        final var round = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(4), VALUE_TYPE);
        final var lastServed = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(5), VALUE_TYPE);
        final boolean any = discovery != null || activeRing != null || deficits != null || round != null || lastServed != null;
        if (!any) {
            return null;
        }
        if (discovery == null || activeRing == null || deficits == null || round == null || lastServed == null) {
            throw new IllegalStateException("scheduler projections are incomplete");
        }
        final SchedulerProjectionsV1.ReadyDiscoveryCursor decodedDiscovery =
                SchedulerProjectionsV1.ReadyDiscoveryCursor.decode(discovery.payload());
        final SchedulerProjectionsV1.ActiveRing decodedActiveRing =
                SchedulerProjectionsV1.ActiveRing.decode(activeRing.payload());
        final SchedulerProjectionsV1.Round decodedRound =
                SchedulerProjectionsV1.Round.decode(round.payload());
        if (decodedDiscovery.activeRingGeneration() != decodedActiveRing.ringGeneration()
                || decodedActiveRing.roundGeneration() != decodedRound.roundGeneration()) {
            throw new IllegalStateException("scheduler projection generations disagree");
        }
        return new PersistedState(decodedDiscovery, decodedActiveRing,
                SchedulerProjectionsV1.DeficitMap.decode(deficits.payload()),
                decodedRound,
                SchedulerProjectionsV1.LastServedMap.decode(lastServed.payload()));
    }

    private static OwnerIdentityV1 defaultOwner(final ShardStore store) {
        Objects.requireNonNull(store, "store");
        final byte[] worker = Bytes.concat(store.shardId().routeIncarnation().bytes(),
                Bytes.u32beBits(store.shardId().partition()));
        return new OwnerIdentityV1(Bytes.utf8("embedded-scheduler"), worker, 1,
                Bytes.sha256(Bytes.utf8("nereus-delay-embedded-scheduler-owner-v1\0"), worker));
    }

    private record LaneKey(DestinationLaneId laneId, byte[] incarnation) {
        private LaneKey {
            incarnation = Bytes.copy(incarnation);
        }

        @Override
        public byte[] incarnation() {
            return Bytes.copy(incarnation);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof LaneKey that && laneId.equals(that.laneId)
                    && Arrays.equals(incarnation, that.incarnation);
        }

        @Override
        public int hashCode() {
            return 31 * laneId.hashCode() + Arrays.hashCode(incarnation);
        }
    }

    private record PersistedState(SchedulerProjectionsV1.ReadyDiscoveryCursor discovery,
                                  SchedulerProjectionsV1.ActiveRing activeRing,
                                  SchedulerProjectionsV1.DeficitMap deficitMap,
                                  SchedulerProjectionsV1.Round round,
                                  SchedulerProjectionsV1.LastServedMap lastServedMap) {
    }

    private record RuntimeSnapshot(LaneScheduler.SchedulerSnapshot schedulerSnapshot,
                                   List<DestinationLaneId> ringOrder,
                                   Map<DestinationLaneId, DiscoveredHead> discoveredHeads,
                                   byte[] lastScannedReadyKey,
                                   long ringGeneration,
                                   long wrapGeneration,
                                   boolean recoveryFirstPass,
                                   Set<DestinationLaneId> recoveryServed,
                                   Map<DestinationLaneId, RuntimeReadiness> readiness) {
        private RuntimeSnapshot {
            Objects.requireNonNull(schedulerSnapshot, "schedulerSnapshot");
            ringOrder = List.copyOf(ringOrder);
            discoveredHeads = Map.copyOf(discoveredHeads);
            lastScannedReadyKey = lastScannedReadyKey == null ? null : Bytes.copy(lastScannedReadyKey);
            recoveryServed = Set.copyOf(recoveryServed);
            readiness = Map.copyOf(readiness);
        }

        @Override
        public byte[] lastScannedReadyKey() {
            return lastScannedReadyKey == null ? null : Bytes.copy(lastScannedReadyKey);
        }
    }

    private record ReadyKey(DestinationLaneId laneId, long nextEligibleAtEpochMs, long laneVersion) {
    }

    private record ReadyProjection(LaneRecord lane, ScheduleWorkItem item, byte[] readyKey) {
        private ReadyProjection {
            readyKey = Bytes.copy(readyKey);
        }

        @Override
        public byte[] readyKey() {
            return Bytes.copy(readyKey);
        }
    }

    private record DiscoveredHead(ScheduleWorkItem item, byte[] readyKey) {
        private DiscoveredHead {
            readyKey = Bytes.copy(readyKey);
        }

        @Override
        public byte[] readyKey() {
            return Bytes.copy(readyKey);
        }
    }

    private record ReadyScan(List<ShardStore.KeyValue> entries, boolean wrapped) {
        private ReadyScan {
            entries = List.copyOf(entries);
        }
    }
}
