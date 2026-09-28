package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.LaneRecordEnvelope;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.SchedulerProjections;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.AdmissionGate;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.NativeCandidateRef;
import com.nereusstream.delay.runtime.ReadyIndexValue;
import com.nereusstream.delay.runtime.RuntimeReadiness;
import com.nereusstream.delay.runtime.TimelineEntry;
import com.nereusstream.delay.runtime.TimelineWorkRef;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Process-local fairness wrapper for a shard-local {@link LaneScheduler}.
 *
 * <p>The five closed Registry scheduler projections are read only to validate
 * legacy state. Lane records and timeline work remain authoritative in their
 * own keys; fairness counters, the active ring, and the discovery cursor are
 * rebuilt as process state.</p>
 */
public final class PersistentLaneScheduler {
    private static final int VALUE_TYPE = 5;
    // READY + Lane + Message + timeline + typed Lane, plus native Message,
    // native timeline and binding. maxMessages remains a logical head limit;
    // every dependent physical record is also charged to this derived cap.
    private static final int MAX_RECORDS_PER_READY_PROJECTION = 8;
    private final ShardStore store;
    private final LaneScheduler delegate;
    private final OwnerIdentity owner;
    private final LongSupplier clockNanos;
    private final ManagedNativeEligibilityAuthority nativeEligibilityAuthority;
    private long lastClockNanos;
    private boolean clockInitialized;
    private final Map<DestinationLaneId, LaneRecord> registered = new HashMap<>();
    private final Set<DestinationLaneId> recoveryServed = new HashSet<>();
    /** Exact READY head last admitted to this process, including a polled head awaiting Claim. */
    private final Map<DestinationLaneId, DiscoveredHead> discoveredHeads = new HashMap<>();

    private byte[] lastScannedReadyKey;
    private long wrapGeneration;
    private boolean recoveryFirstPass = true;
    private boolean processStateInitialized;
    private DiscoveryReadStatistics lastDiscoveryRead = new DiscoveryReadStatistics(0, 0, 0, 0, null);

    PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate) {
        this(store, delegate, defaultOwner(store), System::nanoTime, null);
    }

    public PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate, final OwnerIdentity owner) {
        this(store, delegate, owner, System::nanoTime, null);
    }

    public PersistentLaneScheduler(
            final ShardStore store,
            final LaneScheduler delegate,
            final OwnerIdentity owner,
            final ManagedNativeEligibilityAuthority nativeEligibilityAuthority) {
        this(store, delegate, owner, System::nanoTime, nativeEligibilityAuthority);
    }

    PersistentLaneScheduler(
            final ShardStore store,
            final LaneScheduler delegate,
            final OwnerIdentity owner,
            final LongSupplier clockNanos) {
        this(store, delegate, owner, clockNanos, null);
    }

    PersistentLaneScheduler(
            final ShardStore store,
            final LaneScheduler delegate,
            final OwnerIdentity owner,
            final LongSupplier clockNanos,
            final ManagedNativeEligibilityAuthority nativeEligibilityAuthority) {
        this.store = Objects.requireNonNull(store, "store");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.clockNanos = Objects.requireNonNull(clockNanos, "clockNanos");
        this.nativeEligibilityAuthority = nativeEligibilityAuthority;
        // Decode legacy projections only to fail closed on malformed or
        // partially migrated state. Fairness and discovery cursors belong to
        // this process and always start from a fresh recovery pass.
        validateLegacyProjections(store);
        this.lastScannedReadyKey = null;
        this.wrapGeneration = 0;
    }

    static PersistentLaneScheduler defaults(final ShardStore store) {
        return new PersistentLaneScheduler(store, LaneScheduler.defaults());
    }

    /**
     * Creates the scheduler projection for an accepted active-owner
     * composition. The supplied Lane records must come from the same
     * source-ordered Route/Registry projection that activated the shard; this
     * method only registers those records and validates legacy scheduler
     * projections. Fairness restarts as process state. The owning Worker must
     * still perform the strict Owner/Store check before calling the
     * recovery-bound rebuild entrypoint.
     */
    public static PersistentLaneScheduler forActiveOwner(
            final ShardStore store, final OwnerIdentity owner, final List<LaneRecord> activeLanes) {
        return forActiveOwner(store, owner, activeLanes, null);
    }

    /** Active-owner composition with the live current-policy authority required for Managed Handoff. */
    public static PersistentLaneScheduler forActiveOwner(
            final ShardStore store,
            final OwnerIdentity owner,
            final List<LaneRecord> activeLanes,
            final ManagedNativeEligibilityAuthority nativeEligibilityAuthority) {
        final PersistentLaneScheduler scheduler = new PersistentLaneScheduler(
                Objects.requireNonNull(store, "store"),
                LaneScheduler.defaults(),
                Objects.requireNonNull(owner, "owner"),
                nativeEligibilityAuthority);
        for (LaneRecord lane : List.copyOf(Objects.requireNonNull(activeLanes, "activeLanes"))) {
            scheduler.register(Objects.requireNonNull(lane, "active lane"));
        }
        scheduler.initializeProcessState();
        return scheduler;
    }

    /**
     * Rebuilds the in-memory READY ring from the authoritative Store after
     * the Worker has proved the active Owner/Store binding. The low-level
     * recovery method remains package-local so callers cannot accidentally
     * bypass that production lifecycle gate.
     */
    public synchronized int rebuildAuthoritativeReady(final int maxReadyEntries) {
        return rebuildFromAuthoritativeReady(maxReadyEntries);
    }

    /** Returns the physical shard whose READY projections this scheduler reads. */
    public ShardId shardId() {
        return store.shardId();
    }

    /** Returns the immutable Owner identity for this process-local scheduler interval. */
    public OwnerIdentity ownerIdentity() {
        return owner;
    }

    /** Returns the immutable physical Store Incarnation that owns the READY projection. */
    public byte[] storeIncarnation() {
        return store.metadata().storeIncarnation();
    }

    synchronized void register(final LaneRecord lane) {
        Objects.requireNonNull(lane, "lane");
        delegate.register(lane);
        // Keep the registry update after the delegate's identity fence. A
        // rejected incarnation must not replace the active Lane identity.
        registered.put(lane.laneId(), lane);
    }

    /** Starts fair service from fresh process state after legacy rows were validated. */
    synchronized void initializeProcessState() {
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            resetFairnessForRecovery();
            // Only live Lane registration/readiness and the authoritative
            // READY index rebuild the ring. Old ring order, credit and served
            // counters are intentionally not restored.
            discoveredHeads.clear();
            processStateInitialized = true;
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), null, failure, null);
            throw failure;
        }
    }

    private void resetFairnessForRecovery() {
        final List<LaneScheduler.LaneSnapshot> reset = delegate.snapshot().lanes().stream()
                .map(snapshot -> new LaneScheduler.LaneSnapshot(
                        snapshot.laneId(),
                        snapshot.weight(),
                        0,
                        0,
                        snapshot.pendingItems(),
                        snapshot.schedulable()))
                .toList();
        delegate.restore(new LaneScheduler.SchedulerSnapshot(0, 0, reset));
        lastScannedReadyKey = null;
        wrapGeneration = 0;
        recoveryFirstPass = true;
        recoveryServed.clear();
    }

    /**
     * Rebuilds the scheduler from the authoritative Lane and READY indexes.
     * This method is intended for a fenced owner only: a stale or orphaned
     * projection fails closed instead of being silently dropped. The bound
     * must be at least the certified maximum number of READY Lanes; one extra
     * entry is read internally to detect overflow.
     *
     * @return the number of READY heads installed in the in-memory scheduler
     */
    synchronized int rebuildFromAuthoritativeReady(final int maxReadyEntries) {
        if (maxReadyEntries <= 0) {
            throw new IllegalArgumentException("maxReadyEntries must be positive");
        }
        if (!processStateInitialized) {
            initializeProcessState();
        }
        final RuntimeSnapshot before = runtimeSnapshot();
        final Map<DestinationLaneId, List<ScheduleWorkItem>> queuesBefore = delegate.queueSnapshot();
        try {
            final int scanLimit =
                    maxReadyEntries == Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.addExact(maxReadyEntries, 1);
            // Recovery is a complete bounded pass over the authoritative READY
            // namespace. The rotating discovery cursor is for steady-state
            // promotion only; using it here could consume the cursor entry as
            // look-ahead, fill the remaining bound from the wrapped prefix, and
            // silently omit a READY head without detecting overflow.
            final List<ShardStore.KeyValue> entries = scanAllReadyEntries(scanLimit);
            if (entries.size() > maxReadyEntries) {
                throw new IllegalStateException("READY index exceeds scheduler recovery bound");
            }
            final Map<DestinationLaneId, List<ScheduleWorkItem>> byLane = new HashMap<>();
            final Map<DestinationLaneId, DiscoveredHead> discovered = new HashMap<>();
            final List<DestinationLaneId> activeOrder = new ArrayList<>();
            for (ShardStore.KeyValue entry : entries) {
                final ReadyProjection projection = decodeReadyProjection(entry);
                if (byLane.put(projection.lane().laneId(), projection.items()) != null) {
                    throw new IllegalStateException("multiple READY heads for Lane: "
                            + projection.lane().laneId());
                }
                discovered.put(
                        projection.lane().laneId(), new DiscoveredHead(projection.items(), projection.readyKey()));
                activeOrder.add(projection.lane().laneId());
            }
            final List<ScheduleWorkItem> pending = new ArrayList<>();
            byLane.values().forEach(pending::addAll);
            delegate.replacePending(pending);
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
     * Claim result, is not offered a second time. A changed head for the same
     * Lane replaces that process-local discovery record after the caller has
     * removed the old READY projection from the Store.</p>
     *
     * @return newly promoted work items in discovery order
     */
    synchronized List<ScheduleWorkItem> discoverReady(final SchedulerBudget budget) {
        // The legacy overload intentionally keeps its historical unbounded
        // discovery behavior. Production callers must provide the trusted
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
    synchronized List<ScheduleWorkItem> discoverReady(final long dueThroughEpochMs, final SchedulerBudget budget) {
        return discoverReady(dueThroughEpochMs, null, budget);
    }

    /**
     * Production READY discovery with the complete trusted-time interval.
     * Besides using its earliest bound for due eligibility, this path binds
     * every typed READY certificate to the current scheduler Owner, Store
     * Incarnation and latest pre-expiry bound before promoting a head.
     */
    public synchronized List<ScheduleWorkItem> discoverReady(
            final TrustedUtcIntervalEvidence evidence, final SchedulerBudget budget) {
        final TrustedUtcIntervalEvidence trusted = Objects.requireNonNull(evidence, "evidence");
        return discoverReady(trusted.earliestEpochMs(), trusted, budget);
    }

    private List<ScheduleWorkItem> discoverReady(
            final long dueThroughEpochMs, final TrustedUtcIntervalEvidence evidence, final SchedulerBudget budget) {
        requireDueThrough(dueThroughEpochMs);
        Objects.requireNonNull(budget, "budget");
        if (!processStateInitialized) {
            initializeProcessState();
        }
        final RuntimeSnapshot before = runtimeSnapshot();
        final List<ScheduleWorkItem> offered = new ArrayList<>();
        try {
            final BoundedReadBudget readBudget = new BoundedReadBudget(
                    (int) Math.min(Integer.MAX_VALUE, (long) budget.maxMessages() * MAX_RECORDS_PER_READY_PROJECTION),
                    budget.maxBytes(),
                    budget.maxElapsedNanos(),
                    this::readClock);
            try {
                final ShardStore.ReadPlan<ReadyScan> plan = store.readWithBudget(
                        readBudget, () -> readReadyProjections(budget.maxMessages(), evidence, readBudget));
                // Policy authority may consult another component (including its Shard
                // source position). Never invoke that callback under the Store monitor.
                final List<ReadyProjection> resolved = new ArrayList<>();
                for (ReadyProjection projection : plan.value().projections()) {
                    if (projection.nativeResolution() != null && !readBudget.beforeTimedWork()) {
                        break;
                    }
                    resolved.add(resolveNativeProjection(projection, evidence));
                }
                final ReadyScan completed = new ReadyScan(
                        resolved, Math.min(resolved.size(), plan.value().firstWrappedProjection()));
                return store.withReadView(
                        plan.view(), () -> publishReadyProjections(dueThroughEpochMs, completed, offered));
            } finally {
                lastDiscoveryRead = new DiscoveryReadStatistics(
                        readBudget.actualRecords(),
                        readBudget.actualBytes(),
                        readBudget.chargedBytes(),
                        readBudget.deniedReads(),
                        readBudget.exhaustion());
            }
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), offered, null, failure, null);
            throw failure;
        }
    }

    private List<ScheduleWorkItem> publishReadyProjections(
            final long dueThroughEpochMs,
            final ReadyScan readyScan,
            final List<ScheduleWorkItem> offered) {
        final List<ReadyProjection> projections = readyScan.projections();
        byte[] lastEligibleReadyKey = null;
        boolean eligibleCursorWrapped = false;
        for (int index = 0; index < projections.size(); index++) {
            final ReadyProjection projection = projections.get(index);
            if (projection.items().stream().anyMatch(item -> item.eligibleAtEpochMs() <= dueThroughEpochMs)) {
                lastEligibleReadyKey = projection.readyKey();
                eligibleCursorWrapped = index >= readyScan.firstWrappedProjection();
            }
        }
        final List<ScheduleWorkItem> toOffer = new ArrayList<>();
        final List<ScheduleWorkItem> newlyPromoted = new ArrayList<>();
        final Map<DestinationLaneId, DiscoveredHead> nextHeads = new HashMap<>();
        for (ReadyProjection projection : projections) {
            final DestinationLaneId laneId = projection.lane().laneId();
            final List<ScheduleWorkItem> items = projection.items();
            final List<ScheduleWorkItem> queued = delegate.queueSnapshot().getOrDefault(laneId, List.of());
            final DiscoveredHead known = discoveredHeads.get(laneId);
            if (!queued.isEmpty()) {
                if (known != null
                        && Arrays.equals(known.readyKey(), projection.readyKey())
                        && sameItemsExact(queued, known.items())
                        && !sameItemsExact(queued, items)) {
                    delegate.replaceLanePending(laneId, items);
                    nextHeads.put(laneId, new DiscoveredHead(items, projection.readyKey()));
                    for (ScheduleWorkItem item : items) {
                        if (item.eligibleAtEpochMs() <= dueThroughEpochMs) {
                            toOffer.add(item);
                        }
                    }
                    continue;
                }
                if (!samePendingItems(queued, items)) {
                    throw new IllegalStateException("in-memory READY head differs from authoritative READY: " + laneId);
                }
                if (known != null && !sameHead(known, projection)) {
                    throw new IllegalStateException("in-memory READY key differs from authoritative READY: " + laneId);
                }
                nextHeads.put(laneId, new DiscoveredHead(items, projection.readyKey()));
                continue;
            }
            if (known != null && sameHead(known, projection)) {
                nextHeads.put(laneId, known);
                continue;
            }
            nextHeads.put(laneId, new DiscoveredHead(items, projection.readyKey()));
            newlyPromoted.addAll(items);
            for (ScheduleWorkItem item : items) {
                if (item.eligibleAtEpochMs() <= dueThroughEpochMs) {
                    toOffer.add(item);
                }
            }
        }
        for (ScheduleWorkItem item : newlyPromoted) {
            delegate.activateLane(item.laneId());
            delegate.offer(item);
            offered.add(item);
        }
        // Do not consume a future READY key in the process-local cursor. The
        // future item may be retained in this process-local queue, but a
        // restart must be able to rediscover it before its due turn. READY
        // keys are ordered by eligibility, so the last eligible key is the
        // safe cursor boundary for this time-bounded discovery turn.
        if (lastEligibleReadyKey != null) {
            lastScannedReadyKey = lastEligibleReadyKey;
        }
        discoveredHeads.putAll(nextHeads);
        if (lastEligibleReadyKey != null) {
            wrapGeneration = eligibleCursorWrapped ? incrementWrapGeneration(wrapGeneration) : wrapGeneration;
        }
        return List.copyOf(toOffer);
    }

    /** Actual Store reads for the most recent discovery, including dependency and rejected reads. */
    public synchronized DiscoveryReadStatistics discoveryReadStatistics() {
        return lastDiscoveryRead;
    }

    public record DiscoveryReadStatistics(
            long actualRecords,
            long actualBytes,
            long chargedBytes,
            long deniedReads,
            BoundedReadBudget.Exhaustion exhaustion) {}

    /** Returns a snapshot of the process-local cursor using the legacy shape. */
    public synchronized SchedulerProjections.ReadyDiscoveryCursor discoveryCursor() {
        return new SchedulerProjections.ReadyDiscoveryCursor(lastScannedReadyKey, wrapGeneration, 1);
    }

    synchronized void offer(final ScheduleWorkItem item) {
        delegate.offer(item);
    }

    synchronized List<ScheduleWorkItem> poll(final SchedulerBudget budget) {
        // Compatibility overload; production scheduling must pass the
        // trusted due-through timestamp below.
        return poll(Long.MAX_VALUE, budget);
    }

    /** Polls only work that is due through the supplied trusted time. */
    public synchronized List<ScheduleWorkItem> poll(final long dueThroughEpochMs, final SchedulerBudget budget) {
        requireDueThrough(dueThroughEpochMs);
        Objects.requireNonNull(budget, "budget");
        final RuntimeSnapshot before = runtimeSnapshot();
        List<ScheduleWorkItem> result = List.of();
        try {
            if (recoveryFirstPass) {
                final Set<DestinationLaneId> eligible =
                        dueSchedulableLanesWithinBudget(dueThroughEpochMs, budget.maxBytes());
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
            return result;
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, result, List.of(), null, failure, null);
            throw failure;
        }
    }

    /**
     * Restores one exact head after a selected Claim handoff failed before its
     * shard WriteBatch committed.
     *
     * <p>{@link #poll(long, SchedulerBudget)} deliberately retains the
     * authoritative READY identity in {@code discoveredHeads} while the
     * process hands the selected item to the Claim executor. A pre-commit
     * materialization, permit or Claim validation failure must use this method
     * instead of a bare {@link #requeueFirst(ScheduleWorkItem)}; a
     * duplicate/mismatched handoff fails closed.</p>
     */
    public synchronized void requeueFailedClaim(final ScheduleWorkItem item) {
        final ScheduleWorkItem selected = requirePolledClaimCandidate(item);
        final DiscoveredHead known = discoveredHeads.get(selected.laneId());
        if (store.get(ColumnFamily.TIMELINE, known.readyKey()) == null) {
            throw new IllegalStateException("cannot requeue Claim after its READY key was consumed");
        }
        final RuntimeSnapshot before = runtimeSnapshot();
        final Map<DestinationLaneId, List<ScheduleWorkItem>> queuesBefore = delegate.queueSnapshot();
        try {
            delegate.requeueFirst(selected);
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), null, failure, queuesBefore);
            throw failure;
        }
    }

    /**
     * Completes the process-local half of an exact Claim handoff after the
     * shard Claim WriteBatch has consumed its READY key.
     *
     * <p>The durable Claim and Message runtime index remain authoritative.
     * This method only releases the retained discovery identity so a later
     * READY head for the Lane can be promoted. Observing the old READY key is
     * a caller ordering error: completion must never run before the Claim
     * WriteBatch is known to have succeeded.</p>
     */
    public synchronized void completeClaim(final ScheduleWorkItem item) {
        final ScheduleWorkItem selected = requirePolledClaimCandidate(item);
        final DiscoveredHead known = discoveredHeads.get(selected.laneId());
        if (store.get(ColumnFamily.TIMELINE, known.readyKey()) != null) {
            throw new IllegalStateException("cannot complete Claim while its READY key still exists");
        }
        final List<ScheduleWorkItem> remaining = delegate.queueSnapshot().getOrDefault(selected.laneId(), List.of());
        if (!samePendingItems(remaining, known.items())) {
            throw new IllegalStateException("Claim completion found work outside the consumed READY head");
        }
        // A dual READY value is one durable Lane head. Claiming either the
        // ordinary or native branch consumes that physical head, so its
        // unselected process-local sibling must not survive as phantom work.
        delegate.replaceLanePending(selected.laneId(), List.of());
        discoveredHeads.remove(selected.laneId());
    }

    /**
     * Revalidates the exact polled head against its current durable READY,
     * Message, Timeline, typed Lane and live Ready Certificate projections.
     */
    public synchronized ClaimCandidate requireClaimCandidate(
            final ScheduleWorkItem item, final TrustedUtcIntervalEvidence evidence) {
        final ScheduleWorkItem selected = requirePolledClaimCandidate(item);
        final TrustedUtcIntervalEvidence trusted = Objects.requireNonNull(evidence, "trusted UTC evidence");
        final DiscoveredHead known = discoveredHeads.get(selected.laneId());
        final byte[] encoded = store.get(ColumnFamily.TIMELINE, known.readyKey());
        if (encoded == null) {
            throw new IllegalStateException("Claim candidate READY key no longer exists");
        }
        final ReadyProjection projection =
                decodeReadyProjection(new ShardStore.KeyValue(known.readyKey(), encoded), trusted);
        final ScheduleWorkItem currentCandidate = projection.items().stream()
                .filter(candidate -> sameWork(candidate, selected))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalStateException("Claim candidate differs from current durable READY head"));
        if (selected.isNativeCandidate()
                && (projection.nativeAction() != HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE
                        || currentCandidate.effectiveEligibleAtEpochMs() > trusted.earliestEpochMs())) {
            throw new IllegalStateException("Claim candidate differs from current durable READY head");
        }
        final ActiveLaneState lane = readTypedLane(projection.lane());
        if (lane == null || lane.readyCertificate() == null) {
            throw new IllegalStateException("Claim candidate lacks a typed Ready Certificate");
        }
        final ReadyCertificate certificate = ReadyCertificate.decode(lane.readyCertificate());
        return new ClaimCandidate(currentCandidate, projection.lane().laneIncarnation(), certificate);
    }

    private ScheduleWorkItem requirePolledClaimCandidate(final ScheduleWorkItem item) {
        final ScheduleWorkItem selected = Objects.requireNonNull(item, "Claim work item");
        final DiscoveredHead known = discoveredHeads.get(selected.laneId());
        if (known == null || known.items().stream().noneMatch(candidate -> sameWork(candidate, selected))) {
            throw new IllegalArgumentException("Claim work item is not the discovered Lane head");
        }
        if (delegate.queueSnapshot().getOrDefault(selected.laneId(), List.of()).stream()
                .anyMatch(candidate -> sameWork(candidate, selected))) {
            throw new IllegalStateException("Claim work item has not been polled from its Lane");
        }
        return selected;
    }

    /**
     * A due head that is larger than this turn's global byte budget cannot be
     * claimed in this turn. It must not keep the recovery first pass open and
     * thereby prevent smaller healthy lanes from receiving later turns.
     */
    private Set<DestinationLaneId> dueSchedulableLanesWithinBudget(
            final long dueThroughEpochMs, final long maximumHeadBytes) {
        if (maximumHeadBytes <= 0) {
            throw new IllegalArgumentException("maximum recovery head bytes must be positive");
        }
        return delegate.dueSchedulableLanes(dueThroughEpochMs).stream()
                .filter(laneId -> {
                    return delegate.queueSnapshot().getOrDefault(laneId, List.of()).stream()
                            .filter(item -> item.eligibleAtEpochMs() <= dueThroughEpochMs)
                            .anyMatch(item -> item.accountedBytes() <= maximumHeadBytes);
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
        return new RuntimeSnapshot(
                delegate.snapshot(),
                delegate.ringOrder(),
                new HashMap<>(discoveredHeads),
                lastScannedReadyKey == null ? null : Bytes.copy(lastScannedReadyKey),
                wrapGeneration,
                recoveryFirstPass,
                new HashSet<>(recoveryServed),
                delegate.readinessSnapshot());
    }

    /**
     * Restores process state after a local scheduler operation failed. The
     * original failure remains the primary error; an inability to roll back is
     * attached so the caller never mistakes a partially restored registry for
     * a successful scheduler turn.
     */
    private void rollbackRuntime(
            final RuntimeSnapshot snapshot,
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
            lastScannedReadyKey =
                    snapshot.lastScannedReadyKey() == null ? null : Bytes.copy(snapshot.lastScannedReadyKey());
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

    synchronized void markBlocked(final DestinationLaneId laneId) {
        requireRegisteredLane(laneId);
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            delegate.markBlocked(laneId);
            delegate.deactivateLane(laneId);
            recoveryFirstPass = true;
            recoveryServed.clear();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), laneId, failure, null);
            throw failure;
        }
    }

    /** Returns a registered Lane to evidence recovery before it can become READY. */
    synchronized void markRecoveringEvidence(final DestinationLaneId laneId) {
        requireRegisteredLane(laneId);
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            delegate.markRecoveringEvidence(laneId);
            recoveryFirstPass = true;
            recoveryServed.clear();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), laneId, failure, null);
            throw failure;
        }
    }

    synchronized void markReady(final DestinationLaneId laneId) {
        requireRegisteredLane(laneId);
        final RuntimeSnapshot before = runtimeSnapshot();
        try {
            delegate.markReady(laneId);
            delegate.activateLane(laneId);
            recoveryFirstPass = true;
            recoveryServed.clear();
        } catch (RuntimeException | Error failure) {
            rollbackRuntime(before, List.of(), List.of(), laneId, failure, null);
            throw failure;
        }
    }

    /**
     * Removes a source-ordered terminal Lane from process memory. The
     * exact-incarnation and terminal/empty-queue
     * checks are owned by the shard-local scheduler; this wrapper only
     * removes the corresponding registry and discovery entries after that
     * check succeeds.
     */
    synchronized void unregister(final DestinationLaneId laneId, final byte[] laneIncarnation) {
        Objects.requireNonNull(laneId, "laneId");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        final LaneRecord lane = registered.get(laneId);
        if (lane == null) {
            throw new IllegalArgumentException("lane is not registered: " + laneId);
        }
        if (!Arrays.equals(lane.laneIncarnation(), laneIncarnation)) {
            throw new IllegalArgumentException("lane incarnation mismatch");
        }
        delegate.unregister(laneId, laneIncarnation);
        registered.remove(laneId);
        recoveryServed.remove(laneId);
        discoveredHeads.remove(laneId);
    }

    synchronized void requeueFirst(final ScheduleWorkItem item) {
        delegate.requeueFirst(item);
    }

    public synchronized LaneScheduler.SchedulerSnapshot snapshot() {
        return delegate.snapshot();
    }

    private ReadyScan readReadyProjections(
            final int limit, final TrustedUtcIntervalEvidence evidence, final BoundedReadBudget budget) {
        final byte[] prefix = new byte[] {3, 1};
        final byte[] upper = new byte[] {4, 1};
        final List<ReadyProjection> projections = new ArrayList<>();
        final Set<DestinationLaneId> scannedLanes = new HashSet<>();
        if (lastScannedReadyKey == null) {
            readReadyRange(prefix, upper, limit, evidence, budget, projections, scannedLanes);
            return new ReadyScan(projections, projections.size());
        }
        if (!hasPrefix(lastScannedReadyKey, prefix)) {
            throw new IllegalStateException("persisted READY discovery cursor is outside READY namespace");
        }
        // Appending a zero byte is the exclusive successor of this exact key.
        // No cursor row is fetched merely to skip it, and malformed longer keys
        // still reach the decoder instead of being skipped by a numeric increment.
        final byte[] tailStart = Arrays.copyOf(lastScannedReadyKey, lastScannedReadyKey.length + 1);
        final ShardStore.VisitStop tail =
                readReadyRange(tailStart, upper, limit, evidence, budget, projections, scannedLanes);
        if (tail != ShardStore.VisitStop.RANGE_END || projections.size() == limit) {
            return new ReadyScan(projections, projections.size());
        }
        final int tailCompleted = projections.size();
        final ShardStore.VisitStop head =
                readReadyRange(prefix, lastScannedReadyKey, limit, evidence, budget, projections, scannedLanes);
        if (head != ShardStore.VisitStop.RANGE_END || projections.size() == limit) {
            return new ReadyScan(projections, tailCompleted);
        }
        // Revisit the exact cursor only after both ranges were proven exhausted.
        // This preserves singleton discovery and live native policy refresh.
        try {
            final byte[] cursorValue = store.get(ColumnFamily.TIMELINE, lastScannedReadyKey);
            if (cursorValue != null) {
                appendReadyProjection(
                        new ShardStore.KeyValue(lastScannedReadyKey, cursorValue), evidence, projections, scannedLanes);
            }
        } catch (ReadIncompleteException incomplete) {
            // A failed dependency has not appended a projection or advanced the cursor.
        }
        return new ReadyScan(projections, tailCompleted);
    }

    private ShardStore.VisitStop readReadyRange(
            final byte[] lower,
            final byte[] upper,
            final int limit,
            final TrustedUtcIntervalEvidence evidence,
            final BoundedReadBudget budget,
            final List<ReadyProjection> projections,
            final Set<DestinationLaneId> scannedLanes) {
        try {
            return store.visitResult(
                            ColumnFamily.TIMELINE,
                            lower,
                            upper,
                            limit - projections.size(),
                            budget,
                            (entry, ignored) -> {
                                appendReadyProjection(entry, evidence, projections, scannedLanes);
                                return projections.size() < limit;
                            })
                    .stop();
        } catch (ReadIncompleteException incomplete) {
            return ShardStore.VisitStop.INCOMPLETE;
        }
    }

    private void appendReadyProjection(
            final ShardStore.KeyValue entry,
            final TrustedUtcIntervalEvidence evidence,
            final List<ReadyProjection> projections,
            final Set<DestinationLaneId> scannedLanes) {
        final ReadyProjection projection = decodeStoredReadyProjection(entry, evidence);
        if (!scannedLanes.add(projection.lane().laneId())) {
            throw new IllegalStateException("multiple READY heads discovered for Lane: "
                    + projection.lane().laneId());
        }
        projections.add(projection);
    }

    private static long incrementWrapGeneration(final long current) {
        return current == Long.MAX_VALUE ? Long.MAX_VALUE : current + 1;
    }

    private List<ShardStore.KeyValue> scanAllReadyEntries(final int limit) {
        final byte[] prefix = new byte[] {3, 1};
        final byte[] upper = new byte[] {4, 1};
        return store.scan(ColumnFamily.TIMELINE, prefix, upper, limit);
    }

    private ReadyProjection decodeReadyProjection(final ShardStore.KeyValue entry) {
        return decodeReadyProjection(entry, null);
    }

    private ReadyProjection decodeReadyProjection(
            final ShardStore.KeyValue entry, final TrustedUtcIntervalEvidence evidence) {
        return resolveNativeProjection(decodeStoredReadyProjection(entry, evidence), evidence);
    }

    private ReadyProjection decodeStoredReadyProjection(
            final ShardStore.KeyValue entry, final TrustedUtcIntervalEvidence evidence) {
        final ReadyKey key = decodeReadyKey(entry.key());
        final ReadyIndexValue value =
                ReadyIndexValue.decode(ValueEnvelope.decode(entry.value(), 3).payload());
        if (!key.laneId().equals(value.laneId())
                || key.nextEligibleAtEpochMs() != value.persistentWakeAtEpochMs()
                || key.laneVersion() != value.laneVersion()) {
            throw new IllegalStateException("READY key/value identity mismatch during scheduler rebuild");
        }
        final LaneRecord lane = registered.get(key.laneId());
        if (lane == null) {
            throw new IllegalStateException("READY Lane is not registered: " + key.laneId());
        }
        validateStoredLane(lane);
        if (!lane.schedulable()
                || lane.laneVersion() != key.laneVersion()
                || lane.nextEligibleAtEpochMs() != value.persistentWakeAtEpochMs()) {
            throw new IllegalStateException("stale or non-schedulable READY Lane: " + key.laneId());
        }
        final ValueEnvelope.Decoded messageValue =
                store.getValue(ColumnFamily.ID, KeyCodec.idMessage(value.messageId()), 1);
        if (messageValue == null) {
            throw new IllegalStateException("READY points to a missing message: " + value.messageId());
        }
        final MessageRecord message = MessageRecord.decode(messageValue.payload());
        if (!store.shardId().equals(value.messageId().routingId().shardId())) {
            throw new IllegalStateException("READY message key belongs to another Shard: " + value.messageId());
        }
        final var scheduleSourcePosition = SourcePositionCodec.decode(message.scheduleSourcePosition());
        if (!store.shardId().equals(scheduleSourcePosition.shardId())) {
            throw new IllegalStateException(
                    "READY message source position belongs to another Shard: " + value.messageId());
        }
        if (message.status() != MessageStatus.SCHEDULED
                || message.generation() != value.generation()
                || !message.laneId().equals(key.laneId())) {
            throw new IllegalStateException("READY points to a non-current scheduled message: " + value.messageId());
        }
        final TimelineWorkRef currentWork = message.runtimeIndex().timeline();
        final long timelineEligibleAt =
                message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                        ? message.deliverAtEpochMs()
                        : Math.max(
                                currentWork == null ? message.deliverAtEpochMs() : currentWork.actionAtEpochMs(),
                                message.retryEligibilityAtEpochMs());
        final byte[] timelineKey =
                message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                        ? KeyCodec.timelineOrdered(
                                message.laneId(),
                                timelineEligibleAt,
                                scheduleSourcePosition.sourceOrderToken(),
                                value.messageId(),
                                message.generation())
                        : KeyCodec.timelineDue(
                                message.laneId(),
                                timelineEligibleAt,
                                scheduleSourcePosition.sourceOrderToken(),
                                value.messageId(),
                                message.generation());
        if (!Bytes.constantTimeEquals(value.timelineKeySha256(), Bytes.sha256(timelineKey))) {
            throw new IllegalStateException("READY timeline digest mismatch: " + value.messageId());
        }
        final byte[] timelineBytes = store.get(ColumnFamily.TIMELINE, timelineKey);
        if (timelineBytes == null) {
            throw new IllegalStateException("READY points to a missing timeline entry: " + value.messageId());
        }
        final TimelineWorkRef timeline = validateTimelineValue(
                ValueEnvelope.decode(timelineBytes, 1).payload(), value.messageId(), message, timelineKey);
        if (timeline != null
                && value.nextEligibleAtEpochMs()
                        != Math.max(timeline.actionAtEpochMs(), timeline.retryEligibilityAtEpochMs())) {
            throw new IllegalStateException("READY eligibility disagrees with TimelineWorkRef: " + value.messageId());
        }
        final ActiveLaneState typedLane = readTypedLane(lane);
        if (typedLane != null) {
            validateTypedReadyProjection(typedLane, entry.key(), key, value, evidence);
            final long ordinaryActionAt = timeline == null
                    ? (currentWork == null ? message.deliverAtEpochMs() : currentWork.actionAtEpochMs())
                    : timeline.actionAtEpochMs();
            final long actionAt = value.nativeHead() == null
                    ? ordinaryActionAt
                    : Math.min(ordinaryActionAt, value.nativeHead().nextEligibleAtEpochMs());
            if (typedLane.earliestActionAtEpochMs() == null
                    || typedLane.earliestActionAtEpochMs() != actionAt
                    || typedLane.nextEligibleAtEpochMs() == null
                    || typedLane.nextEligibleAtEpochMs() != value.persistentWakeAtEpochMs()) {
                throw new IllegalStateException(
                        "typed READY action/eligibility projection disagrees with current head: " + value.messageId());
            }
        } else if (evidence != null) {
            throw new IllegalStateException("strict READY discovery requires a typed ACTIVE Lane projection");
        }
        final long accountedBytes = Math.max(1, message.payloadLength());
        final List<ScheduleWorkItem> items = new ArrayList<>();
        items.add(new ScheduleWorkItem(
                key.laneId(),
                value.messageId(),
                value.generation(),
                value.persistentWakeAtEpochMs(),
                value.nextEligibleAtEpochMs(),
                ScheduleWorkItem.CandidateKind.ORDINARY,
                null,
                accountedBytes));
        NativeResolution nativeResolution = null;
        if (value.nativeHead() != null) {
            final ReadyIndexValue nativeHead = value.nativeHead();
            final MessageRecord nativeMessage = validateNativeReadyHead(nativeHead, key.laneId());
            if (evidence != null && nativeEligibilityAuthority != null) {
                nativeResolution =
                        new NativeResolution(nativeHead, nativeMessage, readScheduleBinding(nativeHead.messageId()));
            }
        }
        return new ReadyProjection(lane, items, entry.key(), null, nativeResolution);
    }

    private ReadyProjection resolveNativeProjection(
            final ReadyProjection projection, final TrustedUtcIntervalEvidence evidence) {
        final NativeResolution pending = projection.nativeResolution();
        if (pending == null) {
            return projection;
        }
        final HandoffEligibilityResolver.Decision decision =
                nativeEligibilityAuthority.resolve(pending.message(), pending.binding(), evidence);
        final List<ScheduleWorkItem> items = new ArrayList<>(projection.items());
        if (decision.reason() == HandoffEligibilityReason.ELIGIBLE
                && (decision.action() == HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE
                        || decision.action() == HandoffEligibilityAction.WAIT_UNTIL)
                && decision.policyHeadRef() != null
                && decision.policySnapshot() != null) {
            items.add(new ScheduleWorkItem(
                    projection.lane().laneId(),
                    pending.head().messageId(),
                    pending.head().generation(),
                    projection.items().get(0).persistentWakeAtEpochMs(),
                    decision.effectiveEligibleAtEpochMs(),
                    ScheduleWorkItem.CandidateKind.MANAGED_NATIVE,
                    decision.policyHeadRef(),
                    Math.max(1, pending.message().payloadLength())));
        }
        return new ReadyProjection(projection.lane(), items, projection.readyKey(), decision.action(), null);
    }

    private MessageRecord validateNativeReadyHead(
            final ReadyIndexValue nativeHead, final DestinationLaneId expectedLane) {
        if (!nativeHead.isNativeCandidate() || nativeHead.nextEligibleAtEpochMs() < 0) {
            throw new IllegalStateException("native READY head is malformed");
        }
        final ValueEnvelope.Decoded messageValue =
                store.getValue(ColumnFamily.ID, KeyCodec.idMessage(nativeHead.messageId()), 1);
        if (messageValue == null) {
            throw new IllegalStateException("native READY points to a missing message: " + nativeHead.messageId());
        }
        final MessageRecord message = MessageRecord.decode(messageValue.payload());
        if (message.status() != MessageStatus.SCHEDULED
                || message.generation() != nativeHead.generation()
                || !message.laneId().equals(expectedLane)
                || message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                || message.nativeDeliveryPolicy() == com.nereusstream.delay.protocol.NativeDeliveryPolicy.FORBID
                || message.earliestNativeCandidateAtEpochMs() != nativeHead.nextEligibleAtEpochMs()) {
            throw new IllegalStateException("native READY points to an invalid current message");
        }
        final var sourcePosition = SourcePositionCodec.decode(message.scheduleSourcePosition());
        final byte[] nativeKey = KeyCodec.timelineNativeCandidate(
                expectedLane,
                nativeHead.nextEligibleAtEpochMs(),
                sourcePosition.sourceOrderToken(),
                nativeHead.messageId(),
                nativeHead.generation());
        if (!Bytes.constantTimeEquals(nativeHead.timelineKeySha256(), Bytes.sha256(nativeKey))) {
            throw new IllegalStateException("native READY timeline digest mismatch: " + nativeHead.messageId());
        }
        final byte[] encoded = store.get(ColumnFamily.TIMELINE, nativeKey);
        if (encoded == null) {
            throw new IllegalStateException("native READY points to a missing candidate: " + nativeHead.messageId());
        }
        final NativeCandidateRef candidate =
                NativeCandidateRef.decode(ValueEnvelope.decode(encoded, 1).payload());
        if (!candidate.messageId().equals(nativeHead.messageId())
                || candidate.generation() != nativeHead.generation()
                || candidate.candidateAtEpochMs() != nativeHead.nextEligibleAtEpochMs()
                || !Arrays.equals(candidate.timelineKey(), nativeKey)) {
            throw new IllegalStateException("native READY candidate identity mismatch");
        }
        return message;
    }

    private ScheduleBinding readScheduleBinding(final DelayMessageId messageId) {
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.ID, KeyCodec.idScheduleBinding(messageId), 4);
        if (value == null) {
            throw new IllegalStateException("native READY message has no Schedule binding");
        }
        final ScheduleBinding binding = ScheduleBinding.decode(value.payload());
        if (!binding.delayMessageId().equals(messageId)) {
            throw new IllegalStateException("native READY Schedule binding identity mismatch");
        }
        return binding;
    }

    /**
     * Fences the physical READY index against the complete typed ACTIVE
     * projection. The typed value is the durable witness that the Registry
     * Lane state and the scheduler index were advanced together; checking only
     * Lane/version/time fields would allow a future codec revision to omit the
     * key or certificate while still rebuilding a claimable head.
     */
    private void validateTypedReadyProjection(
            final ActiveLaneState state,
            final byte[] physicalReadyKey,
            final ReadyKey decodedReadyKey,
            final ReadyIndexValue readyValue,
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
        if (readyValue.persistentWakeAtEpochMs() != state.nextEligibleAtEpochMs()
                || decodedReadyKey.laneVersion() != state.laneVersion()
                || !decodedReadyKey.laneId().equals(state.laneId())) {
            throw new IllegalStateException("typed READY key fields disagree with Lane state");
        }
        try {
            final ReadyCertificate certificate = ReadyCertificate.decode(readyCertificate);
            if (evidence != null) {
                validateLiveReadyCertificate(certificate, evidence);
            }
        } catch (IllegalArgumentException malformedCertificate) {
            throw new IllegalStateException(
                    "typed READY projection carries an invalid certificate", malformedCertificate);
        }
    }

    private void validateLiveReadyCertificate(
            final ReadyCertificate certificate, final TrustedUtcIntervalEvidence evidence) {
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

    private ActiveLaneState readTypedLane(final LaneRecord expected) {
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.META, KeyCodec.metaLane(expected.laneId()), 2);
        if (value == null) {
            throw new IllegalStateException(
                    "registered Lane disappeared during READY validation: " + expected.laneId());
        }
        final LaneRecordEnvelope envelope = LaneRecordEnvelope.decode(value.payload());
        return envelope.isActive() ? envelope.typedActiveState().orElse(null) : null;
    }

    private static TimelineWorkRef validateTimelineValue(
            final byte[] encodedValue,
            final DelayMessageId messageId,
            final MessageRecord message,
            final byte[] expectedTimelineKey) {
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
        if (current == null
                && (work.retryEligibilityAtEpochMs() != message.retryEligibilityAtEpochMs()
                        || work.orderedHeadBlocking()
                                != (message.orderingMode()
                                        == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO)
                        || work.actionAtEpochMs() > message.deliverAtEpochMs())) {
            throw new IllegalStateException("READY TimelineWorkRef disagrees with legacy Message: " + messageId);
        }
        return work;
    }

    private static boolean sameWork(final ScheduleWorkItem left, final ScheduleWorkItem right) {
        return left.laneId().equals(right.laneId())
                && left.messageId().equals(right.messageId())
                && left.generation() == right.generation()
                && left.persistentWakeAtEpochMs() == right.persistentWakeAtEpochMs()
                && left.candidateKind() == right.candidateKind()
                && left.accountedBytes() == right.accountedBytes();
    }

    private static boolean sameWorkExact(final ScheduleWorkItem left, final ScheduleWorkItem right) {
        return sameWork(left, right)
                && left.effectiveEligibleAtEpochMs() == right.effectiveEligibleAtEpochMs()
                && Objects.equals(left.policyHeadRef(), right.policyHeadRef());
    }

    private static boolean sameHead(final DiscoveredHead known, final ReadyProjection projection) {
        return sameItems(known.items(), projection.items()) && Arrays.equals(known.readyKey(), projection.readyKey());
    }

    private static boolean sameItems(final List<ScheduleWorkItem> left, final List<ScheduleWorkItem> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!sameWork(left.get(index), right.get(index))) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameItemsExact(final List<ScheduleWorkItem> left, final List<ScheduleWorkItem> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!sameWorkExact(left.get(index), right.get(index))) {
                return false;
            }
        }
        return true;
    }

    /** Allows an already-polled sibling of a dual READY value to be absent from the local queue. */
    private static boolean samePendingItems(
            final List<ScheduleWorkItem> pending, final List<ScheduleWorkItem> expected) {
        if (pending.size() > expected.size()) {
            return false;
        }
        final boolean[] used = new boolean[expected.size()];
        for (ScheduleWorkItem candidate : pending) {
            boolean matched = false;
            for (int index = 0; index < expected.size(); index++) {
                if (!used[index] && sameWork(candidate, expected.get(index))) {
                    used[index] = true;
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return true;
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
        final LaneRecordEnvelope envelope = LaneRecordEnvelope.decode(payload);
        if (!envelope.isActive()) {
            throw new IllegalStateException("READY Lane is terminal: " + expected.laneId());
        }
        final java.util.Optional<ActiveLaneState> typed = envelope.typedActiveState();
        if (typed.isPresent()) {
            final ActiveLaneState state = typed.orElseThrow();
            if (!state.laneId().equals(expected.laneId())
                    || !Arrays.equals(state.laneIncarnation(), expected.laneIncarnation())
                    || state.laneControlVersion() != expected.laneControlVersion()
                    || state.laneVersion() != expected.laneVersion()
                    || state.admissionGate() != expected.admissionGate()
                    || state.runtimeReadiness() != expected.runtimeReadiness()
                    || state.schedulerWeight() != expected.weight()
                    || state.nextEligibleAtEpochMs() == null
                    || state.nextEligibleAtEpochMs() != expected.nextEligibleAtEpochMs()) {
                throw new IllegalStateException(
                        "registered Lane differs from typed meta_cf state: " + expected.laneId());
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

    private static void validateLegacyProjections(final ShardStore store) {
        final var discovery = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(1), VALUE_TYPE);
        final var activeRing = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(2), VALUE_TYPE);
        final var deficits = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(3), VALUE_TYPE);
        final var round = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(4), VALUE_TYPE);
        final var lastServed = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(5), VALUE_TYPE);
        final boolean any =
                discovery != null || activeRing != null || deficits != null || round != null || lastServed != null;
        if (!any) {
            return;
        }
        if (discovery == null || activeRing == null || deficits == null || round == null || lastServed == null) {
            throw new IllegalStateException("scheduler projections are incomplete");
        }
        final SchedulerProjections.ReadyDiscoveryCursor decodedDiscovery =
                SchedulerProjections.ReadyDiscoveryCursor.decode(discovery.payload());
        final SchedulerProjections.ActiveRing decodedActiveRing =
                SchedulerProjections.ActiveRing.decode(activeRing.payload());
        final SchedulerProjections.Round decodedRound = SchedulerProjections.Round.decode(round.payload());
        if (decodedDiscovery.activeRingGeneration() != decodedActiveRing.ringGeneration()
                || decodedActiveRing.roundGeneration() != decodedRound.roundGeneration()) {
            throw new IllegalStateException("scheduler projection generations disagree");
        }
        if (decodedDiscovery.wrapGeneration() < 0
                || decodedDiscovery.activeRingGeneration() < 0
                || decodedActiveRing.ringGeneration() < 0
                || decodedRound.roundGeneration() < 0) {
            throw new IllegalArgumentException("scheduler generations cannot be negative");
        }
        final SchedulerProjections.DeficitMap decodedDeficits = SchedulerProjections.DeficitMap.decode(deficits.payload());
        if (decodedDeficits.entries().stream().anyMatch(entry -> entry.deficitBytes() < 0)) {
            throw new IllegalArgumentException("scheduler deficit cannot be negative");
        }
        final SchedulerProjections.LastServedMap decodedLastServed =
                SchedulerProjections.LastServedMap.decode(lastServed.payload());
        if (decodedLastServed.entries().stream().anyMatch(entry ->
                entry.lastServedRound() < 0
                        || entry.lastServedRound() > decodedRound.roundGeneration()
                        || entry.serviceGapGeneration() < 0)) {
            throw new IllegalArgumentException("scheduler service counters are invalid");
        }
    }

    private static OwnerIdentity defaultOwner(final ShardStore store) {
        Objects.requireNonNull(store, "store");
        final byte[] worker = Bytes.concat(
                store.shardId().routeIncarnation().bytes(),
                Bytes.u32beBits(store.shardId().partition()));
        return new OwnerIdentity(
                Bytes.utf8("embedded-scheduler"),
                worker,
                1,
                Bytes.sha256(Bytes.utf8("nereus-delay-embedded-scheduler-owner\0"), worker));
    }

    private record RuntimeSnapshot(
            LaneScheduler.SchedulerSnapshot schedulerSnapshot,
            List<DestinationLaneId> ringOrder,
            Map<DestinationLaneId, DiscoveredHead> discoveredHeads,
            byte[] lastScannedReadyKey,
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

    private record ReadyKey(DestinationLaneId laneId, long nextEligibleAtEpochMs, long laneVersion) {}

    private record ReadyProjection(
            LaneRecord lane,
            List<ScheduleWorkItem> items,
            byte[] readyKey,
            HandoffEligibilityAction nativeAction,
            NativeResolution nativeResolution) {
        private ReadyProjection {
            Objects.requireNonNull(lane, "lane");
            items = List.copyOf(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("READY projection must contain at least one candidate");
            }
            readyKey = Bytes.copy(readyKey);
        }

        @Override
        public byte[] readyKey() {
            return Bytes.copy(readyKey);
        }
    }

    private record NativeResolution(ReadyIndexValue head, MessageRecord message, ScheduleBinding binding) {}

    private record DiscoveredHead(List<ScheduleWorkItem> items, byte[] readyKey) {
        private DiscoveredHead {
            items = List.copyOf(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("discovered READY head must contain at least one candidate");
            }
            readyKey = Bytes.copy(readyKey);
        }

        @Override
        public byte[] readyKey() {
            return Bytes.copy(readyKey);
        }
    }

    /** Exact live projection handed from scheduler selection to Claim admission. */
    public record ClaimCandidate(ScheduleWorkItem item, byte[] laneIncarnation, ReadyCertificate readyCertificate) {
        public ClaimCandidate {
            Objects.requireNonNull(item, "item");
            Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
            Objects.requireNonNull(readyCertificate, "readyCertificate");
            laneIncarnation = Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }
    }

    private record ReadyScan(List<ReadyProjection> projections, int firstWrappedProjection) {
        private ReadyScan {
            projections = List.copyOf(projections);
        }
    }
}
