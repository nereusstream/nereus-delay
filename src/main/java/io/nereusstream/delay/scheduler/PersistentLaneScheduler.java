package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.ActiveLaneStateV1;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.SchedulerProjectionsV1;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.ReadyIndexValue;
import io.nereusstream.delay.runtime.TimelineEntry;
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
    private final Map<DestinationLaneId, LaneRecord> registered = new HashMap<>();
    private final PersistedState persisted;
    private final Set<DestinationLaneId> recoveryServed = new HashSet<>();
    private long ringGeneration;
    private byte[] lastScannedReadyKey;
    private long wrapGeneration;
    private boolean recoveryFirstPass = true;
    private boolean persistedRestored;

    public PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate) {
        this(store, delegate, defaultOwner(store));
    }

    public PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate,
                                   final OwnerIdentityV1 owner) {
        this.store = Objects.requireNonNull(store, "store");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.persisted = load(store);
        this.ringGeneration = persisted == null ? 0 : persisted.activeRing().ringGeneration();
        this.lastScannedReadyKey = persisted == null ? null : persisted.discovery().lastScannedReadyKey();
        this.wrapGeneration = persisted == null ? 0 : persisted.discovery().wrapGeneration();
    }

    public static PersistentLaneScheduler defaults(final ShardStore store) {
        return new PersistentLaneScheduler(store, LaneScheduler.defaults());
    }

    public synchronized void register(final LaneRecord lane) {
        Objects.requireNonNull(lane, "lane");
        registered.put(lane.laneId(), lane);
        delegate.register(lane);
    }

    /** Applies the saved projections after all currently active lanes are registered. */
    public synchronized void restorePersistedState() {
        if (persisted == null) {
            persistedRestored = true;
            return;
        }
        final List<DestinationLaneId> order = persisted.activeRing().entries().stream()
                .filter(this::matchesRegisteredLane)
                .map(SchedulerProjectionsV1.RingEntry::laneId)
                .toList();
        delegate.rebuildActiveRing(order);
        final Map<LaneKey, SchedulerProjectionsV1.DeficitEntry> deficits = new HashMap<>();
        for (SchedulerProjectionsV1.DeficitEntry entry : persisted.deficitMap().entries()) {
            deficits.put(new LaneKey(entry.laneId(), entry.laneIncarnation()), entry);
        }
        final Map<LaneKey, SchedulerProjectionsV1.LastServedEntry> lastServed = new HashMap<>();
        for (SchedulerProjectionsV1.LastServedEntry entry : persisted.lastServedMap().entries()) {
            lastServed.put(new LaneKey(entry.laneId(), entry.laneIncarnation()), entry);
        }
        final List<LaneScheduler.LaneSnapshot> snapshots = delegate.orderedSnapshot().stream()
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
        delegate.restore(new LaneScheduler.SchedulerSnapshot(persisted.activeRing().nextIndex(),
                persisted.round().roundGeneration(), snapshots));
        final boolean ownerChanged = !persisted.round().owner().equals(owner);
        recoveryFirstPass = ownerChanged || persisted.round().recoveryFirstPass();
        recoveryServed.clear();
        persistedRestored = true;
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
        final int scanLimit = maxReadyEntries == Integer.MAX_VALUE
                ? Integer.MAX_VALUE : Math.addExact(maxReadyEntries, 1);
        final List<ShardStore.KeyValue> entries = scanReadyEntries(scanLimit);
        if (entries.size() > maxReadyEntries) {
            throw new IllegalStateException("READY index exceeds scheduler recovery bound");
        }
        final Map<DestinationLaneId, ScheduleWorkItem> byLane = new HashMap<>();
        final List<DestinationLaneId> activeOrder = new ArrayList<>();
        for (ShardStore.KeyValue entry : entries) {
            final ReadyProjection projection = decodeReadyProjection(entry);
            if (byLane.put(projection.lane().laneId(), projection.item()) != null) {
                throw new IllegalStateException("multiple READY heads for Lane: " + projection.lane().laneId());
            }
            activeOrder.add(projection.lane().laneId());
        }
        delegate.replacePending(new ArrayList<>(byLane.values()));
        delegate.rebuildActiveRing(activeOrder);
        recoveryFirstPass = true;
        recoveryServed.clear();
        if (entries.isEmpty()) {
            lastScannedReadyKey = null;
        } else {
            lastScannedReadyKey = entries.get(entries.size() - 1).key();
        }
        persist();
        return byLane.size();
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
        final List<ScheduleWorkItem> result;
        if (recoveryFirstPass) {
            final Set<DestinationLaneId> eligible = delegate.snapshot().lanes().stream()
                    .filter(state -> state.schedulable() && state.pendingItems() > 0)
                    .map(LaneScheduler.LaneSnapshot::laneId)
                    .collect(java.util.stream.Collectors.toSet());
            recoveryServed.retainAll(eligible);
            result = delegate.pollRecoveryFirstPass(budget, recoveryServed);
            result.forEach(item -> recoveryServed.add(item.laneId()));
            if (!eligible.isEmpty() && recoveryServed.containsAll(eligible)) {
                recoveryFirstPass = false;
                recoveryServed.clear();
            }
        } else {
            result = delegate.poll(budget);
        }
        persist();
        return result;
    }

    public synchronized void markBlocked(final DestinationLaneId laneId) {
        delegate.markBlocked(laneId);
        delegate.deactivateLane(laneId);
        persist();
    }

    public synchronized void markReady(final DestinationLaneId laneId) {
        delegate.markReady(laneId);
        delegate.activateLane(laneId);
        persist();
    }

    public synchronized void requeueFirst(final ScheduleWorkItem item) {
        delegate.requeueFirst(item);
    }

    public synchronized LaneScheduler.SchedulerSnapshot snapshot() {
        return delegate.snapshot();
    }

    public synchronized void persist() {
        final LaneScheduler.SchedulerSnapshot snapshot = delegate.snapshot();
        final long nextRingGeneration = ringGeneration == Long.MAX_VALUE ? Long.MAX_VALUE : ringGeneration + 1;
        ringGeneration = Math.max(1, nextRingGeneration);
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
        final SchedulerProjectionsV1.ActiveRing activeRing = new SchedulerProjectionsV1.ActiveRing(ringGeneration,
                snapshot.roundGeneration(), nextIndex, ringEntries);
        final SchedulerProjectionsV1.ReadyDiscoveryCursor discovery =
                new SchedulerProjectionsV1.ReadyDiscoveryCursor(lastScannedReadyKey, wrapGeneration, ringGeneration);
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
    }

    private List<ShardStore.KeyValue> scanReadyEntries(final int limit) {
        final byte[] prefix = new byte[]{3, 1};
        final byte[] upper = new byte[]{4, 1};
        if (lastScannedReadyKey == null) {
            return store.scan(ColumnFamily.TIMELINE, prefix, upper, limit);
        }
        if (!hasPrefix(lastScannedReadyKey, prefix)) {
            throw new IllegalStateException("persisted READY discovery cursor is outside READY namespace");
        }
        final List<ShardStore.KeyValue> result = new ArrayList<>();
        final List<ShardStore.KeyValue> tail = store.scan(ColumnFamily.TIMELINE, lastScannedReadyKey, upper, limit);
        for (ShardStore.KeyValue entry : tail) {
            if (!Arrays.equals(entry.key(), lastScannedReadyKey)) {
                result.add(entry);
            }
            if (result.size() == limit) {
                return List.copyOf(result);
            }
        }
        final int remaining = limit - result.size();
        if (remaining > 0) {
            final List<ShardStore.KeyValue> head = store.scan(ColumnFamily.TIMELINE, prefix,
                    lastScannedReadyKey, remaining);
            if (!head.isEmpty()) {
                wrapGeneration = wrapGeneration == Long.MAX_VALUE ? Long.MAX_VALUE : wrapGeneration + 1;
                result.addAll(head);
            }
        }
        return List.copyOf(result);
    }

    private ReadyProjection decodeReadyProjection(final ShardStore.KeyValue entry) {
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
        if (message.status() != MessageStatus.SCHEDULED || message.generation() != value.generation()
                || !message.laneId().equals(key.laneId())) {
            throw new IllegalStateException("READY points to a non-current scheduled message: " + value.messageId());
        }
        final long timelineEligibleAt = message.orderingMode()
                == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? message.deliverAtEpochMs() : message.retryEligibilityAtEpochMs();
        final byte[] timelineKey = message.orderingMode()
                == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? KeyCodec.timelineOrdered(message.laneId(), timelineEligibleAt,
                SourcePositionCodec.decode(message.scheduleSourcePosition()).sourceOrderToken(), value.messageId(),
                message.generation())
                : KeyCodec.timelineDue(message.laneId(), timelineEligibleAt,
                SourcePositionCodec.decode(message.scheduleSourcePosition()).sourceOrderToken(), value.messageId(),
                message.generation());
        if (!Bytes.constantTimeEquals(value.timelineKeySha256(), Bytes.sha256(timelineKey))) {
            throw new IllegalStateException("READY timeline digest mismatch: " + value.messageId());
        }
        final byte[] timelineBytes = store.get(ColumnFamily.TIMELINE, timelineKey);
        if (timelineBytes == null) {
            throw new IllegalStateException("READY points to a missing timeline entry: " + value.messageId());
        }
        final TimelineEntry timeline = TimelineEntry.decode(ValueEnvelope.decode(timelineBytes, 1).payload());
        if (!timeline.messageId().equals(value.messageId()) || timeline.generation() != message.generation()) {
            throw new IllegalStateException("READY timeline identity mismatch: " + value.messageId());
        }
        final long accountedBytes = Math.max(1, message.payloadLength());
        return new ReadyProjection(lane, new ScheduleWorkItem(key.laneId(), value.messageId(), value.generation(),
                key.nextEligibleAtEpochMs(), accountedBytes));
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
        final LaneRecord lane = registered.get(entry.laneId());
        return lane != null && Arrays.equals(lane.laneIncarnation(), entry.laneIncarnation())
                && observedVersion(lane) == entry.observedLaneVersion();
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
        return new PersistedState(SchedulerProjectionsV1.ReadyDiscoveryCursor.decode(discovery.payload()),
                SchedulerProjectionsV1.ActiveRing.decode(activeRing.payload()),
                SchedulerProjectionsV1.DeficitMap.decode(deficits.payload()),
                SchedulerProjectionsV1.Round.decode(round.payload()),
                SchedulerProjectionsV1.LastServedMap.decode(lastServed.payload()));
    }

    private static OwnerIdentityV1 defaultOwner(final ShardStore store) {
        Objects.requireNonNull(store, "store");
        final byte[] worker = Bytes.concat(store.shardId().routeIncarnation().bytes(),
                Bytes.u32be(store.shardId().partition()));
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

    private record ReadyKey(DestinationLaneId laneId, long nextEligibleAtEpochMs, long laneVersion) {
    }

    private record ReadyProjection(LaneRecord lane, ScheduleWorkItem item) {
    }
}
