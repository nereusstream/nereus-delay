package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.SchedulerProjectionsV1;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private long ringGeneration;
    private boolean recoveryFirstPass = true;

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
            return;
        }
        final List<DestinationLaneId> order = persisted.activeRing().entries().stream()
                .filter(this::matchesRegisteredLane)
                .map(SchedulerProjectionsV1.RingEntry::laneId)
                .toList();
        delegate.restoreRing(order);
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
        recoveryFirstPass = persisted.round().recoveryFirstPass();
    }

    public synchronized void offer(final ScheduleWorkItem item) {
        delegate.offer(item);
    }

    public synchronized List<ScheduleWorkItem> poll(final SchedulerBudget budget) {
        final List<ScheduleWorkItem> result = delegate.poll(budget);
        persist();
        return result;
    }

    public synchronized void markBlocked(final DestinationLaneId laneId) {
        delegate.markBlocked(laneId);
        persist();
    }

    public synchronized void markReady(final DestinationLaneId laneId) {
        delegate.markReady(laneId);
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
                new SchedulerProjectionsV1.ReadyDiscoveryCursor(null, 0, ringGeneration);
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
        recoveryFirstPass = false;
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
}
