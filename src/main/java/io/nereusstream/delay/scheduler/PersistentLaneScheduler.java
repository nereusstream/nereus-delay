package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * Durable fairness wrapper for a shard-local {@link LaneScheduler}.
 *
 * <p>Lane records and timeline work remain authoritative in their own keys. This
 * class persists the scheduler's cursor, round, deficits and last-served
 * counters so a new owner does not reset fairness after a clean handoff.</p>
 */
public final class PersistentLaneScheduler {
    private static final int VALUE_TYPE = 5;

    private final ShardStore store;
    private final LaneScheduler delegate;
    private final LaneScheduler.SchedulerSnapshot persisted;

    public PersistentLaneScheduler(final ShardStore store, final LaneScheduler delegate) {
        this.store = Objects.requireNonNull(store, "store");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        final var value = store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(4), VALUE_TYPE);
        persisted = value == null ? null : SchedulerStateCodec.decode(value.payload());
    }

    public static PersistentLaneScheduler defaults(final ShardStore store) {
        return new PersistentLaneScheduler(store, LaneScheduler.defaults());
    }

    public synchronized void register(final LaneRecord lane) {
        delegate.register(lane);
    }

    /** Applies the saved counters after all currently active lanes are registered. */
    public synchronized void restorePersistedState() {
        if (persisted != null) {
            delegate.restore(persisted);
        }
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
        final byte[] encoded = SchedulerStateCodec.encode(delegate.snapshot());
        store.write(batch -> batch.putValue(ColumnFamily.META, VALUE_TYPE, KeyCodec.metaScheduler(4), encoded));
    }

    private static final class SchedulerStateCodec {
        private static final int VERSION = 1;

        private SchedulerStateCodec() {
        }

        private static byte[] encode(final LaneScheduler.SchedulerSnapshot snapshot) {
            final List<LaneScheduler.LaneSnapshot> lanes = snapshot.lanes();
            if (lanes.size() > 65535) {
                throw new IllegalArgumentException("scheduler lane count exceeds encoding limit");
            }
            final ByteBuffer output = ByteBuffer.allocate(4 + 4 + 8 + 4 + lanes.size() * (32 + 4 + 8 + 8));
            output.putInt(VERSION).putInt(snapshot.cursor()).putLong(snapshot.roundGeneration())
                    .putInt(lanes.size());
            for (LaneScheduler.LaneSnapshot lane : lanes) {
                output.put(lane.laneId().bytes()).putInt(lane.weight()).putLong(lane.deficit())
                        .putLong(lane.lastServedRound());
            }
            return output.array();
        }

        private static LaneScheduler.SchedulerSnapshot decode(final byte[] encoded) {
            final ByteBuffer input = ByteBuffer.wrap(encoded);
            if (input.remaining() < 4 + 4 + 8 + 4 || input.getInt() != VERSION) {
                throw new IllegalArgumentException("invalid scheduler state version");
            }
            final int cursor = input.getInt();
            final long round = input.getLong();
            final int count = input.getInt();
            if (cursor < 0 || round < 0 || count < 0 || count > 65535
                    || input.remaining() != count * (32 + 4 + 8 + 8)) {
                throw new IllegalArgumentException("invalid scheduler state length");
            }
            final List<LaneScheduler.LaneSnapshot> lanes = new java.util.ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                final byte[] laneBytes = new byte[32];
                input.get(laneBytes);
                final int weight = input.getInt();
                final long deficit = input.getLong();
                final long lastServed = input.getLong();
                if (weight <= 0 || deficit < 0 || lastServed < 0) {
                    throw new IllegalArgumentException("invalid scheduler lane counters");
                }
                lanes.add(new LaneScheduler.LaneSnapshot(new DestinationLaneId(laneBytes), weight, deficit,
                        lastServed, 0, true));
            }
            return new LaneScheduler.SchedulerSnapshot(cursor, round, lanes);
        }
    }
}
