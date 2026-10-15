package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetChannelControlVerifier;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Complete local Source channel facts for startup, never transport/retirement authority. */
public final class TargetWorkerChannelInventory {
    private static final int[] TAGS = {TargetKeyCodec.CHANNEL_CONTROL_TAG,
            TargetKeyCodec.CHANNEL_SLOT_TAG, TargetKeyCodec.CHANNEL_IDENTITY_TAG};
    // One row plus slot/event/identity/predecessor/Target and protected aggregate reads, including NV/key overhead.
    public static final long MINIMUM_READ_BYTES = 2L * TargetChannelControlRecord.MAX_CANONICAL_BYTES
            + 2L * TargetChannelSlot.MAX_CANONICAL_BYTES + 4L * TargetChannelIdentity.MAX_CANONICAL_BYTES
            + CanonicalTargetPartition.MAX_CANONICAL_BYTES + 4L * TargetQuotaAggregate.MAX_CANONICAL_BYTES + 2048;

    public record Limits(int maximumControls, int maximumSlots, int maximumIdentities, long maximumRetainedBytes,
            int rowsPerTurn, int readRecords, long readBytes, long readNanos) {
        public Limits {
            for (int value : new int[] {maximumControls, maximumSlots, maximumIdentities, rowsPerTurn}) {
                if (value < 1 || value == Integer.MAX_VALUE) throw new IllegalArgumentException("finite channel inventory counts required");
            }
            if (maximumRetainedBytes < 1 || maximumRetainedBytes == Long.MAX_VALUE
                    || readRecords < 16 || readRecords == Integer.MAX_VALUE
                    || readBytes < MINIMUM_READ_BYTES || readBytes == Long.MAX_VALUE
                    || readNanos < 1 || readNanos == Long.MAX_VALUE) {
                throw new IllegalArgumentException("channel inventory must serve one complete legal row with finite memory/read bounds");
            }
        }
        BoundedReadBudget budget(LongSupplier nanos) { return new BoundedReadBudget(readRecords, readBytes, readNanos, nanos); }
        public static Limits load(Map<String, String> supplied) {
            final var fields = new HashMap<>(supplied);
            final var result = new Limits(Math.toIntExact(number(fields, "maximumControls")),
                    Math.toIntExact(number(fields, "maximumSlots")), Math.toIntExact(number(fields, "maximumIdentities")),
                    number(fields, "maximumRetainedBytes"), Math.toIntExact(number(fields, "rowsPerTurn")),
                    Math.toIntExact(number(fields, "readRecords")), number(fields, "readBytes"), number(fields, "readNanos"));
            if (!fields.isEmpty()) throw new IllegalArgumentException("unknown channel inventory fields: " + fields.keySet());
            return result;
        }
        private static long number(Map<String, String> fields, String name) {
            final String value = Objects.requireNonNull(fields.remove(name), "missing inventory bound: " + name);
            if (!value.matches("[0-9]{1,19}")) throw new IllegalArgumentException("inventory bound must be an explicit decimal");
            return Long.parseLong(value);
        }
    }

    public record Entry(TargetChannelIdentity identity, CanonicalTargetPartition physical,
            TargetChannelSlot current, TargetChannelControlRecord origin) {
        public boolean currentOpen() {
            return current.phase() == TargetChannelSlot.Phase.OPEN && Arrays.equals(identity.digest(), current.channel().digest());
        }
    }
    public enum Status { READING, YIELD, RESTARTED, COMPLETE }
    public record Turn(Status status, int controls, int slots, int identities, long retainedBytes) {}
    private record Item(byte[] key, TargetChannelControlRecord control, TargetChannelSlot slot,
            TargetChannelIdentity identity, CanonicalTargetPartition physical) {}
    private record Page(TargetQueueSnapshotReader.Cut cut, long sourceSequence, List<Item> items,
            byte[] after, boolean ended, boolean yielded) {}
    private static final class Stale extends IllegalStateException {
        private static final long serialVersionUID = 1L;
    }

    private final TargetSourceApplyRuntime owner;
    private final Limits limits;
    private final byte[] lineage;
    private final LongSupplier clock, nanos;
    private final List<TargetChannelControlRecord> history = new ArrayList<>();
    private final Map<String, TargetChannelControlRecord> origins = new HashMap<>();
    private final Map<String, TargetChannelSlot> slots = new HashMap<>();
    private final Map<String, Entry> identities = new LinkedHashMap<>();
    private TargetQueueSnapshotReader.Cut cut;
    private long sourceSequence, retainedBytes;
    private int phase, controls;
    private byte[] after;
    private boolean complete, failed;
    private volatile boolean released;

    TargetWorkerChannelInventory(TargetSourceApplyRuntime owner, byte[] lineage, Limits limits,
            LongSupplier clock, LongSupplier nanos) {
        this.owner = owner; this.lineage = Bytes.copy(lineage); this.limits = Objects.requireNonNull(limits);
        this.clock = Objects.requireNonNull(clock); this.nanos = Objects.requireNonNull(nanos);
    }
    public Limits limits() { return limits; }

    /** One finite local page; only complete guarded rows advance the original cursor. */
    public synchronized Turn runTurn() {
        if (failed || released) throw new IllegalStateException("original channel inventory failed or exited");
        try {
            if (complete) { requireCurrent(); return turn(Status.COMPLETE); }
            final var page = owner.readChannelInventory(limits.budget(nanos), reader -> page(reader), clock);
            if (cut == null) { cut = page.cut(); sourceSequence = page.sourceSequence(); }
            for (var item : page.items()) retain(item);
            after = page.after();
            if (page.ended()) {
                after = null;
                if (++phase == TAGS.length) {
                    TargetChannelHistoryAudit.requireComplete(history,
                            identities.values().stream().map(Entry::identity).toList(), List.copyOf(slots.values()));
                    complete = true; requireCurrent(); return turn(Status.COMPLETE);
                }
            }
            return turn(page.yielded() ? Status.YIELD : Status.READING);
        } catch (Stale changed) {
            if (complete) { failed = true; throw changed; }
            history.clear(); origins.clear(); slots.clear(); identities.clear(); cut = null; after = null;
            controls = 0; phase = 0; retainedBytes = 0; return turn(Status.RESTARTED);
        } catch (RuntimeException | Error failure) { failed = true; throw failure; }
    }

    private Page page(TargetStoreBackend.Reader reader) {
        final var actual = new TargetQueueSnapshotReader.Cut(reader.metadata().storeIncarnation(), reader.nativeSequence());
        final var aggregate = reader.aggregate();
        if (cut != null && (!cut.equals(actual) || sourceSequence != aggregate.mutation().sequence())) throw new Stale();
        final int tag = TAGS[phase];
        byte[] cursor = after;
        byte[] lower = cursor == null ? new byte[] {(byte) tag} : Bytes.concat(cursor, new byte[] {0});
        final byte[] upper = {(byte) (tag + 1)};
        final var items = new ArrayList<Item>();
        for (int i = 0; i < limits.rowsPerTurn(); i++) {
            try {
                final var row = reader.first(ColumnFamily.META, lower, upper, List.of());
                if (row == null) return new Page(actual, aggregate.mutation().sequence(), items, cursor, true, false);
                final var item = read(reader, row, aggregate);
                reader.requireWithinElapsedBudget(); items.add(item);
                cursor = row.key(); lower = Bytes.concat(cursor, new byte[] {0});
            } catch (ReadIncompleteException yield) {
                return new Page(actual, aggregate.mutation().sequence(), items, cursor, false, true);
            }
        }
        return new Page(actual, aggregate.mutation().sequence(), items, cursor, false, false);
    }

    private Item read(TargetStoreBackend.Reader reader, ShardStore.KeyValue row, TargetQuotaAggregate aggregate) {
        if (phase == 0) {
            final var control = TargetChannelControlRecord.decodeForStore(row.key(),
                    TargetValueEnvelope.decode(row.value(), TargetChannelControlRecord.VALUE_TYPE).payload(), reader.shardId());
            if (!Arrays.equals(control.recoveryLineage(), lineage)) throw new IllegalStateException("channel event lineage differs");
            control.mutation().requireAtOrBefore(aggregate.mutation());
            final var channel = control.body().request().channel();
            requireIdentity(reader, channel);
            final var current = requireSlot(reader, channel);
            control.mutation().requireAtOrBefore(current.mutation());
            final var operation = control.body().request().operationKind();
            if (operation == ControlOperationKind.RENEW_TARGET_CHANNEL) {
                final byte[] key = TargetKeyCodec.channelIdentity(control.body().request().expectedPriorDigest());
                final var prior = TargetChannelIdentity.decodeForStore(key, TargetValueEnvelope.decode(
                        required(reader, key), TargetChannelIdentity.VALUE_TYPE).payload(), reader.shardId());
                channel.requireSuccessorOf(prior);
            } else if (operation != ControlOperationKind.CREATE_TARGET_CHANNEL
                    && operation != ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK) {
                throw new IllegalStateException("unsupported retained channel lifecycle operation");
            }
            return new Item(row.key(), control, null, null, null);
        }
        if (phase == 1) {
            final var slot = TargetChannelSlot.decodeForStore(row.key(),
                    TargetValueEnvelope.decode(row.value(), TargetChannelSlot.VALUE_TYPE).payload(), reader.shardId());
            final var exact = requireSlot(reader, slot.channel());
            if (!Arrays.equals(slot.canonicalBytes(), exact.canonicalBytes())) throw new IllegalStateException("current channel slot differs");
            return new Item(row.key(), null, slot, null, null);
        }
        final var channel = TargetChannelIdentity.decodeForStore(row.key(),
                TargetValueEnvelope.decode(row.value(), TargetChannelIdentity.VALUE_TYPE).payload(), reader.shardId());
        final var origin = origins.get(Bytes.hex(channel.digest()));
        if (origin == null || !Arrays.equals(origin.body().request().channel().canonicalBytes(), channel.canonicalBytes())) {
            throw new IllegalStateException("retained identity lacks its exact first-applied channel event");
        }
        final var current = requireSlot(reader, channel);
        final var scanned = slots.get(Bytes.hex(current.key()));
        if (scanned == null || !Arrays.equals(scanned.canonicalBytes(), current.canonicalBytes())) throw new IllegalStateException("channel slot inventory is incomplete");
        final byte[] physicalKey = TargetKeyCodec.identity(channel.context().target());
        final var physical = CanonicalTargetPartition.decodeForStore(physicalKey,
                TargetValueEnvelope.decode(required(reader, physicalKey), CanonicalTargetPartition.VALUE_TYPE).payload());
        return new Item(row.key(), null, current, channel, physical);
    }

    private TargetChannelSlot requireSlot(TargetStoreBackend.Reader reader, TargetChannelIdentity channel) {
        final var slot = TargetChannelControlVerifier.readSlot(reader, channel, lineage);
        if (slot == null || !Arrays.equals(slot.channel().context().producerIdentity(), channel.context().producerIdentity())
                || Long.compareUnsigned(slot.channel().context().channelGeneration(), channel.context().channelGeneration()) < 0) {
            throw new IllegalStateException("retained channel lacks its same-source current generation");
        }
        if (slot.channel().context().channelGeneration() == channel.context().channelGeneration()) slot.channel().requireExactFrozenIdentity(channel);
        return slot;
    }
    private static byte[] required(TargetStoreBackend.Reader reader, byte[] key) {
        return Objects.requireNonNull(reader.get(ColumnFamily.META, key), "retained channel dependency");
    }
    private static void requireIdentity(TargetStoreBackend.Reader reader, TargetChannelIdentity channel) {
        if (!Arrays.equals(TargetValueEnvelope.decode(required(reader, channel.encodedKey()), TargetChannelIdentity.VALUE_TYPE).payload(), channel.canonicalBytes())) {
            throw new IllegalStateException("channel event lacks its exact retained identity");
        }
    }
    private void retain(Item item) {
        if (phase == 0) {
            if (++controls > limits.maximumControls()) throw new IllegalStateException("channel control inventory limit exceeded");
            charge((long) item.key().length + item.control().canonicalBytes().length);
            history.add(item.control());
            if (item.control().body().request().operationKind() != ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK) {
                final String id = Bytes.hex(item.control().body().request().channel().digest());
                if (origins.size() >= limits.maximumIdentities() || origins.putIfAbsent(id, item.control()) != null) {
                    throw new IllegalStateException("channel origin inventory is duplicate or full");
                }
            }
        } else if (phase == 1) {
            if (slots.size() >= limits.maximumSlots() || slots.putIfAbsent(Bytes.hex(item.key()), item.slot()) != null) {
                throw new IllegalStateException("channel slot inventory is duplicate or full");
            }
            charge(item.key().length + item.slot().canonicalBytes().length);
        } else {
            final String id = Bytes.hex(item.identity().digest());
            if (identities.size() >= limits.maximumIdentities() || identities.putIfAbsent(id,
                    new Entry(item.identity(), item.physical(), slots.get(Bytes.hex(item.slot().key())), origins.get(id))) != null) {
                throw new IllegalStateException("retained channel inventory is duplicate or full");
            }
            charge((long) item.key().length + item.identity().canonicalBytes().length + item.physical().canonicalBytes().length);
        }
    }
    private void charge(long bytes) {
        retainedBytes = Math.addExact(retainedBytes, bytes);
        if (retainedBytes > limits.maximumRetainedBytes()) throw new IllegalStateException("retained channel inputs exceed their original byte bound");
    }
    private Turn turn(Status status) { return new Turn(status, controls, slots.size(), identities.size(), retainedBytes); }

    synchronized void requireOwner(TargetSourceApplyRuntime expected) {
        if (owner != expected) throw new IllegalArgumentException("channel inventory belongs to another original Source runtime");
        requireCurrent();
    }
    public synchronized void requireCurrent() {
        if (!complete || failed || released) throw new IllegalStateException("original channel inventory is not complete/current");
        owner.readChannelInventory(limits.budget(nanos), reader -> {
            if (!cut.equals(new TargetQueueSnapshotReader.Cut(reader.metadata().storeIncarnation(), reader.nativeSequence()))
                    || sourceSequence != reader.aggregate().mutation().sequence()) throw new Stale();
            reader.requireWithinElapsedBudget(); return true;
        }, clock);
    }
    public synchronized List<Entry> entries() { requireCurrent(); return List.copyOf(identities.values()); }
    synchronized Entry requireRetained(byte[] digest) {
        Bytes.requireLength(digest, 32, "original channel digest"); requireCurrent();
        final var entry = identities.get(Bytes.hex(digest));
        if (entry == null) throw new IllegalStateException("channel is absent from the complete retained Source inventory");
        return entry;
    }
    synchronized Entry requireOpen(byte[] digest) {
        final var entry = requireRetained(digest);
        if (!entry.currentOpen()) throw new IllegalStateException("channel is absent from the current OPEN Source inventory");
        return entry;
    }
    synchronized boolean hasOpenChannels() {
        requireCurrent(); return slots.values().stream().anyMatch(slot -> slot.phase() == TargetChannelSlot.Phase.OPEN);
    }
    /** Invalidates only local startup facts after the original Root's SDK/Store/Owner exit; retains audit data. */
    void releaseAfterExit() { released = true; }
}
