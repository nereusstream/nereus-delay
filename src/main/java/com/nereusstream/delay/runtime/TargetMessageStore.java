package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Applies complete Message/index/order/head projections through the actual Target atomic Store backend. */
public final class TargetMessageStore {
    public record Transition(TargetMessageRecord before, TargetMessageRecord after) {
        public Transition {
            if (before == null && after == null) {
                throw new IllegalArgumentException("empty Message transition");
            }
            if (before != null && after != null) {
                if (!before.locator().messageId().equals(after.locator().messageId())
                        || after.stateVersion() != TargetQueueState.nextRevision(before.stateVersion())) {
                    throw new IllegalArgumentException("Message transition changes identity or skips state revision");
                }
                final long oldGeneration =
                        Integer.toUnsignedLong(before.locator().generation());
                final long newGeneration =
                        Integer.toUnsignedLong(after.locator().generation());
                if (newGeneration != oldGeneration && newGeneration != oldGeneration + 1) {
                    throw new IllegalArgumentException("Message generation skips or wraps");
                }
            }
            if (before != null
                    && after == null
                    && (!before.runtime().terminal()
                            || !before.runtime().attemptObligations().isEmpty())) {
                throw new IllegalArgumentException("Message deletion retains work or attempts");
            }
        }
    }

    /** Gate, watermark and barrier are business decisions; serviceableHead is recomputed from the exact overlay. */
    public record OrderTransition(TargetOrderState before, TargetOrderState after) {
        public OrderTransition {
            Objects.requireNonNull(after, "after");
            if (before != null) {
                after.requireSuccessorOf(before);
            } else if (after.stateRevision() != 1) {
                throw new IllegalArgumentException("initial strict revision must be one");
            }
        }
    }

    public record Input(List<Transition> messages, List<OrderTransition> orders, List<TargetStoreBackend.Edit> extra) {
        public Input {
            messages = List.copyOf(messages);
            orders = List.copyOf(orders);
            extra = List.copyOf(extra);
        }
    }

    /** Receives the complete generated business write set before deriving leaf/mirror/total/aggregate deltas. */
    @FunctionalInterface
    public interface AccountingPlanner {
        TargetQuotaTotalsDelta prepare(
                TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> completeBusiness);
    }

    /** May append bookkeeping records derived from the complete business set; it must charge them too. */
    @FunctionalInterface
    public interface AccountingAssembler {
        TargetStoreBackend.Mutation assemble(
                TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> completeBusiness);
    }

    private final TargetStoreBackend backend;
    private final int maximumMessages;
    private final int maximumAffectedDomains;
    private final int maximumDomainSlots;

    public TargetMessageStore(
            final TargetStoreBackend backend,
            final int maximumMessages,
            final int maximumAffectedDomains,
            final int maximumDomainSlots) {
        this.backend = Objects.requireNonNull(backend, "backend");
        if (maximumMessages <= 0
                || maximumAffectedDomains <= 0
                || maximumDomainSlots <= 0
                || maximumDomainSlots > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("positive finite Message/domain limits required");
        }
        this.maximumMessages = maximumMessages;
        this.maximumAffectedDomains = maximumAffectedDomains;
        this.maximumDomainSlots = maximumDomainSlots;
    }

    public TargetStoreBackend.Prepared prepare(
            final BoundedReadBudget budget,
            final Function<TargetStoreBackend.Reader, Input> businessPlanner,
            final AccountingPlanner accounting) {
        Objects.requireNonNull(accounting, "accounting");
        return prepareAccounted(
                budget,
                businessPlanner,
                (reader, complete) -> new TargetStoreBackend.Mutation(accounting.prepare(reader, complete), complete));
    }

    public TargetStoreBackend.Prepared prepareAccounted(
            final BoundedReadBudget budget,
            final Function<TargetStoreBackend.Reader, Input> businessPlanner,
            final AccountingAssembler accounting) {
        Objects.requireNonNull(businessPlanner, "businessPlanner");
        Objects.requireNonNull(accounting, "accounting");
        return backend.prepare(budget, reader -> {
            final var input = Objects.requireNonNull(businessPlanner.apply(reader), "input");
            if (input.messages().size() > maximumMessages
                    || input.orders().size() > maximumAffectedDomains
                    || input.extra().size() > reader.maximumWriteRecords()) {
                throw new IllegalArgumentException("Target Message mutation exceeds its declared bounds");
            }
            final var edits = project(reader, input);
            final var complete =
                    TargetQueueHeadUpdater.complete(reader, edits, maximumAffectedDomains, maximumDomainSlots);
            final var assembled = Objects.requireNonNull(accounting.assemble(reader, complete), "assembled accounting");
            requirePreservedProjection(reader, complete, assembled.business());
            return assembled;
        });
    }

    /** Returns only after the complete business/accounting/source batch has committed. */
    public void apply(
            final BoundedReadBudget budget,
            final Function<TargetStoreBackend.Reader, Input> businessPlanner,
            final AccountingPlanner accounting,
            final TargetStoreBackend.CommitAuthority authority) {
        backend.commit(prepare(budget, businessPlanner, accounting), authority);
    }

    public void applyAccounted(
            final BoundedReadBudget budget,
            final Function<TargetStoreBackend.Reader, Input> businessPlanner,
            final AccountingAssembler accounting,
            final TargetStoreBackend.CommitAuthority authority) {
        backend.commit(prepareAccounted(budget, businessPlanner, accounting), authority);
    }

    private static void requirePreservedProjection(
            final TargetStoreBackend.Reader reader,
            final List<TargetStoreBackend.Edit> generated,
            final List<TargetStoreBackend.Edit> assembled) {
        if (assembled.size() > reader.maximumWriteRecords()) {
            throw new IllegalArgumentException("assembled accounting exceeds the write record limit");
        }
        final var expected = new LinkedHashMap<String, TargetStoreBackend.Edit>();
        for (var edit : generated) {
            add(expected, edit);
        }
        final var actual = new LinkedHashMap<String, TargetStoreBackend.Edit>();
        for (var edit : assembled) {
            add(actual, edit);
        }
        for (var entry : expected.entrySet()) {
            final var found = actual.remove(entry.getKey());
            if (found == null
                    || !Arrays.equals(found.before(), entry.getValue().before())
                    || !Arrays.equals(found.after(), entry.getValue().after())) {
                throw new IllegalStateException(
                        "accounting assembler changed or omitted a generated business projection");
            }
        }
        final byte[] rootKey = reader.aggregate().key();
        rootKey[0] = TargetKeyCodec.QUOTA_BOOKKEEPING_TAG;
        for (var edit : actual.values()) {
            if (edit.family() != ColumnFamily.META || !Arrays.equals(edit.key(), rootKey)) {
                throw new IllegalArgumentException("accounting assembler may append only the exact bookkeeping root");
            }
        }
    }

    private List<TargetStoreBackend.Edit> project(final TargetStoreBackend.Reader reader, final Input input) {
        final var edits = new LinkedHashMap<String, TargetStoreBackend.Edit>();
        for (var extra : input.extra()) {
            final int tag = Byte.toUnsignedInt(extra.key()[0]);
            if (extra.family() == ColumnFamily.TIMELINE
                    || (extra.family() == ColumnFamily.ID && tag == TargetKeyCodec.MESSAGE_TAG)
                    || (extra.family() == ColumnFamily.META && tag == TargetKeyCodec.ORDER_STATE_TAG)) {
                throw new IllegalArgumentException("extra edit bypasses Message/order projection ownership");
            }
            add(edits, extra);
        }
        final var orders = new LinkedHashMap<String, OrderTransition>();
        for (var order : input.orders()) {
            if (orders.putIfAbsent(Bytes.hex(order.after().encodedKey()), order) != null) {
                throw new IllegalArgumentException("duplicate strict ordering domain transition");
            }
        }
        final var ids = new java.util.HashSet<DelayMessageId>();
        for (var transition : input.messages()) {
            final var identity = transition.before() == null ? transition.after() : transition.before();
            if (!ids.add(identity.locator().messageId())) {
                throw new IllegalArgumentException("duplicate Message transition");
            }
            for (var message : new TargetMessageRecord[] {transition.before(), transition.after()}) {
                if (message != null
                        && (!reader.shardId().equals(message.scheduleSource().shardId())
                                || !reader.shardId()
                                        .equals(message.locator()
                                                .messageId()
                                                .routingId()
                                                .shardId()))) {
                    throw new IllegalArgumentException("Message transition crossed source Shard");
                }
            }
            requireOrder(transition.before(), orders);
            requireOrder(transition.after(), orders);
            requireWorkRouting(reader, transition.before(), List.of());
            requireWorkRouting(reader, transition.after(), List.copyOf(edits.values()));
            final var before = records(transition.before());
            final var after = records(transition.after());
            final var keys = new java.util.LinkedHashSet<>(before.keySet());
            keys.addAll(after.keySet());
            for (var key : keys) {
                final var old = before.get(key);
                final var next = after.get(key);
                final var location = old == null ? next : old;
                final byte[] actual = reader.get(location.family(), location.key());
                final byte[] expected = old == null ? null : old.value();
                if (!Arrays.equals(expected, actual)) {
                    throw new IllegalStateException("Message graph differs from the complete expected before");
                }
                final byte[] replacement = next == null ? null : next.value();
                if (!Arrays.equals(actual, replacement)) {
                    add(edits, new TargetStoreBackend.Edit(location.family(), location.key(), actual, replacement));
                }
            }
        }
        for (var transition : orders.values()) {
            final byte[] key = transition.after().encodedKey();
            final byte[] raw = reader.get(ColumnFamily.META, key);
            if (!Arrays.equals(
                    raw,
                    transition.before() == null
                            ? null
                            : TargetValueEnvelope.encode(
                                    TargetOrderState.VALUE_TYPE,
                                    transition.before().canonicalBytes()))) {
                throw new IllegalStateException("strict state changed before Message mutation");
            }
            final var before = transition.before();
            if (before != null) {
                final var priorProjection = orderHead(reader, before, List.of());
                if (!Objects.equals(before.serviceableHead(), priorProjection.head())) {
                    throw new IllegalStateException("strict serviceable head is not its durable minimum");
                }
                if (priorProjection.head() != null) {
                    final byte[] oldHead = reader.get(
                            ColumnFamily.TIMELINE, priorProjection.head().key());
                    if (!Arrays.equals(oldHead, priorProjection.value())) {
                        throw new IllegalStateException("strict serviceable head bytes are missing or changed");
                    }
                    add(
                            edits,
                            new TargetStoreBackend.Edit(
                                    ColumnFamily.TIMELINE,
                                    priorProjection.head().key(),
                                    oldHead,
                                    null));
                }
            }
            final var proposed = transition.after();
            final var projection = orderHead(reader, proposed, List.copyOf(edits.values()));
            final var next = new TargetOrderState(
                    proposed.target(),
                    proposed.orderingDomain(),
                    proposed.sourceShard(),
                    proposed.executionDomain(),
                    proposed.accountingIncarnation(),
                    proposed.orderingContract(),
                    proposed.stateRevision(),
                    proposed.controlVersion(),
                    proposed.gate(),
                    proposed.lastAdmittedOrder() == null
                            ? null
                            : proposed.lastAdmittedOrder().encodedKey(),
                    projection.head(),
                    proposed.barrier());
            if (before != null) {
                next.requireSuccessorOf(before);
            }
            final byte[] queueRaw = reader.projected(
                    ColumnFamily.META, TargetKeyCodec.state(next.target()), List.copyOf(edits.values()));
            if (queueRaw == null) {
                throw new IllegalStateException("strict state lacks its registered queue");
            }
            TargetOrderState.decodeForStore(
                    key,
                    next.canonicalBytes(),
                    reader.shardId(),
                    TargetQueueState.decode(TargetValueEnvelope.decode(queueRaw, TargetQueueState.VALUE_TYPE)
                            .payload()));
            add(
                    edits,
                    new TargetStoreBackend.Edit(
                            ColumnFamily.META,
                            key,
                            raw,
                            TargetValueEnvelope.encode(TargetOrderState.VALUE_TYPE, next.canonicalBytes())));
            if (projection.head() != null) {
                final String id = id(ColumnFamily.TIMELINE, projection.head().key());
                final var deleted = edits.remove(id);
                final byte[] old = deleted == null
                        ? reader.get(ColumnFamily.TIMELINE, projection.head().key())
                        : deleted.before();
                if (deleted == null && old != null) {
                    throw new IllegalStateException("new strict head key is occupied outside its prior projection");
                }
                add(
                        edits,
                        new TargetStoreBackend.Edit(
                                ColumnFamily.TIMELINE, projection.head().key(), old, projection.value()));
            }
        }
        return List.copyOf(edits.values());
    }

    private void requireWorkRouting(
            final TargetStoreBackend.Reader reader,
            final TargetMessageRecord message,
            final List<TargetStoreBackend.Edit> overlay) {
        if (message == null || message.runtime().timeline() == null) {
            return;
        }
        final byte[] queueKey = TargetKeyCodec.state(message.locator().target());
        final byte[] identityKey = TargetKeyCodec.identity(message.locator().target());
        final byte[] queueRaw = reader.projected(ColumnFamily.META, queueKey, overlay);
        final byte[] identityRaw = reader.projected(ColumnFamily.META, identityKey, overlay);
        if (queueRaw == null || identityRaw == null) {
            throw new IllegalStateException("Message work lacks its registered Target/queue");
        }
        final var identity = com.nereusstream.delay.protocol.CanonicalTargetPartition.decodeForStore(
                identityKey,
                TargetValueEnvelope.decode(
                                identityRaw, com.nereusstream.delay.protocol.CanonicalTargetPartition.VALUE_TYPE)
                        .payload());
        final var queue = TargetQueueState.decodeForStore(
                queueKey,
                TargetValueEnvelope.decode(queueRaw, TargetQueueState.VALUE_TYPE)
                        .payload(),
                identity,
                reader.shardId(),
                maximumDomainSlots);
        message.runtime().timeline().requireQueueProjection(queue, identity);
    }

    private record Stored(ColumnFamily family, byte[] key, byte[] value) {}

    private record OrderProjection(TargetHeadRef head, byte[] value) {}

    private static Map<String, Stored> records(final TargetMessageRecord message) {
        final var records = new LinkedHashMap<String, Stored>();
        if (message == null) {
            return records;
        }
        put(records, ColumnFamily.ID, message.encodedKey(), TargetMessageRecord.VALUE_TYPE, message.canonicalBytes());
        if (!message.runtime().terminal()) {
            final var expiry = new TargetExpiryRef(message.locator(), message.expireAtEpochMs());
            put(
                    records,
                    ColumnFamily.TIMELINE,
                    expiry.encodedKey(),
                    TargetExpiryRef.VALUE_TYPE,
                    expiry.canonicalBytes());
        }
        final var work = message.runtime().timeline();
        if (work != null) {
            put(
                    records,
                    ColumnFamily.TIMELINE,
                    work.ordinaryKey(),
                    TargetTimelineWorkRef.VALUE_TYPE,
                    work.canonicalBytes());
            if (work.nativeCandidate()) {
                put(
                        records,
                        ColumnFamily.TIMELINE,
                        work.nativeKey(),
                        TargetTimelineWorkRef.VALUE_TYPE,
                        work.canonicalBytes());
            }
        }
        return records;
    }

    private static void put(
            final Map<String, Stored> records,
            final ColumnFamily family,
            final byte[] key,
            final int type,
            final byte[] value) {
        records.put(id(family, key), new Stored(family, key, TargetValueEnvelope.encode(type, value)));
    }

    private static void requireOrder(final TargetMessageRecord message, final Map<String, OrderTransition> orders) {
        if (message != null
                && message.locator().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                && !orders.containsKey(Bytes.hex(TargetKeyCodec.orderState(
                        message.locator().target(), message.locator().orderingDomain())))) {
            throw new IllegalArgumentException("strict Message mutation omitted its ordering domain transition");
        }
    }

    private static OrderProjection orderHead(
            final TargetStoreBackend.Reader reader,
            final TargetOrderState state,
            final List<TargetStoreBackend.Edit> overlay) {
        if (!reader.shardId().equals(state.sourceShard())) {
            throw new IllegalArgumentException("foreign strict state source");
        }
        if (state.barrier() != null) {
            state.requireBarrierProjection(
                    readMessage(reader, state.barrier().locator().messageId(), overlay));
        }
        if (state.gate() != TargetOrderState.Gate.OPEN || state.barrier() != null) {
            return new OrderProjection(null, null);
        }
        final byte[] prefix = Bytes.concat(
                new byte[] {TargetKeyCodec.ORDERED_TAG, 1}, state.target().bytes(), state.orderingDomain());
        final byte[] upper = Bytes.copy(prefix);
        int end = upper.length - 1;
        while (end >= 0 && Byte.toUnsignedInt(upper[end]) == 255) {
            end--;
        }
        if (end < 0) {
            throw new IllegalStateException("strict prefix has no successor");
        }
        upper[end]++;
        final var row = reader.first(ColumnFamily.TIMELINE, prefix, Arrays.copyOf(upper, end + 1), overlay);
        if (row == null) {
            return new OrderProjection(null, null);
        }
        final byte[] workBytes = TargetValueEnvelope.decode(row.value(), TargetTimelineWorkRef.VALUE_TYPE)
                .payload();
        final var work = TargetTimelineWorkRef.decode(workBytes);
        final var message = readMessage(reader, work.locator().messageId(), overlay);
        message.requireTimelineProjection(row.key(), workBytes);
        final byte[] key = TargetKeyCodec.orderedHead(
                state.target(), state.executionDomain(), work.ordinaryEligibilityAtEpochMs(), state.orderingDomain());
        final var head = new TargetHeadRef(
                key, work.locator().messageId(), work.locator().generation(), work.ordinaryEligibilityAtEpochMs());
        final var projected = new TargetOrderState(
                state.target(),
                state.orderingDomain(),
                state.sourceShard(),
                state.executionDomain(),
                state.accountingIncarnation(),
                state.orderingContract(),
                state.stateRevision(),
                state.controlVersion(),
                state.gate(),
                state.lastAdmittedOrder() == null
                        ? null
                        : state.lastAdmittedOrder().encodedKey(),
                head,
                null);
        projected.requireServiceableProjection(key, workBytes, message);
        return new OrderProjection(head, row.value());
    }

    private static TargetMessageRecord readMessage(
            final TargetStoreBackend.Reader reader,
            final DelayMessageId id,
            final List<TargetStoreBackend.Edit> overlay) {
        final byte[] key = TargetKeyCodec.message(id);
        final byte[] raw = reader.projected(ColumnFamily.ID, key, overlay);
        if (raw == null) {
            throw new IllegalStateException("strict projection has no Message");
        }
        return TargetMessageRecord.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(),
                reader.shardId());
    }

    private static String id(final ColumnFamily family, final byte[] key) {
        return family.name() + ':' + Bytes.hex(key);
    }

    private static void add(final Map<String, TargetStoreBackend.Edit> edits, final TargetStoreBackend.Edit edit) {
        if (edits.putIfAbsent(id(edit.family(), edit.key()), edit) != null) {
            throw new IllegalArgumentException("duplicate generated Target projection");
        }
    }
}
