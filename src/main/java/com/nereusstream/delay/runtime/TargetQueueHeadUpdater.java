package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Computes actual Target heads from bounded RocksDB minima and the exact pending mutation overlay. */
public final class TargetQueueHeadUpdater {
    private TargetQueueHeadUpdater() {}

    public static List<TargetStoreBackend.Edit> complete(
            final TargetStoreBackend.Reader reader,
            final List<TargetStoreBackend.Edit> input,
            final int maximumAffectedDomains,
            final int maximumDomainSlots) {
        if (maximumAffectedDomains <= 0
                || maximumDomainSlots <= 0
                || maximumDomainSlots > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("finite activated Target domain limits required");
        }
        if (input.size() > reader.maximumWriteRecords()) {
            throw new IllegalArgumentException("Target head overlay exceeds the write record limit");
        }
        final Map<TargetPartitionId, Set<TargetKeyCodec.Domain>> affected = new LinkedHashMap<>();
        final var distinct = new LinkedHashSet<String>();
        for (var edit : input) {
            final byte[] key = edit.key();
            if (!Arrays.equals(edit.before(), reader.get(edit.family(), key))) {
                throw new IllegalStateException("input Target overlay differs from its exact Store before");
            }
            if (!distinct.add(edit.family() + ":" + Bytes.hex(key))) {
                throw new IllegalArgumentException("duplicate Target mutation key");
            }
            if (edit.family() == ColumnFamily.TIMELINE) {
                if (key[0] == TargetKeyCodec.DUE_TAG || key[0] == TargetKeyCodec.NATIVE_TAG) {
                    final var item = TargetKeyCodec.decodeCandidate(key);
                    add(affected, item.target(), item.domain());
                } else if (key[0] == TargetKeyCodec.ORDER_HEAD_TAG) {
                    final var item = TargetKeyCodec.decodeOrderedHead(key);
                    add(affected, item.target(), item.domain());
                }
            } else if (edit.family() == ColumnFamily.META && key[0] == TargetKeyCodec.STATE_TAG) {
                if (edit.after() == null) {
                    throw new IllegalArgumentException("queue retirement requires its protected lifecycle path");
                }
                final var queue = TargetQueueState.decode(payload(edit.after(), TargetQueueState.VALUE_TYPE));
                if (!Arrays.equals(key, TargetKeyCodec.state(queue.targetId()))) {
                    throw new IllegalArgumentException("Target queue edit key mismatch");
                }
                affected.computeIfAbsent(queue.targetId(), ignored -> new LinkedHashSet<>());
                for (var domain : queue.domains()) {
                    add(affected, queue.targetId(), domain.domain());
                }
            }
        }
        if (affected.values().stream().mapToLong(Set::size).sum() > maximumAffectedDomains
                || affected.size() > maximumAffectedDomains) {
            throw new IllegalArgumentException("Target mutation exceeds affected domain limit");
        }
        final var result = new ArrayList<>(input);
        for (var entry : affected.entrySet()) {
            final byte[] key = TargetKeyCodec.state(entry.getKey());
            final byte[] rawBefore = reader.get(ColumnFamily.META, key);
            final byte[] rawBase = reader.projected(ColumnFamily.META, key, input);
            if (rawBase == null) {
                throw new IllegalStateException("candidate has no registered Target queue");
            }
            final byte[] identityKey = TargetKeyCodec.identity(entry.getKey());
            final var identity = CanonicalTargetPartition.decodeForStore(
                    identityKey,
                    payload(
                            reader.projected(ColumnFamily.META, identityKey, input),
                            CanonicalTargetPartition.VALUE_TYPE));
            final var base = TargetQueueState.decodeForStore(
                    key, payload(rawBase, TargetQueueState.VALUE_TYPE), identity, reader.shardId(), maximumDomainSlots);
            final var before = rawBefore == null
                    ? null
                    : TargetQueueState.decodeForStore(
                            key,
                            payload(rawBefore, TargetQueueState.VALUE_TYPE),
                            identity,
                            reader.shardId(),
                            maximumDomainSlots);
            if (before != null && !Arrays.equals(rawBefore, rawBase)) {
                base.requireSuccessorOf(before);
            }
            if (before == null && base.headRevision() != 1) {
                throw new IllegalStateException("initial Target queue revision must be one");
            }
            final var domains = new ArrayList<>(base.domains());
            for (var domain : entry.getValue()) {
                if (domain.slot() >= domains.size()
                        || !domains.get(domain.slot()).domain().equals(domain)) {
                    throw new IllegalStateException("candidate points at missing/reused execution domain");
                }
                final var oldDomain =
                        before == null || domain.slot() >= before.domains().size()
                                ? null
                                : before.domains().get(domain.slot());
                // Existing minima are validated, never silently repaired by an unrelated mutation.
                if (oldDomain != null && oldDomain.domain().equals(domain)) {
                    final var oldOrdinary = ordinary(reader, before, identity, domain, List.of());
                    final var oldNative = head(reader, before, identity, domain, TargetKeyCodec.NATIVE_TAG, List.of());
                    if (!Objects.equals(oldOrdinary, oldDomain.ordinaryHead())
                            || !Objects.equals(oldNative, oldDomain.nativeHead())) {
                        throw new IllegalStateException("stored Target head disagrees with durable candidate minimum");
                    }
                }
                if (oldDomain == null || !oldDomain.domain().equals(domain)) {
                    if (ordinary(reader, base, identity, domain, List.of()) != null
                            || head(reader, base, identity, domain, TargetKeyCodec.NATIVE_TAG, List.of()) != null) {
                        throw new IllegalStateException("new execution domain already contains orphan candidates");
                    }
                }
                final var prior = domains.get(domain.slot());
                final var ordinary = ordinary(reader, base, identity, domain, input);
                final var nativeHead = head(reader, base, identity, domain, TargetKeyCodec.NATIVE_TAG, input);
                domains.set(
                        domain.slot(),
                        new TargetDomainState(
                                domain,
                                prior.lifecycle(),
                                prior.dispatchCompatibilityRef(),
                                prior.controlScopeRef(),
                                prior.nativePolicyScopeRef(),
                                ordinary,
                                nativeHead));
            }
            final long revision = before == null ? 1 : TargetQueueState.nextRevision(before.headRevision());
            final var next = new TargetQueueState(
                    base.targetId(),
                    revision,
                    base.controlVersion(),
                    base.admissionState(),
                    base.accountingIncarnation(),
                    base.nativeIndexLeadCapMs(),
                    domains);
            if (before != null) {
                next.requireSuccessorOf(before);
            }
            result.removeIf(edit -> edit.family() == ColumnFamily.META && Arrays.equals(edit.key(), key));
            result.add(new TargetStoreBackend.Edit(
                    ColumnFamily.META,
                    key,
                    rawBefore,
                    TargetValueEnvelope.encode(TargetQueueState.VALUE_TYPE, next.canonicalBytes())));
        }
        return List.copyOf(result);
    }

    private static void add(
            final Map<TargetPartitionId, Set<TargetKeyCodec.Domain>> affected,
            final TargetPartitionId target,
            final TargetKeyCodec.Domain domain) {
        affected.computeIfAbsent(target, ignored -> new LinkedHashSet<>()).add(domain);
    }

    private static TargetHeadRef ordinary(
            final TargetStoreBackend.Reader reader,
            final TargetQueueState queue,
            final CanonicalTargetPartition identity,
            final TargetKeyCodec.Domain domain,
            final List<TargetStoreBackend.Edit> overlay) {
        final var due = head(reader, queue, identity, domain, TargetKeyCodec.DUE_TAG, overlay);
        final var ordered = head(reader, queue, identity, domain, TargetKeyCodec.ORDER_HEAD_TAG, overlay);
        if (due == null) {
            return ordered;
        }
        if (ordered == null) {
            return due;
        }
        final int time = Long.compare(due.timeEpochMs(), ordered.timeEpochMs());
        return time < 0 || (time == 0 && Arrays.compareUnsigned(due.key(), ordered.key()) <= 0) ? due : ordered;
    }

    private static TargetHeadRef head(
            final TargetStoreBackend.Reader reader,
            final TargetQueueState queue,
            final CanonicalTargetPartition identity,
            final TargetKeyCodec.Domain domain,
            final int tag,
            final List<TargetStoreBackend.Edit> overlay) {
        final byte[] lower = TargetKeyCodec.candidatePrefix(TargetKeyCodec.CandidateKind.DUE, queue.targetId(), domain);
        lower[0] = (byte) tag;
        final ShardStore.KeyValue row = reader.first(ColumnFamily.TIMELINE, lower, upperBound(lower), overlay);
        if (row == null) {
            return null;
        }
        final byte[] workBytes = payload(row.value(), TargetTimelineWorkRef.VALUE_TYPE);
        final var work = TargetTimelineWorkRef.decode(workBytes);
        final byte[] messageKey = TargetKeyCodec.message(work.locator().messageId());
        final var message = TargetMessageRecord.decodeForStore(
                messageKey,
                payload(reader.projected(ColumnFamily.ID, messageKey, overlay), TargetMessageRecord.VALUE_TYPE),
                reader.shardId());
        work.requireQueueProjection(queue, identity);
        final long time =
                tag == TargetKeyCodec.NATIVE_TAG ? work.deliverAtEpochMs() : work.ordinaryEligibilityAtEpochMs();
        final var head = new TargetHeadRef(
                row.key(), work.locator().messageId(), work.locator().generation(), time);
        work.requireHeadProjection(head);
        if (tag == TargetKeyCodec.ORDER_HEAD_TAG) {
            final byte[] orderKey = TargetKeyCodec.orderState(
                    work.locator().target(), work.locator().orderingDomain());
            final var order = TargetOrderState.decodeForStore(
                    orderKey,
                    payload(reader.projected(ColumnFamily.META, orderKey, overlay), TargetOrderState.VALUE_TYPE),
                    reader.shardId(),
                    queue);
            order.requireServiceableProjection(row.key(), workBytes, message);
            final byte[] ordinary = reader.projected(ColumnFamily.TIMELINE, work.ordinaryKey(), overlay);
            if (!Arrays.equals(ordinary, row.value())) {
                throw new IllegalStateException("strict head lacks its exact ORDERED work");
            }
        } else {
            message.requireTimelineProjection(row.key(), workBytes);
            if (work.nativeCandidate()) {
                final byte[] siblingKey = tag == TargetKeyCodec.NATIVE_TAG ? work.ordinaryKey() : work.nativeKey();
                if (!Arrays.equals(reader.projected(ColumnFamily.TIMELINE, siblingKey, overlay), row.value())) {
                    throw new IllegalStateException("Target ordinary/native sibling is missing or changed");
                }
            }
        }
        return head;
    }

    private static byte[] payload(final byte[] raw, final int type) {
        if (raw == null) {
            throw new IllegalStateException("missing Target projection dependency");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    private static byte[] upperBound(final byte[] prefix) {
        final byte[] upper = Bytes.copy(prefix);
        for (int i = upper.length - 1; i >= 0; i--) {
            if (Byte.toUnsignedInt(upper[i]) != 255) {
                upper[i]++;
                return Arrays.copyOf(upper, i + 1);
            }
        }
        throw new IllegalArgumentException("Target prefix has no finite successor");
    }
}
