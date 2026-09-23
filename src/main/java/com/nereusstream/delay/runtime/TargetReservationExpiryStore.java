package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One bounded materialization of a committed time-fence decision; never appends or renumbers source records. */
public final class TargetReservationExpiryStore {
    private final TargetStoreBackend backend;
    private final TargetReservationQueryStore queries;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumDomains;
    private final TargetReservationControls.Authority controls;

    public TargetReservationExpiryStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumDomains,
            TargetReservationControls.Authority controls) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.controls = Objects.requireNonNull(controls, "controls");
        queries = new TargetReservationQueryStore(backend, scope, lineage, controls);
        this.scope = scope;
        this.lineage = Bytes.copy(lineage);
        if (maximumDomains < 1 || maximumDomains > 64) {
            throw new IllegalArgumentException("expiry requires a finite domain bound");
        }
        this.maximumDomains = maximumDomains;
    }

    /** Process-local sweep continuation; never a write authorization or a recovery checkpoint. */
    public static final class Cursor {
        private final TargetReservationExpiryStore owner;
        private final long cutoff;
        private final byte[] after;

        private Cursor(TargetReservationExpiryStore owner, long cutoff, byte[] after) {
            this.owner = owner;
            this.cutoff = cutoff;
            this.after = Bytes.copy(after);
        }
    }

    /** An empty candidate ends this sweep; the next discovery must start again with a null cursor. */
    public record Discovery(Optional<TargetReservationRecord> candidate, Cursor nextCursor) {
        public Discovery {
            Objects.requireNonNull(candidate, "candidate");
            if (candidate.isPresent() != (nextCursor != null)) {
                throw new IllegalArgumentException("expiry discovery cursor and candidate disagree");
            }
        }
    }

    public void requireShard(com.nereusstream.delay.protocol.ShardId shard) {
        queries.requireShard(shard);
    }

    /** One guarded range seek. The cutoff is frozen for a sweep and derives only from committed TIME_FENCE. */
    public Discovery discover(BoundedReadBudget budget, Cursor cursor, TargetStoreBackend.ReadAuthority authority) {
        if (cursor != null && cursor.owner != this) {
            throw new IllegalArgumentException("foreign reservation expiry cursor");
        }
        return backend.guardedRead(
                budget,
                reader -> {
                    if (!reader.shardId().equals(scope.shard()) || reader.source() == null) {
                        throw new IllegalStateException("expiry discovery requires its established Shard source");
                    }
                    final byte[] rawFence = reader.get(ColumnFamily.META, KeyCodec.metaFixed(4));
                    final var fence = rawFence == null
                            ? null
                            : IngressFenceState.decode(
                                    TargetValueEnvelope.decode(rawFence, 1).payload());
                    final long watermark = fence == null ? IngressFenceState.OPEN : fence.closedThroughEpochMs();
                    if (watermark >= 0 && fence.proofId() == null) {
                        throw new IllegalStateException("expiry discovery requires an authenticated persisted fence");
                    }
                    if (cursor != null && watermark < cursor.cutoff) {
                        throw new IllegalStateException("expiry discovery fence regressed; discard the sweep");
                    }
                    final long cutoff = cursor == null ? watermark : cursor.cutoff;
                    if (cutoff == IngressFenceState.OPEN) {
                        return new Discovery(Optional.empty(), null);
                    }
                    final byte[] prefix = {TargetKeyCodec.RESERVATION_EXPIRY_TAG, 1};
                    final byte[] lower = cursor == null ? prefix : Bytes.concat(cursor.after, new byte[] {0});
                    final byte[] upper = Bytes.concat(
                            prefix, cutoff == Long.MAX_VALUE ? new byte[] {(byte) 0x80} : Bytes.u64be(cutoff + 1));
                    final var row = reader.first(ColumnFamily.TIMELINE, lower, upper, List.of());
                    if (row == null) {
                        return new Discovery(Optional.empty(), null);
                    }
                    final var candidate = TargetReservationRecord.decode(
                            TargetValueEnvelope.decode(row.value(), TargetReservationRecord.VALUE_TYPE)
                                    .payload());
                    if (!Arrays.equals(row.key(), candidate.expiryKey())
                            || candidate.status() != PayloadReservationStatus.RESERVED
                            || candidate.expiryEpochMs() > cutoff
                            || !Arrays.equals(lineage, candidate.recoveryLineage())
                            || !candidate
                                    .locator()
                                    .messageId()
                                    .routingId()
                                    .shardId()
                                    .equals(scope.shard())) {
                        throw new IllegalStateException("expiry discovery index differs from its reservation identity");
                    }
                    return new Discovery(Optional.of(candidate), new Cursor(this, cutoff, row.key()));
                },
                authority);
    }

    /** Close won before expiry; leave this reservation intact for source-ordered closure materialization. */
    public static final class ClosedTargetException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private ClosedTargetException() {
            super("reservation Close precedes expiry and requires closure materialization");
        }
    }

    /**
     * A guarded no-op returns false. A changed candidate or failed/unknown write throws, requiring fresh discovery.
     * Quota/fence/Owner/Store authority must remain valid through commit. Does not delete external payload objects.
     */
    public boolean materialize(
            BoundedReadBudget budget,
            byte[] reservationId,
            TargetStoreBackend.ReadAuthority reads,
            TargetStoreBackend.CommitAuthority writes,
            TargetQuotaDelta.ReservationExpiryAuthority authority) {
        return materializeTerminal(budget, reservationId, reads, writes, authority, false);
    }

    /** Shared exact-projection accounting; the public Close facade supplies its separate authority. */
    boolean materializeTerminal(
            BoundedReadBudget budget,
            byte[] reservationId,
            TargetStoreBackend.ReadAuthority reads,
            TargetStoreBackend.CommitAuthority writes,
            TargetQuotaDelta.ReservationExpiryAuthority authority,
            boolean closure) {
        Objects.requireNonNull(writes, "writes");
        Objects.requireNonNull(authority, "authority");
        final var terminal = closure ? PayloadReservationStatus.ABANDONED : PayloadReservationStatus.EXPIRED;
        final var found = queries.read(budget, reservationId, reads);
        if (!closure
                && found.isPresent()
                && found.orElseThrow().reservation().status() == PayloadReservationStatus.RESERVED
                && found.orElseThrow().effectiveStatus() == PayloadReservationStatus.ABANDONED) {
            throw new ClosedTargetException();
        }
        if (found.isEmpty()
                || found.orElseThrow().reservation().status() != PayloadReservationStatus.RESERVED
                || found.orElseThrow().effectiveStatus() != terminal) {
            return false;
        }
        final var expected = found.orElseThrow().reservation();
        final var plan = backend.prepare(budget, reader -> {
            final byte[] raw = reader.get(ColumnFamily.ID, expected.lookupKey());
            if (raw == null
                    || !Arrays.equals(
                            TargetValueEnvelope.decode(raw, TargetReservationRecord.VALUE_TYPE)
                                    .payload(),
                            expected.canonicalBytes())) {
                throw new IllegalStateException("reservation changed before expiry materialization; rediscover");
            }
            final byte[] rawFence = reader.get(ColumnFamily.META, KeyCodec.metaFixed(4));
            if (!closure) {
                if (rawFence == null) {
                    throw new IllegalStateException("reservation expiry lost its committed fence");
                }
                final var fence = IngressFenceState.decode(
                        TargetValueEnvelope.decode(rawFence, 1).payload());
                if (fence.proofId() == null
                        || expected.effectiveStatus(fence.closedThroughEpochMs()) != PayloadReservationStatus.EXPIRED) {
                    throw new IllegalStateException("reservation has no authenticated persisted expiry decision");
                }
            }
            final var queue = TargetQueueState.decode(TargetValueEnvelope.decode(
                            reader.get(
                                    ColumnFamily.META,
                                    TargetKeyCodec.state(expected.locator().target())),
                            TargetQueueState.VALUE_TYPE)
                    .payload());
            if (!queue.targetId().equals(expected.locator().target())) {
                throw new IllegalStateException("expiry Target queue identity differs from its key");
            }
            final byte[] bindingKey =
                    TargetKeyCodec.scheduleBinding(expected.locator().scheduleBindingDigest());
            final var binding = TargetScheduleBinding.decodeForStore(
                    bindingKey,
                    TargetValueEnvelope.decode(
                                    reader.get(ColumnFamily.ID, bindingKey), TargetScheduleBinding.VALUE_TYPE)
                            .payload(),
                    scope.shard());
            final var decision = TargetReservationControls.resolve(reader, expected, binding, queue, controls);
            if (!closure && decision.status() == PayloadReservationStatus.ABANDONED) {
                throw new ClosedTargetException();
            }
            if (decision.status() != terminal || (closure && decision.closure().isEmpty())) {
                throw new IllegalStateException("expiry decision changed before materialization; rediscover");
            }
            if (reader.get(
                            ColumnFamily.ID,
                            TargetKeyCodec.message(expected.locator().messageId()))
                    != null) {
                throw new IllegalStateException("reserved expiry candidate already has a Message");
            }
            final var aggregate = reader.aggregate();
            if (aggregate.mutation() == null || reader.source() == null) {
                throw new IllegalStateException("local expiry requires an established aggregate/source");
            }
            aggregate.mutation().requireAtOrBefore(reader.sourceSequence(), reader.source());
            final byte[] digest = Bytes.sha256(
                    Bytes.utf8(
                            closure
                                    ? "nereus-delay-target-reservation-closure-materialization\0"
                                    : "nereus-delay-target-reservation-expiry\0"),
                    expected.canonicalBytes(),
                    rawFence == null ? new byte[0] : rawFence,
                    aggregate.mutation().canonicalBytes(),
                    decision.closure()
                            .map(TargetReservationControls.Closure::digest)
                            .orElseGet(() -> new byte[32]));
            final var stamp = new TargetQuotaMutation(
                    reader.sourceSequence(),
                    reader.source(),
                    digest,
                    TargetQuotaMutation.increment(aggregate.mutation().localOrdinal()),
                    !closure,
                    closure);
            final byte[] ownerKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                    expected.locator().messageId().bytes());
            final var owner = TargetQuotaPayloadOwner.decodeForStore(
                    ownerKey,
                    TargetValueEnvelope.decode(
                                    reader.get(ColumnFamily.META, ownerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    scope.tenantScope());
            expected.requireOwner(owner);
            final var expired = closure
                    ? expected.finishClosed(stamp, decision.closure().orElseThrow())
                    : expected.finish(PayloadReservationStatus.EXPIRED, stamp, null);
            final var retained = owner.retain(stamp, (prior, next, floor) -> {
                if (prior != owner || !next.mutation().equals(stamp) || floor != null) {
                    throw new IllegalStateException("expiry changed its exact payload transition");
                }
            });
            expired.requireOwner(retained);
            final var edits = List.of(
                    reader.replace(
                            ColumnFamily.ID,
                            expected.key(),
                            TargetReservationRecord.VALUE_TYPE,
                            expired.canonicalBytes()),
                    reader.replace(
                            ColumnFamily.ID,
                            expected.lookupKey(),
                            TargetReservationRecord.VALUE_TYPE,
                            expired.canonicalBytes()),
                    reader.replace(
                            ColumnFamily.ID, expected.targetIndexKey(), TargetReservationRecord.VALUE_TYPE, null),
                    reader.replace(
                            ColumnFamily.TIMELINE, expected.expiryKey(), TargetReservationRecord.VALUE_TYPE, null),
                    reader.replace(
                            ColumnFamily.META,
                            ownerKey,
                            TargetQuotaPayloadOwner.VALUE_TYPE,
                            retained.canonicalBytes()));
            return account(reader, edits, stamp, authority);
        });
        backend.commit(plan, writes);
        return true;
    }

    private TargetStoreBackend.Mutation account(
            TargetStoreBackend.Reader reader,
            List<TargetStoreBackend.Edit> edits,
            TargetQuotaMutation stamp,
            TargetQuotaDelta.ReservationExpiryAuthority authority) {
        final var aggregate = reader.aggregate();
        final var rootId = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null);
        final byte[] rootKey = aggregate.key();
        rootKey[0] = TargetKeyCodec.QUOTA_BOOKKEEPING_TAG;
        final var root = TargetQuotaBookkeeping.decodeForStore(
                rootKey,
                TargetValueEnvelope.decode(reader.get(ColumnFamily.META, rootKey), TargetQuotaBookkeeping.VALUE_TYPE)
                        .payload(),
                rootId,
                scope.tenantScope());
        root.requireRoot(aggregate);
        final var before = new TargetRecordAccounting(reader, List.of(), scope, lineage, stamp, root, maximumDomains);
        final var after = new TargetRecordAccounting(reader, edits, scope, lineage, stamp, root, maximumDomains);
        final Map<TargetQuotaIdentity, TargetQuotaUsage> removed = new LinkedHashMap<>();
        final Map<TargetQuotaIdentity, TargetQuotaUsage> added = new LinkedHashMap<>();
        for (var edit : edits) {
            if (edit.before() == null) {
                throw new IllegalStateException("expiry cannot invent a missing projection");
            }
            final var priorCharge = before.charge(edit.family(), edit.key(), edit.before());
            accumulate(removed, priorCharge);
            if (edit.after() != null) {
                final var nextCharge = after.charge(edit.family(), edit.key(), edit.after());
                if (!priorCharge.owner().identity().equals(nextCharge.owner().identity())
                        || !priorCharge
                                .owner()
                                .accounting()
                                .equals(nextCharge.owner().accounting())) {
                    throw new IllegalStateException("expiry changed its frozen owner/accounting artifact");
                }
                accumulate(added, nextCharge);
            }
        }
        if (removed.size() != 2 || !added.keySet().equals(removed.keySet())) {
            throw new IllegalStateException("expiry must retain its one original owner and mirror");
        }
        final var updates = new ArrayList<TargetQuotaDelta.Update>();
        for (var identity : removed.keySet()) {
            final var prior = reader.counter(identity);
            if (prior == null) {
                throw new IllegalStateException("expiry cannot allocate a counter");
            }
            updates.add(new TargetQuotaDelta.Update(
                    identity, prior.usage().subtract(removed.get(identity)).add(added.get(identity))));
        }
        final TargetQuotaDelta.ReservationExpiryAuthority guarded = value -> {
            try {
                authority.requireAuthorized(value);
            } catch (ReadIncompleteException external) {
                throw new IllegalStateException(
                        "external reservation materialization authority did not complete", external);
            }
        };
        final var delta = stamp.reservationClosure()
                ? TargetQuotaDelta.prepareReservationClosure(
                        aggregate,
                        reader.sourceSequence(),
                        reader.source(),
                        stamp.mutationDigest(),
                        updates,
                        reader::counter,
                        guarded::requireAuthorized)
                : TargetQuotaDelta.prepareReservationExpiry(
                        aggregate,
                        reader.sourceSequence(),
                        reader.source(),
                        stamp.mutationDigest(),
                        updates,
                        reader::counter,
                        guarded);
        if (!delta.mutation().equals(stamp)) {
            throw new IllegalStateException("expiry accounting differs from its materialization stamp");
        }
        return new TargetStoreBackend.Mutation(TargetQuotaTotalsDelta.prepare(delta, scope, 1, reader::total), edits);
    }

    private static void accumulate(
            Map<TargetQuotaIdentity, TargetQuotaUsage> values, TargetRecordAccounting.Charge charge) {
        values.merge(charge.owner().identity(), charge.primary(), TargetQuotaUsage::add);
        values.merge(charge.owner().tenantIdentity(), charge.mirror(), TargetQuotaUsage::add);
    }
}
