package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.runtime.TargetQuotaTotalsDelta;
import com.nereusstream.delay.runtime.TargetResultLedgerAudit;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Real RocksDB adapter for Target plans; production composition must still supply business/lease authority. */
public final class TargetStoreBackend {
    public record WriteLimits(int maximumRecords, long maximumEncodedBytes) {
        public WriteLimits {
            if (maximumRecords <= 0 || maximumEncodedBytes <= 0) {
                throw new IllegalArgumentException("positive finite Target write limits required");
            }
        }
    }

    /** Exact complete NV before/after, with null denoting absence/deletion. */
    public record Edit(ColumnFamily family, byte[] key, byte[] before, byte[] after) {
        public Edit {
            Objects.requireNonNull(family, "family");
            key = Bytes.copy(Objects.requireNonNull(key, "key"));
            before = before == null ? null : Bytes.copy(before);
            after = after == null ? null : Bytes.copy(after);
            if (before == null && after == null) {
                throw new IllegalArgumentException("empty Target edit");
            }
            final int type = businessType(family, key);
            if (before != null) {
                TargetValueEnvelope.decode(before, type);
            }
            if (after != null) {
                TargetValueEnvelope.decode(after, type);
            }
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }

        @Override
        public byte[] before() {
            return before == null ? null : Bytes.copy(before);
        }

        @Override
        public byte[] after() {
            return after == null ? null : Bytes.copy(after);
        }
    }

    public record Mutation(TargetQuotaTotalsDelta quota, List<Edit> business) {
        public Mutation {
            Objects.requireNonNull(quota, "quota");
            business = List.copyOf(business);
        }
    }

    /** Must hold source/Owner/Store/Route/grant and complete business semantics through native commit. */
    @FunctionalInterface
    public interface CommitAuthority {
        CommitGuard acquire(StoreMetadata store, TargetQuotaScope scope, Mutation mutation);
    }

    public interface CommitGuard extends AutoCloseable {
        void requireCurrent();

        @Override
        void close();
    }

    public static final class Prepared {
        private final TargetStoreBackend backend;
        private final ShardStore.ReadView view;
        private final Mutation mutation;
        private final int writeRecords;
        private final long encodedBytes;
        private boolean attempted;

        private Prepared(
                final TargetStoreBackend backend,
                final ShardStore.ReadView view,
                final Mutation mutation,
                final int writeRecords,
                final long encodedBytes) {
            this.backend = backend;
            this.view = view;
            this.mutation = mutation;
            this.writeRecords = writeRecords;
            this.encodedBytes = encodedBytes;
        }

        public Mutation mutation() {
            return mutation;
        }

        public int writeRecords() {
            return writeRecords;
        }

        public long encodedBytes() {
            return encodedBytes;
        }
    }

    /** Read methods are valid only during prepare, under one Store view and shared read budget. */
    public final class Reader {
        private boolean active = true;
        private final BoundedReadBudget budget;

        private Reader(final BoundedReadBudget budget) {
            this.budget = budget;
        }

        private void requireActive() {
            if (!active || !Thread.holdsLock(store)) {
                throw new IllegalStateException("Target reader escaped its bounded Store plan");
            }
        }

        public byte[] get(final ColumnFamily family, final byte[] key) {
            requireActive();
            return store.get(family, key);
        }

        public StoreMetadata metadata() {
            requireActive();
            return store.metadata();
        }

        public int maximumWriteRecords() {
            requireActive();
            return limits.maximumRecords();
        }

        public com.nereusstream.delay.protocol.ShardId shardId() {
            requireActive();
            return store.shardId();
        }

        /** Exact overlay read; absence is explicit and never inferred from a bounded range scan. */
        public byte[] projected(final ColumnFamily family, final byte[] key, final List<Edit> overlay) {
            requireActive();
            requireOverlayLimit(overlay);
            Edit found = null;
            for (var edit : overlay) {
                if (edit.family == family && Arrays.equals(edit.key, key)) {
                    if (found != null) {
                        throw new IllegalArgumentException("duplicate overlay key");
                    }
                    found = edit;
                }
            }
            return found == null ? get(family, key) : found.after();
        }

        /** Finds the first surviving key using at most overlay.size()+1 persisted entries. */
        public ShardStore.KeyValue first(
                final ColumnFamily family, final byte[] lower, final byte[] upper, final List<Edit> overlay) {
            requireActive();
            requireOverlayLimit(overlay);
            if (lower == null || upper == null || Arrays.compareUnsigned(lower, upper) >= 0) {
                throw new IllegalArgumentException("finite ordered range required");
            }
            final var replacements = new java.util.HashMap<String, Edit>();
            ShardStore.KeyValue selected = null;
            for (var edit : overlay) {
                if (edit.family != family
                        || Arrays.compareUnsigned(edit.key, lower) < 0
                        || Arrays.compareUnsigned(edit.key, upper) >= 0) {
                    continue;
                }
                if (replacements.putIfAbsent(HexFormat.of().formatHex(edit.key), edit) != null) {
                    throw new IllegalArgumentException("duplicate range overlay key");
                }
                if (edit.after != null && (selected == null || Arrays.compareUnsigned(edit.key, selected.key()) < 0)) {
                    selected = new ShardStore.KeyValue(edit.key, edit.after);
                }
            }
            final ShardStore.KeyValue[] persisted = {null};
            final var result = store.visitResult(
                    family, lower, upper, Math.incrementExact(replacements.size()), budget, (row, shared) -> {
                        if (replacements.containsKey(HexFormat.of().formatHex(row.key()))) {
                            return true;
                        }
                        persisted[0] = row;
                        return false;
                    });
            if (result.stop() == ShardStore.VisitStop.INCOMPLETE) {
                throw new ReadIncompleteException(result.reason());
            }
            if (result.stop() != ShardStore.VisitStop.RANGE_END
                    && result.stop() != ShardStore.VisitStop.VISITOR_STOPPED) {
                throw new IllegalStateException("Target overlay minimum was not established");
            }
            return persisted[0] != null
                            && (selected == null || Arrays.compareUnsigned(persisted[0].key(), selected.key()) < 0)
                    ? persisted[0]
                    : selected;
        }

        private void requireOverlayLimit(final List<Edit> overlay) {
            if (Objects.requireNonNull(overlay, "overlay").size() > limits.maximumRecords()) {
                throw new IllegalArgumentException("Target overlay exceeds the write record limit");
            }
        }

        public SourcePosition source() {
            requireActive();
            return store.appliedShardLogPosition();
        }

        public long sourceSequence() {
            requireActive();
            return store.shardMutationSequence();
        }

        public TargetQuotaAggregate aggregate() {
            requireActive();
            return readAggregate();
        }

        public TargetQuotaCounter counter(final TargetQuotaIdentity identity) {
            requireActive();
            return readCounter(identity);
        }

        public TargetQuotaTotal total(final TargetQuotaScope targetScope) {
            requireActive();
            return readTotal(targetScope);
        }

        public Edit replace(final ColumnFamily family, final byte[] key, final int type, final byte[] payload) {
            requireActive();
            return new Edit(
                    family, key, get(family, key), payload == null ? null : TargetValueEnvelope.encode(type, payload));
        }
    }

    private final ShardStore store;
    private final TargetQuotaScope scope;
    private final TargetQuotaAggregate genesis;
    private final byte[] lineage;
    private final WriteLimits limits;

    public TargetStoreBackend(
            final ShardStore store,
            final TargetQuotaScope scope,
            final byte[] shardAccountingIncarnation,
            final byte[] lineage,
            final WriteLimits limits) {
        this.store = Objects.requireNonNull(store, "store");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.limits = Objects.requireNonNull(limits, "limits");
        Bytes.requireLength(lineage, 16, "lineage");
        this.lineage = Bytes.copy(lineage);
        if (store.metadata().storeFormatVersion() != 2
                || scope.target() != null
                || !store.shardId().equals(scope.shard())
                || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("Target backend requires explicit format and Shard/lineage binding");
        }
        genesis = TargetQuotaAggregate.genesis(scope.shard(), shardAccountingIncarnation);
    }

    public Prepared prepare(final BoundedReadBudget budget, final Function<Reader, Mutation> planner) {
        Objects.requireNonNull(planner, "planner");
        final var read = store.readWithBudget(budget, () -> {
            final var reader = new Reader(budget);
            final Mutation mutation;
            try {
                mutation = Objects.requireNonNull(planner.apply(reader), "mutation");
                if (!scope.equals(mutation.quota().shardScope())) {
                    throw new IllegalArgumentException("Target mutation belongs to another scope");
                }
                mutation.quota()
                        .requireCurrent(
                                reader.aggregate(),
                                reader.sourceSequence(),
                                reader.source(),
                                reader::counter,
                                reader::total);
                for (var edit : mutation.business()) {
                    if (!Arrays.equals(edit.before, reader.get(edit.family, edit.key))) {
                        throw new IllegalStateException("Target business read set changed");
                    }
                }
                return measure(mutation);
            } finally {
                reader.active = false;
            }
        });
        return new Prepared(this, read.view(), read.value().mutation, read.value().records, read.value().bytes);
    }

    private record Measured(Mutation mutation, int records, long bytes) {}

    private Measured measure(final Mutation mutation) {
        final var keys = new HashSet<String>();
        long bytes = 0;
        for (var edit : mutation.business()) {
            if (!keys.add(edit.family.name() + ':' + HexFormat.of().formatHex(edit.key))) {
                throw new IllegalArgumentException("duplicate Target business edit key");
            }
            bytes = Math.addExact(
                    bytes, Math.addExact((long) edit.key.length, edit.after == null ? 0 : edit.after.length));
        }
        int records = mutation.business().size();
        for (var change : mutation.quota().counters().changes()) {
            bytes = Math.addExact(
                    bytes,
                    encodedSize(change.next().identity().key(), change.next().canonicalBytes()));
            records = Math.incrementExact(records);
        }
        for (var change : mutation.quota().changes()) {
            bytes = Math.addExact(
                    bytes, encodedSize(change.next().key(), change.next().canonicalBytes()));
            records = Math.incrementExact(records);
        }
        final var aggregate = mutation.quota().counters().nextAggregate();
        bytes = Math.addExact(bytes, encodedSize(aggregate.key(), aggregate.canonicalBytes()));
        records = Math.incrementExact(records);
        final var stamp = mutation.quota().counters().mutation();
        if (!stamp.isLocalClaim()) {
            bytes = Math.addExact(
                    bytes, encodedSize(KeyCodec.metaFixed(3), stamp.source().canonicalBytes()));
            bytes = Math.addExact(bytes, encodedSize(KeyCodec.metaFixed(5), Bytes.u64beBits(stamp.sequence())));
            records = Math.addExact(records, 2);
        }
        if (records > limits.maximumRecords || bytes > limits.maximumEncodedBytes) {
            throw new IllegalStateException("Target atomic write exceeds its configured budget");
        }
        return new Measured(mutation, records, bytes);
    }

    /** Consumes the plan once. Failed/unknown attempts are reconciled by a fresh read/recovery, never blind retry. */
    public void commit(final Prepared prepared, final CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(authority, "authority");
        synchronized (prepared) {
            if (prepared.backend != this || prepared.attempted) {
                throw new IllegalStateException("foreign or already attempted Target plan");
            }
            prepared.attempted = true;
        }
        try (var guard =
                Objects.requireNonNull(authority.acquire(store.metadata(), scope, prepared.mutation), "guard")) {
            store.withReadView(prepared.view, () -> {
                guard.requireCurrent();
                store.write(batch -> {
                    batch.requireReadView(prepared.view);
                    guard.requireCurrent();
                    for (var edit : prepared.mutation.business()) {
                        if (edit.after == null) {
                            batch.delete(edit.family, edit.key);
                        } else {
                            batch.put(edit.family, edit.key, edit.after);
                        }
                    }
                    final var quota = prepared.mutation.quota();
                    for (var change : quota.counters().changes()) {
                        batch.put(
                                ColumnFamily.META,
                                change.next().identity().key(),
                                TargetValueEnvelope.encode(
                                        TargetQuotaCounter.VALUE_TYPE,
                                        change.next().canonicalBytes()));
                    }
                    for (var change : quota.changes()) {
                        batch.put(
                                ColumnFamily.META,
                                change.next().key(),
                                TargetValueEnvelope.encode(
                                        TargetQuotaTotal.VALUE_TYPE,
                                        change.next().canonicalBytes()));
                    }
                    final var aggregate = quota.counters().nextAggregate();
                    batch.put(
                            ColumnFamily.META,
                            aggregate.key(),
                            TargetValueEnvelope.encode(TargetQuotaAggregate.VALUE_TYPE, aggregate.canonicalBytes()));
                    final var stamp = quota.counters().mutation();
                    if (!stamp.isLocalClaim()) {
                        batch.putValue(
                                ColumnFamily.META,
                                1,
                                KeyCodec.metaFixed(3),
                                stamp.source().canonicalBytes());
                        batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(stamp.sequence()));
                    }
                });
                return null;
            });
        }
    }

    public static final class AuditedResults {
        private final TargetStoreBackend backend;
        private final ShardStore.ReadView view;
        private final TargetResultLedgerAudit.Summary summary;

        private AuditedResults(
                final TargetStoreBackend backend, final ShardStore.ReadPlan<TargetResultLedgerAudit.Summary> read) {
            this.backend = backend;
            view = read.view();
            summary = read.value();
        }

        public TargetResultLedgerAudit.Summary summary() {
            return summary;
        }
    }

    /** Complete DEDUPE range and META dependency reads under the same Store monitor and shared budget. */
    public AuditedResults auditResults(
            final BoundedReadBudget budget,
            final TargetResultLedgerAudit.Limits auditLimits,
            final TargetResultLedgerAudit.CompletenessAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        final var read = store.readWithBudget(budget, () -> {
            final var rows = new ArrayList<TargetResultLedgerAudit.Stored>();
            // One extra row distinguishes an exact maximum from truncated traversal.
            final int scanLimit = Math.incrementExact(auditLimits.maximumResults());
            final long[] encoded = {0};
            final var traversal = store.visitResult(
                    ColumnFamily.DEDUPE, new byte[] {6}, new byte[] {10}, scanLimit, budget, (row, shared) -> {
                        if (rows.size() == auditLimits.maximumResults()) {
                            throw new IllegalStateException("Target result count limit reached before range end");
                        }
                        encoded[0] =
                                Math.addExact(encoded[0], Math.addExact((long) row.key().length, row.value().length));
                        if (encoded[0] > auditLimits.maximumEncodedBytes()) {
                            throw new IllegalStateException("Target result encoded byte limit reached");
                        }
                        final var value = TargetValueEnvelope.decode(row.value(), TargetResultRecord.VALUE_TYPE);
                        rows.add(new TargetResultLedgerAudit.Stored(row.key(), value.valueType(), value.payload()));
                        return true;
                    });
            if (traversal.stop() != ShardStore.VisitStop.RANGE_END) {
                throw new IllegalStateException("Target result Store traversal is incomplete: " + traversal.stop());
            }
            return TargetResultLedgerAudit.audit(
                    scope,
                    lineage,
                    store.shardMutationSequence(),
                    store.appliedShardLogPosition(),
                    auditLimits,
                    rows,
                    key -> {
                        final byte[] raw = store.get(ColumnFamily.META, key);
                        if (raw == null) {
                            return null;
                        }
                        final var value = TargetValueEnvelope.decodeAny(raw);
                        return new TargetResultLedgerAudit.Stored(key, value.valueType(), value.payload());
                    },
                    authority);
        });
        return new AuditedResults(this, read);
    }

    /** Publishes a completed audit only while its exact Store view still holds. */
    public <T> T withAuditView(
            final AuditedResults audit,
            final TargetResultLedgerAudit.CompletenessAuthority authority,
            final Function<TargetResultLedgerAudit.Summary, T> publisher) {
        Objects.requireNonNull(audit, "audit");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(publisher, "publisher");
        if (audit.backend != this) {
            throw new IllegalArgumentException("result audit belongs to another Target backend");
        }
        return store.withReadView(audit.view, () -> {
            authority.requireComplete(audit.summary);
            return publisher.apply(audit.summary);
        });
    }

    private TargetQuotaAggregate readAggregate() {
        final byte[] raw = store.get(ColumnFamily.META, genesis.key());
        if (raw == null) {
            if (store.shardMutationSequence() != 0 || store.appliedShardLogPosition() != null) {
                throw new IllegalStateException("nonempty Target source has no aggregate");
            }
            return genesis;
        }
        final var value = TargetQuotaAggregate.decodeForStore(
                genesis.key(),
                TargetValueEnvelope.decode(raw, TargetQuotaAggregate.VALUE_TYPE).payload(),
                scope.shard());
        if (!Arrays.equals(value.accountingIncarnation(), genesis.accountingIncarnation())) {
            throw new IllegalStateException("Target aggregate accounting incarnation changed");
        }
        return value;
    }

    private TargetQuotaCounter readCounter(final TargetQuotaIdentity identity) {
        if (!scope.shard().equals(identity.shard())
                || (identity.kind().isMirror() && !Arrays.equals(scope.tenantScope(), identity.tenantScope()))) {
            throw new IllegalArgumentException("Target counter lookup crossed scope");
        }
        final byte[] raw = store.get(ColumnFamily.META, identity.key());
        return raw == null
                ? null
                : TargetQuotaCounter.decodeForStore(
                        identity.key(),
                        TargetValueEnvelope.decode(raw, TargetQuotaCounter.VALUE_TYPE)
                                .payload(),
                        scope.shard());
    }

    private TargetQuotaTotal readTotal(final TargetQuotaScope targetScope) {
        if (targetScope.target() == null
                || !scope.forTarget(targetScope.target()).equals(targetScope)) {
            throw new IllegalArgumentException("Target total lookup crossed scope");
        }
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_TOTAL_TAG, 1}, targetScope.keySuffix());
        final byte[] raw = store.get(ColumnFamily.META, key);
        if (raw == null) {
            return null;
        }
        final var total = TargetQuotaTotal.decode(
                TargetValueEnvelope.decode(raw, TargetQuotaTotal.VALUE_TYPE).payload());
        if (!targetScope.equals(total.scope()) || !Arrays.equals(key, total.key())) {
            throw new IllegalStateException("Target total key/value mismatch");
        }
        return total;
    }

    private static long encodedSize(final byte[] key, final byte[] payload) {
        return Math.addExact(12L, Math.addExact((long) key.length, payload.length));
    }

    /** Excludes fixed metadata and derived counter/total/aggregate keys, owned only by this backend. */
    private static int businessType(final ColumnFamily family, final byte[] key) {
        if (key.length < 3 || key[1] != 1) {
            throw new IllegalArgumentException("invalid Target business key");
        }
        final int tag = Byte.toUnsignedInt(key[0]);
        final int type =
                switch (family) {
                    case INFLIGHT -> tag == TargetKeyCodec.CLAIM_TAG ? 36 : 0;
                    case DEDUPE -> tag >= 6 && tag <= 9 ? 35 : 0;
                    case ID ->
                        switch (tag) {
                            case 5 -> 15;
                            case 6 -> 21;
                            default -> 0;
                        };
                    case TIMELINE ->
                        switch (tag) {
                            case 8, 9, 11, 12 -> 14;
                            case 10 -> 16;
                            default -> 0;
                        };
                    case META ->
                        switch (tag) {
                            case 9 -> 12;
                            case 10 -> 17;
                            case 11 -> 13;
                            case 12 -> 18;
                            case 13 -> 19;
                            case 14 -> 20;
                            case 15 -> 22;
                            case 16 -> 23;
                            case 17 -> 24;
                            case 18 -> 25;
                            case 21 -> 28;
                            case 23 -> 30;
                            case 24 -> 31;
                            case 25 -> 32;
                            case 26 -> 33;
                            case 27 -> 34;
                            default -> 0;
                        };
                    default -> 0;
                };
        if (type == 0) {
            throw new IllegalArgumentException("unregistered or backend-owned Target key");
        }
        return type;
    }
}
