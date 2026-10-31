package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMigrationActivationAttempt;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.LegacyMigrationBundle;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Offline import into a separately owned Store; no method authorizes an old writer to resume. */
public final class TargetMigrationImporter {
    public record Limits(
            TargetStoreBackend.WriteLimits writes,
            int maximumCounters,
            int maximumTargets,
            int maximumDomains,
            int maximumAuditRecords) {
        public Limits {
            Objects.requireNonNull(writes);
            if (maximumCounters < 2
                    || maximumTargets <= 0
                    || maximumDomains <= 0
                    || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS
                    || maximumAuditRecords <= 0) {
                throw new IllegalArgumentException("migration requires finite import and audit limits");
            }
        }
    }

    /** Complete Target row plus an exact reference to its original conversion record in the immutable bundle. */
    public record ImportedRow(ColumnFamily family, byte[] key, byte[] value, TargetMigrationOrigin origin) {
        public ImportedRow {
            Objects.requireNonNull(family);
            Objects.requireNonNull(origin);
            key = Bytes.copy(Objects.requireNonNull(key));
            value = Bytes.copy(Objects.requireNonNull(value));
            new TargetStoreBackend.Edit(family, key, null, value);
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }

        @Override
        public byte[] value() {
            return Bytes.copy(value);
        }
    }

    public record ImportSummary(
            long targetRows,
            long originRows,
            long guardRows,
            Map<TargetQuotaIdentity, TargetQuotaUsage> actualCharges,
            TargetQuotaUsage primaryTotal) {
        public ImportSummary {
            actualCharges = Map.copyOf(actualCharges);
        }
    }

    /** Produces the actual immutable C candidate after all imported accounting/frontier bytes are durable. */
    @FunctionalInterface
    public interface CandidateSealer {
        TargetMigrationObjectReference seal(TargetMigrationPlan plan, ShardStore store, ShardStore.ReadView cut);
    }

    /**
     * The original process recovery root owns this actual acquisition, its Pin/history/custody and cleanup.
     * External checks run outside the Store monitor; local checks validate the same retained acquisition.
     * Implementations must independently authenticate raw bundle contents, converted/legacy disjointness,
     * old fee custody, added-copy funding, mapping and complete management/reader authority.
     */
    public interface Held {
        void requireExternalCurrent(TargetMigrationPlan plan, StoreMetadata target);

        void requireLocalCurrent();

        void requireRow(TargetMigrationPlan plan, ImportedRow row);

        void requireGuard(TargetMigrationPlan plan, TargetMigrationCustodyGuard guard);

        void requireComplete(TargetMigrationPlan plan, ImportSummary summary);

        void requireCandidate(
                TargetMigrationPlan plan, TargetMigrationObjectReference candidate, ShardStore.ReadView cut);

        void requirePrefix(
                TargetMigrationPlan plan,
                SourceReplayEntry original,
                ShardStore legacyAfterApply,
                List<TargetStoreBackend.Edit> completeProjection);

        void requireActivationAttempt(
                TargetMigrationPlan plan, PreparedControlOperation exactControl, SystemMutation exactInput);
    }

    /** Sealed C image. This opaque handle is neither an Initialized Store nor a restore/reader qualification. */
    public static final class PreparedCandidate {
        private final TargetMigrationImporter importer;
        private final TargetMigrationObjectReference candidate;
        private final ImportSummary summary;

        private PreparedCandidate(
                TargetMigrationImporter importer, TargetMigrationObjectReference candidate, ImportSummary summary) {
            this.importer = importer;
            this.candidate = candidate;
            this.summary = summary;
        }

        public TargetMigrationPlan plan() {
            return importer.plan;
        }

        public TargetQuotaIncarnation root() {
            return importer.root;
        }

        public TargetMigrationObjectReference candidate() {
            return candidate;
        }

        public ImportSummary summary() {
            return summary;
        }

        public Limits importerLimits() {
            return importer.limits;
        }

        public ShardStore store() {
            return importer.store;
        }

        public void requireStore(ShardStore exact) {
            if (exact != importer.store) {
                throw new IllegalArgumentException("candidate belongs to another Store");
            }
            importer.held.requireLocalCurrent();
        }

        public void requireExternalCurrent() {
            importer.held.requireExternalCurrent(importer.plan, importer.store.metadata());
        }
    }

    /** Returned only after durable intent+actual fees, or after exact Native reconciliation of an unknown write. */
    public static final class StagedActivation {
        private final TargetMigrationImporter importer;
        private final PreparedCandidate candidate;
        private final TargetMigrationActivationAttempt attempt;

        private StagedActivation(
                TargetMigrationImporter importer,
                PreparedCandidate candidate,
                TargetMigrationActivationAttempt attempt) {
            this.importer = importer;
            this.candidate = candidate;
            this.attempt = attempt;
        }

        public PreparedControlOperation control() {
            return attempt.control();
        }

        public SystemMutation input() {
            return attempt.input();
        }

        public PreparedCandidate candidate() {
            return candidate;
        }

        public TargetMigrationActivationAttempt attempt() {
            return attempt;
        }
        /** SDK-lock check of this already durable opaque acquisition; no Native read or external I/O. */
        public void requireLocalCurrent() {
            if (!importer.sealed || candidate.importer != importer) {
                throw new IllegalStateException("append permit lost its original sealed migration acquisition");
            }
            candidate.requireStore(importer.store);
        }

        public void requireCurrent() {
            candidate.requireExternalCurrent();
            final long bytes = TargetMigrationActivationAttempt.MAX_CANONICAL_BYTES
                    + (long) TargetQuotaAggregate.MAX_CANONICAL_BYTES
                    + TargetMigrationRoot.MAX_CANONICAL_BYTES;
            final var budget = new BoundedReadBudget(
                    16, bytes, java.time.Duration.ofSeconds(5).toNanos(), System::nanoTime);
            final var retained = TargetMigrationStore.readAttemptNative(importer.store, importer.plan, budget);
            if (!attempt.equals(retained)) {
                throw new IllegalStateException("staged activation changed its durable exact frame");
            }
            candidate.requireExternalCurrent();
            candidate.requireStore(importer.store);
        }
    }

    private final ShardStore store;
    private final TargetMigrationPlan plan;
    private final TargetQuotaIncarnation root;
    private final Limits limits;
    private final Held held;
    private final TargetQuotaMutation baseline;
    private boolean sealed;

    private TargetMigrationImporter(
            ShardStore store, TargetMigrationPlan plan, TargetQuotaIncarnation root, Limits limits, Held held) {
        this.store = store;
        this.plan = plan;
        this.root = root;
        this.limits = limits;
        this.held = held;
        baseline = migrationBaseline(plan);
    }

    public TargetQuotaIncarnation root() {
        return root;
    }

    public TargetMigrationPlan plan() {
        return plan;
    }

    public Limits limits() {
        return limits;
    }

    /** Must finish before SDK ownership. An unknown Native response returns no append permit. */
    public StagedActivation stageActivation(
            PreparedCandidate candidate,
            PreparedControlOperation control,
            SystemMutation input,
            BoundedReadBudget budget,
            TargetStoreBackend.CommitAuthority commits) {
        candidate.requireStore(store);
        held.requireExternalCurrent(plan, store.metadata());
        held.requireActivationAttempt(plan, control, input);
        final var existing = TargetMigrationStore.readAttemptNative(store, plan, budget);
        if (existing != null) {
            if (!Arrays.equals(existing.control().canonicalBytes(), control.canonicalBytes())
                    || !Arrays.equals(existing.input().canonicalEnvelope(), input.canonicalEnvelope())
                    || !existing.candidate().equals(candidate.candidate())) {
                throw new IllegalStateException(
                        "an exact durable migration append may already be exposed; cannot reseal/resign");
            }
            return new StagedActivation(this, candidate, existing);
        }
        final var backend = TargetStoreBackend.forMigration(
                store,
                plan.scope(),
                root.identity().accountingIncarnation(),
                plan.newLineage(),
                limits.writes(),
                candidate);
        final TargetMigrationActivationAttempt[] next = {null};
        final var batch = backend.prepare(budget, reader -> {
            held.requireLocalCurrent();
            TargetMigrationStore.requirePrepared(store, plan);
            if (reader.get(ColumnFamily.META, TargetMigrationActivationAttempt.keyFor(plan.migrationId())) != null) {
                throw new IllegalStateException("durable activation intent appeared during preparation");
            }
            final var aggregate = reader.aggregate();
            final long ordinal =
                    TargetQuotaMutation.increment(aggregate.mutation().localOrdinal());
            final var stamp = new TargetQuotaMutation(
                    reader.sourceSequence(),
                    reader.source(),
                    Bytes.sha256(input.canonicalEnvelope()),
                    ordinal,
                    false,
                    false,
                    false,
                    true);
            next[0] = new TargetMigrationActivationAttempt(
                    plan.digest(), plan.migrationId(), candidate.candidate(), control, input, stamp);
            next[0].requirePlan(plan);
            final var edit = reader.replace(
                    ColumnFamily.META,
                    next[0].key(),
                    TargetMigrationActivationAttempt.VALUE_TYPE,
                    next[0].canonicalBytes());
            final var fee = TargetRecordAccounting.resources(root.accounting()
                    .recordCharge(
                            TargetQuotaAccounting.RecordClass.STATE,
                            edit.key().length,
                            next[0].canonicalBytes().length));
            final var updates = List.of(
                    new TargetQuotaDelta.Update(
                            root.identity(),
                            reader.counter(root.identity()).usage().add(fee)),
                    new TargetQuotaDelta.Update(
                            root.tenantIdentity(),
                            reader.counter(root.tenantIdentity()).usage().add(fee)));
            final var delta = TargetQuotaDelta.prepareMigrationPreparation(
                    aggregate,
                    reader.sourceSequence(),
                    reader.source(),
                    stamp.mutationDigest(),
                    updates,
                    reader::counter,
                    candidate);
            if (!delta.mutation().equals(stamp)) {
                throw new IllegalStateException("migration intent changed exact local purpose");
            }
            return new TargetStoreBackend.Mutation(
                    TargetQuotaTotalsDelta.prepare(delta, plan.scope(), 1, reader::total), List.of(edit));
        });
        held.requireExternalCurrent(plan, store.metadata());
        backend.commit(batch, (metadata, scope, mutation) -> {
            final var guard = Objects.requireNonNull(commits.acquire(metadata, scope, mutation));
            return new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    guard.requireCurrent();
                    held.requireLocalCurrent();
                }

                @Override
                public void close() {
                    guard.close();
                }
            };
        });
        return new StagedActivation(this, candidate, next[0]);
    }

    public StagedActivation resumeStaged(PreparedCandidate candidate, BoundedReadBudget budget) {
        candidate.requireStore(store);
        held.requireExternalCurrent(plan, store.metadata());
        final var attempt = Objects.requireNonNull(
                TargetMigrationStore.readAttemptNative(store, plan, budget),
                "original durable before-SDK activation intent");
        if (!attempt.candidate().equals(candidate.candidate())) {
            throw new IllegalStateException("staged activation changed original sealed C candidate");
        }
        held.requireActivationAttempt(plan, attempt.control(), attempt.input());
        held.requireExternalCurrent(plan, store.metadata());
        return new StagedActivation(this, candidate, attempt);
    }

    public static TargetMigrationImporter begin(
            ShardStore store,
            TargetMigrationPlan plan,
            TargetQuotaAccounting accounting,
            Limits limits,
            Held held,
            BoundedReadBudget budget) {
        Objects.requireNonNull(store);
        Objects.requireNonNull(plan);
        Objects.requireNonNull(limits);
        Objects.requireNonNull(held);
        Objects.requireNonNull(budget);
        TargetMigrationStore.requireIdentity(store.metadata(), plan);
        held.requireExternalCurrent(plan, store.metadata());
        final var root = TargetQuotaIncarnation.allocate(
                plan.scope(),
                accounting,
                plan.newLineage(),
                migrationBaseline(plan),
                (prior, next) -> held.requireLocalCurrent());
        final var marker = TargetMigrationRoot.prepared(plan, root);
        final var bookkeeping = new TargetQuotaBookkeeping(
                root.identity(),
                root.tenantScope(),
                accounting,
                new TargetQuotaBookkeeping.Inventory(2, 0, 0),
                1,
                migrationBaseline(plan));
        final var read = store.readWithBudget(budget, () -> {
            store.requireMigrationUninitialized(budget);
            held.requireLocalCurrent();
            return Boolean.TRUE;
        });
        final var rows = List.of(
                raw(ColumnFamily.META, root.key(), TargetQuotaIncarnation.VALUE_TYPE, root.canonicalBytes()),
                raw(ColumnFamily.META, marker.key(), TargetMigrationRoot.VALUE_TYPE, marker.canonicalBytes()),
                raw(
                        ColumnFamily.META,
                        bookkeeping.key(),
                        TargetQuotaBookkeeping.VALUE_TYPE,
                        bookkeeping.canonicalBytes()));
        write(store, read.view(), rows, limits.writes(), held);
        return new TargetMigrationImporter(store, plan, root, limits, held);
    }

    /** Reuses only the same exact partially built candidate after a crash; it does not infer completeness. */
    public static TargetMigrationImporter resume(
            ShardStore store, TargetMigrationPlan plan, Limits limits, Held held, BoundedReadBudget budget) {
        TargetMigrationStore.requireIdentity(store.metadata(), plan);
        held.requireExternalCurrent(plan, store.metadata());
        final var read = store.readWithBudget(budget, () -> {
            held.requireLocalCurrent();
            final var marker = Objects.requireNonNull(
                    TargetMigrationStore.decodeRoot(store.get(
                            ColumnFamily.META,
                            TargetMigrationRoot.keyFor(plan.scope().shard()))),
                    "original migration root");
            if (!marker.plan().equals(plan)) {
                throw new IllegalStateException("resume changed original plan");
            }
            return marker.owner();
        });
        return new TargetMigrationImporter(store, plan, read.value(), limits, held);
    }

    /** Idempotent exact rows only. A partial candidate never exposes a normal Target backend. */
    public void importBatch(
            List<ImportedRow> rows, List<TargetMigrationCustodyGuard> guards, BoundedReadBudget budget) {
        if (sealed) {
            throw new IllegalStateException("sealed candidate cannot accept baseline rows");
        }
        rows = List.copyOf(rows);
        guards = List.copyOf(guards);
        held.requireExternalCurrent(plan, store.metadata());
        for (var row : rows) {
            TargetMigrationStore.requireImportOrigin(plan, row);
            held.requireRow(plan, row);
        }
        for (var guard : guards) {
            held.requireGuard(plan, guard);
        }
        final var changes = new ArrayList<StoredEdit>();
        final var exactRows = rows;
        final var exactGuards = guards;
        final var read = store.readWithBudget(budget, () -> {
            held.requireLocalCurrent();
            TargetMigrationStore.requirePrepared(store, plan);
            for (var row : exactRows) {
                addExact(changes, row.family(), row.key(), row.value());
                addExact(
                        changes,
                        ColumnFamily.META,
                        row.origin().key(),
                        TargetValueEnvelope.encode(
                                TargetMigrationOrigin.VALUE_TYPE, row.origin().canonicalBytes()));
            }
            for (var guard : exactGuards) {
                TargetMigrationStore.requireGuard(plan, guard);
                addExact(
                        changes,
                        ColumnFamily.META,
                        guard.key(),
                        TargetValueEnvelope.encode(TargetMigrationCustodyGuard.VALUE_TYPE, guard.canonicalBytes()));
            }
            return Boolean.TRUE;
        });
        write(store, read.view(), changes, limits.writes(), held);
    }

    /**
     * Independently folds actual complete Store rows and charges their new indexes/copies. Only a bounded
     * counter map is retained. The original bundle authority must separately prove the whole old universe.
     */
    public PreparedCandidate seal(TargetMigrationObjectReference candidate, BoundedReadBudget budget) {
        return seal((original, actual, cut) -> candidate, budget);
    }

    public PreparedCandidate seal(CandidateSealer sealer, BoundedReadBudget budget) {
        if (sealed) {
            throw new IllegalStateException("candidate already sealed");
        }
        held.requireExternalCurrent(plan, store.metadata());
        final var read = store.readWithBudget(budget, () -> {
            held.requireLocalCurrent();
            TargetMigrationStore.requirePrepared(store, plan);
            return fold(budget);
        });
        held.requireExternalCurrent(plan, store.metadata());
        final var summary = read.value();
        final long totals = summary.actualCharges().keySet().stream()
                .filter(id -> id.kind() == TargetQuotaIdentity.Kind.TARGET)
                .map(TargetQuotaIdentity::target)
                .distinct()
                .count();
        final var book = new TargetQuotaBookkeeping(
                root.identity(),
                root.tenantScope(),
                root.accounting(),
                new TargetQuotaBookkeeping.Inventory(summary.actualCharges().size(), totals, 0),
                1,
                baseline);
        final var charges = new LinkedHashMap<>(summary.actualCharges());
        merge(charges, root.identity(), TargetRecordAccounting.resources(book.charge()));
        merge(charges, root.tenantIdentity(), TargetRecordAccounting.resources(book.charge()));
        TargetQuotaUsage sum = TargetQuotaUsage.empty();
        final var materialization = new ArrayList<StoredEdit>();
        for (var entry : charges.entrySet()) {
            final var counter = new TargetQuotaCounter(entry.getKey(), entry.getValue(), 1, baseline);
            materialization.add(raw(
                    ColumnFamily.META,
                    counter.identity().key(),
                    TargetQuotaCounter.VALUE_TYPE,
                    counter.canonicalBytes()));
            if (!entry.getKey().kind().isMirror()) {
                sum = sum.add(entry.getValue());
            }
        }
        final var targetSums = new LinkedHashMap<TargetPartitionId, TargetQuotaUsage>();
        charges.forEach((id, usage) -> {
            if (id.kind() == TargetQuotaIdentity.Kind.TARGET) {
                targetSums.merge(id.target(), usage, TargetQuotaUsage::add);
            }
        });
        targetSums.forEach((target, usage) -> {
            final var total = new TargetQuotaTotal(plan.scope().forTarget(target), usage, 1, baseline);
            materialization.add(
                    raw(ColumnFamily.META, total.key(), TargetQuotaTotal.VALUE_TYPE, total.canonicalBytes()));
        });
        final var aggregate = new TargetQuotaAggregate(
                plan.scope().shard(), root.identity().accountingIncarnation(), sum, 1, baseline);
        materialization.add(new StoredEdit(
                ColumnFamily.META,
                book.key(),
                store.get(ColumnFamily.META, book.key()),
                TargetValueEnvelope.encode(TargetQuotaBookkeeping.VALUE_TYPE, book.canonicalBytes())));
        materialization.add(
                raw(ColumnFamily.META, aggregate.key(), TargetQuotaAggregate.VALUE_TYPE, aggregate.canonicalBytes()));
        materialization.add(raw(
                ColumnFamily.META,
                KeyCodec.metaFixed(3),
                1,
                plan.baseline().source().canonicalBytes()));
        materialization.add(raw(
                ColumnFamily.META,
                KeyCodec.metaFixed(5),
                1,
                Bytes.u64beBits(plan.baseline().sequence())));
        final var actualSummary =
                new ImportSummary(summary.targetRows(), summary.originRows(), summary.guardRows(), charges, sum);
        held.requireComplete(plan, actualSummary);
        write(store, read.view(), materialization, limits.writes(), held);
        sealed = true;
        final var cut = store.captureReadView();
        final var candidate = Objects.requireNonNull(sealer.seal(plan, store, cut), "actual sealed migration image");
        held.requireExternalCurrent(plan, store.metadata());
        held.requireCandidate(plan, candidate, cut);
        return store.withReadView(cut, () -> new PreparedCandidate(this, candidate, actualSummary));
    }

    /** Already sealed accounting is read and audited; no source reset or second materialization is permitted. */
    public PreparedCandidate resumeSealed(TargetMigrationObjectReference candidate, BoundedReadBudget budget) {
        held.requireExternalCurrent(plan, store.metadata());
        final var read = store.readWithBudget(budget, () -> {
            held.requireLocalCurrent();
            final var marker = Objects.requireNonNull(
                    TargetMigrationStore.decodeRoot(store.get(
                            ColumnFamily.META,
                            TargetMigrationRoot.keyFor(plan.scope().shard()))),
                    "original migration root");
            if (!marker.plan().equals(plan)) {
                throw new IllegalStateException("resume changed original plan");
            }
            final var book = TargetQuotaBookkeeping.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    TargetQuotaBookkeeping.keyFor(plan.scope().shard())),
                            TargetQuotaBookkeeping.VALUE_TYPE)
                    .payload());
            final var aggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    TargetQuotaAggregate.genesis(
                                                    plan.scope().shard(),
                                                    root.identity().accountingIncarnation())
                                            .key()),
                            TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            final var proof = new TargetCheckpointRootVerifier.RootProof(
                    store.metadata(),
                    store.appliedShardLogPosition(),
                    store.shardMutationSequence(),
                    marker.owner(),
                    book,
                    aggregate);
            final var dependencies = TargetCheckpointRootVerifier.auditLiveStoreDependencies(
                    store,
                    proof,
                    new TargetCheckpointRootVerifier.LedgerAuditLimits(
                            limits.maximumAuditRecords(),
                            budget.maxBytes(),
                            limits.maximumAuditRecords(),
                            budget.maxBytes()),
                    budget);
            final var charges = new LinkedHashMap<TargetQuotaIdentity, TargetQuotaUsage>();
            dependencies.quotaCounters().forEach(counter -> charges.put(counter.identity(), counter.usage()));
            long businessRows = 0;
            for (var counter : dependencies.quotaCounters()) {
                if (!counter.identity().kind().isMirror()) {
                    businessRows = Math.addExact(businessRows, dependencies.ownerRecordCount(counter.identity()));
                }
            }
            final long targetRows = Math.subtractExact(
                    businessRows,
                    Math.addExact(
                            Math.addExact(
                                    (long) dependencies.migrationOrigins().size(),
                                    dependencies.migrationGuards().size()),
                            Math.addExact(
                                    dependencies.migrationPrefixes().size()
                                            + 1L
                                            + (dependencies.migrationAttempt() == null ? 0L : 1L),
                                    dependencies.resultRecords().size())));
            return new ImportSummary(
                    targetRows,
                    dependencies.migrationOrigins().size(),
                    dependencies.migrationGuards().size(),
                    charges,
                    aggregate.usage());
        });
        held.requireComplete(plan, read.value());
        held.requireCandidate(plan, candidate, read.view());
        held.requireExternalCurrent(plan, store.metadata());
        sealed = true;
        return store.withReadView(read.view(), () -> new PreparedCandidate(this, candidate, read.value()));
    }

    /** Complete C→A old-domain application; original frame/successor and projected before/after are independently held. */
    public void applyLegacyPrefix(
            PreparedCandidate candidate,
            SourceReplayEntry original,
            ShardStore legacyAfterApply,
            List<TargetStoreBackend.Edit> projection,
            BoundedReadBudget budget,
            TargetStoreBackend.CommitAuthority commits) {
        applyLegacyPrefix(candidate, original, legacyAfterApply, projection, budget, commits, null);
    }

    private void applyLegacyPrefix(
            PreparedCandidate candidate,
            SourceReplayEntry original,
            ShardStore legacyAfterApply,
            List<TargetStoreBackend.Edit> projection,
            BoundedReadBudget budget,
            TargetStoreBackend.CommitAuthority commits,
            ShardStore.ReadView originalProjectionView) {
        candidate.requireStore(store);
        held.requireExternalCurrent(plan, store.metadata());
        projection = List.copyOf(projection);
        held.requirePrefix(plan, original, legacyAfterApply, projection);
        if (!Arrays.equals(legacyAfterApply.metadata().storeIncarnation(), plan.oldStore())
                || !Arrays.equals(
                        legacyAfterApply.appliedShardLogPosition().canonicalBytes(),
                        original.position().canonicalBytes())) {
            throw new IllegalStateException("old-domain Source was not actually applied to the protected old Store");
        }
        final byte[] frame =
                switch (original) {
                    case SourceReplayRecord record -> CommandCodec.encodeFrame(record.command());
                    case SourceReplayMutation record -> record.mutation().canonicalEnvelope();
                };
        final var backend = TargetStoreBackend.forMigration(
                store,
                plan.scope(),
                root.identity().accountingIncarnation(),
                plan.newLineage(),
                limits.writes(),
                candidate);
        final var edits = projection;
        final var duplicate = store.readWithBudget(budget, () -> {
            held.requireLocalCurrent();
            final var migration = TargetMigrationStore.requireMaintenance(store, plan, candidate);
            final long actualSequence = legacyAfterApply.shardMutationSequence();
            final byte[] raw = store.get(
                    ColumnFamily.META, TargetMigrationPrefixRecord.keyFor(plan.migrationId(), actualSequence));
            if (raw == null) {
                return Boolean.FALSE;
            }
            final var retained = TargetMigrationPrefixRecord.decode(
                    TargetValueEnvelope.decode(raw, TargetMigrationPrefixRecord.VALUE_TYPE)
                            .payload());
            requirePrefixRecord(retained, original);
            final var expected = edits.stream()
                    .filter(edit -> edit.family() == ColumnFamily.META && Arrays.equals(edit.key(), retained.key()))
                    .findFirst()
                    .orElseThrow();
            if (!Arrays.equals(raw, expected.after())) {
                throw new IllegalStateException("prefix retry changed exact retained mapping object");
            }
            return Boolean.TRUE;
        });
        if (duplicate.value()) {
            held.requireExternalCurrent(plan, store.metadata());
            store.withReadView(duplicate.view(), () -> {
                held.requireLocalCurrent();
                return Boolean.TRUE;
            });
            return;
        }
        final var batch = backend.prepare(budget, reader -> {
            if (originalProjectionView != null) {
                store.requireMigrationReadView(originalProjectionView, candidate);
            }
            final var migration = TargetMigrationStore.requireMaintenance(store, plan, candidate);
            held.requireLocalCurrent();
            if (reader.source().compareTo(original.position()) >= 0) {
                throw new IllegalStateException("prefix is already applied or is not the next held original entry");
            }
            final var records = edits.stream()
                    .filter(edit -> edit.family() == ColumnFamily.META
                            && edit.key().length > 2
                            && edit.key()[0] == TargetKeyCodec.MIGRATION_PREFIX_TAG)
                    .toList();
            if (records.size() != 1
                    || records.getFirst().before() != null
                    || records.getFirst().after() == null) {
                throw new IllegalStateException(
                        "prefix requires exactly one immutable original Source migration record");
            }
            final var prefix = TargetMigrationPrefixRecord.decode(
                    TargetValueEnvelope.decode(records.getFirst().after(), TargetMigrationPrefixRecord.VALUE_TYPE)
                            .payload());
            requirePrefixRecord(prefix, original);
            final var expectedPrior = TargetMigrationStore.actualFrontier(store, migration);
            if (!prefix.prior().equals(expectedPrior)
                    || prefix.at().sequence() != TargetQuotaMutation.increment(reader.sourceSequence())) {
                throw new IllegalStateException("prefix changed the actual exact predecessor/frontier");
            }
            return new TargetSourceAccounting(
                            plan.scope(),
                            plan.newLineage(),
                            original.position(),
                            Bytes.sha256(frame),
                            limits.maximumCounters(),
                            limits.maximumTargets(),
                            limits.maximumDomains())
                    .assemble(reader, edits);
        });
        try {
            backend.commit(batch, commits);
        } catch (IllegalStateException failure) {
            if (originalProjectionView != null) {
                try {
                    store.requireMigrationReadView(originalProjectionView, candidate);
                } catch (ViewChanged changed) {
                    changed.addSuppressed(failure);
                    throw changed;
                } catch (IllegalStateException unavailable) {
                    if (unavailable != failure) {
                        failure.addSuppressed(unavailable);
                    }
                    throw failure;
                }
            }
            throw failure;
        }
    }

    private void requirePrefixRecord(TargetMigrationPrefixRecord record, SourceReplayEntry original) {
        record.requirePlan(plan);
        final byte[] identity = original instanceof SourceReplayRecord command
                ? command.command().commandId().bytes()
                : ((SourceReplayMutation) original).mutation().systemMutationId();
        final byte[] hash = original instanceof SourceReplayRecord command
                ? command.command().commandHash()
                : ((SourceReplayMutation) original).mutation().mutationHash();
        if (!Arrays.equals(record.originalIdentity(), identity)
                || !Arrays.equals(record.originalSemanticHash(), hash)
                || !Arrays.equals(
                        record.at().source().canonicalBytes(),
                        original.position().canonicalBytes())
                || !Arrays.equals(record.at().mutationDigest(), originalDigest(original))
                || !Arrays.equals(record.rawFrameDigest(), originalFrameDigest(original))) {
            throw new IllegalStateException("prefix Native record differs from complete original raw Source identity");
        }
    }

    /** Verifies each actually replayed old entry against its retained migration fact during crash recovery. */
    public boolean replayPersistedPrefix(
            PreparedCandidate candidate,
            SourceReplayEntry original,
            ShardStore legacyAfterApply,
            BoundedReadBudget budget) {
        candidate.requireStore(store);
        held.requireExternalCurrent(plan, store.metadata());
        if (!Arrays.equals(legacyAfterApply.metadata().storeIncarnation(), plan.oldStore())
                || !Arrays.equals(legacyAfterApply.metadata().dbIdentity(), plan.oldDbIdentity())
                || !Arrays.equals(
                        legacyAfterApply.appliedShardLogPosition().canonicalBytes(),
                        original.position().canonicalBytes())) {
            throw new IllegalStateException(
                    "recovery must actually replay the original old entry before its migration fact");
        }
        final var read = store.readWithBudget(budget, () -> {
            held.requireLocalCurrent();
            final var migration = Objects.requireNonNull(
                    TargetMigrationStore.decodeRoot(store.get(
                            ColumnFamily.META,
                            TargetMigrationRoot.keyFor(plan.scope().shard()))),
                    "original migration root");
            if (!migration.plan().equals(plan)) {
                throw new IllegalStateException("recovery changed the original migration plan");
            }
            final long actualSequence = legacyAfterApply.shardMutationSequence();
            final byte[] key = TargetMigrationPrefixRecord.keyFor(plan.migrationId(), actualSequence);
            final byte[] raw = store.get(ColumnFamily.META, key);
            if (raw == null) {
                if (store.appliedShardLogPosition().compareTo(original.position()) >= 0) {
                    throw new IllegalStateException("applied migration frontier has no original prefix record");
                }
                return List.<TargetStoreBackend.Edit>of();
            }
            final var prefix = TargetMigrationPrefixRecord.decode(
                    TargetValueEnvelope.decode(raw, TargetMigrationPrefixRecord.VALUE_TYPE)
                            .payload());
            requirePrefixRecord(prefix, original);
            if (prefix.at().sequence() != actualSequence) {
                throw new IllegalStateException("persisted prefix differs from actually replayed old sequence");
            }
            return List.of(new TargetStoreBackend.Edit(ColumnFamily.META, key, raw, raw));
        });
        if (!read.value().isEmpty()) {
            held.requirePrefix(plan, original, legacyAfterApply, read.value());
        }
        held.requireExternalCurrent(plan, store.metadata());
        return store.withReadView(read.view(), () -> {
            held.requireLocalCurrent();
            return !read.value().isEmpty();
        });
    }

    /** Builds only changed rows from the protected old applier's actual after-view, including physical deletions. */
    public void applyLegacyPrefix(
            PreparedCandidate candidate,
            ShardStore legacyAfterApply,
            LegacyMigrationBundle.TailProjection projection,
            BoundedReadBudget budget,
            TargetStoreBackend.CommitAuthority commits) {
        candidate.requireStore(store);
        held.requireExternalCurrent(plan, store.metadata());
        store.requireMigrationReadView(projection.targetView(), candidate);
        final var original = projection.original();
        if (!Arrays.equals(legacyAfterApply.metadata().storeIncarnation(), plan.oldStore())
                || !Arrays.equals(legacyAfterApply.metadata().dbIdentity(), plan.oldDbIdentity())
                || !Arrays.equals(
                        legacyAfterApply.appliedShardLogPosition().canonicalBytes(),
                        original.position().canonicalBytes())) {
            throw new IllegalStateException("prefix was not actually applied by the original protected old Store");
        }
        final var read = store.readWithBudget(budget, () -> {
            store.requireMigrationReadView(projection.targetView(), candidate);
            held.requireLocalCurrent();
            final var migration = TargetMigrationStore.requireMaintenance(store, plan, candidate);
            if (!Arrays.equals(
                    store.appliedShardLogPosition().canonicalBytes(),
                    projection.prior().canonicalBytes())) {
                throw new IllegalStateException("prefix projection was built against another actual Target frontier");
            }
            final var priorAggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    TargetQuotaAggregate.genesis(
                                                    plan.scope().shard(),
                                                    root.identity().accountingIncarnation())
                                            .key()),
                            TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            final var prior = TargetMigrationStore.actualFrontier(store, migration);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(store.shardMutationSequence()),
                    original.position(),
                    originalDigest(original));
            if (legacyAfterApply.shardMutationSequence() != stamp.sequence()) {
                throw new IllegalStateException("old and new Store prefix sequences diverged");
            }
            final var prefix = new TargetMigrationPrefixRecord(
                    plan.migrationId(),
                    plan.digest(),
                    original instanceof SourceReplayRecord
                            ? ProtocolTuple.CLIENT_COMMAND
                            : ProtocolTuple.SYSTEM_MUTATION,
                    original instanceof SourceReplayRecord record
                            ? record.command().commandId().bytes()
                            : ((SourceReplayMutation) original).mutation().systemMutationId(),
                    original instanceof SourceReplayRecord record
                            ? record.command().commandHash()
                            : ((SourceReplayMutation) original).mutation().mutationHash(),
                    projection.tailEvidence(),
                    originalFrameDigest(original),
                    prior,
                    stamp,
                    projection.projectionEvidence().object());
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            for (var row : projection.desired()) {
                if (TargetMigrationStore.isImportInfrastructure(row.family(), row.key())
                        || row.family() == ColumnFamily.DEDUPE) {
                    throw new IllegalArgumentException(
                            "old prefix cannot fabricate ordinary Target infrastructure/results");
                }
                final byte[] before = store.get(row.family(), row.key());
                if (!Arrays.equals(before, row.value())) {
                    edits.add(new TargetStoreBackend.Edit(row.family(), row.key(), before, row.value()));
                }
            }
            for (var row : projection.desired()) {
                final var value = TargetValueEnvelope.decodeAny(row.value());
                if (row.family() != ColumnFamily.ID || value.valueType() != TargetScheduleBinding.VALUE_TYPE) {
                    continue;
                }
                final var binding = TargetScheduleBinding.decodeForStore(
                        row.key(), value.payload(), plan.scope().shard());
                if (!binding.imported() || !Arrays.equals(binding.migrationPlanDigest(), plan.digest())) {
                    throw new IllegalStateException(
                            "old prefix cannot introduce an ordinary or differently authorized Target binding");
                }
                final byte[] originKey = TargetMigrationOrigin.keyFor(ColumnFamily.ID, Bytes.sha256(row.key()));
                final byte[] retainedOrigin = store.get(ColumnFamily.META, originKey);
                if (retainedOrigin != null) {
                    final var origin = TargetMigrationOrigin.decode(
                            TargetValueEnvelope.decode(retainedOrigin, TargetMigrationOrigin.VALUE_TYPE)
                                    .payload());
                    origin.requirePlan(plan);
                    origin.requireRow(ColumnFamily.ID, row.key(), row.value());
                    continue;
                }
                if (binding.migrationSource() == null
                        || !binding.migrationSource().equals(stamp)) {
                    throw new IllegalStateException("new prefix binding lost its exact actual Target Source stamp");
                }
                final byte[] oldKey = KeyCodec.idScheduleBinding(binding.messageId());
                final byte[] oldRaw = Objects.requireNonNull(
                        legacyAfterApply.get(ColumnFamily.ID, oldKey),
                        "complete actual old binding at this prefix Source");
                final var id = new TargetQuotaIdentity(
                        TargetQuotaIdentity.Kind.TARGET,
                        plan.scope().shard(),
                        binding.accountingIncarnation(),
                        binding.target(),
                        null);
                final byte[] descriptorKey = id.key();
                descriptorKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
                byte[] descriptorRaw = null;
                for (var proposed : projection.desired()) {
                    if (proposed.family() == ColumnFamily.META && Arrays.equals(proposed.key(), descriptorKey)) {
                        descriptorRaw = proposed.value();
                        break;
                    }
                }
                if (descriptorRaw == null) {
                    descriptorRaw = store.get(ColumnFamily.META, descriptorKey);
                }
                final var owner = TargetQuotaIncarnation.decodeForStore(
                        descriptorKey,
                        TargetValueEnvelope.decode(
                                        Objects.requireNonNull(descriptorRaw, "actual C allocated prefix owner"),
                                        TargetQuotaIncarnation.VALUE_TYPE)
                                .payload(),
                        plan.scope().shard(),
                        plan.scope().tenantScope());
                final var origin = new TargetMigrationOrigin(
                        plan.digest(),
                        plan.migrationId(),
                        plan.baseline(),
                        binding.migrationRecord(),
                        ColumnFamily.ID,
                        Bytes.sha256(oldKey),
                        Bytes.sha256(oldRaw),
                        ColumnFamily.ID,
                        Bytes.sha256(row.key()),
                        Bytes.sha256(row.value()),
                        owner,
                        TargetRecordAccounting.resources(owner.accounting()
                                .recordCharge(
                                        TargetQuotaAccounting.RecordClass.STATE,
                                        row.key().length,
                                        value.payload().length)),
                        CapacityVector.empty());
                edits.add(new TargetStoreBackend.Edit(
                        ColumnFamily.META,
                        origin.key(),
                        null,
                        TargetValueEnvelope.encode(TargetMigrationOrigin.VALUE_TYPE, origin.canonicalBytes())));
            }
            for (var key : projection.deletions()) {
                final byte[] before = store.get(key.family(), key.key());
                if (before != null) {
                    edits.add(new TargetStoreBackend.Edit(key.family(), key.key(), before, null));
                }
            }
            for (var entity : projection.retained()) {
                final byte[] existing =
                        store.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(entity.messageId()));
                if (existing != null) {
                    TargetMigrationStore.requireGuard(
                            plan,
                            TargetMigrationCustodyGuard.decode(
                                    TargetValueEnvelope.decode(existing, TargetMigrationCustodyGuard.VALUE_TYPE)
                                            .payload()));
                    continue;
                }
                final var guard = entity.identityOnly()
                        ? TargetMigrationCustodyGuard.heldIdentity(
                                plan.migrationId(),
                                plan.digest(),
                                entity.messageId(),
                                entity.original(),
                                entity.resultDigest(),
                                root,
                                entity.retainedCharge())
                        : TargetMigrationCustodyGuard.held(
                                plan.migrationId(),
                                plan.digest(),
                                entity.messageId(),
                                entity.generation(),
                                entity.original(),
                                entity.resultDigest(),
                                root,
                                entity.retainedCharge());
                edits.add(new TargetStoreBackend.Edit(
                        ColumnFamily.META,
                        guard.key(),
                        null,
                        TargetValueEnvelope.encode(TargetMigrationCustodyGuard.VALUE_TYPE, guard.canonicalBytes())));
            }
            edits.add(new TargetStoreBackend.Edit(
                    ColumnFamily.META,
                    prefix.key(),
                    null,
                    TargetValueEnvelope.encode(TargetMigrationPrefixRecord.VALUE_TYPE, prefix.canonicalBytes())));
            return List.copyOf(edits);
        });
        held.requirePrefix(plan, original, legacyAfterApply, read.value());
        held.requireExternalCurrent(plan, store.metadata());
        store.withReadView(read.view(), () -> {
            held.requireLocalCurrent();
            return Boolean.TRUE;
        });
        applyLegacyPrefix(
                candidate, original, legacyAfterApply, read.value(), budget, commits, projection.targetView());
    }

    private static byte[] originalDigest(SourceReplayEntry original) {
        return original instanceof SourceReplayRecord record
                ? record.command().canonicalFrameDigest()
                : Bytes.sha256(((SourceReplayMutation) original).mutation().canonicalEnvelope());
    }

    /** The same actual Source entry must be reprojected from the retained old after-view. */
    public static final class ViewChanged extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public ViewChanged(Throwable cause) {
            super("migration Target Native view changed while preparing the same Source entry", cause);
        }
    }

    private static byte[] originalFrameDigest(SourceReplayEntry original) {
        return original instanceof SourceReplayRecord record
                ? record.command().canonicalFrameDigest()
                : Bytes.sha256(((SourceReplayMutation) original).mutation().encodeFrame());
    }

    /** Concrete provenance-to-Native conversion; contribution fields come from the independent typed row fold. */
    public static List<ImportedRow> importedRows(
            TargetMigrationPlan plan,
            TargetQuotaIncarnation root,
            List<LegacyMigrationBundle.OriginInput> inputs,
            Limits limits,
            long maximumRetainedOriginBytes) {
        inputs = List.copyOf(inputs);
        if (inputs.size() > limits.maximumAuditRecords()
                || maximumRetainedOriginBytes <= 0
                || maximumRetainedOriginBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "origin construction requires independent finite record/retained-byte bounds");
        }
        final var rows = new LinkedHashMap<String, LegacyMigrationBundle.TargetRow>();
        final var origins = new LinkedHashMap<String, TargetMigrationOrigin>();
        final var rootMarker = TargetMigrationRoot.prepared(plan, root);
        final var book = new TargetQuotaBookkeeping(
                root.identity(),
                root.tenantScope(),
                root.accounting(),
                new TargetQuotaBookkeeping.Inventory(2, 0, 0),
                1,
                TargetMigrationRoot.baselineFor(plan));
        putRow(
                rows,
                new LegacyMigrationBundle.TargetRow(
                        ColumnFamily.META,
                        root.key(),
                        TargetValueEnvelope.encode(TargetQuotaIncarnation.VALUE_TYPE, root.canonicalBytes())));
        putRow(
                rows,
                new LegacyMigrationBundle.TargetRow(
                        ColumnFamily.META,
                        rootMarker.key(),
                        TargetValueEnvelope.encode(TargetMigrationRoot.VALUE_TYPE, rootMarker.canonicalBytes())));
        putRow(
                rows,
                new LegacyMigrationBundle.TargetRow(
                        ColumnFamily.META,
                        book.key(),
                        TargetValueEnvelope.encode(TargetQuotaBookkeeping.VALUE_TYPE, book.canonicalBytes())));
        for (var input : inputs) {
            putRow(rows, input.row());
            final var original = input.original();
            final var origin = new TargetMigrationOrigin(
                    plan.digest(),
                    plan.migrationId(),
                    plan.baseline(),
                    input.originalRef(),
                    original.family(),
                    Bytes.sha256(original.key()),
                    Bytes.sha256(original.value()),
                    input.row().family(),
                    Bytes.sha256(input.row().key()),
                    Bytes.sha256(input.row().value()),
                    input.owner(),
                    TargetQuotaUsage.empty(),
                    input.retainedLegacyCharge());
            origin.requirePlan(plan);
            if (origins.putIfAbsent(rowId(input.row().family(), input.row().key()), origin) != null) {
                throw new IllegalArgumentException("duplicate concrete import origin");
            }
            putRow(
                    rows,
                    new LegacyMigrationBundle.TargetRow(
                            ColumnFamily.META,
                            origin.key(),
                            TargetValueEnvelope.encode(TargetMigrationOrigin.VALUE_TYPE, origin.canonicalBytes())));
        }
        final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
            @Override
            public ShardId shardId() {
                return plan.scope().shard();
            }

            @Override
            public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) {
                    throw new IllegalArgumentException("origin construction has no live overlay");
                }
                final var row = rows.get(rowId(family, key));
                return row == null ? null : row.value();
            }

            @Override
            public ShardStore.KeyValue first(
                    ColumnFamily family, byte[] lower, byte[] upper, List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) {
                    throw new IllegalArgumentException("origin construction has no live overlay");
                }
                return rows.values().stream()
                        .filter(row -> row.family() == family
                                && Arrays.compareUnsigned(row.key(), lower) >= 0
                                && Arrays.compareUnsigned(row.key(), upper) < 0)
                        .min((a, b) -> Arrays.compareUnsigned(a.key(), b.key()))
                        .map(row -> new ShardStore.KeyValue(row.key(), row.value()))
                        .orElse(null);
            }
        };
        final var accounting = new TargetRecordAccounting(
                view,
                List.of(),
                plan.scope(),
                plan.newLineage(),
                TargetMigrationRoot.baselineFor(plan),
                book,
                limits.maximumDomains());
        final var result = new ArrayList<ImportedRow>();
        long retained = 0;
        for (var input : inputs) {
            final var row = input.row();
            final var fee = Objects.requireNonNull(
                    accounting.charge(row.family(), row.key(), row.value()),
                    "imported row must have actual explicit accounting");
            if (!fee.owner().identity().equals(input.owner().identity())
                    || !fee.owner().accounting().equals(input.owner().accounting())) {
                throw new IllegalStateException(
                        "mapping fee attribution differs from independently decoded Target owner");
            }
            final var skeleton = origins.get(rowId(row.family(), row.key()));
            final var origin = new TargetMigrationOrigin(
                    skeleton.planDigest(),
                    skeleton.migrationId(),
                    skeleton.baseline(),
                    skeleton.oldRecord(),
                    skeleton.oldFamily(),
                    skeleton.oldKeyDigest(),
                    skeleton.oldValueDigest(),
                    skeleton.newFamily(),
                    skeleton.newKeyDigest(),
                    skeleton.newValueDigest(),
                    fee.owner(),
                    fee.primary(),
                    skeleton.retainedLegacyCharge());
            final var imported = new ImportedRow(row.family(), row.key(), row.value(), origin);
            TargetMigrationStore.requireImportOrigin(plan, imported);
            retained = Math.addExact(retained, origin.canonicalBytes().length);
            if (retained > maximumRetainedOriginBytes) {
                throw new IllegalStateException("complete origin bytes exceed held import window");
            }
            result.add(imported);
        }
        return List.copyOf(result);
    }

    private static String rowId(ColumnFamily family, byte[] key) {
        return family.rocksName() + ":" + Bytes.hex(key);
    }

    private static void putRow(Map<String, LegacyMigrationBundle.TargetRow> rows, LegacyMigrationBundle.TargetRow row) {
        final var previous = rows.putIfAbsent(rowId(row.family(), row.key()), row);
        if (previous != null && !Arrays.equals(previous.value(), row.value())) {
            throw new IllegalStateException("concrete migration projections disagree on same physical key");
        }
    }

    private ImportSummary fold(BoundedReadBudget budget) {
        final var seed = TargetQuotaBookkeeping.decode(TargetValueEnvelope.decode(
                        store.get(
                                ColumnFamily.META,
                                TargetQuotaBookkeeping.keyFor(plan.scope().shard())),
                        TargetQuotaBookkeeping.VALUE_TYPE)
                .payload());
        final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
            @Override
            public ShardId shardId() {
                return plan.scope().shard();
            }

            @Override
            public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) {
                    throw new IllegalArgumentException("import fold has no overlay");
                }
                return store.get(family, key);
            }

            @Override
            public ShardStore.KeyValue first(
                    ColumnFamily family, byte[] lower, byte[] upper, List<TargetStoreBackend.Edit> overlay) {
                final ShardStore.KeyValue[] first = {null};
                final var result = store.visitResult(family, lower, upper, 1, budget, (row, shared) -> {
                    first[0] = row;
                    return false;
                });
                if (result.stop() == ShardStore.VisitStop.INCOMPLETE) {
                    throw budget.incomplete();
                }
                return first[0];
            }
        };
        final var accounting = new TargetRecordAccounting(
                view, List.of(), plan.scope(), plan.newLineage(), baseline, seed, limits.maximumDomains());
        final var charges = new LinkedHashMap<TargetQuotaIdentity, TargetQuotaUsage>();
        final long[] count = new long[3];
        for (var family : ColumnFamily.values()) {
            final var result = store.visitResult(
                    family, new byte[0], null, limits.maximumAuditRecords(), budget, (row, shared) -> {
                        if (TargetMigrationStore.isImportInfrastructure(family, row.key())) {
                            return true;
                        }
                        final var value = TargetValueEnvelope.decodeAny(row.value());
                        requireInitialSummaryRow(value.valueType(), value.payload());
                        if (value.valueType() == TargetMigrationOrigin.VALUE_TYPE) {
                            count[1]++;
                        } else if (value.valueType() == TargetMigrationCustodyGuard.VALUE_TYPE) {
                            count[2]++;
                        } else if (value.valueType() != TargetMigrationRoot.VALUE_TYPE) {
                            count[0]++;
                            if (!Arrays.equals(row.key(), root.key())) {
                                TargetMigrationStore.requireBaselineRow(view, plan, family, row.key(), row.value());
                            }
                        }
                        final var fee = accounting.charge(family, row.key(), row.value());
                        if (fee != null) {
                            if (value.valueType() != TargetMigrationRoot.VALUE_TYPE
                                    && value.valueType() != TargetMigrationOrigin.VALUE_TYPE
                                    && value.valueType() != TargetMigrationCustodyGuard.VALUE_TYPE
                                    && !Arrays.equals(row.key(), root.key())) {
                                final var origin =
                                        TargetMigrationStore.origin(view, List.of(), plan, family, row.key());
                                if (!origin.owner()
                                                .identity()
                                                .equals(fee.owner().identity())
                                        || !origin.owner()
                                                .accounting()
                                                .equals(fee.owner().accounting())
                                        || !origin.contribution().equals(fee.primary())) {
                                    throw new IllegalStateException(
                                            "independent import fee differs from actual typed Target row");
                                }
                            }
                            merge(charges, fee.owner().identity(), fee.primary());
                            merge(charges, fee.owner().tenantIdentity(), fee.mirror());
                            if (charges.size() > limits.maximumCounters()) {
                                throw new IllegalStateException("import counter bound exhausted");
                            }
                        }
                        return true;
                    });
            if (result.stop() != ShardStore.VisitStop.RANGE_END) {
                if (result.stop() == ShardStore.VisitStop.INCOMPLETE) {
                    throw budget.incomplete();
                }
                throw new IllegalStateException("migration fold did not reach complete CF end");
            }
        }
        TargetQuotaUsage sum = TargetQuotaUsage.empty();
        for (var entry : charges.entrySet()) {
            if (!entry.getKey().kind().isMirror()) {
                sum = sum.add(entry.getValue());
            }
        }
        return new ImportSummary(count[0], count[1], count[2], charges, sum);
    }

    /** The C candidate has no Target Admission: every original old attempt remains in legacy custody. */
    private void requireInitialSummaryRow(int type, byte[] payload) {
        if (type == TargetQuotaPayloadOwner.VALUE_TYPE) {
            final var owner = TargetQuotaPayloadOwner.decode(payload);
            owner.requireQualifiedSummary();
            if (owner.unresolvedCount() != 0
                    || owner.summaryOrigin() != TargetQuotaPayloadOwner.SummaryOrigin.IMPORTED
                    || !owner.initializationSource().equals(baseline)) {
                throw new IllegalStateException(
                        "initial imported payload summary differs from its complete C qualification");
            }
        } else if (type == TargetMessageRecord.VALUE_TYPE) {
            if (!TargetMessageRecord.decode(payload)
                    .runtime()
                    .attemptObligations()
                    .isEmpty()) {
                throw new IllegalStateException(
                        "initial import cannot omit a current generation attempt from Native32");
            }
        } else if (type == TargetTerminalGenerationRecord.VALUE_TYPE) {
            if (!TargetTerminalGenerationRecord.decode(payload)
                    .runtime()
                    .attemptObligations()
                    .isEmpty()) {
                throw new IllegalStateException(
                        "initial import cannot omit a historical generation attempt from Native32");
            }
        } else if (type == TargetQuotaAttemptBudget.VALUE_TYPE) {
            throw new IllegalStateException("original legacy Admission cannot be relabeled as a Target Budget at C");
        } else if (type == TargetResultRecord.VALUE_TYPE) {
            final var result = TargetResultRecord.decode(payload);
            if (result.kind() == TargetResultRecord.Kind.SYSTEM
                    && result.appliedFirst()
                    && SystemMutationResult.decode(result.typedPayload()).mutationType()
                            == SystemMutationType.TARGET_PUBLISH_ADMISSION) {
                throw new IllegalStateException("initial import cannot manufacture a Target Admission first at C");
            }
        }
    }

    private void addExact(List<StoredEdit> changes, ColumnFamily family, byte[] key, byte[] after) {
        final byte[] before = store.get(family, key);
        if (before != null && !Arrays.equals(before, after)) {
            throw new IllegalStateException("candidate row changed across retries");
        }
        if (before == null) {
            changes.add(new StoredEdit(family, key, null, after));
        }
    }

    private static StoredEdit raw(ColumnFamily family, byte[] key, int type, byte[] payload) {
        return new StoredEdit(family, key, null, TargetValueEnvelope.encode(type, payload));
    }

    private static void merge(
            Map<TargetQuotaIdentity, TargetQuotaUsage> map, TargetQuotaIdentity id, TargetQuotaUsage fee) {
        map.merge(id, fee, TargetQuotaUsage::add);
    }

    private record StoredEdit(ColumnFamily family, byte[] key, byte[] before, byte[] after) {}

    public static TargetQuotaMutation migrationBaseline(TargetMigrationPlan plan) {
        return new TargetQuotaMutation(
                plan.baseline().sequence(), plan.baseline().source(), plan.digest());
    }

    private static void write(
            ShardStore store,
            ShardStore.ReadView view,
            List<StoredEdit> rows,
            TargetStoreBackend.WriteLimits limits,
            Held held) {
        long bytes = 0;
        final var keys = new HashSet<String>();
        for (var row : rows) {
            bytes = Math.addExact(bytes, row.key().length + (long) (row.after() == null ? 0 : row.after().length));
            if (!keys.add(row.family() + ":" + Bytes.hex(row.key()))) {
                throw new IllegalArgumentException("duplicate import key");
            }
        }
        if (rows.size() > limits.maximumRecords() || bytes > limits.maximumEncodedBytes()) {
            throw new IllegalStateException("migration Native import batch exceeds its finite write budget");
        }
        store.withReadView(view, () -> {
            held.requireLocalCurrent();
            store.write(batch -> {
                batch.requireReadView(view);
                held.requireLocalCurrent();
                for (var row : rows) {
                    if (row.after() == null) {
                        batch.delete(row.family(), row.key());
                    } else {
                        batch.put(row.family(), row.key(), row.after());
                    }
                }
            });
            return Boolean.TRUE;
        });
    }
}
