package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetControlSnapshotInput;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** First signed Shard grant creates the entire root/result/source batch in a provably uninitialized Target Store. */
public final class TargetStoreBootstrap {
    private static final TargetCheckpointRootVerifier.LedgerAuditLimits DEFAULT_REOPEN_LEDGER_AUDIT_LIMITS =
            new TargetCheckpointRootVerifier.LedgerAuditLimits(100_000, 256L << 20, 500_000, 256L << 20);

    private TargetStoreBootstrap() {}

    /**
     * Proves the exact source start for this Store/lineage: a new route or certified empty cut with no omitted
     * obligations, recovery/legacy protections or unapplied earlier records. Also proves capacity for the complete
     * proposed root/result batch, including tenant placement and physical reserves. The commit guard must retain
     * and recheck these external snapshots along with Control/Owner authority until the native write completes.
     * This is not permission to initialize an empty directory over a nonempty source history.
     */
    @FunctionalInterface
    public interface StartAuthority {
        void requireAuthorized(
                StoreMetadata metadata, TargetQuotaIncarnation root, TargetStoreBackend.Mutation complete);
    }

    public static final class Prepared {
        private final TargetStoreBackend backend;
        private final TargetStoreBackend.Prepared batch;
        private final TargetQuotaIncarnation root;
        private final SystemMutationResult result;

        private Prepared(
                final TargetStoreBackend backend,
                final TargetStoreBackend.Prepared batch,
                final TargetQuotaIncarnation root,
                final SystemMutationResult result) {
            this.backend = backend;
            this.batch = batch;
            this.root = root;
            this.result = result;
        }
    }

    /** Obtained only after native commit. This establishes Store data, not Worker/Producer activation authority. */
    public static final class Initialized {
        private final Prepared committed;

        private Initialized(final Prepared committed) {
            this.committed = committed;
        }

        public TargetStoreBackend backend() {
            return committed.backend;
        }

        public TargetQuotaIncarnation root() {
            return committed.root;
        }

        public SystemMutationResult result() {
            return committed.result;
        }
    }

    /** Root reconstructed from the fixed bookkeeping anchor under a guarded, bounded Store view. */
    public static final class Reopened {
        private final TargetStoreBackend backend;
        private final TargetQuotaIncarnation root;
        private final ShardStore.ReadView view;

        private Reopened(final TargetStoreBackend backend, final TargetQuotaIncarnation root,
                final ShardStore.ReadView view) {
            this.backend = backend;
            this.root = root;
            this.view = view;
        }

        public TargetStoreBackend backend() {
            return backend;
        }

        public TargetQuotaIncarnation root() {
            return root;
        }

        /** Exact complete ledger audit cut; local writes invalidate it even without Source advancement. */
        public ShardStore.ReadView view() {
            return view;
        }
    }

    /** Actual Source-applied SLO material at one audited reopen cut; this does not close any old writer domain. */
    public static final class SloRecoveryAudit {
        private final Reopened reopened;
        private final TargetQuotaMutation cut;
        private final List<TargetSloObservationGeneration> generations;
        private final List<TargetSloWriterEpoch> writers;
        private final long retainedBytes;

        private SloRecoveryAudit(Reopened reopened, SloFacts facts) {
            this.reopened = reopened;
            cut = facts.cut(); generations = facts.generations(); writers = facts.writers();
            retainedBytes = facts.retainedBytes();
        }

        public Reopened reopened() { return reopened; }
        public TargetQuotaIncarnation root() { return reopened.root(); }
        public TargetQuotaScope scope() { return root().scope(); }
        public byte[] lineage() { return root().recoveryLineage(); }
        public TargetQuotaMutation cut() { return cut; }
        public List<TargetSloObservationGeneration> generations() { return generations; }
        public List<TargetSloWriterEpoch> writers() { return writers; }
        public long retainedBytes() { return retainedBytes; }
    }

    private record SloFacts(TargetQuotaMutation cut, List<TargetSloObservationGeneration> generations,
            List<TargetSloWriterEpoch> writers, long retainedBytes) {}
    private record ReopenFacts(TargetCheckpointRootVerifier.RootProof proof, SloFacts slo) {}
    private record ReopenResult(Reopened reopened, SloRecoveryAudit slo) {}

    /** Rebuilds the exact Target root after Store reopen; external Owner/recovery authority must guard the read. */
    public static Reopened reopen(
            final ShardStore store,
            final TargetQuotaScope scope,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final TargetStoreBackend.ReadAuthority reads) {
        return reopen(store, scope, limits, budget, reads, DEFAULT_REOPEN_LEDGER_AUDIT_LIMITS);
    }

    /** Reopen with caller-selected finite limits for the independent live business-ledger fold. */
    public static Reopened reopen(
            final ShardStore store,
            final TargetQuotaScope scope,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final TargetStoreBackend.ReadAuthority reads,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return reopen0(store, scope, limits, budget, reads, ledgerLimits, 0).reopened();
    }

    public static SloRecoveryAudit reopenForSloRecovery(
            final ShardStore store,
            final TargetQuotaScope scope,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final TargetStoreBackend.ReadAuthority reads,
            final long maximumRetainedBytes) {
        return reopenForSloRecovery(store, scope, limits, budget, reads, DEFAULT_REOPEN_LEDGER_AUDIT_LIMITS,
                maximumRetainedBytes);
    }

    /** Retains every SLO generation and writer row from the original full fold under a separate finite byte bound. */
    public static SloRecoveryAudit reopenForSloRecovery(
            final ShardStore store,
            final TargetQuotaScope scope,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final TargetStoreBackend.ReadAuthority reads,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final long maximumRetainedBytes) {
        if (maximumRetainedBytes <= 0 || maximumRetainedBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("SLO recovery facts require an independent finite retained-byte bound");
        }
        return reopen0(store, scope, limits, budget, reads, ledgerLimits, maximumRetainedBytes).slo();
    }

    private static ReopenResult reopen0(
            final ShardStore store,
            final TargetQuotaScope scope,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final TargetStoreBackend.ReadAuthority reads,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final long maximumRetainedBytes) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(reads, "reads");
        Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        if (scope.target() != null
                || !scope.shard().equals(store.shardId())
                || store.metadata().storeFormatVersion() != 2) {
            throw new IllegalArgumentException("Target reopen requires its exact format-2 Shard Store/scope");
        }
        try (var guard = Objects.requireNonNull(reads.acquire(store.metadata(), scope), "Target reopen read guard")) {
            guard.requireCurrent();
            final var plan = store.readWithBudget(budget, () -> {
                TargetMigrationStore.requireServiceable(store);
                final byte[] bookkeepingKey = TargetQuotaBookkeeping.keyFor(scope.shard());
                final byte[] bookkeepingRaw = store.get(ColumnFamily.META, bookkeepingKey);
                if (bookkeepingRaw == null) {
                    throw new IllegalStateException("Target reopen has no fixed Shard bookkeeping root");
                }
                final var bookkeeping = TargetQuotaBookkeeping.decode(
                        TargetValueEnvelope.decode(bookkeepingRaw, TargetQuotaBookkeeping.VALUE_TYPE)
                                .payload());
                if (!Arrays.equals(bookkeeping.key(), bookkeepingKey)
                        || bookkeeping.owner().kind() != TargetQuotaIdentity.Kind.SHARD
                        || !bookkeeping.owner().shard().equals(scope.shard())
                        || !Arrays.equals(bookkeeping.tenantScope(), scope.tenantScope())) {
                    throw new IllegalStateException("Target reopen bookkeeping belongs to another Shard/tenant");
                }
                final byte[] rootKey = bookkeeping.owner().key();
                rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
                final byte[] rootRaw = store.get(ColumnFamily.META, rootKey);
                if (rootRaw == null) {
                    throw new IllegalStateException("Target reopen bookkeeping has no root incarnation");
                }
                final var persistedRoot = TargetQuotaIncarnation.decodeForStore(
                        rootKey,
                        TargetValueEnvelope.decode(rootRaw, TargetQuotaIncarnation.VALUE_TYPE)
                                .payload(),
                        scope.shard(),
                        scope.tenantScope());
                if (!bookkeeping.owner().equals(persistedRoot.identity())
                        || !Arrays.equals(
                                bookkeeping.accounting().canonicalBytes(),
                                persistedRoot.accounting().canonicalBytes())) {
                    throw new IllegalStateException("Target reopen root and bookkeeping disagree");
                }
                final byte[] aggregateKey = TargetQuotaAggregate.genesis(
                                scope.shard(), persistedRoot.identity().accountingIncarnation())
                        .key();
                final byte[] aggregateRaw = store.get(ColumnFamily.META, aggregateKey);
                if (aggregateRaw == null) {
                    throw new IllegalStateException("Target reopen has no aggregate for its root");
                }
                final var aggregate = TargetQuotaAggregate.decodeForStore(
                        aggregateKey,
                        TargetValueEnvelope.decode(aggregateRaw, TargetQuotaAggregate.VALUE_TYPE)
                                .payload(),
                        scope.shard());
                final SourcePosition source = store.appliedShardLogPosition();
                final long sequence = store.shardMutationSequence();
                if (!Arrays.equals(
                                aggregate.accountingIncarnation(),
                                persistedRoot.identity().accountingIncarnation())
                        || aggregate.mutation() == null
                        || aggregate.mutation().sequence() != sequence
                        || source == null
                        || !Arrays.equals(aggregate.mutation().source().canonicalBytes(), source.canonicalBytes())) {
                    throw new IllegalStateException("Target reopen aggregate/source frontier disagrees with root");
                }
                persistedRoot.latestMutation().requireAtOrBefore(aggregate.mutation());
                bookkeeping.mutation().requireAtOrBefore(aggregate.mutation());
                final var proof = new TargetCheckpointRootVerifier.RootProof(
                        store.metadata(), source, sequence, persistedRoot, bookkeeping, aggregate);
                final var dependencies = TargetCheckpointRootVerifier.auditLiveStoreDependencies(
                        store, proof, ledgerLimits, budget);
                return new ReopenFacts(proof, maximumRetainedBytes == 0 ? null
                        : sloFacts(proof, dependencies, maximumRetainedBytes));
            });
            return store.withReadView(plan.view(), () -> {
                guard.requireCurrent();
                final var root = plan.value().proof().root();
                final var reopened = new Reopened(
                        new TargetStoreBackend(
                                store, scope, root.identity().accountingIncarnation(), root.recoveryLineage(), limits),
                        root, plan.view());
                final var facts = plan.value().slo();
                return new ReopenResult(reopened, facts == null ? null : new SloRecoveryAudit(reopened, facts));
            });
        }
    }

    private static SloFacts sloFacts(TargetCheckpointRootVerifier.RootProof proof,
            TargetCheckpointDependencies dependencies, long maximumRetainedBytes) {
        final var cut = proof.aggregate().mutation();
        long bytes = retained(0, proof.root().canonicalBytes().length, maximumRetainedBytes);
        bytes = retained(bytes, cut.canonicalBytes().length, maximumRetainedBytes);
        final var generations = dependencies.sloGenerations();
        final var writers = dependencies.sloWriterEpochs();
        for (var generation : generations) {
            bytes = retained(bytes, generation.canonicalBytes().length, maximumRetainedBytes);
        }
        for (var writer : writers) {
            bytes = retained(bytes, writer.canonicalBytes().length, maximumRetainedBytes);
        }
        return new SloFacts(cut, List.copyOf(generations), List.copyOf(writers), bytes);
    }

    private static long retained(long before, int canonicalBytes, long maximumRetainedBytes) {
        final long after = Math.addExact(before, canonicalBytes);
        if (after > maximumRetainedBytes) {
            throw new IllegalArgumentException("SLO recovery facts exceed their independent retained-byte bound");
        }
        return after;
    }

    /** Invalid first input fails startup without recording a rejection, inventing a root or acknowledging source. */
    public static Prepared prepare(
            final ShardStore store,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final PreparedControlOperation control,
            final SystemMutation mutation,
            final SourcePosition source,
            final TargetQuotaGrantControlVerifier.Authority grants,
            final StartAuthority start) {
        return prepareInitial(store, scope, lineage, limits, budget, control, mutation, source, grants, start,
                null, null);
    }

    /** Authenticates issuer/reader set/immutable profile pins before any Store read or mutation. */
    @FunctionalInterface
    public interface InitialControlAuthority {
        java.security.PublicKey authenticate(TargetControlSnapshotInput snapshot, PreparedControlOperation control,
                SystemMutation mutation, SourcePosition source);
    }

    /** Complete initial-control form; old Source grant bytes and their fixed projection fees are unchanged. */
    public static Prepared prepareWithControlSnapshot(final ShardStore store, final TargetQuotaScope scope,
            final byte[] lineage, final TargetStoreBackend.WriteLimits limits, final BoundedReadBudget budget,
            final PreparedControlOperation control, final SystemMutation mutation, final SourcePosition source,
            final TargetQuotaGrantControlVerifier.Authority grants, final StartAuthority start,
            final TargetControlSnapshotInput snapshot, final InitialControlAuthority snapshotAuthority) {
        return prepareInitial(store, scope, lineage, limits, budget, control, mutation, source, grants, start,
                Objects.requireNonNull(snapshot, "snapshot"),
                Objects.requireNonNull(snapshotAuthority, "snapshotAuthority"));
    }

    private static Prepared prepareInitial(
            final ShardStore store,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final TargetStoreBackend.WriteLimits limits,
            final BoundedReadBudget budget,
            final PreparedControlOperation control,
            final SystemMutation mutation,
            final SourcePosition source,
            final TargetQuotaGrantControlVerifier.Authority grants,
            final StartAuthority start, final TargetControlSnapshotInput snapshot,
            final InitialControlAuthority snapshotAuthority) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(grants, "grantAuthority");
        Objects.requireNonNull(start, "startAuthority");
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(mutation, "mutation");
        TargetSourcePosition.requireBounded(source);
        final var body = TargetQuotaGrantControlBody.decode(mutation.canonicalBody());
        final var request = body.request();
        if (scope.target() != null
                || !scope.equals(request.next().scope())
                || request.prior() != null
                || !scope.shard().equals(source.shardId())) {
            throw new IllegalArgumentException("bootstrap requires the first exact Shard grant and source");
        }
        if (snapshot != null) {
            snapshot.requireInitial(scope, lineage, request.next(), source, Bytes.sha256(mutation.canonicalEnvelope()));
            final var key = Objects.requireNonNull(snapshotAuthority.authenticate(snapshot, control, mutation, source),
                    "snapshot issuer key");
            if (!snapshot.verifySignature(key)) {
                throw new IllegalArgumentException("Target snapshot signature invalid");
            }
        }
        final var stamp = new TargetQuotaMutation(1, source, Bytes.sha256(mutation.canonicalEnvelope()));
        final var prepared = new Prepared[1];
        TargetQuotaIncarnation.allocate(scope, request.next().accounting(), lineage, stamp, (prior, root) -> {
            final var backend =
                    new TargetStoreBackend(store, scope, root.identity().accountingIncarnation(), lineage, limits);
            final var outcome =
                    SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK, source.canonicalBytes());
            final var batch = backend.prepare(budget, reader -> {
                reader.requireUninitialized();
                final var view = new TargetQuotaGrantControlVerifier.View(
                        null, reader.aggregate(), null, reader.sourceSequence(), reader.source(), lineage);
                final var change =
                        TargetQuotaGrantControlVerifier.verifyFirstApplication(control, mutation, source, view, grants);
                if (!change.after().mutation().equals(stamp) || change.after().allocation() != null) {
                    throw new IllegalStateException("bootstrap grant changed its exact source/root role");
                }
                final TargetResultRecord.CreationAuthority results = (record, first) -> {
                    record.requireOwner(root);
                    if (!record.mutation().equals(stamp)
                            || record.allocation() != null
                            || reader.get(ColumnFamily.DEDUPE, record.key()) != null) {
                        throw new IllegalStateException("bootstrap result changed its root/source/absence proof");
                    }
                    if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                        if (first != null || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                            throw new IllegalStateException("bootstrap first result differs from the verified grant");
                        }
                    } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                        if (first == null || !Arrays.equals(first.typedPayload(), outcome.encode())) {
                            throw new IllegalStateException("bootstrap physical result differs from its first result");
                        }
                        record.requireFirst(first);
                    } else {
                        throw new IllegalStateException("bootstrap cannot create another result kind");
                    }
                };
                final var first = TargetResultRecord.system(root, outcome, stamp, null, results);
                final var position = TargetResultRecord.position(root, first, stamp, results);
                final var edits = new java.util.ArrayList<TargetStoreBackend.Edit>(List.of(
                                        reader.replace(
                                                ColumnFamily.META,
                                                root.key(),
                                                TargetQuotaIncarnation.VALUE_TYPE,
                                                root.canonicalBytes()),
                                        reader.replace(
                                                ColumnFamily.META,
                                                change.after().key(),
                                                TargetQuotaGrantActivation.VALUE_TYPE,
                                                change.after().canonicalBytes()),
                                        reader.replace(
                                                ColumnFamily.DEDUPE,
                                                first.key(),
                                                TargetResultRecord.VALUE_TYPE,
                                                first.canonicalBytes()),
                                        reader.replace(
                                                ColumnFamily.DEDUPE,
                                                position.key(),
                                                TargetResultRecord.VALUE_TYPE,
                                                position.canonicalBytes())));
                if (snapshot != null) {
                    final var persisted = new TargetInitialControlSnapshot(snapshot, root.identity(), stamp,
                            change.after(), mutation.authorIdentity());
                    edits.add(reader.replace(ColumnFamily.META, persisted.key(),
                            TargetInitialControlSnapshot.VALUE_TYPE,
                            persisted.canonicalBytes()));
                }
                final var complete = new TargetSourceAccounting(scope, lineage, source, stamp.mutationDigest(), 2, 1, 1)
                        .assemble(reader, edits);
                if (!request.next()
                        .limit()
                        .permitsGrowth(
                                TargetQuotaUsage.empty(),
                                complete.quota().counters().nextAggregate().usage())) {
                    throw new IllegalStateException("initial Shard grant cannot fund the entire bootstrap batch");
                }
                start.requireAuthorized(reader.metadata(), root, complete);
                return complete;
            });
            prepared[0] = new Prepared(backend, batch, root, outcome);
        });
        return prepared[0];
    }

    public static Initialized commit(final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        prepared.backend.commit(prepared.batch, Objects.requireNonNull(authority, "commitAuthority"));
        return new Initialized(prepared);
    }
}
