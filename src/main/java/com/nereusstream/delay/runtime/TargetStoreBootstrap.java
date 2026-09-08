package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** First signed Shard grant creates the entire root/result/source batch in a provably uninitialized Target Store. */
public final class TargetStoreBootstrap {
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
                final var complete = new TargetSourceAccounting(scope, lineage, source, stamp.mutationDigest(), 2, 1, 1)
                        .assemble(
                                reader,
                                List.of(
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
