package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** Source-dispatched grant-control application, immutable System replay and actual result/source accounting. */
public final class TargetQuotaGrantStore {
    public static final class Prepared {
        private final TargetQuotaGrantStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                final TargetQuotaGrantStore owner,
                final TargetStoreBackend.Prepared batch,
                final SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    public static final class Dispatch {
        private final TargetQuotaGrantStore owner;
        private final Prepared first;
        private final TargetSystemReplayStore.Prepared duplicate;

        private Dispatch(
                final TargetQuotaGrantStore owner,
                final Prepared first,
                final TargetSystemReplayStore.Prepared duplicate) {
            this.owner = owner;
            this.first = first;
            this.duplicate = duplicate;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetSystemReplayStore replay;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetQuotaGrantStore(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final int maximumCounters,
            final int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null
                || maximumCounters < 2
                || maximumDomains < 1
                || maximumDomains > 64
                || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("grant Store requires bounded Shard/lineage accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
        replay = new TargetSystemReplayStore(backend, scope, lineage, maximumCounters, maximumDomains);
    }

    /** Routes physical replay, later duplicates and first application with one cumulative read budget. */
    public Dispatch prepare(
            final BoundedReadBudget budget,
            final PreparedControlOperation control,
            final SystemMutation mutation,
            final SourcePosition source,
            final TargetQuotaGrantControlVerifier.Authority authority) {
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(authority, "authority");
        TargetQuotaGrantControlBody.decode(
                Objects.requireNonNull(mutation, "mutation").canonicalBody());
        final var duplicate = replay.prepareIfPresent(budget, mutation, source);
        return duplicate.isPresent()
                ? new Dispatch(this, null, duplicate.orElseThrow())
                : new Dispatch(this, prepareFirst(budget, control, mutation, source, authority), null);
    }

    public SystemMutationResult commit(
            final Dispatch dispatch,
            final TargetStoreBackend.CommitAuthority writes,
            final TargetStoreBackend.ReadAuthority reads) {
        Objects.requireNonNull(dispatch, "dispatch");
        if (dispatch.owner != this) {
            throw new IllegalArgumentException("foreign grant dispatch plan");
        }
        return dispatch.first != null
                ? commit(dispatch.first, writes)
                : replay.commit(dispatch.duplicate, writes, reads);
    }

    /**
     * Requires an established controlled root and a first logical/physical record. A source dispatcher must
     * use prepare for duplicate routing through the immutable-result/POSITION path; this method never reapplies them.
     * Authority snapshots must be retained/rechecked by the actual commit guard through native commit.
     */
    public Prepared prepareFirst(
            final BoundedReadBudget budget,
            final PreparedControlOperation control,
            final SystemMutation mutation,
            final SourcePosition source,
            final TargetQuotaGrantControlVerifier.Authority authority) {
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId())
                || !scope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException("grant mutation/source belongs to another Shard or operation kind");
        }
        final var body = TargetQuotaGrantControlBody.decode(mutation.canonicalBody());
        final var result = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException(
                        "first grant apply requires an established, strictly earlier source frontier");
            }
            final byte[] firstKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                    mutation.systemMutationId());
            final byte[] positionKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    source.canonicalBytes());
            requireAbsent(reader, ColumnFamily.DEDUPE, firstKey);
            requireAbsent(reader, ColumnFamily.DEDUPE, positionKey);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(mutation.canonicalEnvelope()));
            final var root = root(reader, stamp);
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            TargetQuotaIncarnation allocation = null;
            SystemMutationResult outcome;
            if (reader.closedIngressDeadlineThrough() >= mutation.retryUntilEpochMs()) {
                outcome = SystemMutationResult.from(
                        mutation,
                        ApplyStatus.REJECTED,
                        StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                        source.canonicalBytes());
            } else if (!scope.equals(body.request().next().scope().shardScope())) {
                outcome = SystemMutationResult.from(
                        mutation,
                        ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                        source.canonicalBytes());
            } else {
                final var targetScope = body.request().next().scope();
                final var decision = TargetQuotaGrantControlVerifier.decideFirstApplication(
                        control, mutation, source, view(reader, targetScope), authority);
                if (decision.rejection() != null) {
                    outcome = SystemMutationResult.from(
                            mutation, ApplyStatus.REJECTED, decision.rejection(), source.canonicalBytes());
                } else {
                    final var change = decision.change();
                    change.requireCurrent(view(reader, targetScope));
                    final var next = change.after();
                    if (!next.mutation().equals(stamp)) {
                        throw new IllegalStateException("verified grant uses another exact source mutation");
                    }
                    if (next.allocation() != null
                            && next.allocation().allocation().equals(stamp)) {
                        allocation = next.allocation();
                        requireAbsent(reader, ColumnFamily.META, allocation.key());
                        edits.add(reader.replace(
                                ColumnFamily.META,
                                allocation.key(),
                                TargetQuotaIncarnation.VALUE_TYPE,
                                allocation.canonicalBytes()));
                    }
                    edits.add(reader.replace(
                            ColumnFamily.META,
                            next.key(),
                            TargetQuotaGrantActivation.VALUE_TYPE,
                            next.canonicalBytes()));
                    outcome = SystemMutationResult.from(
                            mutation, ApplyStatus.APPLIED, StableCode.OK, source.canonicalBytes());
                }
            }
            final var expected = outcome;
            final var assigned = allocation;
            final TargetResultRecord.CreationAuthority resultAuthority = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp)) {
                    throw new IllegalStateException("grant result changed its actual source stamp");
                }
                requireAbsent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null
                            || !Arrays.equals(record.typedPayload(), expected.encode())
                            || !Arrays.equals(record.key(), firstKey)
                            || !sameAllocation(record.allocation(), assigned)) {
                        throw new IllegalStateException("grant first result differs from the verified decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null
                            || !Arrays.equals(first.key(), firstKey)
                            || !Arrays.equals(first.typedPayload(), expected.encode())
                            || !Arrays.equals(record.key(), positionKey)) {
                        throw new IllegalStateException("grant POSITION differs from its exact first result");
                    }
                    record.requireFirst(first);
                } else {
                    throw new IllegalStateException("grant applier cannot create another result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome, stamp, allocation, resultAuthority);
            final var position = TargetResultRecord.position(root, first, stamp, resultAuthority);
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
            final var accounted = new TargetSourceAccounting(
                            scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .assemble(reader, edits);
            result[0] = outcome;
            return accounted;
        });
        return new Prepared(this, batch, result[0]);
    }

    /** Returns a result only after the complete activation/result/counter/source batch succeeds. */
    public SystemMutationResult commit(final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("grant plan belongs to another wrapper");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "commitAuthority"));
        return prepared.result;
    }

    private TargetQuotaGrantControlVerifier.View view(
            final TargetStoreBackend.Reader reader, final TargetQuotaScope targetScope) {
        final byte[] key = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                targetScope.keySuffix());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        final var activation = raw == null
                ? null
                : TargetQuotaGrantActivation.decodeForStore(
                        key,
                        TargetValueEnvelope.decode(raw, TargetQuotaGrantActivation.VALUE_TYPE)
                                .payload(),
                        scope.shard(),
                        scope.tenantScope());
        return new TargetQuotaGrantControlVerifier.View(
                activation,
                reader.aggregate(),
                targetScope.target() == null ? null : reader.total(targetScope),
                reader.sourceSequence(),
                reader.source(),
                lineage);
    }

    private TargetQuotaIncarnation root(final TargetStoreBackend.Reader reader, final TargetQuotaMutation operation) {
        final var identity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("grant apply lacks its controlled Shard accounting root");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("grant result root differs from its actual Shard/lineage");
        }
        final var prior = reader.aggregate().mutation();
        if (prior == null) {
            throw new IllegalStateException("grant root lacks its applied mutation frontier");
        }
        root.latestMutation().requireAtOrBefore(prior);
        prior.requireAtOrBefore(operation);
        return root;
    }

    private static void requireAbsent(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("first grant application encountered an existing logical/physical record");
        }
    }

    private static boolean sameAllocation(final TargetQuotaIncarnation left, final TargetQuotaIncarnation right) {
        return left == null
                ? right == null
                : right != null && Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }
}
