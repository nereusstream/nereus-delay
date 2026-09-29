package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetNativePolicyControlBody;
import com.nereusstream.delay.protocol.TargetNativePolicyControlRecord;
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

/** First application of authenticated Native policy authority, source results and quota in one Store batch. */
public final class TargetNativePolicyControlStore {
    public static final class Prepared {
        private final TargetNativePolicyControlStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                final TargetNativePolicyControlStore owner,
                final TargetStoreBackend.Prepared batch,
                final SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetNativePolicyControlStore(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final int maximumCounters,
            final int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null
                || Arrays.equals(lineage, new byte[16])
                || maximumCounters < 2
                || maximumDomains < 1
                || maximumDomains > 64) {
            throw new IllegalArgumentException("Native control Store needs bounded Shard/lineage accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
    }

    public Prepared prepareFirst(
            final BoundedReadBudget budget,
            final PreparedControlOperation control,
            final SystemMutation mutation,
            final SourcePosition source,
            final TargetNativePolicyControlVerifier.Authority authority) {
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId())
                || !scope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException("Native control mutation/source belongs to another Shard or kind");
        }
        final var body = TargetNativePolicyControlBody.decode(mutation.canonicalBody());
        final var result = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("first Native control needs a strictly earlier source frontier");
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
            StableCode rejection = null;
            TargetNativePolicyControlVerifier.Change change = null;
            if (reader.closedIngressDeadlineThrough() >= mutation.retryUntilEpochMs()) {
                rejection = StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
            } else {
                try {
                    change = TargetNativePolicyControlVerifier.verifyFirstApplication(
                            reader, scope, lineage, control, mutation, source, authority);
                } catch (CommandResolutionException denied) {
                    if (denied.stableCode() != StableCode.UNAUTHORIZED_SYSTEM_MUTATION
                            && denied.stableCode() != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED) {
                        throw denied;
                    }
                    rejection = denied.stableCode();
                }
            }
            if (rejection == null) {
                if (change == null) {
                    throw new IllegalStateException(
                            "Native control verification returned neither a plan nor rejection");
                }
                if (change.registersScope()) {
                    final var nativeScope = change.body().request().scope();
                    requireAbsent(reader, ColumnFamily.META, nativeScope.encodedKey());
                    edits.add(reader.replace(
                            ColumnFamily.META,
                            nativeScope.encodedKey(),
                            com.nereusstream.delay.protocol.TargetNativePolicyScope.VALUE_TYPE,
                            nativeScope.canonicalBytes()));
                }
                if (change.createsRecord()) {
                    final var record =
                            new TargetNativePolicyControlRecord(body, stamp, control.author(), change.grant(), lineage);
                    requireAbsent(reader, ColumnFamily.META, record.key());
                    edits.add(reader.replace(
                            ColumnFamily.META,
                            record.key(),
                            TargetNativePolicyControlRecord.VALUE_TYPE,
                            record.canonicalBytes()));
                }
            }
            final var outcome = SystemMutationResult.from(
                    mutation,
                    rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection,
                    source.canonicalBytes());
            final TargetResultRecord.CreationAuthority resultAuthority = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) {
                    throw new IllegalStateException("Native control result changed its exact source root");
                }
                requireAbsent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null
                            || !Arrays.equals(record.key(), firstKey)
                            || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                        throw new IllegalStateException("Native control first result differs from its decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null
                            || !Arrays.equals(first.key(), firstKey)
                            || !Arrays.equals(first.typedPayload(), outcome.encode())
                            || !Arrays.equals(record.key(), positionKey)) {
                        throw new IllegalStateException("Native control POSITION differs from its exact first result");
                    }
                    record.requireFirst(first);
                } else {
                    throw new IllegalStateException("Native control cannot create another result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome, stamp, null, resultAuthority);
            final var position = TargetResultRecord.position(root, first, stamp, resultAuthority);
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
            result[0] = outcome;
            return new TargetSourceAccounting(
                            scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .assemble(reader, edits);
        });
        return new Prepared(this, batch, result[0]);
    }

    public SystemMutationResult commit(final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign Native control plan");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "authority"));
        return prepared.result;
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
            throw new IllegalStateException("Native control lacks its controlled Shard root");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Native control result root differs from Store/lineage");
        }
        final var prior = Objects.requireNonNull(reader.aggregate().mutation(), "applied root mutation");
        root.latestMutation().requireAtOrBefore(prior);
        prior.requireAtOrBefore(operation);
        return root;
    }

    private static void requireAbsent(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("first Native control encountered an existing result/history key");
        }
    }
}
