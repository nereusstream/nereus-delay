package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Atomic source-ordered fence, immutable first results and exact root-owned fixed-record accounting. */
public final class TargetTimeFenceStore {
    public static final class Prepared {
        private final TargetTimeFenceStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(TargetTimeFenceStore owner, TargetStoreBackend.Prepared batch, SystemMutationResult result) {
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

    public TargetTimeFenceStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumCounters,
            int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null
                || Arrays.equals(lineage, new byte[16])
                || maximumCounters < 2
                || maximumDomains < 1
                || maximumDomains > 64) {
            throw new IllegalArgumentException("fence Store requires bounded Shard/lineage accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
    }

    /** Caller must route immutable first-result replay before this method and protect authority through commit. */
    public Prepared prepareFirst(
            BoundedReadBudget budget,
            SystemMutation mutation,
            SourcePosition source,
            TargetTimeFenceVerifier.Authority authority) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId())
                || !scope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.TIME_FENCE) {
            throw new IllegalArgumentException("fence source/operation mismatch");
        }
        final var result = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("first fence needs an established strictly earlier source frontier");
            }
            final byte[] firstKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, mutation.systemMutationId());
            final byte[] positionKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, source.canonicalBytes());
            requireAbsent(reader, ColumnFamily.DEDUPE, firstKey);
            requireAbsent(reader, ColumnFamily.DEDUPE, positionKey);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(mutation.canonicalEnvelope()));
            final var root = root(reader, stamp);
            final byte[] priorBytes = reader.get(ColumnFamily.META, KeyCodec.metaFixed(4));
            final var prior = priorBytes == null
                    ? new IngressFenceState(IngressFenceState.OPEN, null)
                    : IngressFenceState.decode(
                            TargetValueEnvelope.decode(priorBytes, 1).payload());
            final TargetTimeFenceVerifier.Decision decision;
            if (prior.closedThroughEpochMs() >= mutation.retryUntilEpochMs()) {
                decision = null;
            } else {
                try {
                    decision = TargetTimeFenceVerifier.decideFirstApplication(scope, mutation, source, authority);
                } catch (ReadIncompleteException external) {
                    throw new IllegalStateException("external fence authority did not complete", external);
                }
            }
            final var rejection =
                    decision == null ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED : decision.rejection();
            final var outcome = SystemMutationResult.from(
                    mutation,
                    rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection,
                    source.canonicalBytes());
            final TargetStoreBackend.IngressFenceChange fence = rejection != null
                    ? null
                    : new TargetStoreBackend.IngressFenceChange(
                            priorBytes,
                            new IngressFenceState(
                                    Math.max(
                                            prior.closedThroughEpochMs(),
                                            decision.body().closeThrough()),
                                    decision.body().proofId()));
            final TargetResultRecord.CreationAuthority resultAuthority = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) {
                    throw new IllegalStateException("fence result changed its root/source");
                }
                requireAbsent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null
                            || !Arrays.equals(record.key(), firstKey)
                            || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                        throw new IllegalStateException("fence first result differs from verified decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null
                            || !Arrays.equals(first.key(), firstKey)
                            || !Arrays.equals(first.typedPayload(), outcome.encode())
                            || !Arrays.equals(record.key(), positionKey)) {
                        throw new IllegalStateException("fence POSITION differs from first result");
                    }
                    record.requireFirst(first);
                } else {
                    throw new IllegalStateException("unexpected fence result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome, stamp, null, resultAuthority);
            final var position = TargetResultRecord.position(root, first, stamp, resultAuthority);
            final var edits = List.of(
                    reader.replace(
                            ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()),
                    reader.replace(
                            ColumnFamily.DEDUPE,
                            position.key(),
                            TargetResultRecord.VALUE_TYPE,
                            position.canonicalBytes()));
            result[0] = outcome;
            return new TargetSourceAccounting(
                            scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .assemble(reader, edits, fence);
        });
        return new Prepared(this, batch, result[0]);
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign fence plan");
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
            throw new IllegalStateException("fence apply lacks its controlled Shard accounting root");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("fence result root differs from its actual Shard/lineage");
        }
        final var prior = reader.aggregate().mutation();
        if (prior == null) {
            throw new IllegalStateException("fence root lacks its applied mutation frontier");
        }
        root.latestMutation().requireAtOrBefore(prior);
        prior.requireAtOrBefore(operation);
        return root;
    }

    private static void requireAbsent(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("first fence application encountered an existing logical/physical record");
        }
    }
}
