package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Exact System-result replay and later physical duplicate accounting, without rerunning the original business. */
public final class TargetSystemReplayStore {
    private record Inspection(TargetResultRecord first, SystemMutationResult result, boolean samePosition) {}

    public static final class Prepared {
        private final TargetSystemReplayStore owner;
        private final TargetStoreBackend.ReadPlan<Inspection> read;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                final TargetSystemReplayStore owner,
                final TargetStoreBackend.ReadPlan<Inspection> read,
                final TargetStoreBackend.Prepared batch,
                final SystemMutationResult result) {
            this.owner = owner;
            this.read = read;
            this.batch = batch;
            this.result = result;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetSystemReplayStore(
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
            throw new IllegalArgumentException("System replay requires bounded Shard/lineage accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
    }

    /** An empty result selects first application, which must independently recheck absence in its own view. */
    public Optional<Prepared> prepareIfPresent(
            final BoundedReadBudget budget, final SystemMutation mutation, final SourcePosition source) {
        Objects.requireNonNull(mutation, "mutation");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId()) || !scope.shard().equals(mutation.shardId())) {
            throw new IllegalArgumentException("System replay belongs to another Shard");
        }
        final var probe = backend.prepareRead(budget, reader -> inspect(reader, mutation, source));
        if (probe.value().first() == null) {
            return Optional.empty();
        }
        if (probe.value().samePosition()) {
            return Optional.of(new Prepared(this, probe, null, probe.value().result()));
        }
        final var result = new SystemMutationResult[1];
        // The probe chooses a branch only. The actual write plan rereads all facts using the same shared budget.
        final var batch = backend.prepare(budget, reader -> {
            final var actual = inspect(reader, mutation, source);
            if (actual.first() == null || actual.samePosition()) {
                throw new IllegalStateException(
                        "System replay branch changed; prepare again from the actual source view");
            }
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(mutation.canonicalEnvelope()));
            final var rootId = new TargetQuotaIdentity(
                    TargetQuotaIdentity.Kind.SHARD,
                    scope.shard(),
                    reader.aggregate().accountingIncarnation(),
                    null,
                    null);
            final var root = descriptor(reader, rootId);
            final var position = TargetResultRecord.position(root, actual.first(), stamp, (record, first) -> {
                record.requireOwner(root);
                record.requireFirst(actual.first());
                if (first == null
                        || !Arrays.equals(first.canonicalBytes(), actual.first().canonicalBytes())
                        || !record.mutation().equals(stamp)
                        || reader.get(ColumnFamily.DEDUPE, record.key()) != null) {
                    throw new IllegalStateException("duplicate POSITION changed its exact first/source/absence proof");
                }
            });
            final var accounted = new TargetSourceAccounting(
                            scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .assemble(
                            reader,
                            List.of(reader.replace(
                                    ColumnFamily.DEDUPE,
                                    position.key(),
                                    TargetResultRecord.VALUE_TYPE,
                                    position.canonicalBytes())));
            result[0] = actual.result();
            return accounted;
        });
        return Optional.of(new Prepared(this, null, batch, result[0]));
    }

    public SystemMutationResult commit(
            final Prepared prepared,
            final TargetStoreBackend.CommitAuthority writes,
            final TargetStoreBackend.ReadAuthority reads) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign System replay plan");
        }
        if (prepared.read != null) {
            return backend.completeRead(prepared.read, Objects.requireNonNull(reads, "readAuthority"))
                    .result();
        }
        backend.commit(prepared.batch, Objects.requireNonNull(writes, "writeAuthority"));
        return prepared.result;
    }

    private Inspection inspect(
            final TargetStoreBackend.Reader reader, final SystemMutation mutation, final SourcePosition source) {
        final var frontier = reader.source();
        if (frontier == null) {
            throw new IllegalStateException("System replay requires an established source root");
        }
        final int order = source.compareTo(frontier);
        if (order < 0 || (order == 0 && !Arrays.equals(source.canonicalBytes(), frontier.canonicalBytes()))) {
            throw new IllegalStateException("System replay source regressed or changed its complete metadata");
        }
        final byte[] firstKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, mutation.systemMutationId());
        final byte[] positionKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
        final var first = result(reader, firstKey);
        final var position = result(reader, positionKey);
        if (first == null) {
            if (order == 0 || position != null) {
                throw new IllegalStateException("applied physical source lacks the exact first System result");
            }
            return new Inspection(null, null, false);
        }
        if (first.kind() != TargetResultRecord.Kind.SYSTEM) {
            throw new IllegalStateException("System key has another result kind");
        }
        final var original = SystemMutationResult.decode(first.typedPayload());
        if (!Arrays.equals(original.mutationId(), mutation.systemMutationId())
                || !Arrays.equals(original.mutationHash(), mutation.mutationHash())
                || original.mutationType() != mutation.type()
                || original.retryUntilEpochMs() != mutation.retryUntilEpochMs()
                || !Arrays.equals(original.authorIdentity(), mutation.authorIdentity())) {
            throw new IllegalStateException("System logical identity was reused with different bytes");
        }
        if (order == 0) {
            if (position == null
                    || position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                    || position.mutation().sequence() != reader.sourceSequence()
                    || !Arrays.equals(
                            position.mutation().mutationDigest(), Bytes.sha256(mutation.canonicalEnvelope()))) {
                throw new IllegalStateException("same physical System replay lacks its exact committed envelope audit");
            }
            position.requireFirst(first);
        } else if (position != null) {
            throw new IllegalStateException("future System position is already present beyond the source frontier");
        }
        final var outcome = source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()
                ? SystemMutationResult.from(
                        mutation,
                        ApplyStatus.REJECTED,
                        StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                        source.canonicalBytes())
                : original;
        return new Inspection(first, outcome, order == 0);
    }

    private TargetResultRecord result(final TargetStoreBackend.Reader reader, final byte[] key) {
        final byte[] raw = reader.get(ColumnFamily.DEDUPE, key);
        if (raw == null) {
            return null;
        }
        final var value = TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE);
        final var record = TargetResultRecord.decode(value.payload());
        record.requireStored(key, value.valueType(), value.payload());
        if (!Arrays.equals(record.recoveryLineage(), lineage)
                || !Arrays.equals(record.tenantScope(), scope.tenantScope())) {
            throw new IllegalStateException("System result belongs to another tenant/lineage");
        }
        final var current = reader.aggregate().mutation();
        if (current == null) {
            throw new IllegalStateException("System result has no applied aggregate mutation");
        }
        record.mutation().requireAtOrBefore(current);
        record.requireOwner(descriptor(reader, record.primaryIdentity()));
        return record;
    }

    private TargetQuotaIncarnation descriptor(
            final TargetStoreBackend.Reader reader, final TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("System result owner descriptor is absent");
        }
        final var owner = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!owner.identity().equals(identity) || !Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("System result owner descriptor changed its identity/lineage");
        }
        final var current = reader.aggregate().mutation();
        if (current == null) {
            throw new IllegalStateException("result owner has no current source mutation");
        }
        owner.latestMutation().requireAtOrBefore(current);
        return owner;
    }
}
