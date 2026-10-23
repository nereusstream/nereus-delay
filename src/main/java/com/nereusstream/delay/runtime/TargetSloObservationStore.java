package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.List;

/** Ordered certification mode, exact first/POS and frozen Root accounting in the original Source batch. */
public final class TargetSloObservationStore {
    public static final class Prepared {
        private final TargetSloObservationStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Runnable qualification;
        private Prepared(TargetSloObservationStore owner, TargetStoreBackend.Prepared batch,
                SystemMutationResult result, Runnable qualification) {
            this.owner = owner; this.batch = batch; this.result = result; this.qualification = qualification;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetSloObservationStore(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            int maximumCounters, int maximumDomains) {
        this.backend = Objects.requireNonNull(backend); this.scope = Objects.requireNonNull(scope);
        Bytes.requireLength(lineage, 16, "Target SLO Store lineage");
        if (scope.target() != null || Arrays.equals(lineage, new byte[16]) || maximumCounters < 2
                || maximumDomains < 1 || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS) throw new IllegalArgumentException("Target SLO Store requires original bounded Source scope");
        this.lineage = Bytes.copy(lineage); this.maximumCounters = maximumCounters; this.maximumDomains = maximumDomains;
    }

    public Prepared prepareFirst(BoundedReadBudget budget, PreparedControlOperation control, SystemMutation input,
            SourcePosition source, TargetSloObservationVerifier.Authority authority) {
        Objects.requireNonNull(control); Objects.requireNonNull(input); Objects.requireNonNull(authority);
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(input.shardId()) || !scope.shard().equals(source.shardId())
                || input.type() != SystemMutationType.APPLY_SHARD_CONTROL) throw new IllegalArgumentException("Target SLO input belongs to another Source scope");
        final var outcome = new SystemMutationResult[1];
        final boolean[] applied = {false};
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) throw new IllegalStateException("Target SLO first apply requires a later actual Source");
            final var firstKey = firstKey(input.systemMutationId()); final var positionKey = positionKey(source);
            absent(reader, ColumnFamily.DEDUPE, firstKey); absent(reader, ColumnFamily.DEDUPE, positionKey);
            final var stamp = new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()), source, Bytes.sha256(input.canonicalEnvelope()));
            final var root = root(reader, stamp);
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            StableCode rejection = null;
            TargetSloObservationVerifier.Change change = null;
            if (reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()) rejection = StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
            else {
                try { change = TargetSloObservationVerifier.verifyFirstApplication(reader, scope, lineage, control, input, source, authority); }
                catch (CommandResolutionException denied) {
                    if (denied.stableCode() != StableCode.UNAUTHORIZED_SYSTEM_MUTATION
                            && denied.stableCode() != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                            && denied.stableCode() != StableCode.STALE_SYSTEM_MUTATION
                            && denied.stableCode() != StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION
                            && denied.stableCode() != StableCode.HARD_QUOTA_EXCEEDED) throw denied;
                    rejection = denied.stableCode();
                }
            }
            if (rejection == null) {
                final var selected = Objects.requireNonNull(change, "original Target SLO Control transition");
                final var generation = selected.body().request().operationKind() == ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION
                        ? TargetSloObservationGeneration.enabled(input, stamp)
                        : Objects.requireNonNull(selected.retained()).stopped(input, stamp);
                if (generation.active()) absent(reader, ColumnFamily.META, generation.key());
                final var state = TargetSloObservationState.from(generation);
                edits.add(reader.replace(ColumnFamily.META, generation.key(), TargetSloObservationGeneration.VALUE_TYPE, generation.canonicalBytes()));
                edits.add(reader.replace(ColumnFamily.META, state.key(), TargetSloObservationState.VALUE_TYPE, state.canonicalBytes()));
                applied[0] = true;
            }
            outcome[0] = SystemMutationResult.from(input, rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection, source.canonicalBytes());
            final TargetResultRecord.CreationAuthority resultAuthority = (record, original) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) throw new IllegalStateException("Target SLO first/POS changed its actual frozen Root");
                absent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (original != null || !Arrays.equals(record.key(), firstKey) || !Arrays.equals(record.typedPayload(), outcome[0].encode())) throw new IllegalStateException("Target SLO first result changed its decision");
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (original == null || !Arrays.equals(original.key(), firstKey) || !Arrays.equals(record.key(), positionKey)
                            || !Arrays.equals(original.typedPayload(), outcome[0].encode())) throw new IllegalStateException("Target SLO POSITION lost original first");
                    record.requireFirst(original);
                } else throw new IllegalStateException("Target SLO Control cannot create another result kind");
            };
            final var first = TargetResultRecord.system(root, outcome[0], stamp, null, resultAuthority);
            final var position = TargetResultRecord.position(root, first, stamp, resultAuthority);
            edits.add(reader.replace(ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
            if (applied[0]) authority.materialization().requireCurrent();
            return new TargetSourceAccounting(scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains).assemble(reader, edits);
        });
        return new Prepared(this, batch, outcome[0], applied[0] ? authority.materialization()::requireCurrent : () -> {});
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority actual) {
        Objects.requireNonNull(prepared); Objects.requireNonNull(actual);
        if (prepared.owner != this) throw new IllegalArgumentException("foreign Target SLO Source plan");
        prepared.qualification.run();
        backend.commit(prepared.batch, (metadata, actualScope, mutation) -> {
            final var original = actual.acquire(metadata, actualScope, mutation);
            return new TargetStoreBackend.CommitGuard() {
                public void requireCurrent() { prepared.qualification.run(); original.requireCurrent(); prepared.qualification.run(); }
                public void close() { original.close(); }
            };
        });
        return prepared.result;
    }

    static byte[] firstKey(byte[] id) { return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id); }
    static byte[] positionKey(SourcePosition at) { return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, at.canonicalBytes()); }
    private TargetQuotaIncarnation root(TargetStoreBackend.Reader reader, TargetQuotaMutation at) {
        final var identity = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(), reader.aggregate().accountingIncarnation(), null, null);
        final var key = identity.key(); key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var raw = Objects.requireNonNull(reader.get(ColumnFamily.META, key), "original Target SLO Shard Root");
        final var root = TargetQuotaIncarnation.decodeForStore(key, TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) throw new IllegalStateException("Target SLO Root changed frozen scope");
        root.latestMutation().requireAtOrBefore(reader.aggregate().mutation()); reader.aggregate().mutation().requireAtOrBefore(at);
        return root;
    }
    private static void absent(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        if (reader.get(family, key) != null) throw new IllegalStateException("Target SLO first apply found an existing exact key");
    }

    /** Structural Source linkage only; public Control/history/collector authority is authenticated independently. */
    public static void requireOrigin(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity root,
            TargetSloObservationGeneration generation, TargetQuotaMutation frontier) {
        generation.mutation().requireAtOrBefore(frontier);
        final var activationKey = TargetProtocolActivationRecord.key(ProtocolTuple.targetSloObservation());
        final var activation = TargetProtocolActivationRecord.decodeForStore(activationKey,
                payload(view, overlay, ColumnFamily.META, activationKey, TargetProtocolActivationRecord.VALUE_TYPE), scope, lineage);
        if (activation.body().payload().descriptor().schemaVersion() < 15
                || activation.appliedAt().source().compareTo(generation.enable().appliedAt().source()) >= 0) {
            throw new IllegalStateException("Target SLO generation lacks prior independent activation");
        }
        TargetProtocolActivationStore.requireApplied(view, overlay, scope, lineage, root, activation, frontier);
        requireControlOrigin(view, overlay, scope, lineage, root, generation.enable(), frontier);
        if (generation.stop() != null) requireControlOrigin(view, overlay, scope, lineage, root, generation.stop(), frontier);
        if (generation.generation() != 1) {
            final var priorKey = TargetSloObservationGeneration.key(scope, generation.generation() - 1);
            final var prior = TargetSloObservationGeneration.decodeForStore(priorKey,
                    payload(view, overlay, ColumnFamily.META, priorKey, TargetSloObservationGeneration.VALUE_TYPE), scope, lineage);
            if (prior.active() || !Arrays.equals(TargetSloObservationState.from(prior).digest(),
                    generation.enable().body().request().expectedPriorDigest())) {
                throw new IllegalStateException("Target SLO generation lost its actual stopped predecessor");
            }
            generation.enable().appliedAt().requireAfter(prior.control().appliedAt());
            prior.policy().requireSource(generation.enable().appliedAt().source());
            requireControlOrigin(view, overlay, scope, lineage, root, prior.control(), frontier);
        }
        if (!generation.counts().equals(TargetSloObservationGeneration.Counts.empty())
                || !generation.mutation().equals(generation.control().appliedAt())) {
            throw new IllegalStateException("Target SLO sample accounting and its actual business Source links are not yet wired");
        }
    }

    private static void requireControlOrigin(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity root,
            TargetSloObservationGeneration.Control control, TargetQuotaMutation frontier) {
        control.appliedAt().requireAtOrBefore(frontier);
        final var firstKey = firstKey(control.input().systemMutationId());
        final var first = result(view, overlay, firstKey);
        if (first.kind() != TargetResultRecord.Kind.SYSTEM || !first.appliedFirst() || !first.mutation().equals(control.appliedAt())
                || !root.equals(first.primaryIdentity()) || !Arrays.equals(lineage, first.recoveryLineage())
                || !Arrays.equals(scope.tenantScope(), first.tenantScope())
                || !Arrays.equals(first.typedPayload(), SystemMutationResult.from(control.input(), ApplyStatus.APPLIED,
                        StableCode.OK, control.appliedAt().source().canonicalBytes()).encode())) {
            throw new IllegalStateException("Target SLO generation lost its actual original APPLIED first");
        }
        final var position = result(view, overlay, positionKey(control.appliedAt().source()));
        if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM || !position.mutation().equals(control.appliedAt())) {
            throw new IllegalStateException("Target SLO Control lost its first Source POSITION");
        }
        position.requireFirst(first);
    }

    private static TargetResultRecord result(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, byte[] key) {
        final var result = TargetResultRecord.decode(payload(view, overlay, ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE));
        result.requireStored(key, TargetResultRecord.VALUE_TYPE, result.canonicalBytes());
        return result;
    }

    private static byte[] payload(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family, byte[] key, int type) {
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family, key, overlay), "Target SLO original Native dependency"), type).payload();
    }
}
