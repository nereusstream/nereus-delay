package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Applies logical grant policy to actual activations and all-incarnation totals in the bounded mutation view. */
public final class TargetQuotaStoreGate {
    /** A deterministic capacity rejection, distinct from corruption, missing authority or an incomplete read. */
    public static final class Rejected extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final byte[] scope;
        private final TargetQuotaGrantGate.Decision decision;

        private Rejected(final TargetQuotaScope scope, final TargetQuotaGrantGate.Decision decision) {
            super("Target logical grant rejected growth: " + decision);
            this.scope = scope.canonicalBytes();
            this.decision = decision;
        }

        public TargetQuotaScope scope() {
            return TargetQuotaScope.decode(scope);
        }

        public TargetQuotaGrantGate.Decision decision() {
            return decision;
        }
    }

    public record Evaluation(TargetQuotaScope scope, TargetQuotaGrantGate.Decision decision) {}

    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumTargets;

    public TargetQuotaStoreGate(final TargetQuotaScope scope, final byte[] lineage, final int maximumTargets) {
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null || maximumTargets <= 0 || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("logical Store gate requires a bounded Shard/lineage scope");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumTargets = maximumTargets;
    }

    /**
     * Run after the real record accounting assembler and before prepare captures its ReadView. Operation and
     * ingress measurement must come from the authenticated business decision; this is not a source classifier.
     * Bootstrap/grant control uses its distinct authenticated activation path, never an ordinary work label.
     */
    public List<Evaluation> check(
            final TargetStoreBackend.Reader reader,
            final TargetStoreBackend.Mutation mutation,
            final TargetQuotaGrantGate.Operation operation,
            final TargetQuotaAccounting ingressAccounting) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(operation, "operation");
        if (isIngress(operation) && ingressAccounting == null) {
            throw new IllegalArgumentException("new ingress requires its actual pinned measurement artifact");
        }
        final var quota = mutation.quota();
        if (!scope.equals(quota.shardScope())
                || !scope.shard().equals(reader.shardId())
                || mutation.business().size() > reader.maximumWriteRecords()
                || quota.changes().isEmpty()
                || quota.changes().size() > maximumTargets) {
            throw new IllegalArgumentException("logical Store gate requires its bounded affected Target set");
        }
        final boolean local = quota.counters().mutation().isLocalClaim();
        final boolean claim = operation == TargetQuotaGrantGate.Operation.CLAIM
                || operation == TargetQuotaGrantGate.Operation.CLAIM_REVOKE;
        if (local != claim) {
            throw new IllegalStateException("logical grant operation disagrees with source/local mutation kind");
        }
        for (var edit : mutation.business()) {
            if (edit.family() == ColumnFamily.META && edit.key()[0] == TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG) {
                throw new IllegalStateException("ordinary business cannot replace the grants that authorize it");
            }
        }
        quota.requireCurrent(
                reader.aggregate(), reader.sourceSequence(), reader.source(), reader::counter, reader::total);
        final var rootIdentity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                quota.counters().priorAggregate().accountingIncarnation(),
                null,
                null);
        final byte[] rootKey = rootIdentity.key();
        rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] rootRaw = reader.get(ColumnFamily.META, rootKey);
        if (rootRaw == null) {
            throw new IllegalStateException("logical gate lacks its actual root descriptor");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(
                rootKey,
                TargetValueEnvelope.decode(rootRaw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("logical gate root belongs to another lineage");
        }
        final var frontier = quota.counters().priorAggregate().mutation();
        if (frontier == null) {
            throw new IllegalStateException("logical gate has no applied mutation frontier");
        }
        root.latestMutation().requireAtOrBefore(frontier);
        final var shardGrant = activation(reader, scope);
        final var evaluations = new ArrayList<Evaluation>();
        for (var change : quota.changes()) {
            final var targetScope = change.next().scope();
            final var targetGrant = activation(reader, targetScope);
            if (change.prior() != null && targetGrant.allocation() == null) {
                throw new IllegalStateException("existing Target total lacks its first grant allocation");
            }
            final var decision = TargetQuotaGrantGate.evaluate(
                    operation,
                    shardGrant.grant(),
                    targetGrant.grant(),
                    ingressAccounting == null ? shardGrant.grant().accounting() : ingressAccounting,
                    quota.counters().priorAggregate(),
                    quota.counters().nextAggregate(),
                    change.prior(),
                    change.next());
            if (decision == TargetQuotaGrantGate.Decision.SHARD_LIMIT) {
                throw new Rejected(scope, decision);
            }
            if (decision == TargetQuotaGrantGate.Decision.TARGET_LIMIT) {
                throw new Rejected(targetScope, decision);
            }
            evaluations.add(new Evaluation(targetScope, decision));
        }
        return List.copyOf(evaluations);
    }

    public TargetMessageStore.AccountingAssembler wrap(
            final TargetMessageStore.AccountingAssembler accounting,
            final TargetQuotaGrantGate.Operation operation,
            final TargetQuotaAccounting ingressAccounting) {
        Objects.requireNonNull(accounting, "accounting");
        Objects.requireNonNull(operation, "operation");
        if (isIngress(operation) && ingressAccounting == null) {
            throw new IllegalArgumentException("new ingress requires its actual pinned measurement artifact");
        }
        return (reader, business) -> {
            final var mutation = accounting.assemble(reader, business);
            check(reader, mutation, operation, ingressAccounting);
            return mutation;
        };
    }

    private static boolean isIngress(final TargetQuotaGrantGate.Operation operation) {
        return operation == TargetQuotaGrantGate.Operation.FIRST_SCHEDULE
                || operation == TargetQuotaGrantGate.Operation.PREPARE
                || operation == TargetQuotaGrantGate.Operation.DLQ_REPLAY;
    }

    private TargetQuotaGrantActivation activation(
            final TargetStoreBackend.Reader reader, final TargetQuotaScope expected) {
        final byte[] key = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                expected.keySuffix());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("logical grant activation is absent");
        }
        final var activation = TargetQuotaGrantActivation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaGrantActivation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!activation.grant().scope().equals(expected)) {
            throw new IllegalStateException("actual grant activation has another scope");
        }
        final var current = reader.aggregate().mutation();
        if (current == null) {
            throw new IllegalStateException("logical grant has no applied mutation frontier");
        }
        activation.mutation().requireAtOrBefore(current);
        final var origin = activation.allocation();
        if (origin != null) {
            final byte[] descriptorRaw = reader.get(ColumnFamily.META, origin.key());
            if (descriptorRaw == null) {
                throw new IllegalStateException("grant first allocation descriptor is absent");
            }
            final var actual = TargetQuotaIncarnation.decodeForStore(
                    origin.key(),
                    TargetValueEnvelope.decode(descriptorRaw, TargetQuotaIncarnation.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    scope.tenantScope());
            if (!Arrays.equals(actual.recoveryLineage(), lineage)
                    || !actual.identity().equals(origin.identity())
                    || !actual.allocation().equals(origin.allocation())
                    || !actual.accounting().equals(origin.accounting())
                    || !Arrays.equals(actual.tenantScope(), origin.tenantScope())
                    || !Arrays.equals(origin.recoveryLineage(), lineage)) {
                throw new IllegalStateException("grant first allocation differs from its actual frozen descriptor");
            }
            actual.latestMutation().requireAtOrBefore(current);
        }
        return activation;
    }
}
