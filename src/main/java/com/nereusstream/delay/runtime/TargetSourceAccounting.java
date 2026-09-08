package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Actual source-mutation accounting from typed before/after records, including fixed projection inventory growth. */
public final class TargetSourceAccounting implements TargetMessageStore.AccountingAssembler {
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final SourcePosition source;
    private final byte[] mutationDigest;
    private final int maximumCounters;
    private final int maximumTargets;
    private final int maximumDomains;

    public TargetSourceAccounting(
            final TargetQuotaScope scope,
            final byte[] lineage,
            final SourcePosition source,
            final byte[] mutationDigest,
            final int maximumCounters,
            final int maximumTargets,
            final int maximumDomains) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.source = TargetSourcePosition.requireBounded(source);
        Bytes.requireLength(lineage, 16, "lineage");
        Bytes.requireLength(mutationDigest, 32, "mutationDigest");
        this.lineage = Bytes.copy(lineage);
        this.mutationDigest = Bytes.copy(mutationDigest);
        if (scope.target() != null
                || !scope.shard().equals(source.shardId())
                || maximumCounters < 2
                || maximumTargets <= 0
                || maximumDomains <= 0
                || maximumDomains > 64) {
            throw new IllegalArgumentException("source accounting requires Shard scope and finite bounds");
        }
        this.maximumCounters = maximumCounters;
        this.maximumTargets = maximumTargets;
        this.maximumDomains = maximumDomains;
    }

    @Override
    public TargetStoreBackend.Mutation assemble(
            final TargetStoreBackend.Reader reader, final List<TargetStoreBackend.Edit> business) {
        if (business.size() > reader.maximumWriteRecords() || !scope.shard().equals(reader.shardId())) {
            throw new IllegalArgumentException("source accounting record/scope limit mismatch");
        }
        final var aggregate = reader.aggregate();
        final var stamp =
                new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()), source, mutationDigest);
        final var rootId = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null);
        final byte[] descriptorKey = rootId.key();
        descriptorKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var descriptor = TargetQuotaIncarnation.decodeForStore(
                descriptorKey,
                payload(
                        reader.projected(ColumnFamily.META, descriptorKey, business),
                        TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (!descriptor.identity().equals(rootId) || !Arrays.equals(descriptor.recoveryLineage(), lineage)) {
            throw new IllegalStateException("source accounting root descriptor mismatch");
        }
        descriptor.latestMutation().requireAtOrBefore(stamp);
        final byte[] rootKey = aggregate.key();
        rootKey[0] = TargetKeyCodec.QUOTA_BOOKKEEPING_TAG;
        final byte[] rawRoot = reader.get(ColumnFamily.META, rootKey);
        final var priorRoot = rawRoot == null
                ? null
                : TargetQuotaBookkeeping.decodeForStore(
                        rootKey, payload(rawRoot, TargetQuotaBookkeeping.VALUE_TYPE), rootId, scope.tenantScope());
        if (priorRoot == null) {
            if (reader.sourceSequence() != 0 || reader.source() != null) {
                throw new IllegalStateException("applied source lacks bookkeeping inventory");
            }
        } else {
            priorRoot.requireRoot(aggregate);
            if (!priorRoot.accounting().equals(descriptor.accounting())) {
                throw new IllegalStateException("root accounting artifact changed");
            }
        }
        final var seed = priorRoot == null
                ? new TargetQuotaBookkeeping(
                        rootId,
                        scope.tenantScope(),
                        descriptor.accounting(),
                        new TargetQuotaBookkeeping.Inventory(2, 0, 0),
                        1,
                        stamp)
                : priorRoot;
        final var beforeView =
                new TargetRecordAccounting(reader, List.of(), scope, lineage, stamp, seed, maximumDomains);
        final var afterView = new TargetRecordAccounting(reader, business, scope, lineage, stamp, seed, maximumDomains);
        final Map<TargetQuotaIdentity, TargetQuotaUsage> removed = new LinkedHashMap<>();
        final Map<TargetQuotaIdentity, TargetQuotaUsage> added = new LinkedHashMap<>();
        final var keys = new LinkedHashSet<String>();
        long addedGrants = 0;
        long removedGrants = 0;
        for (var edit : business) {
            if (!keys.add(edit.family() + ":" + Bytes.hex(edit.key()))
                    || (edit.family() == ColumnFamily.META && edit.key()[0] == TargetKeyCodec.QUOTA_BOOKKEEPING_TAG)) {
                throw new IllegalArgumentException("duplicate business key or externally supplied bookkeeping root");
            }
            if (!Arrays.equals(edit.before(), reader.get(edit.family(), edit.key()))) {
                throw new IllegalStateException("source accounting before differs from Store");
            }
            final var beforeCharge =
                    edit.before() == null ? null : beforeView.charge(edit.family(), edit.key(), edit.before());
            final var afterCharge =
                    edit.after() == null ? null : afterView.charge(edit.family(), edit.key(), edit.after());
            if (beforeCharge != null
                    && afterCharge != null
                    && (!beforeCharge
                                    .owner()
                                    .identity()
                                    .equals(afterCharge.owner().identity())
                            || !beforeCharge
                                    .owner()
                                    .accounting()
                                    .equals(afterCharge.owner().accounting()))) {
                throw new IllegalStateException("same business key attempted to change its frozen accounting owner");
            }
            if (edit.family() == ColumnFamily.DEDUPE
                    && edit.before() != null
                    && edit.after() != null
                    && !Arrays.equals(edit.before(), edit.after())) {
                throw new IllegalStateException("immutable logical result or physical audit cannot be rewritten");
            }
            accumulate(removed, beforeCharge);
            accumulate(added, afterCharge);
            if (edit.family() == ColumnFamily.META && edit.key()[0] == TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG) {
                if (edit.before() == null) {
                    addedGrants = Math.incrementExact(addedGrants);
                }
                if (edit.after() == null) {
                    removedGrants = Math.incrementExact(removedGrants);
                }
            }
        }
        final var identities = new LinkedHashSet<>(removed.keySet());
        identities.addAll(added.keySet());
        // Both root counters exist even when the bootstrap has no other root-owned business records.
        identities.add(rootId);
        identities.add(seed.tenantOwner());
        if (identities.size() > maximumCounters) {
            throw new IllegalArgumentException("source accounting touches too many counters");
        }
        final var priors = new LinkedHashMap<TargetQuotaIdentity, TargetQuotaCounter>();
        final var targets = new LinkedHashSet<TargetQuotaScope>();
        long newCounters = 0;
        for (var identity : identities) {
            final var prior = reader.counter(identity);
            priors.put(identity, prior);
            if (prior == null) {
                newCounters = Math.incrementExact(newCounters);
            }
            if (identity.kind() == TargetQuotaIdentity.Kind.TARGET) {
                targets.add(scope.forTarget(identity.target()));
            }
        }
        if (priorRoot != null) {
            for (var identity : List.of(rootId, seed.tenantOwner())) {
                final var prior = priors.get(identity);
                if (prior == null || !prior.usage().resources().covers(priorRoot.charge())) {
                    throw new IllegalStateException("bookkeeping root counter is missing or undercharged");
                }
            }
        } else if (priors.values().stream().anyMatch(Objects::nonNull)) {
            throw new IllegalStateException("bookkeeping bootstrap encountered an existing counter");
        }
        if (targets.size() > maximumTargets) {
            throw new IllegalArgumentException("source accounting touches too many Targets");
        }
        long newTotals = 0;
        for (var target : targets) {
            if (reader.total(target) == null) {
                if (priors.entrySet().stream()
                        .anyMatch(entry -> entry.getValue() != null
                                && entry.getKey().kind() == TargetQuotaIdentity.Kind.TARGET
                                && target.target().equals(entry.getKey().target()))) {
                    throw new IllegalStateException("existing Target counter lacks its retained total");
                }
                newTotals = Math.incrementExact(newTotals);
            }
        }
        final var inventory = priorRoot == null
                ? new TargetQuotaBookkeeping.Inventory(newCounters, newTotals, addedGrants)
                : priorRoot
                        .inventory()
                        .change(TargetQuotaBookkeeping.Projection.COUNTER, 0, newCounters)
                        .change(TargetQuotaBookkeeping.Projection.TOTAL, 0, newTotals)
                        .change(TargetQuotaBookkeeping.Projection.GRANT_ACTIVATION, removedGrants, addedGrants);
        if (priorRoot == null && (removedGrants != 0 || business.stream().anyMatch(edit -> edit.before() != null))) {
            throw new IllegalStateException("bookkeeping bootstrap cannot replace existing business records");
        }
        final var nextRoot = priorRoot == null
                ? new TargetQuotaBookkeeping(rootId, scope.tenantScope(), descriptor.accounting(), inventory, 1, stamp)
                : inventory.equals(priorRoot.inventory()) ? priorRoot : priorRoot.advance(inventory, stamp);
        final var complete = new ArrayList<>(business);
        if (priorRoot != nextRoot) {
            if (priorRoot != null) {
                merge(removed, rootId, TargetRecordAccounting.resources(priorRoot.charge()));
                merge(removed, seed.tenantOwner(), TargetRecordAccounting.resources(priorRoot.charge()));
            }
            merge(added, rootId, TargetRecordAccounting.resources(nextRoot.charge()));
            merge(added, seed.tenantOwner(), TargetRecordAccounting.resources(nextRoot.charge()));
            complete.add(new TargetStoreBackend.Edit(
                    ColumnFamily.META,
                    rootKey,
                    rawRoot,
                    TargetValueEnvelope.encode(TargetQuotaBookkeeping.VALUE_TYPE, nextRoot.canonicalBytes())));
        }
        final var updates = new ArrayList<TargetQuotaDelta.Update>();
        for (var identity : identities) {
            final var prior = priors.get(identity);
            final var usage = (prior == null ? TargetQuotaUsage.empty() : prior.usage())
                    .subtract(removed.getOrDefault(identity, TargetQuotaUsage.empty()))
                    .add(added.getOrDefault(identity, TargetQuotaUsage.empty()));
            updates.add(new TargetQuotaDelta.Update(identity, usage));
        }
        final var delta = TargetQuotaDelta.prepare(
                aggregate,
                reader.sourceSequence(),
                reader.source(),
                source,
                mutationDigest,
                updates,
                maximumCounters,
                reader::counter);
        final var totals = TargetQuotaTotalsDelta.prepare(delta, scope, maximumTargets, reader::total);
        nextRoot.requireRoot(delta.nextAggregate());
        return new TargetStoreBackend.Mutation(totals, complete);
    }

    private void accumulate(
            final Map<TargetQuotaIdentity, TargetQuotaUsage> target, final TargetRecordAccounting.Charge charge) {
        if (charge == null) {
            return;
        }
        merge(target, charge.owner().identity(), charge.primary());
        merge(target, charge.owner().tenantIdentity(), charge.mirror());
        if (target.size() > maximumCounters) {
            throw new IllegalArgumentException("record accounting exceeds counter limit");
        }
    }

    private static void merge(
            final Map<TargetQuotaIdentity, TargetQuotaUsage> target,
            final TargetQuotaIdentity key,
            final TargetQuotaUsage usage) {
        target.merge(key, usage, TargetQuotaUsage::add);
    }

    private static byte[] payload(final byte[] raw, final int type) {
        if (raw == null) {
            throw new IllegalStateException("missing source accounting root dependency");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }
}
