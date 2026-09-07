package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Adds bounded all-incarnation Target totals to the same immutable leaf/aggregate batch plan. */
public final class TargetQuotaTotalsDelta {
    public record Change(TargetQuotaTotal prior, TargetQuotaTotal next) {
        public Change {
            Objects.requireNonNull(next, "next");
            if (prior != null && !prior.scope().equals(next.scope())) {
                throw new IllegalArgumentException("Target total replacement changes scope");
            }
        }
    }

    private final TargetQuotaDelta counters;
    private final TargetQuotaScope shardScope;
    private final List<Change> changes;

    private TargetQuotaTotalsDelta(
            final TargetQuotaDelta counters, final TargetQuotaScope shardScope, final List<Change> changes) {
        this.counters = counters;
        this.shardScope = shardScope;
        this.changes = List.copyOf(changes);
    }

    /** Route scope must already be authenticated; a lookup cannot enumerate unrelated Targets. */
    public static TargetQuotaTotalsDelta prepare(
            final TargetQuotaDelta counters,
            final TargetQuotaScope shardScope,
            final int maximumTouchedTargets,
            final Function<TargetQuotaScope, TargetQuotaTotal> lookup) {
        Objects.requireNonNull(counters, "counters");
        Objects.requireNonNull(shardScope, "shardScope");
        Objects.requireNonNull(lookup, "lookup");
        if (shardScope.target() != null
                || !shardScope.shard().equals(counters.priorAggregate().shard())
                || maximumTouchedTargets <= 0) {
            throw new IllegalArgumentException("Target total planner needs an exact shard scope and finite budget");
        }
        final var grouped = new LinkedHashMap<TargetQuotaScope, List<TargetQuotaDelta.Change>>();
        for (var change : counters.changes()) {
            final var identity = change.next().identity();
            requireTenant(shardScope, identity);
            if (identity.kind() == TargetQuotaIdentity.Kind.TARGET) {
                final var scope = shardScope.forTarget(identity.target());
                var members = grouped.get(scope);
                if (members == null) {
                    if (grouped.size() == maximumTouchedTargets) {
                        throw new IllegalArgumentException("quota mutation exceeds its touched-Target budget");
                    }
                    members = new ArrayList<>();
                    grouped.put(scope, members);
                }
                members.add(change);
            }
        }
        final var changes = new ArrayList<Change>();
        for (var entry : grouped.entrySet()) {
            final var scope = entry.getKey();
            final var prior = lookup.apply(scope);
            if (prior != null) {
                if (!scope.equals(prior.scope())) {
                    throw new IllegalStateException("Target total lookup returned another scope");
                }
                prior.requireAggregate(counters.priorAggregate());
            }
            TargetQuotaUsage sum = prior == null ? TargetQuotaUsage.empty() : prior.usage();
            for (var change : entry.getValue()) {
                if (change.prior() != null) {
                    if (prior == null) {
                        throw new IllegalStateException("existing Target counter has no total");
                    }
                    prior.requireCounter(change.prior());
                    sum = sum.subtract(change.prior().usage());
                }
            }
            for (var change : entry.getValue()) {
                sum = sum.add(change.next().usage());
            }
            final var next = prior == null
                    ? new TargetQuotaTotal(scope, sum, 1, counters.mutation())
                    : prior.advance(sum, counters.mutation());
            next.requireAggregate(counters.nextAggregate());
            for (var change : entry.getValue()) {
                next.requireCounter(change.next());
            }
            changes.add(new Change(prior, next));
        }
        return new TargetQuotaTotalsDelta(counters, shardScope, changes);
    }

    private static void requireTenant(final TargetQuotaScope shardScope, final TargetQuotaIdentity identity) {
        if (!shardScope.shard().equals(identity.shard())
                || (identity.kind().isMirror() && !Arrays.equals(shardScope.tenantScope(), identity.tenantScope()))) {
            throw new IllegalStateException("quota counter disagrees with the Route tenant/shard binding");
        }
    }

    public TargetQuotaDelta counters() {
        return counters;
    }

    public TargetQuotaScope shardScope() {
        return shardScope;
    }

    public List<Change> changes() {
        return changes;
    }

    /** Run within the same Store guard as the business records, before any write or in-memory publication. */
    public void requireCurrent(
            final TargetQuotaAggregate aggregate,
            final long sequence,
            final SourcePosition source,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> counterLookup,
            final Function<TargetQuotaScope, TargetQuotaTotal> totalLookup) {
        counters.requireCurrent(aggregate, sequence, source, counterLookup);
        for (Change change : changes) {
            final var actual = totalLookup.apply(change.next().scope());
            if (change.prior() == null
                    ? actual != null
                    : actual == null || !Arrays.equals(change.prior().canonicalBytes(), actual.canonicalBytes())) {
                throw new IllegalStateException("Target quota total changed before batch commit");
            }
        }
    }

    /** Recovery-only projection audit; callers must separately rebuild and audit leaf charges from real ledgers. */
    public static void audit(
            final TargetQuotaScope shardScope,
            final TargetQuotaAggregate aggregate,
            final Iterable<TargetQuotaCounter> counters,
            final Iterable<TargetQuotaTotal> totals) {
        if (shardScope.target() != null || !shardScope.shard().equals(aggregate.shard())) {
            throw new IllegalArgumentException("Target total audit requires its exact shard scope");
        }
        final var actual = new HashMap<TargetQuotaScope, TargetQuotaTotal>();
        for (var total : totals) {
            total.scope().requireRoute(shardScope.shard(), shardScope.tenantScope());
            total.requireAggregate(aggregate);
            if (actual.put(total.scope(), total) != null) {
                throw new IllegalStateException("duplicate Target quota total");
            }
        }
        final var seen = new HashMap<TargetQuotaIdentity, TargetQuotaCounter>();
        final Map<TargetQuotaScope, TargetQuotaUsage> sums = new HashMap<>();
        for (var counter : counters) {
            aggregate.requireCounter(counter);
            requireTenant(shardScope, counter.identity());
            if (seen.put(counter.identity(), counter) != null) {
                throw new IllegalStateException("duplicate Target quota counter");
            }
            if (counter.identity().kind() == TargetQuotaIdentity.Kind.TARGET) {
                final var scope = shardScope.forTarget(counter.identity().target());
                final var total = actual.get(scope);
                if (total == null) {
                    throw new IllegalStateException("Target quota total missing during recovery");
                }
                total.requireCounter(counter);
                sums.merge(scope, counter.usage(), TargetQuotaUsage::add);
            }
        }
        if (sums.size() != actual.size()) {
            throw new IllegalStateException("Target total identity set differs from primary counters");
        }
        for (var entry : sums.entrySet()) {
            if (!entry.getValue().equals(actual.get(entry.getKey()).usage())) {
                throw new IllegalStateException("Target total differs from all-incarnation primary usage");
            }
        }
    }
}
