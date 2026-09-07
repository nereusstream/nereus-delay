package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Pure, bounded accounting plan over caller-validated ledger changes.
 *
 * <p>The source committer performs dedupe first, validates complete ledger ownership and retirement protections,
 * then commits this plan with business records and the Source Position in one batch. In-memory publication is
 * permitted only after that batch has definitely committed. An uncertain write requires Store recovery.</p>
 */
public final class TargetQuotaDelta {
    public record Update(TargetQuotaIdentity identity, TargetQuotaUsage nextUsage) {
        public Update {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(nextUsage, "nextUsage");
        }
    }

    public record Change(TargetQuotaCounter prior, TargetQuotaCounter next) {
        public Change {
            Objects.requireNonNull(next, "next");
            if (prior != null && !prior.identity().equals(next.identity())) {
                throw new IllegalArgumentException("quota counter replacement changes identity");
            }
        }
    }

    private final TargetQuotaAggregate priorAggregate;
    private final TargetQuotaAggregate nextAggregate;
    private final long expectedMutationSequence;
    private final SourcePosition expectedSource;
    private final TargetQuotaMutation mutation;
    private final List<Change> changes;

    private TargetQuotaDelta(
            final TargetQuotaAggregate priorAggregate,
            final TargetQuotaAggregate nextAggregate,
            final long expectedMutationSequence,
            final SourcePosition expectedSource,
            final TargetQuotaMutation mutation,
            final List<Change> changes) {
        this.priorAggregate = priorAggregate;
        this.nextAggregate = nextAggregate;
        this.expectedMutationSequence = expectedMutationSequence;
        this.expectedSource = expectedSource;
        this.mutation = mutation;
        this.changes = List.copyOf(changes);
    }

    /** The lookup is by complete identity; no enumeration of the live counter collection is available here. */
    public static TargetQuotaDelta prepare(
            final TargetQuotaAggregate aggregate,
            final long lastStoreSequence,
            final SourcePosition lastStoreSource,
            final SourcePosition source,
            final byte[] mutationDigest,
            final List<Update> updates,
            final int maximumTouchedCounters,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> lookup) {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(updates, "updates");
        Objects.requireNonNull(lookup, "lookup");
        if (maximumTouchedCounters <= 0 || updates.size() > maximumTouchedCounters) {
            throw new IllegalArgumentException("quota mutation exceeds its activated touched-counter budget");
        }
        requireStoreStamp(aggregate, lastStoreSequence, lastStoreSource);
        final var mutation =
                new TargetQuotaMutation(TargetQuotaMutation.increment(lastStoreSequence), source, mutationDigest);
        if (!aggregate.shard().equals(source.shardId())
                || (lastStoreSource != null && source.compareTo(lastStoreSource) <= 0)) {
            throw new IllegalStateException("quota source must advance the Store Source Position");
        }
        final var identities = new HashSet<TargetQuotaIdentity>();
        final var changes = new ArrayList<Change>();
        for (Update update : updates) {
            if (identities.size() == maximumTouchedCounters
                    || !identities.add(update.identity())
                    || !update.identity().shard().equals(aggregate.shard())) {
                throw new IllegalArgumentException("duplicate, foreign or excess quota update");
            }
            final var prior = lookup.apply(update.identity());
            if (prior != null) {
                if (!prior.identity().equals(update.identity())) {
                    throw new IllegalStateException("quota lookup returned another identity");
                }
                aggregate.requireCounter(prior);
                if (!prior.usage().equals(update.nextUsage())) {
                    changes.add(new Change(prior, prior.advance(update.nextUsage(), mutation)));
                }
            } else {
                if (update.nextUsage().isZero()) {
                    throw new IllegalArgumentException("cannot allocate an empty retired quota counter");
                }
                changes.add(
                        new Change(null, new TargetQuotaCounter(update.identity(), update.nextUsage(), 1, mutation)));
            }
        }
        // Remove all touched prior primary contributions before adding replacements. This is the checked
        // componentwise delta, without transient overflow caused by iteration order during an ownership transfer.
        TargetQuotaUsage sum = aggregate.usage();
        for (Change change : changes) {
            if (change.prior() != null && !change.next().identity().kind().isMirror()) {
                sum = sum.subtract(change.prior().usage());
            }
        }
        for (Change change : changes) {
            if (!change.next().identity().kind().isMirror()) {
                sum = sum.add(change.next().usage());
            }
        }
        return new TargetQuotaDelta(
                aggregate,
                changes.isEmpty() ? aggregate : aggregate.advance(sum, mutation),
                lastStoreSequence,
                lastStoreSource,
                mutation,
                changes);
    }

    private static void requireStoreStamp(
            final TargetQuotaAggregate aggregate, final long sequence, final SourcePosition source) {
        if ((sequence == 0) != (source == null)
                || (source != null && !aggregate.shard().equals(source.shardId()))) {
            throw new IllegalStateException("Store mutation sequence/source mismatch");
        }
        final var stamp = aggregate.mutation();
        if (stamp != null) {
            if (source == null || Long.compareUnsigned(stamp.sequence(), sequence) > 0) {
                throw new IllegalStateException("aggregate is ahead of Store mutation sequence");
            }
            final int order = stamp.source().compareTo(source);
            if (order > 0
                    || (order == 0) != (stamp.sequence() == sequence)
                    || (order == 0 && !Arrays.equals(stamp.source().canonicalBytes(), source.canonicalBytes()))) {
                throw new IllegalStateException("aggregate disagrees with Store Source Position");
            }
        }
    }

    public TargetQuotaAggregate priorAggregate() {
        return priorAggregate;
    }

    public TargetQuotaAggregate nextAggregate() {
        return nextAggregate;
    }

    public long expectedMutationSequence() {
        return expectedMutationSequence;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public List<Change> changes() {
        return changes;
    }

    /** Run inside the Store mutation guard; this check alone is not a lock or a committed-write receipt. */
    public void requireCurrent(
            final TargetQuotaAggregate current,
            final long sequence,
            final SourcePosition source,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> lookup) {
        if (sequence != expectedMutationSequence
                || !sameSource(expectedSource, source)
                || !Arrays.equals(priorAggregate.canonicalBytes(), current.canonicalBytes())) {
            throw new IllegalStateException("quota plan was prepared against another Store view");
        }
        for (Change change : changes) {
            final var actual = lookup.apply(change.next().identity());
            if (change.prior() == null
                    ? actual != null
                    : actual == null || !Arrays.equals(change.prior().canonicalBytes(), actual.canonicalBytes())) {
                throw new IllegalStateException("quota counter changed before batch commit");
            }
        }
    }

    private static boolean sameSource(final SourcePosition left, final SourcePosition right) {
        return left == null
                ? right == null
                : right != null && Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }

    /**
     * Full recovery/audit only. Rebuilt usage must come from the independent durable ledgers and identity registry,
     * including zero protected tombstones. Missing and extra identities are errors, not repair instructions.
     */
    public static void audit(
            final TargetQuotaAggregate aggregate,
            final Iterable<TargetQuotaCounter> counters,
            final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuiltLedgerUsage) {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(rebuiltLedgerUsage, "rebuiltLedgerUsage");
        final var seen = new HashMap<TargetQuotaIdentity, TargetQuotaCounter>();
        TargetQuotaUsage sum = TargetQuotaUsage.empty();
        for (TargetQuotaCounter counter : counters) {
            aggregate.requireCounter(counter);
            if (seen.put(counter.identity(), counter) != null
                    || !counter.usage().equals(rebuiltLedgerUsage.get(counter.identity()))) {
                throw new IllegalStateException("quota counter differs from independently rebuilt ledger usage");
            }
            if (!counter.identity().kind().isMirror()) {
                sum = sum.add(counter.usage());
            }
        }
        if (seen.size() != rebuiltLedgerUsage.size() || !sum.equals(aggregate.usage())) {
            throw new IllegalStateException("quota aggregate/ledger identity set mismatch");
        }
        final var mirrorSums = new HashMap<TargetQuotaIdentity, CapacityVector>();
        for (TargetQuotaCounter counter : seen.values()) {
            if (counter.identity().kind().isMirror()) {
                final var primary = seen.get(counter.identity().primary());
                final var mirrors = mirrorSums.merge(
                        counter.identity().primary(), counter.usage().resources(), CapacityVector::add);
                if (primary == null || !primary.usage().resources().covers(mirrors)) {
                    throw new IllegalStateException("tenant quota mirror has no covering primary counter");
                }
            }
        }
    }
}
