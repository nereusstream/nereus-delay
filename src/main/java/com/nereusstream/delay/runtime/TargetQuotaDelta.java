package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CapacityDimension;
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
    public static final int MAX_LOCAL_CLAIM_COUNTERS = 4;

    public enum LocalClaimKind {
        CLAIM,
        REVOKE
    }

    /**
     * Verifies the exact durable Claim create/revoke and original charge, Message/index before/after records,
     * full operation digest, route tenant, current Owner epoch/Store incarnation and source frontier. The
     * authority and read set must remain valid through the same atomic local commit. No source append or
     * Admission/attempt/reserve mutation can be relabeled as a local Claim. There is no production default.
     */
    @FunctionalInterface
    public interface LocalClaimAuthority {
        void requireAuthorized(LocalClaimKind kind, TargetQuotaDelta delta);
    }

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
        return prepareInternal(
                aggregate,
                lastStoreSequence,
                lastStoreSource,
                source,
                mutationDigest,
                updates,
                maximumTouchedCounters,
                lookup,
                false,
                false,
                false,
                false);
    }

    /** Reversible local Claim accounting advances its ordinal without renumbering source replay. */
    public static TargetQuotaDelta prepareLocalClaim(
            final TargetQuotaAggregate aggregate,
            final long lastStoreSequence,
            final SourcePosition sourceFrontier,
            final byte[] operationDigest,
            final LocalClaimKind kind,
            final List<Update> updates,
            final int maximumTouchedCounters,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> lookup,
            final LocalClaimAuthority authority) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(authority, "localClaimAuthority");
        if (sourceFrontier == null || lastStoreSequence == 0) {
            throw new IllegalStateException("local Claim requires an existing applied source frontier");
        }
        final var delta = prepareInternal(
                aggregate,
                lastStoreSequence,
                sourceFrontier,
                sourceFrontier,
                operationDigest,
                updates,
                Math.min(maximumTouchedCounters, MAX_LOCAL_CLAIM_COUNTERS),
                lookup,
                true,
                false,
                false,
                false);
        requireLocalClaimChanges(kind, delta.changes);
        authority.requireAuthorized(kind, delta);
        return delta;
    }

    /** Full physical/fence/Owner authority remains mandatory for source-preserving reservation materialization. */
    @FunctionalInterface
    public interface ReservationExpiryAuthority {
        void requireAuthorized(TargetQuotaDelta delta);
    }

    public static TargetQuotaDelta prepareReservationExpiry(
            final TargetQuotaAggregate aggregate,
            final long sequence,
            final SourcePosition source,
            final byte[] digest,
            final List<Update> updates,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> lookup,
            final ReservationExpiryAuthority authority) {
        Objects.requireNonNull(authority, "expiryAuthority");
        if (source == null || sequence == 0) {
            throw new IllegalStateException("reservation expiry requires an applied source frontier");
        }
        final var delta = prepareInternal(
                aggregate, sequence, source, source, digest, updates, 2, lookup, true, true, false, false);
        requireReservationExpiryChanges(delta.changes);
        authority.requireAuthorized(delta);
        return delta;
    }

    /** Exact source-preserving Close materialization; no grant/ingress/Claim authority is implied. */
    @FunctionalInterface
    public interface ReservationClosureAuthority {
        void requireAuthorized(TargetQuotaDelta delta);
    }

    public static TargetQuotaDelta prepareReservationClosure(
            TargetQuotaAggregate aggregate,
            long sequence,
            SourcePosition source,
            byte[] digest,
            List<Update> updates,
            Function<TargetQuotaIdentity, TargetQuotaCounter> lookup,
            ReservationClosureAuthority authority) {
        Objects.requireNonNull(authority, "closureAuthority");
        if (source == null || sequence == 0) {
            throw new IllegalStateException("reservation closure requires an applied source frontier");
        }
        final var delta = prepareInternal(
                aggregate, sequence, source, source, digest, updates, 2, lookup, true, false, true, false);
        requireReservationExpiryChanges(delta.changes);
        authority.requireAuthorized(delta);
        return delta;
    }

    /** Source-preserving cursor completion with no payload transition. */
    @FunctionalInterface
    public interface CloseCursorAuthority {
        void requireAuthorized(TargetQuotaDelta delta);
    }

    public static TargetQuotaDelta prepareCloseCursor(
            TargetQuotaAggregate aggregate,
            long sequence,
            SourcePosition source,
            byte[] digest,
            List<Update> updates,
            Function<TargetQuotaIdentity, TargetQuotaCounter> lookup,
            CloseCursorAuthority authority) {
        Objects.requireNonNull(authority, "closeCursorAuthority");
        if (source == null || sequence == 0) {
            throw new IllegalStateException("Close cursor requires an established source frontier");
        }
        final var delta = prepareInternal(
                aggregate, sequence, source, source, digest, updates, 2, lookup, true, false, false, true);
        requireCloseCursorChanges(delta.changes);
        authority.requireAuthorized(delta);
        return delta;
    }

    private static void requireCloseCursorChanges(List<Change> changes) {
        if (changes.size() != 2) {
            throw new IllegalStateException("Close cursor requires exact Target and tenant mirror counters");
        }
        final var primary = changes.stream()
                .filter(c -> !c.next().identity().kind().isMirror())
                .findFirst()
                .orElseThrow();
        final var mirror = changes.stream()
                .filter(c -> c.next().identity().kind().isMirror())
                .findFirst()
                .orElseThrow();
        if (!primary.next().identity().target().equals(mirror.next().identity().target())
                || !Arrays.equals(
                        primary.next().identity().accountingIncarnation(),
                        mirror.next().identity().accountingIncarnation())) {
            throw new IllegalStateException("Close cursor owner/mirror differ");
        }
        for (var change : changes) {
            if (change.prior().usage().targets() != change.next().usage().targets()
                    || change.prior().usage().executionDomains()
                            != change.next().usage().executionDomains()
                    || change.prior().usage().strictOrderDomains()
                            != change.next().usage().strictOrderDomains()
                    || change.prior().usage().accountingIncarnations()
                            != change.next().usage().accountingIncarnations()) {
                throw new IllegalStateException("Close cursor changed accounting cardinality");
            }
            for (var dimension : CapacityDimension.values()) {
                if (difference(change, dimension) != difference(primary, dimension)
                        || (dimension != CapacityDimension.LOGICAL_STATE_BYTES && difference(change, dimension) != 0)) {
                    throw new IllegalStateException("Close cursor changed payload or unmatched mirror dimension");
                }
            }
        }
    }

    private static void requireReservationExpiryChanges(final List<Change> changes) {
        if (changes.size() != 2) {
            throw new IllegalArgumentException("expiry requires exactly one original owner and tenant mirror");
        }
        final var primary = changes.stream()
                .filter(c -> !c.next().identity().kind().isMirror())
                .findFirst()
                .orElseThrow();
        final var mirror = changes.stream()
                .filter(c -> c.next().identity().kind().isMirror())
                .findFirst()
                .orElseThrow();
        if (!primary.next().identity().target().equals(mirror.next().identity().target())
                || !Arrays.equals(
                        primary.next().identity().accountingIncarnation(),
                        mirror.next().identity().accountingIncarnation())) {
            throw new IllegalArgumentException("expiry owner/mirror identities differ");
        }
        for (var change : changes) {
            final var before = change.prior().usage();
            final var after = change.next().usage();
            if (before.targets() != after.targets()
                    || before.executionDomains() != after.executionDomains()
                    || before.strictOrderDomains() != after.strictOrderDomains()
                    || before.accountingIncarnations() != after.accountingIncarnations()) {
                throw new IllegalArgumentException("expiry cannot change accounting cardinality");
            }
            for (var dimension : CapacityDimension.values()) {
                if (difference(change, dimension) != difference(primary, dimension)
                        || (dimension != CapacityDimension.LOGICAL_STATE_BYTES
                                && dimension != CapacityDimension.RESERVATION_MESSAGES
                                && dimension != CapacityDimension.RESERVATION_PAYLOAD_BYTES
                                && dimension != CapacityDimension.RETAINED_BYTES
                                && difference(change, dimension) != 0)) {
                    throw new IllegalArgumentException("expiry changed an unrelated dimension or mirror delta");
                }
            }
        }
        if (difference(primary, CapacityDimension.RESERVATION_MESSAGES) != -1
                || difference(primary, CapacityDimension.RESERVATION_PAYLOAD_BYTES) > 0
                || difference(primary, CapacityDimension.RETAINED_BYTES)
                        != -difference(primary, CapacityDimension.RESERVATION_PAYLOAD_BYTES)) {
            throw new IllegalArgumentException("expiry must transfer exactly one reservation into retained bytes");
        }
    }

    private static TargetQuotaDelta prepareInternal(
            final TargetQuotaAggregate aggregate,
            final long lastStoreSequence,
            final SourcePosition lastStoreSource,
            final SourcePosition source,
            final byte[] mutationDigest,
            final List<Update> updates,
            final int maximumTouchedCounters,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> lookup,
            final boolean localClaim,
            final boolean reservationExpiry,
            final boolean reservationClosure,
            final boolean reservationCloseCursor) {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(updates, "updates");
        Objects.requireNonNull(lookup, "lookup");
        if (maximumTouchedCounters <= 0 || updates.size() > maximumTouchedCounters) {
            throw new IllegalArgumentException("quota mutation exceeds its activated touched-counter budget");
        }
        requireStoreStamp(aggregate, lastStoreSequence, lastStoreSource);
        final long ordinal = localClaim
                ? aggregate.mutation() != null && aggregate.mutation().sequence() == lastStoreSequence
                        ? TargetQuotaMutation.increment(aggregate.mutation().localOrdinal())
                        : 1
                : 0;
        final var mutation = new TargetQuotaMutation(
                localClaim ? lastStoreSequence : TargetQuotaMutation.increment(lastStoreSequence),
                source,
                mutationDigest,
                ordinal,
                reservationExpiry,
                reservationClosure,
                reservationCloseCursor);
        if (!aggregate.shard().equals(source.shardId())
                || (!localClaim && lastStoreSource != null && source.compareTo(lastStoreSource) <= 0)) {
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
            if (localClaim
                    && update.identity().kind() != TargetQuotaIdentity.Kind.TARGET
                    && update.identity().kind() != TargetQuotaIdentity.Kind.TENANT_TARGET) {
                throw new IllegalArgumentException("local Claim cannot change a Shard counter");
            }
            final var prior = lookup.apply(update.identity());
            if (localClaim && prior == null) {
                throw new IllegalStateException("local Claim cannot allocate a counter or incarnation");
            }
            if (prior != null) {
                if (!prior.identity().equals(update.identity())) {
                    throw new IllegalStateException("quota lookup returned another identity");
                }
                aggregate.requireCounter(prior);
                if (reservationCloseCursor || !prior.usage().equals(update.nextUsage())) {
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

    private static void requireLocalClaimChanges(final LocalClaimKind kind, final List<Change> changes) {
        if (changes.isEmpty() || changes.size() % 2 != 0) {
            throw new IllegalArgumentException("local Claim needs changed primary and tenant counter pairs");
        }
        final var target = changes.getFirst().next().identity().target();
        byte[] tenant = null;
        int executionOwners = 0;
        for (var change : changes) {
            final var before = change.prior().usage();
            final var after = change.next().usage();
            if (!target.equals(change.next().identity().target())
                    || before.targets() != after.targets()
                    || before.executionDomains() != after.executionDomains()
                    || before.strictOrderDomains() != after.strictOrderDomains()
                    || before.accountingIncarnations() != after.accountingIncarnations()) {
                throw new IllegalArgumentException("local Claim cannot move Target scope or allocate cardinality");
            }
            for (var dimension : CapacityDimension.values()) {
                if (dimension != CapacityDimension.LOGICAL_STATE_BYTES
                        && dimension != CapacityDimension.INFLIGHT_MESSAGES
                        && dimension != CapacityDimension.INFLIGHT_BYTES
                        && before.resources().amount(dimension)
                                != after.resources().amount(dimension)) {
                    throw new IllegalArgumentException("local Claim cannot change payload, reserve or outcome charges");
                }
            }
            if (change.next().identity().kind().isMirror()) {
                final byte[] currentTenant = change.next().identity().tenantScope();
                if (tenant != null && !Arrays.equals(tenant, currentTenant)) {
                    throw new IllegalArgumentException("local Claim cannot span tenant routing scopes");
                }
                tenant = currentTenant;
                continue;
            }
            final long count = difference(change, CapacityDimension.INFLIGHT_MESSAGES);
            final long bytes = difference(change, CapacityDimension.INFLIGHT_BYTES);
            if (count != 0 || bytes != 0) {
                if (count != (kind == LocalClaimKind.CLAIM ? 1 : -1)
                        || (kind == LocalClaimKind.CLAIM ? bytes <= 0 : bytes >= 0)) {
                    throw new IllegalArgumentException(
                            "local Claim must add or remove exactly one frozen execution charge");
                }
                executionOwners++;
            }
            final var mirrors = changes.stream()
                    .filter(candidate -> candidate.next().identity().kind().isMirror()
                            && Arrays.equals(
                                    candidate.next().identity().accountingIncarnation(),
                                    change.next().identity().accountingIncarnation()))
                    .toList();
            if (mirrors.size() != 1) {
                throw new IllegalArgumentException("local Claim requires the exact corresponding tenant mirror");
            }
            for (var dimension : CapacityDimension.values()) {
                if (difference(change, dimension) != difference(mirrors.getFirst(), dimension)) {
                    throw new IllegalArgumentException("local Claim primary and tenant charge deltas disagree");
                }
            }
        }
        if (executionOwners != 1
                || changes.stream()
                                        .filter(c -> !c.next().identity().kind().isMirror())
                                        .count()
                                * 2
                        != changes.size()) {
            throw new IllegalArgumentException("local Claim requires one execution owner and matching tenant pairs");
        }
    }

    private static long difference(final Change change, final CapacityDimension dimension) {
        return change.next().usage().resources().amount(dimension)
                - change.prior().usage().resources().amount(dimension);
    }

    private static void requireStoreStamp(
            final TargetQuotaAggregate aggregate, final long sequence, final SourcePosition source) {
        if ((sequence == 0) != (source == null)
                || (source != null && !aggregate.shard().equals(source.shardId()))) {
            throw new IllegalStateException("Store mutation sequence/source mismatch");
        }
        final var stamp = aggregate.mutation();
        if (stamp != null) {
            stamp.requireAtOrBefore(sequence, source);
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
