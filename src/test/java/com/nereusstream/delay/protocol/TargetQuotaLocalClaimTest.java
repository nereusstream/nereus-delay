package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetQuotaTotalsDelta;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaLocalClaimTest {
    private final TargetQuotaCounter primary = TargetQuotaCounter.decode(raw("target-quota", "counter"));
    private final TargetQuotaCounter mirror = TargetQuotaCounter.decode(raw("target-quota", "mirror"));
    private final TargetQuotaAggregate aggregate = TargetQuotaAggregate.decode(raw("target-quota", "aggregate"));
    private final KafkaSourcePosition source =
            (KafkaSourcePosition) primary.mutation().source();
    private final TargetQuotaScope scope = new TargetQuotaScope(
            source.shardId(),
            mirror.identity().tenantScope(),
            primary.identity().target());
    private final Map<TargetQuotaIdentity, TargetQuotaCounter> initial =
            Map.of(primary.identity(), primary, mirror.identity(), mirror);
    private final TargetQuotaDelta.LocalClaimAuthority allow = (kind, delta) -> {};

    @Test
    void localClaimRevokeAndFollowingSourceMutationMatchIndependentBytes() {
        final var claim =
                local(aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        checkVector("claim", claim);
        final var claimed = applied(claim);
        final var revoke = local(
                claim.nextAggregate(),
                1,
                claimed,
                updates(0, 0, 0),
                TargetQuotaDelta.LocalClaimKind.REVOKE,
                0x78,
                allow);
        checkVector("revoke", revoke);
        final var nextSource = TargetQuotaDelta.prepare(
                revoke.nextAggregate(), 1, source, at(11), bytes(32, 0x79), updates(1, 0, 0), 4, applied(revoke)::get);
        checkVector("source", nextSource);
        assertEquals(primary.usage(), revoke.nextAggregate().usage());
        assertArrayEquals(source.canonicalBytes(), revoke.mutation().source().canonicalBytes());
        assertEquals(2, nextSource.mutation().sequence());
        assertEquals(0, nextSource.mutation().localClaimOrdinal());
    }

    @Test
    void repeatedLocalClaimsAdvanceOnlyAffectedRevisionsAndKeepExactFrontier() {
        var current = aggregate;
        Map<TargetQuotaIdentity, TargetQuotaCounter> counters = initial;
        long sequence = 1;
        for (int i = 0; i < 40; i++) {
            final boolean claim = i % 2 == 0;
            final var calls = new AtomicInteger();
            final var before = counters;
            final var delta = TargetQuotaDelta.prepareLocalClaim(
                    current,
                    sequence,
                    source,
                    bytes(32, i + 1),
                    claim ? TargetQuotaDelta.LocalClaimKind.CLAIM : TargetQuotaDelta.LocalClaimKind.REVOKE,
                    updates(claim ? 100 : 0, claim ? 1 : 0, claim ? 64 : 0),
                    4,
                    identity -> {
                        calls.incrementAndGet();
                        return before.get(identity);
                    },
                    allow);
            delta.requireCurrent(current, sequence, source, identity -> {
                calls.incrementAndGet();
                return before.get(identity);
            });
            assertEquals(4, calls.get()); // Two plan reads plus the two exact precommit counter checks.
            assertEquals(i + 2, delta.nextAggregate().revision());
            assertEquals(i + 1, delta.mutation().localClaimOrdinal());
            assertArrayEquals(source.canonicalBytes(), delta.mutation().source().canonicalBytes());
            counters = applied(delta);
            current = delta.nextAggregate();
        }
        assertEquals(1, sequence);
        assertEquals(41, current.revision());
        assertEquals(primary.usage(), current.usage());
    }

    @Test
    void sourceEntryPointStillRejectsRepeatedPositionAfterLocalCommits() {
        final var claim =
                local(aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.prepare(
                        claim.nextAggregate(),
                        1,
                        source,
                        source,
                        bytes(32, 0x78),
                        updates(0, 0, 0),
                        4,
                        applied(claim)::get));
        assertThrows(IllegalStateException.class, () -> claim.mutation().requireAfter(primary.mutation()));
        claim.mutation().requireStoreSuccessorOf(primary.mutation());
        assertThrows(IllegalStateException.class, () -> primary.mutation().requireStoreSuccessorOf(claim.mutation()));
    }

    @Test
    void missingAuthorityAndAuthorityFailuresCannotProduceAuthorizedPlans() {
        assertThrows(
                NullPointerException.class,
                () -> local(
                        aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, null));
        final var calls = new AtomicInteger();
        assertThrows(
                IllegalStateException.class,
                () -> local(
                        aggregate,
                        1,
                        initial,
                        updates(100, 1, 64),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        0x77,
                        (kind, delta) -> {
                            calls.incrementAndGet();
                            assertEquals(TargetQuotaDelta.LocalClaimKind.CLAIM, kind);
                            assertEquals(2, delta.changes().size());
                            assertArrayEquals(
                                    source.canonicalBytes(),
                                    delta.mutation().source().canonicalBytes());
                            throw new IllegalStateException("Owner/Store/Claim read set is not authorized");
                        }));
        assertEquals(1, calls.get());
        assertThrows(
                AssertionError.class,
                () -> local(
                        aggregate,
                        1,
                        initial,
                        updates(100, 1, 64),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        0x77,
                        (k, d) -> {
                            throw new AssertionError("authority unavailable");
                        }));
        assertEquals(1, aggregate.revision());
    }

    @Test
    void localClaimsCannotAllocateCountersUseGenesisOrExceedFourReads() {
        final var never = new AtomicInteger();
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.prepareLocalClaim(
                        aggregate,
                        0,
                        null,
                        bytes(32, 0x77),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        updates(100, 1, 64),
                        4,
                        k -> {
                            never.incrementAndGet();
                            return null;
                        },
                        allow));
        assertEquals(0, never.get());
        assertThrows(
                IllegalStateException.class,
                () -> local(
                        aggregate,
                        1,
                        Map.of(),
                        updates(100, 1, 64),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        0x77,
                        allow));
        final var excessive = List.of(
                updates(100, 1, 64).getFirst(),
                updates(100, 1, 64).getFirst(),
                updates(100, 1, 64).getFirst(),
                updates(100, 1, 64).getFirst(),
                updates(100, 1, 64).getFirst());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaDelta.prepareLocalClaim(
                        aggregate,
                        1,
                        source,
                        bytes(32, 0x77),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        excessive,
                        1000,
                        k -> {
                            never.incrementAndGet();
                            return initial.get(k);
                        },
                        allow));
        assertEquals(0, never.get());
    }

    @Test
    void operationKindMirrorDeltasAndExecutionCountAreNotCallerSelectable() {
        final var never = new AtomicInteger();
        final TargetQuotaDelta.LocalClaimAuthority authority = (k, d) -> never.incrementAndGet();
        for (var changes : List.of(
                updates(100, 2, 128),
                updates(100, 1, 0),
                updates(100, 0, 0),
                List.of(updates(100, 1, 64).getFirst()),
                List.of(updates(100, 1, 64).getFirst(), updates(99, 1, 64).getLast()))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> local(
                            aggregate, 1, initial, changes, TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, authority));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> local(
                        aggregate,
                        1,
                        initial,
                        updates(100, 1, 64),
                        TargetQuotaDelta.LocalClaimKind.REVOKE,
                        0x77,
                        authority));
        assertEquals(0, never.get());
    }

    @Test
    void localChangesCannotReleasePayloadOutcomeReserveOrCardinality() {
        for (int dimension : List.of(1, 4, 5, 9, 11, 13, 15)) {
            final var p = adjust(primary.usage(), 100, 1, 64);
            final var m = adjust(mirror.usage(), 100, 1, 64);
            final long[] pv = p.resources().amounts(), mv = m.resources().amounts();
            pv[dimension - 1]++;
            mv[dimension - 1]++;
            final var changes = List.of(
                    new TargetQuotaDelta.Update(primary.identity(), withResources(p, pv)),
                    new TargetQuotaDelta.Update(mirror.identity(), withResources(m, mv)));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> local(aggregate, 1, initial, changes, TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow));
        }
        final var changed = adjust(primary.usage(), 100, 1, 64);
        final var cardinality = new TargetQuotaUsage(changed.resources(), 1, 2, 0, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> local(
                        aggregate,
                        1,
                        initial,
                        List.of(
                                new TargetQuotaDelta.Update(primary.identity(), cardinality),
                                updates(100, 1, 64).getLast()),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        0x77,
                        allow));
    }

    @Test
    void twoFrozenOwnersCanShareOneLocalClaimAndOneTargetTotalDelta() {
        final var secondId = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET, source.shardId(), bytes(16, 0x23), scope.target(), null);
        final var secondMirror = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                source.shardId(),
                bytes(16, 0x23),
                scope.target(),
                scope.tenantScope());
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[2] = 42;
        final var secondUsage = new TargetQuotaUsage(new CapacityVector(amounts), 0, 0, 0, 1);
        final var secondMirrorUsage = new TargetQuotaUsage(new CapacityVector(amounts), 0, 0, 0, 0);
        final var counters = new HashMap<>(initial);
        counters.put(secondId, new TargetQuotaCounter(secondId, secondUsage, 1, primary.mutation()));
        counters.put(secondMirror, new TargetQuotaCounter(secondMirror, secondMirrorUsage, 1, primary.mutation()));
        final var allUsage = primary.usage().add(secondUsage);
        final var all = new TargetQuotaAggregate(
                source.shardId(), aggregate.accountingIncarnation(), allUsage, 1, primary.mutation());
        final var changes = List.of(
                updates(100, 1, 64).getFirst(),
                updates(100, 1, 64).getLast(),
                new TargetQuotaDelta.Update(secondId, adjust(secondUsage, -10, 0, 0)),
                new TargetQuotaDelta.Update(secondMirror, adjust(secondMirrorUsage, -10, 0, 0)));
        final var delta = local(all, 1, counters, changes, TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        final var total = new TargetQuotaTotal(scope, allUsage, 1, primary.mutation());
        final var totals = TargetQuotaTotalsDelta.prepare(delta, scope.shardScope(), 1, s -> total);
        assertEquals(4, delta.changes().size());
        assertEquals(1, totals.changes().size());
        assertEquals(
                allUsage.resources().amount(CapacityDimension.LOGICAL_STATE_BYTES) + 90,
                delta.nextAggregate().usage().resources().amount(CapacityDimension.LOGICAL_STATE_BYTES));
        assertEquals(
                delta.nextAggregate().usage(),
                totals.changes().getFirst().next().usage());
        totals.changes().getFirst().next().requireAggregate(delta.nextAggregate());
        final var oldMirror = counters.remove(secondMirror);
        final var foreign = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                source.shardId(),
                bytes(16, 0x23),
                scope.target(),
                bytes(32, 0x45));
        counters.put(foreign, new TargetQuotaCounter(foreign, oldMirror.usage(), 1, primary.mutation()));
        final var mixed = new java.util.ArrayList<>(changes);
        mixed.set(3, new TargetQuotaDelta.Update(foreign, changes.getLast().nextUsage()));
        assertThrows(
                IllegalArgumentException.class,
                () -> local(all, 1, counters, mixed, TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow));
    }

    @Test
    void sameFrontierAllowsLocalOrdinalsButNeverChangedSourceSequenceOrMetadata() {
        final var later = new TargetQuotaMutation(1, source, bytes(32, 0x77), 1);
        primary.mutation().requireAtOrBefore(later);
        primary.mutation().requireAtOrBefore(1, source);
        assertThrows(IllegalStateException.class, () -> primary.mutation().requireAtOrBefore(2, source));
        assertThrows(IllegalStateException.class, () -> later.requireAtOrBefore(primary.mutation()));
        assertThrows(IllegalStateException.class, () -> primary.mutation()
                .requireAtOrBefore(new TargetQuotaMutation(1, source, bytes(32, 0x77))));
        final var metadata = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                source.offset(),
                7,
                source.brokerLogAppendTimeEpochMs());
        assertThrows(IllegalStateException.class, () -> primary.mutation().requireAtOrBefore(2, metadata));
        assertThrows(IllegalStateException.class, () -> primary.mutation().requireAtOrBefore(1, at(11)));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.prepareLocalClaim(
                        aggregate,
                        2,
                        metadata,
                        bytes(32, 0x77),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        updates(100, 1, 64),
                        4,
                        initial::get,
                        allow));
    }

    @Test
    void staleLocalPlanCannotCommitAfterAnotherLocalMutationAtSameSource() {
        final var plan =
                local(aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        plan.requireCurrent(aggregate, 1, source, initial::get);
        assertThrows(
                IllegalStateException.class,
                () -> plan.requireCurrent(plan.nextAggregate(), 1, source, applied(plan)::get));
        assertThrows(IllegalStateException.class, () -> plan.requireCurrent(aggregate, 2, source, initial::get));
        assertThrows(IllegalStateException.class, () -> plan.requireCurrent(aggregate, 1, source, id -> null));
    }

    @Test
    void grantViewAndFrozenRootCanPrecedeLocalAccountingAtSameFrontier() {
        final var grant = TargetQuotaGrantActivation.decode(raw("target-quota-grant", "target.initial.activation"));
        final var local =
                local(aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        final var total = new TargetQuotaTotal(scope, local.nextAggregate().usage(), 2, local.mutation());
        new TargetQuotaGrantControlVerifier.View(
                grant,
                local.nextAggregate(),
                total,
                1,
                source,
                grant.allocation().recoveryLineage());
        final var root = TargetQuotaBookkeeping.decode(raw("target-quota-bookkeeping", "initial.anchor"));
        final var withRoot =
                new TargetQuotaUsage(local.nextAggregate().usage().resources().add(root.charge()), 1, 1, 0, 2);
        root.requireRoot(new TargetQuotaAggregate(
                source.shardId(), root.owner().accountingIncarnation(), withRoot, 2, local.mutation()));
    }

    @Test
    void rawUnsignedLocalOrdinalCrossesSignedBoundaryAndNeverWraps() {
        final var high = new TargetQuotaAggregate(
                source.shardId(),
                aggregate.accountingIncarnation(),
                primary.usage(),
                Long.MIN_VALUE,
                new TargetQuotaMutation(1, source, bytes(32, 0x66), Long.MAX_VALUE));
        final var next =
                local(high, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        assertEquals(1, next.mutation().sequence());
        assertEquals(Long.MIN_VALUE, next.mutation().localClaimOrdinal());
        assertEquals(next.mutation(), TargetQuotaMutation.decode(next.mutation().canonicalBytes()));
        final var exhausted = new TargetQuotaAggregate(
                source.shardId(),
                aggregate.accountingIncarnation(),
                primary.usage(),
                -1,
                new TargetQuotaMutation(1, source, bytes(32, 0x66), -2));
        assertThrows(
                IllegalStateException.class,
                () -> local(
                        exhausted,
                        1,
                        initial,
                        updates(100, 1, 64),
                        TargetQuotaDelta.LocalClaimKind.CLAIM,
                        0x77,
                        allow));
    }

    @Test
    void localOrdinalCannotBecomeAnAllocationGrantOrPrematureFloorCoverage() {
        final var stamp = new TargetQuotaMutation(1, source, bytes(32, 0x77), 1);
        final var floor =
                new RecoveryFloorRef(bytes(16, 0xcc), bytes(16, 0xdd), bytes(32, 0xee), 1, source, 1, List.of());
        assertThrows(IllegalStateException.class, () -> stamp.requireCoveredByFloor(floor));
        primary.mutation().requireCoveredByFloor(floor);
        stamp.requireCoveredByFloor(
                new RecoveryFloorRef(bytes(16, 0xcc), bytes(16, 0xdd), bytes(32, 0xee), 2, at(11), 2, List.of()));
        final var accounting = new TargetQuotaAccounting(bytes(32, 0xbb), 32, 24, 40, 64);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaIncarnation.allocate(scope, accounting, bytes(16, 0xcc), stamp, (p, n) -> {}));
        final var grant = TargetQuotaGrantActivation.decode(raw("target-quota-grant", "target.initial.activation"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaGrantActivation(
                        grant.request(),
                        grant.controlRef(),
                        stamp,
                        grant.systemMutationId(),
                        grant.systemMutationHash(),
                        grant.allocation()));
    }

    @Test
    void sourceAllocationIdentityDoesNotDependOnLocalClaimHistory() {
        final var claim =
                local(aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        final var revoke = local(
                claim.nextAggregate(),
                1,
                applied(claim),
                updates(0, 0, 0),
                TargetQuotaDelta.LocalClaimKind.REVOKE,
                0x78,
                allow);
        final var direct = TargetQuotaDelta.prepare(
                aggregate, 1, source, at(11), bytes(32, 0x79), updates(1, 0, 0), 4, initial::get);
        final var afterLocal = TargetQuotaDelta.prepare(
                revoke.nextAggregate(), 1, source, at(11), bytes(32, 0x79), updates(1, 0, 0), 4, applied(revoke)::get);
        assertEquals(direct.mutation(), afterLocal.mutation());
        final var accounting = new TargetQuotaAccounting(bytes(32, 0xbb), 32, 24, 40, 64);
        final var one =
                TargetQuotaIncarnation.allocate(scope, accounting, bytes(16, 0xcc), direct.mutation(), (p, n) -> {});
        final var two = TargetQuotaIncarnation.allocate(
                scope, accounting, bytes(16, 0xcc), afterLocal.mutation(), (p, n) -> {});
        assertArrayEquals(one.canonicalBytes(), two.canonicalBytes());
    }

    @Test
    void advancingAnUntouchedSourceFrontierRestartsLocalOrdinalAndRejectsExplicitZeroWireField() {
        final var claim =
                local(aggregate, 1, initial, updates(100, 1, 64), TargetQuotaDelta.LocalClaimKind.CLAIM, 0x77, allow);
        final var revoke = TargetQuotaDelta.prepareLocalClaim(
                claim.nextAggregate(),
                2,
                at(11),
                bytes(32, 0x78),
                TargetQuotaDelta.LocalClaimKind.REVOKE,
                updates(0, 0, 0),
                4,
                applied(claim)::get,
                allow);
        assertEquals(2, revoke.mutation().sequence());
        assertEquals(1, revoke.mutation().localClaimOrdinal());
        assertEquals(3, revoke.nextAggregate().revision());
        final byte[] sourceOnly = primary.mutation().canonicalBytes();
        final byte[] noncanonical = Arrays.copyOf(sourceOnly, sourceOnly.length + 2);
        noncanonical[sourceOnly.length] = 32;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaMutation.decode(noncanonical));
        assertArrayEquals(sourceOnly, TargetQuotaMutation.decode(sourceOnly).canonicalBytes());
    }

    private TargetQuotaDelta local(
            TargetQuotaAggregate value,
            long seq,
            Map<TargetQuotaIdentity, TargetQuotaCounter> counters,
            List<TargetQuotaDelta.Update> changes,
            TargetQuotaDelta.LocalClaimKind kind,
            int digest,
            TargetQuotaDelta.LocalClaimAuthority authority) {
        return TargetQuotaDelta.prepareLocalClaim(
                value, seq, source, bytes(32, digest), kind, changes, 4, counters::get, authority);
    }

    private List<TargetQuotaDelta.Update> updates(long state, long count, long executionBytes) {
        return List.of(
                new TargetQuotaDelta.Update(primary.identity(), adjust(primary.usage(), state, count, executionBytes)),
                new TargetQuotaDelta.Update(mirror.identity(), adjust(mirror.usage(), state, count, executionBytes)));
    }

    private static TargetQuotaUsage adjust(TargetQuotaUsage usage, long state, long count, long executionBytes) {
        final long[] amounts = usage.resources().amounts();
        amounts[2] += state;
        amounts[6] += count;
        amounts[7] += executionBytes;
        return withResources(usage, amounts);
    }

    private static TargetQuotaUsage withResources(TargetQuotaUsage usage, long[] resources) {
        return new TargetQuotaUsage(
                new CapacityVector(resources),
                usage.targets(),
                usage.executionDomains(),
                usage.strictOrderDomains(),
                usage.accountingIncarnations());
    }

    private Map<TargetQuotaIdentity, TargetQuotaCounter> applied(TargetQuotaDelta delta) {
        final var result = new HashMap<TargetQuotaIdentity, TargetQuotaCounter>();
        for (var change : delta.changes()) {
            result.put(change.next().identity(), change.next());
        }
        return result;
    }

    private void checkVector(String name, TargetQuotaDelta delta) {
        assertArrayEquals(
                raw("target-quota-local", name + ".mutation"), delta.mutation().canonicalBytes());
        assertArrayEquals(
                raw("target-quota-local", name + ".aggregate"),
                delta.nextAggregate().canonicalBytes());
        for (var change : delta.changes()) {
            assertArrayEquals(
                    raw(
                            "target-quota-local",
                            name + (change.next().identity().kind().isMirror() ? ".mirror" : ".counter")),
                    change.next().canonicalBytes());
        }
    }

    private KafkaSourcePosition at(long offset) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                offset,
                null,
                100 + offset - 10);
    }

    private static byte[] bytes(int n, int value) {
        final byte[] result = new byte[n];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static byte[] raw(String name, String key) {
        final var p = new Properties();
        try (var in = TargetQuotaLocalClaimTest.class.getResourceAsStream("/ndip3/" + name + "-vectors.properties")) {
            p.load(in);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return HexFormat.of().parseHex(p.getProperty(key));
    }
}
