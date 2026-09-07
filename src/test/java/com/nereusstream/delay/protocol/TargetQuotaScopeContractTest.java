package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetQuotaGrantGate;
import com.nereusstream.delay.runtime.TargetQuotaTotalsDelta;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class TargetQuotaScopeContractTest {
    private final Properties vectors = load();
    private final KafkaSourcePosition source = (KafkaSourcePosition) TargetSourcePosition.decode(hex("source"));
    private final TargetQuotaScope scope = TargetQuotaScope.decode(hex("scope.target"));
    private final TargetQuotaAccounting accounting = TargetQuotaAccounting.decode(hex("accounting"));
    private final TargetQuotaUsage usage = TargetQuotaUsage.decode(hex("usage"));

    @Test
    void completeScopeTotalGrantAndZeroVectorsMatchIndependentEncoding() {
        assertArrayEquals(hex("scope.shard"), scope.shardScope().canonicalBytes());
        assertArrayEquals(
                hex("scope.target"),
                scope.shardScope().forTarget(scope.target()).canonicalBytes());
        assertArrayEquals(hex("total"), new TargetQuotaTotal(scope, usage, 1, stamp(1)).canonicalBytes());
        assertArrayEquals(
                hex("total.zero"), new TargetQuotaTotal(scope, TargetQuotaUsage.empty(), 1, stamp(1)).canonicalBytes());
        assertArrayEquals(
                hex("total.key"), TargetQuotaTotal.decode(hex("total")).key());
        for (String name : List.of("shard", "target", "zero")) {
            final var grant = grant(
                    name.equals("shard") ? scope.shardScope() : scope,
                    name.equals("zero") ? TargetQuotaUsage.empty() : usage,
                    name.equals("zero") ? 2 : 1,
                    7,
                    0xaa);
            assertArrayEquals(hex("grant." + name), grant.canonicalBytes());
            assertArrayEquals(
                    grant.canonicalBytes(),
                    TargetQuotaGrant.decode(grant.canonicalBytes()).canonicalBytes());
        }
    }

    @Test
    void maximumLegalBranchesHaveIndependentLengthsAndHashes() {
        final long[] amounts = new long[CapacityDimension.COUNT];
        Arrays.fill(amounts, 0, 15, Long.MAX_VALUE);
        final var targetUsage =
                new TargetQuotaUsage(new CapacityVector(amounts), 1, 64, Long.MAX_VALUE, Long.MAX_VALUE);
        final var maximumSource = new PulsarSourcePosition(
                source.shardId(),
                repeat(32, 0x77),
                "x".repeat(TargetSourcePosition.MAX_TOPIC_UTF8_BYTES),
                -1,
                -1,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                Long.MAX_VALUE);
        final var total = new TargetQuotaTotal(
                scope, targetUsage, -1, new TargetQuotaMutation(-1, maximumSource, repeat(32, 0x66)));
        maximum("total", total.canonicalBytes(), TargetQuotaTotal.MAX_CANONICAL_BYTES);
        assertEquals(-1, TargetQuotaTotal.decode(total.canonicalBytes()).revision());
        final var maximumAccounting = new TargetQuotaAccounting(
                repeat(32, 0xbb), Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        maximum(
                "grant.target",
                new TargetQuotaGrant(scope, repeat(32, 0x99), -1, maximumAccounting, targetUsage, -1, repeat(32, 0xaa))
                        .canonicalBytes(),
                TargetQuotaGrant.MAX_CANONICAL_BYTES);
        Arrays.fill(amounts, 50, 55, Long.MAX_VALUE);
        final var shardUsage = new TargetQuotaUsage(
                new CapacityVector(amounts), Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        final var shardGrant = new TargetQuotaGrant(
                scope.shardScope(), repeat(32, 0x99), -1, maximumAccounting, shardUsage, -1, repeat(32, 0xaa));
        maximum("grant.shard", shardGrant.canonicalBytes(), TargetQuotaGrant.MAX_CANONICAL_BYTES);
        assertEquals(-1, TargetQuotaGrant.decode(shardGrant.canonicalBytes()).tenantPolicyVersion());
    }

    @Test
    void closedDecodersRejectMalformedCanonicalAndNestedData() {
        final Map<String, Function<byte[], ?>> decoders = Map.of(
                "scope.target",
                TargetQuotaScope::decode,
                "total",
                TargetQuotaTotal::decode,
                "grant.target",
                TargetQuotaGrant::decode);
        for (var entry : decoders.entrySet()) {
            final byte[] raw = hex(entry.getKey());
            final byte[] version = raw.clone();
            version[1] = 2;
            final byte[] digest = raw.clone();
            digest[digest.length - 1] ^= 1;
            for (byte[] invalid : List.of(
                    version,
                    digest,
                    Arrays.copyOf(raw, raw.length - 1),
                    Bytes.concat(raw, new byte[] {0x08, 0x01}),
                    Bytes.concat(new byte[] {8, (byte) 0x81, 0}, Arrays.copyOfRange(raw, 2, raw.length)))) {
                assertThrows(
                        IllegalArgumentException.class, () -> entry.getValue().apply(invalid));
            }
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaScope.decode(new byte[TargetQuotaScope.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaTotal.decode(new byte[TargetQuotaTotal.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrant.decode(new byte[TargetQuotaGrant.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void storeScopeChecksFullSourceTenantTargetAndOldReaderRejectsNewNvType() {
        final var total = TargetQuotaTotal.decode(hex("total"));
        assertArrayEquals(
                total.canonicalBytes(),
                TargetQuotaTotal.decodeForStore(
                                total.key(), total.canonicalBytes(), source.shardId(), scope.tenantScope())
                        .canonicalBytes());
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotal.decodeForStore(
                        total.key(), total.canonicalBytes(), source.shardId(), repeat(32, 0x45)));
        final byte[] key = total.key();
        key[key.length - 1] ^= 1;
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaTotal.decodeForStore(
                        key, total.canonicalBytes(), source.shardId(), scope.tenantScope()));
        assertThrows(
                IllegalStateException.class,
                () -> scope.requireRoute(new ShardId(source.shardId().routeIncarnation(), 4), scope.tenantScope()));
        final byte[] prefix = ByteBuffer.allocate(8)
                .putShort((short) 0x4e56)
                .put((byte) TargetQuotaTotal.VALUE_TYPE)
                .put((byte) 1)
                .putInt(total.canonicalBytes().length)
                .array();
        final byte[] content = Bytes.concat(prefix, total.canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(content, Bytes.crc32cbe(content))));
    }

    @Test
    void scopeHasNoIncarnationProfileOrDomainAndCopiesTenantBytes() {
        final byte[] tenant = scope.tenantScope();
        final var copy = new TargetQuotaScope(source.shardId(), tenant, scope.target());
        tenant[0] ^= 1;
        assertEquals(scope, copy);
        assertNotEquals(scope, new TargetQuotaScope(source.shardId(), repeat(32, 0x45), scope.target()));
        assertNotEquals(scope, scope.shardScope());
        assertEquals(scope, scope.forTarget(scope.target()));
        final var old = identity(0x22, scope.target(), false);
        final var next = identity(0x23, scope.target(), false);
        assertNotEquals(old, next);
        assertEquals(scope.forTarget(old.target()), scope.forTarget(next.target()));
    }

    @Test
    void sumIncludesProtectedOldIncarnationWithoutDuplicatingTheTarget() {
        final var old = counter(0x22, used(0, 500, 0, 1), 1);
        final var current = counter(0x23, used(2, 0, 1, 1), 1);
        final var priorUsage = old.usage().add(current.usage());
        final var aggregate = aggregate(priorUsage, 1);
        final var total = new TargetQuotaTotal(scope, priorUsage, 1, stamp(1));
        final Map<TargetQuotaIdentity, TargetQuotaCounter> leaves =
                Map.of(old.identity(), old, current.identity(), current);
        final var delta = plan(
                aggregate,
                List.of(new TargetQuotaDelta.Update(current.identity(), used(3, 0, 1, 1))),
                leaves::get,
                total);
        assertEquals(
                500, delta.changes().getFirst().next().usage().resources().amount(CapacityDimension.RETAINED_BYTES));
        assertEquals(2, delta.changes().getFirst().next().usage().accountingIncarnations());
        assertEquals(1, delta.changes().getFirst().next().usage().targets());
        assertEquals(
                delta.counters().nextAggregate().usage(),
                delta.changes().getFirst().next().usage());
        assertSame(old, leaves.get(old.identity()));
    }

    @Test
    void incarnationRotationCannotCopyQueueOrDomainCardinality() {
        final var old = counter(0x22, used(1, 0, 1, 1), 1);
        final var aggregate = aggregate(old.usage(), 1);
        final var total = new TargetQuotaTotal(scope, old.usage(), 1, stamp(1));
        final var nextIdentity = identity(0x23, scope.target(), false);
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        aggregate,
                        List.of(new TargetQuotaDelta.Update(nextIdentity, used(1, 0, 1, 1))),
                        key -> key.equals(old.identity()) ? old : null,
                        total));
        final var tooManyDomains = new TargetQuotaUsage(CapacityVector.empty(), 0, 65, 0, 2);
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaTotal(scope, tooManyDomains, 1, stamp(1)));
    }

    @Test
    void netZeroTransferAtSignedMaximumSubtractsAllPriorsFirst() {
        final var old = counter(0x22, used(Long.MAX_VALUE, 0, 1, 1), 1);
        final var aggregate = aggregate(old.usage(), 1);
        final var total = new TargetQuotaTotal(scope, old.usage(), 1, stamp(1));
        final var nextIdentity = identity(0x23, scope.target(), false);
        final var delta = plan(
                aggregate,
                List.of(
                        new TargetQuotaDelta.Update(nextIdentity, old.usage()),
                        new TargetQuotaDelta.Update(old.identity(), TargetQuotaUsage.empty())),
                key -> key.equals(old.identity()) ? old : null,
                total);
        assertEquals(total.usage(), delta.changes().getFirst().next().usage());
        assertEquals(2, delta.changes().getFirst().next().revision());
        assertEquals(stamp(2), delta.changes().getFirst().next().mutation());
        assertEquals(aggregate.usage(), delta.counters().nextAggregate().usage());
        assertThrows(UnsupportedOperationException.class, () -> delta.changes().clear());
    }

    @Test
    void mirrorOnlyAndUnattributedShardChangesDoNotCreateTargetTotals() {
        final var primary = counter(0x22, used(2, 0, 1, 1), 1);
        final var mirrorId = identity(0x22, scope.target(), true);
        final var mirror = new TargetQuotaCounter(mirrorId, used(1, 0, 1, 0), 1, stamp(1));
        final var aggregate = aggregate(primary.usage(), 1);
        final var delta = TargetQuotaDelta.prepare(
                aggregate,
                1,
                source,
                source(2),
                repeat(32, 0x66),
                List.of(new TargetQuotaDelta.Update(mirrorId, used(2, 0, 1, 0))),
                1,
                key -> mirror);
        final var totals = TargetQuotaTotalsDelta.prepare(delta, scope.shardScope(), 1, key -> {
            throw new AssertionError("mirror-only mutation must not read primary total");
        });
        assertTrue(totals.changes().isEmpty());
        assertEquals(aggregate.usage(), delta.nextAggregate().usage());
        final var shardId =
                new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, source.shardId(), repeat(16, 0x22), null, null);
        final var shardUsage = new TargetQuotaUsage(capacity(3, 20), 0, 0, 0, 1);
        final var shardDelta = TargetQuotaDelta.prepare(
                aggregate,
                1,
                source,
                source(2),
                repeat(32, 0x66),
                List.of(new TargetQuotaDelta.Update(shardId, shardUsage)),
                1,
                key -> null);
        assertTrue(TargetQuotaTotalsDelta.prepare(shardDelta, scope.shardScope(), 1, key -> {
                    throw new AssertionError("unattributed shard mutation must not read a Target total");
                })
                .changes()
                .isEmpty());
    }

    @Test
    void unchangedLeavesCauseNoTotalReadOrWrite() {
        final var prior = counter(0x22, used(2, 0, 1, 1), 1);
        final var aggregate = aggregate(prior.usage(), 1);
        final var delta = plan(
                aggregate, List.of(new TargetQuotaDelta.Update(prior.identity(), prior.usage())), key -> prior, null);
        assertTrue(delta.changes().isEmpty());
        assertSame(aggregate, delta.counters().nextAggregate());
    }

    @Test
    void mutationCostStaysAtOneTotalLookupAsUnrelatedTargetsGrow() {
        for (int size : List.of(1, 100, 10000)) {
            final var all = new HashMap<TargetQuotaScope, TargetQuotaTotal>();
            TargetQuotaUsage sum = TargetQuotaUsage.empty();
            for (int n = 0; n < size; n++) {
                final byte[] target = repeat(32, 0x33);
                ByteBuffer.wrap(target).putInt(n);
                final var key = scope.forTarget(new TargetPartitionId(target));
                final var total = new TargetQuotaTotal(key, used(1, 0, 1, 1), 1, stamp(1));
                all.put(key, total);
                sum = sum.add(total.usage());
            }
            final byte[] target = repeat(32, 0x33);
            ByteBuffer.wrap(target).putInt(0);
            final var selected = scope.forTarget(new TargetPartitionId(target));
            final var priorTotal = all.get(selected);
            final var id = identity(0x22, selected.target(), false);
            final var counter = new TargetQuotaCounter(id, priorTotal.usage(), 1, stamp(1));
            final var base = TargetQuotaDelta.prepare(
                    aggregate(sum, 1),
                    1,
                    source,
                    source(2),
                    repeat(32, 0x66),
                    List.of(new TargetQuotaDelta.Update(id, used(2, 0, 1, 1))),
                    1,
                    key -> counter);
            final var reads = new AtomicInteger();
            final var delta = TargetQuotaTotalsDelta.prepare(base, scope.shardScope(), 1, key -> {
                reads.incrementAndGet();
                return all.get(key);
            });
            assertEquals(1, reads.get());
            assertEquals(1, delta.changes().size());
            assertEquals(size, all.size());
            assertSame(priorTotal, all.get(selected));
        }
    }

    @Test
    void finiteBudgetAndForeignMirrorFailBeforeAnyTotalLookup() {
        final var first = identity(0x22, scope.target(), false);
        final var second = identity(0x22, new TargetPartitionId(repeat(32, 0x34)), false);
        final var genesis = TargetQuotaAggregate.genesis(source.shardId(), repeat(16, 0x22));
        final var delta = TargetQuotaDelta.prepare(
                genesis,
                0,
                null,
                source,
                repeat(32, 0x66),
                List.of(
                        new TargetQuotaDelta.Update(first, used(1, 0, 1, 1)),
                        new TargetQuotaDelta.Update(second, used(1, 0, 1, 1))),
                2,
                key -> null);
        final Function<TargetQuotaScope, TargetQuotaTotal> noLookup = key -> {
            throw new AssertionError("budget/scope rejection precedes total lookups");
        };
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaTotalsDelta.prepare(delta, scope.shardScope(), 1, noLookup));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaTotalsDelta.prepare(delta, scope.shardScope(), 0, noLookup));
        final var foreign = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                source.shardId(),
                repeat(16, 0x22),
                scope.target(),
                repeat(32, 0x45));
        final var mirrorDelta = TargetQuotaDelta.prepare(
                genesis,
                0,
                null,
                source,
                repeat(32, 0x66),
                List.of(new TargetQuotaDelta.Update(foreign, used(1, 0, 1, 0))),
                1,
                key -> null);
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotalsDelta.prepare(mirrorDelta, scope.shardScope(), 1, noLookup));
    }

    @Test
    void missingExistingTotalAndWrongLookupScopeCannotBeReconstructedOnTheHotPath() {
        final var prior = counter(0x22, used(2, 0, 1, 1), 1);
        final var aggregate = aggregate(prior.usage(), 1);
        final var updates = List.of(new TargetQuotaDelta.Update(prior.identity(), used(3, 0, 1, 1)));
        assertThrows(IllegalStateException.class, () -> plan(aggregate, updates, key -> prior, null));
        final var wrong = new TargetQuotaTotal(
                scope.forTarget(new TargetPartitionId(repeat(32, 0x34))), prior.usage(), 1, stamp(1));
        assertThrows(IllegalStateException.class, () -> plan(aggregate, updates, key -> prior, wrong));
    }

    @Test
    void exactPriorReadSetRejectsChangedTotalAndRepeatedSource() {
        final var prior = counter(0x22, used(2, 0, 1, 1), 1);
        final var aggregate = aggregate(prior.usage(), 1);
        final var total = new TargetQuotaTotal(scope, prior.usage(), 1, stamp(1));
        final var updates = List.of(new TargetQuotaDelta.Update(prior.identity(), used(3, 0, 1, 1)));
        final var delta = plan(aggregate, updates, key -> prior, total);
        delta.requireCurrent(aggregate, 1, source, key -> prior, key -> total);
        final var changed = new TargetQuotaTotal(scope, used(4, 0, 1, 1), 1, stamp(1));
        assertThrows(
                IllegalStateException.class,
                () -> delta.requireCurrent(aggregate, 1, source, key -> prior, key -> changed));
        assertThrows(
                IllegalStateException.class,
                () -> delta.requireCurrent(aggregate, 1, source, key -> prior, key -> null));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.prepare(
                        aggregate, 1, source, source, repeat(32, 0x66), updates, 1, key -> prior));
    }

    @Test
    void recoveryAuditsEveryPrimaryIncarnationAndRejectsMissingExtraOrForgedTotals() {
        final var old = counter(0x22, used(0, 50, 0, 1), 1);
        final var current = counter(0x23, used(2, 0, 1, 1), 1);
        final var mirror = new TargetQuotaCounter(identity(0x23, scope.target(), true), used(2, 0, 1, 0), 1, stamp(1));
        final var sum = old.usage().add(current.usage());
        final var aggregate = aggregate(sum, 1);
        final var total = new TargetQuotaTotal(scope, sum, 1, stamp(1));
        final var leaves = List.of(old, current, mirror);
        TargetQuotaDelta.audit(
                aggregate,
                leaves,
                Map.of(
                        old.identity(),
                        old.usage(),
                        current.identity(),
                        current.usage(),
                        mirror.identity(),
                        mirror.usage()));
        TargetQuotaTotalsDelta.audit(scope.shardScope(), aggregate, leaves, List.of(total));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotalsDelta.audit(scope.shardScope(), aggregate, leaves, List.of()));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotalsDelta.audit(
                        scope.shardScope(),
                        aggregate,
                        leaves,
                        List.of(new TargetQuotaTotal(scope, current.usage(), 1, stamp(1)))));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotalsDelta.audit(scope.shardScope(), aggregate, leaves, List.of(total, total)));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotalsDelta.audit(
                        scope.shardScope(), aggregate, List.of(old, current, current), List.of(total)));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaTotalsDelta.audit(scope.shardScope(), aggregate, List.of(), List.of(total)));
    }

    @Test
    void localRevisionAndFullSourceConsistencyAreCheckedWithUnsignedRules() {
        final var later = new TargetQuotaTotal(scope, usage, 1, stamp(2));
        assertThrows(IllegalStateException.class, () -> later.requireAggregate(aggregate(usage, 1)));
        final var altered = new TargetQuotaMutation(
                2,
                new KafkaSourcePosition(
                        source.shardId(), source.authenticatedClusterId(), source.nativeTopicUuid(), 11, null, 999),
                repeat(32, 0x66));
        assertThrows(
                IllegalStateException.class,
                () -> later.requireAggregate(
                        new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), usage, 2, altered)));
        final var wrongSequence = new TargetQuotaMutation(3, source(2), repeat(32, 0x66));
        assertThrows(
                IllegalStateException.class,
                () -> later.requireAggregate(
                        new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), usage, 3, wrongSequence)));
        final var exhausted =
                new TargetQuotaTotal(scope, usage, -1, new TargetQuotaMutation(-1, source(2), repeat(32, 0x66)));
        assertThrows(IllegalStateException.class, () -> exhausted.advance(usage, stamp(3)));
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaTotal(scope, usage, 0, stamp(1)));
    }

    @Test
    void grantSuccessorRejectsIdentityPolicyAndVersionAbaIncludingUnsignedExhaustion() {
        final var prior = grant(scope, usage, 1, 7, 0xaa);
        prior.requireSuccessor(null);
        grant(scope, TargetQuotaUsage.empty(), 2, 7, 0xaa).requireSuccessor(prior);
        for (var next : List.of(
                grant(scope, usage, 3, 7, 0xaa),
                grant(scope, usage, 2, 6, 0xaa),
                grant(scope, usage, 2, 7, 0xab),
                grant(scope.shardScope(), usage, 2, 7, 0xaa))) {
            assertThrows(IllegalStateException.class, () -> next.requireSuccessor(prior));
        }
        assertThrows(IllegalStateException.class, () -> grant(scope, usage, 2, 7, 0xaa)
                .requireSuccessor(null));
        final var signedBoundary = grant(scope, usage, Long.MAX_VALUE, Long.MAX_VALUE, 0xaa);
        grant(scope, usage, Long.MIN_VALUE, Long.MIN_VALUE, 0xab).requireSuccessor(signedBoundary);
        assertThrows(IllegalStateException.class, () -> prior.requireSuccessor(grant(scope, usage, -1, -1, 0xaa)));
        assertThrows(IllegalArgumentException.class, () -> grant(scope, usage, 0, 1, 0xaa));
        assertThrows(IllegalArgumentException.class, () -> grant(scope, usage, 1, 0, 0xaa));
    }

    @Test
    void grantReductionGatesNewIngressAgainstShardAndAllIncarnationTargetTotals() {
        final var prior = new TargetQuotaTotal(scope, used(2, 50, 1, 2), 1, stamp(1));
        final var next = prior.advance(used(3, 50, 1, 2), stamp(2));
        final var shardLimit = grant(scope.shardScope(), used(20, 500, 10, 20), 2, 7, 0xaa);
        final var targetLimit = grant(scope, used(2, 50, 1, 2), 2, 7, 0xaa);
        for (var op : List.of(
                TargetQuotaGrantGate.Operation.FIRST_SCHEDULE,
                TargetQuotaGrantGate.Operation.PREPARE,
                TargetQuotaGrantGate.Operation.DLQ_REPLAY)) {
            assertEquals(TargetQuotaGrantGate.Decision.TARGET_LIMIT, gate(op, shardLimit, targetLimit, prior, next));
            assertEquals(
                    TargetQuotaGrantGate.Decision.SHARD_LIMIT,
                    gate(
                            op,
                            grant(scope.shardScope(), TargetQuotaUsage.empty(), 2, 7, 0xaa),
                            targetLimit,
                            prior,
                            next));
        }
        assertEquals(
                TargetQuotaGrantGate.Decision.WITHIN_LOGICAL_GRANTS,
                gate(
                        TargetQuotaGrantGate.Operation.FIRST_SCHEDULE,
                        shardLimit,
                        grant(scope, used(3, 50, 1, 2), 2, 7, 0xaa),
                        prior,
                        next));
    }

    @Test
    void existingWorkDrainsBelowReducedGrantsEvenWhenItsExecutionOrRetainedChargeGrows() {
        final var prior = new TargetQuotaTotal(scope, used(2, 0, 1, 1), 1, stamp(1));
        final var nextUsage = new TargetQuotaUsage(
                prior.usage()
                        .resources()
                        .add(capacity(7, 1))
                        .add(capacity(8, 100))
                        .add(capacity(4, 50)),
                1,
                0,
                0,
                1);
        final var next = prior.advance(nextUsage, stamp(2));
        final var zeroShard = grant(scope.shardScope(), TargetQuotaUsage.empty(), 2, 7, 0xaa);
        final var zeroTarget = grant(scope, TargetQuotaUsage.empty(), 2, 7, 0xaa);
        for (var op : TargetQuotaGrantGate.Operation.values()) {
            if (op != TargetQuotaGrantGate.Operation.FIRST_SCHEDULE
                    && op != TargetQuotaGrantGate.Operation.PREPARE
                    && op != TargetQuotaGrantGate.Operation.DLQ_REPLAY) {
                assertEquals(
                        TargetQuotaGrantGate.Decision.EXISTING_WORK_DRAIN,
                        gate(op, zeroShard, zeroTarget, prior, next));
            }
        }
    }

    @Test
    void grantGateRejectsForeignScopeAndNewAccountingButAcceptsDecodedArtifacts() {
        final var prior = new TargetQuotaTotal(scope, used(1, 0, 1, 1), 1, stamp(1));
        final var next = prior.advance(used(2, 0, 1, 1), stamp(2));
        final var shard = TargetQuotaGrant.decode(
                grant(scope.shardScope(), next.usage(), 1, 7, 0xaa).canonicalBytes());
        final var target =
                TargetQuotaGrant.decode(grant(scope, next.usage(), 1, 7, 0xaa).canonicalBytes());
        assertEquals(
                TargetQuotaGrantGate.Decision.WITHIN_LOGICAL_GRANTS,
                gate(TargetQuotaGrantGate.Operation.FIRST_SCHEDULE, shard, target, prior, next));
        final var other = new TargetQuotaGrant(
                scope,
                repeat(32, 0x99),
                1,
                new TargetQuotaAccounting(repeat(32, 0xbc), 32, 24, 40, 64),
                next.usage(),
                7,
                repeat(32, 0xaa));
        assertThrows(
                IllegalStateException.class,
                () -> gate(TargetQuotaGrantGate.Operation.FIRST_SCHEDULE, shard, other, prior, next));
        assertEquals(
                TargetQuotaGrantGate.Decision.EXISTING_WORK_DRAIN,
                gate(TargetQuotaGrantGate.Operation.OUTCOME, shard, other, prior, next));
        final var foreign =
                grant(new TargetQuotaScope(scope.shard(), repeat(32, 0x45), null), next.usage(), 1, 7, 0xaa);
        assertThrows(
                IllegalStateException.class,
                () -> gate(TargetQuotaGrantGate.Operation.OUTCOME, foreign, target, prior, next));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaGrantGate.evaluate(
                        TargetQuotaGrantGate.Operation.CLAIM,
                        shard,
                        target,
                        accounting,
                        aggregate(prior.usage(), 1),
                        aggregate(next.usage(), 2),
                        null,
                        next));
    }

    @Test
    void underflowOverflowAndUnattributedDimensionsCannotBeHiddenInATotal() {
        final var prior = counter(0x22, used(2, 0, 1, 1), 1);
        final var insufficient = new TargetQuotaTotal(scope, used(1, 0, 1, 1), 1, stamp(1));
        assertThrows(
                IllegalStateException.class,
                () -> plan(
                        aggregate(prior.usage(), 1),
                        List.of(new TargetQuotaDelta.Update(prior.identity(), used(3, 0, 1, 1))),
                        key -> prior,
                        insufficient));
        final var atLimit = new TargetQuotaTotal(scope, used(Long.MAX_VALUE, 0, 1, 1), 1, stamp(1));
        final var newId = identity(0x23, scope.target(), false);
        assertThrows(
                ArithmeticException.class,
                () -> TargetQuotaDelta.prepare(
                        aggregate(atLimit.usage(), 1),
                        1,
                        source,
                        source(2),
                        repeat(32, 0x66),
                        List.of(new TargetQuotaDelta.Update(newId, used(1, 0, 0, 1))),
                        1,
                        key -> null));
        final var shardOnly = new TargetQuotaUsage(capacity(51, 1), 0, 0, 0, 1);
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaTotal(scope, shardOnly, 1, stamp(1)));
        assertThrows(IllegalArgumentException.class, () -> grant(scope, shardOnly, 1, 7, 0xaa));
    }

    @Test
    void matchingStampsCannotHideAnUnderstatedPrimaryParent() {
        final var prior = new TargetQuotaTotal(scope, used(1, 0, 1, 1), 1, stamp(1));
        final var next = prior.advance(used(2, 0, 1, 1), stamp(2));
        assertThrows(IllegalStateException.class, () -> next.requireAggregate(aggregate(used(1, 0, 1, 1), 2)));
        assertThrows(IllegalStateException.class, () -> prior.requireCounter(counter(0x22, used(2, 0, 1, 1), 1)));
        final var fewerIncarnations = new TargetQuotaUsage(next.usage().resources(), 1, 0, 0, 0);
        assertThrows(IllegalStateException.class, () -> next.requireAggregate(aggregate(fewerIncarnations, 2)));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaGrantGate.evaluate(
                        TargetQuotaGrantGate.Operation.FIRST_SCHEDULE,
                        grant(scope.shardScope(), used(10, 0, 1, 2), 1, 7, 0xaa),
                        grant(scope, used(10, 0, 1, 2), 1, 7, 0xaa),
                        accounting,
                        aggregate(prior.usage(), 1),
                        aggregate(prior.usage(), 2),
                        prior,
                        next));
    }

    @Test
    void initialAbsenceIsGuardedAndDrainedScopeReusesItsTotalRevision() {
        final var genesis = TargetQuotaAggregate.genesis(source.shardId(), repeat(16, 0x22));
        final var id = identity(0x22, scope.target(), false);
        final var allocation = TargetQuotaDelta.prepare(
                genesis,
                0,
                null,
                source,
                repeat(32, 0x66),
                List.of(new TargetQuotaDelta.Update(id, used(1, 0, 1, 1))),
                1,
                key -> null);
        final var first = TargetQuotaTotalsDelta.prepare(allocation, scope.shardScope(), 1, key -> null);
        first.requireCurrent(genesis, 0, null, key -> null, key -> null);
        final var firstTotal = first.changes().getFirst().next();
        assertEquals(1, firstTotal.revision());
        assertThrows(
                IllegalStateException.class,
                () -> first.requireCurrent(genesis, 0, null, key -> null, key -> firstTotal));
        final var firstCounter = allocation.changes().getFirst().next();
        final var drain = plan(
                allocation.nextAggregate(),
                List.of(new TargetQuotaDelta.Update(id, TargetQuotaUsage.empty())),
                key -> firstCounter,
                firstTotal);
        final var zeroTotal = drain.changes().getFirst().next();
        final var zeroCounter = drain.counters().changes().getFirst().next();
        TargetQuotaTotalsDelta.audit(
                scope.shardScope(), drain.counters().nextAggregate(), List.of(zeroCounter), List.of(zeroTotal));
        final var newId = identity(0x23, scope.target(), false);
        final var later = TargetQuotaDelta.prepare(
                drain.counters().nextAggregate(),
                2,
                source(2),
                source(3),
                repeat(32, 0x66),
                List.of(new TargetQuotaDelta.Update(newId, used(1, 0, 1, 1))),
                1,
                key -> null);
        final var reused = TargetQuotaTotalsDelta.prepare(later, scope.shardScope(), 1, key -> zeroTotal);
        assertEquals(3, reused.changes().getFirst().next().revision());
        assertEquals(1, later.changes().getFirst().next().revision());
        assertEquals(scope, reused.changes().getFirst().next().scope());
    }

    private TargetQuotaGrantGate.Decision gate(
            final TargetQuotaGrantGate.Operation op,
            final TargetQuotaGrant shard,
            final TargetQuotaGrant target,
            final TargetQuotaTotal prior,
            final TargetQuotaTotal next) {
        return TargetQuotaGrantGate.evaluate(
                op, shard, target, accounting, aggregate(prior.usage(), 1), aggregate(next.usage(), 2), prior, next);
    }

    private TargetQuotaTotalsDelta plan(
            final TargetQuotaAggregate aggregate,
            final List<TargetQuotaDelta.Update> updates,
            final Function<TargetQuotaIdentity, TargetQuotaCounter> lookup,
            final TargetQuotaTotal total) {
        return TargetQuotaTotalsDelta.prepare(
                TargetQuotaDelta.prepare(aggregate, 1, source, source(2), repeat(32, 0x66), updates, 4, lookup),
                scope.shardScope(),
                2,
                key -> total);
    }

    private TargetQuotaCounter counter(final int incarnation, final TargetQuotaUsage value, final long revision) {
        return new TargetQuotaCounter(identity(incarnation, scope.target(), false), value, revision, stamp(revision));
    }

    private TargetQuotaIdentity identity(final int incarnation, final TargetPartitionId target, final boolean mirror) {
        return new TargetQuotaIdentity(
                mirror ? TargetQuotaIdentity.Kind.TENANT_TARGET : TargetQuotaIdentity.Kind.TARGET,
                source.shardId(),
                repeat(16, incarnation),
                target,
                mirror ? scope.tenantScope() : null);
    }

    private TargetQuotaGrant grant(
            final TargetQuotaScope value,
            final TargetQuotaUsage limit,
            final long version,
            final long policy,
            final int policyHash) {
        return new TargetQuotaGrant(
                value, repeat(32, 0x99), version, accounting, limit, policy, repeat(32, policyHash));
    }

    private TargetQuotaAggregate aggregate(final TargetQuotaUsage value, final long revision) {
        return new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), value, revision, stamp(revision));
    }

    private static TargetQuotaUsage used(final long pending, final long retained, final long targets, final long incs) {
        return new TargetQuotaUsage(capacity(2, pending).add(capacity(4, retained)), targets, 0, 0, incs);
    }

    private static CapacityVector capacity(final int dimension, final long amount) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[dimension - 1] = amount;
        return new CapacityVector(values);
    }

    private TargetQuotaMutation stamp(final long sequence) {
        return new TargetQuotaMutation(sequence, source(sequence), repeat(32, 0x66));
    }

    private KafkaSourcePosition source(final long sequence) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                9 + sequence,
                null,
                99 + sequence);
    }

    private void maximum(final String name, final byte[] raw, final int bound) {
        assertEquals(Integer.parseInt(vectors.getProperty("maximum." + name + ".length")), raw.length);
        assertEquals(
                vectors.getProperty("maximum." + name + ".sha256"),
                HexFormat.of().formatHex(Bytes.sha256(raw)));
        assertTrue(raw.length <= bound);
    }

    private byte[] hex(final String name) {
        return HexFormat.of().parseHex(vectors.getProperty(name));
    }

    private static byte[] repeat(final int count, final int value) {
        final byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static Properties load() {
        final var result = new Properties();
        try (var input = TargetQuotaScopeContractTest.class.getResourceAsStream(
                "/ndip3/target-quota-scope-vectors.properties")) {
            result.load(input);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
        return result;
    }
}
