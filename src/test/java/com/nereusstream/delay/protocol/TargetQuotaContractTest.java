package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaContractTest {
    private final Properties vectors = load();
    private final KafkaSourcePosition source = (KafkaSourcePosition) TargetSourcePosition.decode(hex("source"));
    private final TargetQuotaIdentity identity = TargetQuotaIdentity.decode(hex("target.identity"));
    private final TargetQuotaIdentity mirrorIdentity = TargetQuotaIdentity.decode(hex("tenantTarget.identity"));
    private final TargetQuotaUsage usage = TargetQuotaUsage.decode(hex("usage"));
    private final TargetQuotaUsage mirrorUsage = TargetQuotaUsage.decode(hex("mirrorUsage"));
    private final TargetQuotaCounter counter = TargetQuotaCounter.decode(hex("counter"));
    private final TargetQuotaCounter mirror = TargetQuotaCounter.decode(hex("mirror"));
    private final TargetQuotaAggregate aggregate = TargetQuotaAggregate.decode(hex("aggregate"));

    @Test
    void allIdentityBranchesAndKeysMatchIndependentEncoding() {
        int kind = 1;
        for (String name : List.of("target", "tenantTarget", "shard", "tenantShard")) {
            final var decoded = TargetQuotaIdentity.decode(hex(name + ".identity"));
            final var expected = new TargetQuotaIdentity(
                    TargetQuotaIdentity.Kind.values()[kind - 1],
                    source.shardId(),
                    repeat(16, 0x22),
                    kind <= 2 ? new TargetPartitionId(repeat(32, 0x33)) : null,
                    kind == 2 || kind == 4 ? repeat(32, 0x44) : null);
            assertEquals(expected, decoded);
            assertArrayEquals(hex(name + ".identity"), expected.canonicalBytes());
            assertArrayEquals(hex(name + ".key"), expected.key());
            kind++;
        }
        assertEquals(identity, mirrorIdentity.primary());
        assertNotEquals(identity, mirrorIdentity);
    }

    @Test
    void counterAggregateAndMutationMatchIndependentEncoding() {
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[0] = 2;
        amounts[1] = 20;
        amounts[6] = 1;
        amounts[7] = 7;
        amounts[8] = 1;
        amounts[9] = 64;
        final var expected = new TargetQuotaUsage(new CapacityVector(amounts), 1, 1, 0, 1);
        final var stamp = new TargetQuotaMutation(1, source, repeat(32, 0x66));
        assertArrayEquals(hex("usage"), expected.canonicalBytes());
        assertArrayEquals(hex("mutation"), stamp.canonicalBytes());
        assertArrayEquals(hex("counter"), new TargetQuotaCounter(identity, expected, 1, stamp).canonicalBytes());
        assertArrayEquals(
                hex("aggregate"),
                new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), expected, 1, stamp).canonicalBytes());
        assertArrayEquals(
                hex("genesis"),
                TargetQuotaAggregate.genesis(source.shardId(), repeat(16, 0x22)).canonicalBytes());
        assertArrayEquals(hex("emptyUsage"), TargetQuotaUsage.empty().canonicalBytes());
        assertArrayEquals(hex("aggregate.key"), aggregate.key());
    }

    @Test
    void maximumValuesHaveIndependentHashesAndStayInsideBounds() {
        final long[] resources = new long[CapacityDimension.COUNT];
        for (int n = 0; n < 15; n++) {
            resources[n] = Long.MAX_VALUE;
        }
        final var maxCounterUsage = new TargetQuotaUsage(new CapacityVector(resources), 1, 64, Long.MAX_VALUE, 1);
        for (int n = 50; n < 55; n++) {
            resources[n] = Long.MAX_VALUE;
        }
        final var maxUsage = new TargetQuotaUsage(
                new CapacityVector(resources), Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        final var maxSource = new PulsarSourcePosition(
                source.shardId(),
                repeat(32, 0x77),
                "x".repeat(TargetSourcePosition.MAX_TOPIC_UTF8_BYTES),
                -1,
                -1,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                Long.MAX_VALUE);
        final var stamp = new TargetQuotaMutation(-1, maxSource, repeat(32, 0x66));
        final var maxCounter = new TargetQuotaCounter(identity, maxCounterUsage, -1, stamp);
        final var maxAggregate =
                new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), maxCounterUsage, -1, stamp);
        maximum("usage", maxUsage.canonicalBytes(), TargetQuotaUsage.MAX_CANONICAL_BYTES);
        maximum("counter", maxCounter.canonicalBytes(), TargetQuotaCounter.MAX_CANONICAL_BYTES);
        maximum("aggregate", maxAggregate.canonicalBytes(), TargetQuotaAggregate.MAX_CANONICAL_BYTES);
        assertEquals(-1, TargetQuotaCounter.decode(maxCounter.canonicalBytes()).revision());
        assertEquals(
                -1, TargetQuotaAggregate.decode(maxAggregate.canonicalBytes()).revision());
    }

    @Test
    void closedDecodersRejectVersionFieldsDigestsTruncationAndOversize() {
        for (byte[] value : List.of(hex("counter"), hex("aggregate"), hex("usage"), hex("target.identity"))) {
            final var decode = decoderFor(value);
            byte[] changed = value.clone();
            changed[1] = 2;
            final byte[] version = changed;
            assertThrows(IllegalArgumentException.class, () -> decode.accept(version));
            changed = value.clone();
            changed[changed.length - 1] ^= 1;
            final byte[] digest = changed;
            assertThrows(IllegalArgumentException.class, () -> decode.accept(digest));
            assertThrows(IllegalArgumentException.class, () -> decode.accept(Arrays.copyOf(value, value.length - 1)));
            assertThrows(
                    IllegalArgumentException.class, () -> decode.accept(Bytes.concat(value, new byte[] {0x78, 0})));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaCounter.decode(new byte[TargetQuotaCounter.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAggregate.decode(new byte[TargetQuotaAggregate.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void storeChecksFullKeyShardIncarnationAndTenant() {
        assertArrayEquals(
                counter.canonicalBytes(),
                TargetQuotaCounter.decodeForStore(identity.key(), counter.canonicalBytes(), source.shardId())
                        .canonicalBytes());
        assertArrayEquals(
                aggregate.canonicalBytes(),
                TargetQuotaAggregate.decodeForStore(aggregate.key(), aggregate.canonicalBytes(), source.shardId())
                        .canonicalBytes());
        final var other =
                new TargetQuotaIdentity(identity.kind(), identity.shard(), repeat(16, 0x23), identity.target(), null);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaCounter.decodeForStore(other.key(), counter.canonicalBytes(), source.shardId()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaCounter.decodeForStore(
                        mirrorIdentity.key(), counter.canonicalBytes(), source.shardId()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAggregate.decodeForStore(
                        aggregate.key(),
                        aggregate.canonicalBytes(),
                        new ShardId(source.shardId().routeIncarnation(), 4)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaIdentity(
                        identity.kind(), identity.shard(), new byte[16], identity.target(), null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaIdentity(
                        TargetQuotaIdentity.Kind.TENANT_TARGET,
                        identity.shard(),
                        repeat(16, 1),
                        identity.target(),
                        null));
    }

    @Test
    void legacyVectorAndEnvelopeReadersRejectNewPayloads() {
        assertThrows(IllegalArgumentException.class, () -> CapacityVector.decode(usage.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> LaneQuotaUsageMap.decode(counter.canonicalBytes()));
        for (int type : List.of(TargetQuotaCounter.VALUE_TYPE, TargetQuotaAggregate.VALUE_TYPE)) {
            final byte[] prefix = ByteBuffer.allocate(8)
                    .putShort((short) 0x4e56)
                    .put((byte) type)
                    .put((byte) 1)
                    .putInt(counter.canonicalBytes().length)
                    .array();
            final byte[] raw = Bytes.concat(prefix, counter.canonicalBytes());
            final byte[] value = Bytes.concat(raw, Bytes.crc32cbe(raw));
            assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        }
    }

    @Test
    void durableAndPhysicalResourcesAndLegacyCardinalitiesCannotAlias() {
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final int n = dimension.wireValue();
            final long[] values = new long[CapacityDimension.COUNT];
            values[n - 1] = 1;
            if (n <= 15 || (n >= 51 && n <= 55)) {
                assertEquals(
                        1,
                        new TargetQuotaUsage(new CapacityVector(values), 0, 0, 0, 0)
                                .resources()
                                .amount(dimension));
            } else {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new TargetQuotaUsage(new CapacityVector(values), 0, 0, 0, 0));
            }
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaCounter(
                        identity, new TargetQuotaUsage(usage.resources(), 1, 65, 0, 1), 1, counter.mutation()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaCounter(mirrorIdentity, usage, 1, counter.mutation()));
        final var shard = TargetQuotaIdentity.decode(hex("shard.identity"));
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaCounter(shard, usage, 1, counter.mutation()));
    }

    @Test
    void checkedResourceAndCardinalityArithmeticNeverSaturates() {
        final var one = resource(1, 0);
        final var max = resource(Long.MAX_VALUE, 0);
        assertThrows(ArithmeticException.class, () -> max.add(one));
        assertThrows(IllegalStateException.class, () -> one.subtract(max));
        final var cardinality = new TargetQuotaUsage(CapacityVector.empty(), Long.MAX_VALUE, 0, 0, 0);
        assertThrows(
                ArithmeticException.class,
                () -> cardinality.add(new TargetQuotaUsage(CapacityVector.empty(), 1, 0, 0, 0)));
        assertThrows(IllegalStateException.class, () -> TargetQuotaUsage.empty().subtract(cardinality));
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaUsage(CapacityVector.empty(), -1, 0, 0, 0));
    }

    @Test
    void localAndAggregateRevisionsAreIndependentAndUnaffectedCountersStayIdentical() {
        final var second =
                new TargetQuotaIdentity(identity.kind(), identity.shard(), repeat(16, 0x25), identity.target(), null);
        final var old = new TargetQuotaCounter(second, usage, 1, counter.mutation());
        final var sum = new TargetQuotaAggregate(
                aggregate.shard(), aggregate.accountingIncarnation(), usage.add(usage), 1, counter.mutation());
        final var map = new HashMap<>(Map.of(identity, counter, second, old, mirrorIdentity, mirror));
        final var plan = plan(
                sum,
                1,
                source,
                List.of(
                        new TargetQuotaDelta.Update(identity, changedUsage(3)),
                        new TargetQuotaDelta.Update(mirrorIdentity, changedMirrorUsage(3))),
                map);
        assertEquals(2, plan.changes().size());
        assertEquals(2, plan.nextAggregate().revision());
        assertEquals(1, map.get(second).revision());
        assertArrayEquals(old.canonicalBytes(), map.get(second).canonicalBytes());
        assertEquals(5, plan.nextAggregate().usage().resources().amount(CapacityDimension.ACTIVE_MESSAGES));
        assertSame(counter, map.get(identity));
        plan.requireCurrent(sum, 1, source, map::get);
        for (var change : plan.changes()) {
            map.put(change.next().identity(), change.next());
        }
        final var rebuilt = new HashMap<TargetQuotaIdentity, TargetQuotaUsage>();
        map.forEach((key, value) -> rebuilt.put(key, value.usage()));
        TargetQuotaDelta.audit(plan.nextAggregate(), map.values(), rebuilt);
    }

    @Test
    void sequenceCanAdvanceWithoutQuotaAndNoopDoesNotRewriteAggregate() {
        final var map = Map.of(identity, counter);
        final var plan = plan(aggregate, 5, at(14), List.of(new TargetQuotaDelta.Update(identity, usage)), map);
        assertTrue(plan.changes().isEmpty());
        assertSame(aggregate, plan.nextAggregate());
        assertEquals(6, plan.mutation().sequence());
        final var changed =
                plan(aggregate, 5, at(14), List.of(new TargetQuotaDelta.Update(identity, changedUsage(3))), map);
        assertEquals(2, changed.changes().getFirst().next().revision());
        assertEquals(2, changed.nextAggregate().revision());
        assertEquals(6, changed.nextAggregate().mutation().sequence());
    }

    @Test
    void replayAndChangedViewsCannotApplyTheSameChargeTwice() {
        final var map = new HashMap<>(Map.of(identity, counter));
        final var plan =
                plan(aggregate, 1, source, List.of(new TargetQuotaDelta.Update(identity, changedUsage(3))), map);
        assertThrows(IllegalStateException.class, () -> plan.requireCurrent(plan.nextAggregate(), 2, at(11), map::get));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.prepare(
                        aggregate,
                        1,
                        source,
                        source,
                        repeat(32, 0x66),
                        List.of(new TargetQuotaDelta.Update(identity, changedUsage(3))),
                        2,
                        map::get));
        map.put(identity, plan.changes().getFirst().next());
        assertThrows(IllegalStateException.class, () -> plan.requireCurrent(aggregate, 1, source, map::get));
    }

    @Test
    void sourceResourceAndSameOffsetMetadataDriftAreRejected() {
        final var altered = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                source.offset(),
                1,
                source.brokerLogAppendTimeEpochMs());
        final var foreign =
                new KafkaSourcePosition(source.shardId(), "another-cluster", source.nativeTopicUuid(), 11, null, 101);
        assertThrows(IllegalStateException.class, () -> plan(aggregate, 1, altered, List.of(), Map.of()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaDelta.prepare(
                        aggregate, 1, source, foreign, repeat(32, 0x66), List.of(), 2, ignored -> null));
        final var bogus =
                new TargetQuotaCounter(identity, usage, 1, new TargetQuotaMutation(1, altered, repeat(32, 0x66)));
        assertThrows(IllegalStateException.class, () -> aggregate.requireCounter(bogus));
    }

    @Test
    void duplicateIdentityAndTouchedBudgetAreRejectedBeforeAnyPlanIsPublished() {
        final var update = new TargetQuotaDelta.Update(identity, changedUsage(3));
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(aggregate, 1, source, List.of(update, update), Map.of(identity, counter)));
        final var reads = new AtomicInteger();
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaDelta.prepare(
                        aggregate, 1, source, at(11), repeat(32, 0x66), List.of(update, update), 1, id -> {
                            reads.incrementAndGet();
                            return counter;
                        }));
        assertEquals(0, reads.get());
    }

    @Test
    void deltaReadsOnlyOneCounterAsTargetPopulationGrows() {
        for (int population : List.of(1, 100, 10000)) {
            final var map = new HashMap<TargetQuotaIdentity, TargetQuotaCounter>();
            map.put(identity, counter);
            TargetQuotaUsage sum = usage;
            for (int n = 1; n < population; n++) {
                final byte[] targetBytes = repeat(32, 0x70);
                ByteBuffer.wrap(targetBytes).putInt(n);
                final var id = new TargetQuotaIdentity(
                        identity.kind(),
                        identity.shard(),
                        identity.accountingIncarnation(),
                        new TargetPartitionId(targetBytes),
                        null);
                map.put(id, new TargetQuotaCounter(id, usage, 1, counter.mutation()));
                sum = sum.add(usage);
            }
            final var total = new TargetQuotaAggregate(
                    aggregate.shard(), aggregate.accountingIncarnation(), sum, 1, counter.mutation());
            final var reads = new AtomicInteger();
            final var plan = TargetQuotaDelta.prepare(
                    total,
                    1,
                    source,
                    at(11),
                    repeat(32, 0x66),
                    List.of(new TargetQuotaDelta.Update(identity, changedUsage(3))),
                    1,
                    id -> {
                        reads.incrementAndGet();
                        return map.get(id);
                    });
            assertEquals(1, reads.get());
            assertEquals(1, plan.changes().size());
            assertEquals(
                    2L * population + 1,
                    plan.nextAggregate().usage().resources().amount(CapacityDimension.ACTIVE_MESSAGES));
            assertSame(counter, map.get(identity));
        }
    }

    @Test
    void netNeutralOwnershipTransferDoesNotOverflowBecauseOfUpdateOrder() {
        final var oldId =
                new TargetQuotaIdentity(identity.kind(), identity.shard(), repeat(16, 0x26), identity.target(), null);
        final var full = new TargetQuotaCounter(oldId, resource(Long.MAX_VALUE, 1), 1, counter.mutation());
        final var total = new TargetQuotaAggregate(
                aggregate.shard(), aggregate.accountingIncarnation(), full.usage(), 1, counter.mutation());
        final var transfer = plan(
                total,
                1,
                source,
                List.of(
                        new TargetQuotaDelta.Update(identity, full.usage()),
                        new TargetQuotaDelta.Update(oldId, TargetQuotaUsage.empty())),
                Map.of(oldId, full));
        assertEquals(full.usage(), transfer.nextAggregate().usage());
        assertEquals(2, transfer.nextAggregate().revision());
        assertEquals(2, transfer.changes().size());
    }

    @Test
    void underflowAndOverflowLeaveInputsUntouched() {
        final var tooSmall = new TargetQuotaAggregate(
                aggregate.shard(), aggregate.accountingIncarnation(), TargetQuotaUsage.empty(), 1, counter.mutation());
        assertThrows(
                IllegalStateException.class,
                () -> plan(
                        tooSmall,
                        1,
                        source,
                        List.of(new TargetQuotaDelta.Update(identity, changedUsage(3))),
                        Map.of(identity, counter)));
        final var full = new TargetQuotaCounter(identity, resource(Long.MAX_VALUE, 1), 1, counter.mutation());
        final var total = new TargetQuotaAggregate(
                aggregate.shard(), aggregate.accountingIncarnation(), full.usage(), 1, counter.mutation());
        final var another =
                new TargetQuotaIdentity(identity.kind(), identity.shard(), repeat(16, 0x27), identity.target(), null);
        assertThrows(
                ArithmeticException.class,
                () -> plan(
                        total,
                        1,
                        source,
                        List.of(new TargetQuotaDelta.Update(another, resource(1, 1))),
                        Map.of(identity, full)));
        assertEquals(Long.MAX_VALUE, total.usage().resources().amount(CapacityDimension.ACTIVE_MESSAGES));
        assertArrayEquals(hex("counter"), counter.canonicalBytes());
    }

    @Test
    void unsignedRevisionOverflowAndRetiredIdentityRevivalAreRejected() {
        assertEquals(Long.MIN_VALUE, TargetQuotaMutation.increment(Long.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> TargetQuotaMutation.increment(-1));
        final var exhausted =
                new TargetQuotaCounter(identity, usage, -1, new TargetQuotaMutation(-1, source, repeat(32, 0x66)));
        assertThrows(
                IllegalStateException.class,
                () -> exhausted.advance(changedUsage(3), new TargetQuotaMutation(-1, at(11), repeat(32, 0x66))));
        final var retired =
                counter.advance(TargetQuotaUsage.empty(), new TargetQuotaMutation(2, at(11), repeat(32, 0x66)));
        assertThrows(
                IllegalStateException.class,
                () -> retired.advance(usage, new TargetQuotaMutation(3, at(12), repeat(32, 0x66))));
        assertThrows(
                IllegalStateException.class,
                () -> counter.advance(usage, new TargetQuotaMutation(2, at(11), repeat(32, 0x66))));
    }

    @Test
    void newCountersStartAtOneAndGenesisContainsNoUsage() {
        final var genesis = TargetQuotaAggregate.genesis(aggregate.shard(), aggregate.accountingIncarnation());
        final var plan = TargetQuotaDelta.prepare(
                genesis,
                0,
                null,
                source,
                repeat(32, 0x66),
                List.of(
                        new TargetQuotaDelta.Update(identity, usage),
                        new TargetQuotaDelta.Update(mirrorIdentity, mirrorUsage)),
                2,
                ignored -> null);
        assertEquals(1, plan.nextAggregate().revision());
        assertEquals(usage, plan.nextAggregate().usage());
        assertTrue(plan.changes().stream()
                .allMatch(change -> change.prior() == null && change.next().revision() == 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaAggregate(aggregate.shard(), aggregate.accountingIncarnation(), usage, 0, null));
    }

    @Test
    void grantReductionAllowsReleaseButRejectsNewGrowthPerDimension() {
        final var lower = resource(1, 1);
        final var current = resource(5, 1);
        assertTrue(lower.permitsGrowth(current, resource(4, 1)));
        assertTrue(lower.permitsGrowth(current, current));
        assertFalse(lower.permitsGrowth(current, resource(6, 1)));
        assertTrue(lower.permitsGrowth(TargetQuotaUsage.empty(), resource(1, 1)));
        assertFalse(lower.permitsGrowth(current, new TargetQuotaUsage(current.resources(), 1, 0, 0, 1)));
    }

    @Test
    void auditRejectsAggregateLedgerMirrorAndRevisionDisagreement() {
        final var list = List.of(counter, mirror);
        final var rebuilt = Map.of(identity, usage, mirrorIdentity, mirrorUsage);
        TargetQuotaDelta.audit(aggregate, list, rebuilt);
        assertThrows(IllegalStateException.class, () -> TargetQuotaDelta.audit(aggregate, List.of(counter), rebuilt));
        assertThrows(
                IllegalStateException.class, () -> TargetQuotaDelta.audit(aggregate, list, Map.of(identity, usage)));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.audit(aggregate, List.of(counter, counter), Map.of(identity, usage)));
        final var wrongTotal = new TargetQuotaAggregate(
                aggregate.shard(), aggregate.accountingIncarnation(), usage.add(usage), 1, counter.mutation());
        assertThrows(IllegalStateException.class, () -> TargetQuotaDelta.audit(wrongTotal, list, rebuilt));
        final var secondTenant = new TargetQuotaIdentity(
                mirrorIdentity.kind(),
                identity.shard(),
                identity.accountingIncarnation(),
                identity.target(),
                repeat(32, 0x45));
        final var overMirror = new TargetQuotaCounter(secondTenant, mirrorUsage, 1, counter.mutation());
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaDelta.audit(
                        aggregate,
                        List.of(counter, mirror, overMirror),
                        Map.of(identity, usage, mirrorIdentity, mirrorUsage, secondTenant, mirrorUsage)));
        final var futureRevision =
                new TargetQuotaCounter(identity, usage, 2, new TargetQuotaMutation(2, at(11), repeat(32, 0x66)));
        assertThrows(IllegalStateException.class, () -> aggregate.requireCounter(futureRevision));
    }

    @Test
    void accessorsAndPreparedPlansDoNotExposeMutableInputs() {
        final byte[] original = identity.canonicalBytes();
        identity.accountingIncarnation()[0] ^= 1;
        identity.digest()[0] ^= 1;
        mirrorIdentity.tenantScope()[0] ^= 1;
        counter.mutation().mutationDigest()[0] ^= 1;
        usage.resources().amounts()[0] = 0;
        assertArrayEquals(original, identity.canonicalBytes());
        assertArrayEquals(hex("counter"), counter.canonicalBytes());
        final var updates = new ArrayList<>(List.of(new TargetQuotaDelta.Update(identity, changedUsage(3))));
        final var plan = plan(aggregate, 1, source, updates, Map.of(identity, counter));
        updates.clear();
        assertEquals(1, plan.changes().size());
        assertThrows(UnsupportedOperationException.class, () -> plan.changes().clear());
    }

    private TargetQuotaDelta plan(
            final TargetQuotaAggregate total,
            final long sequence,
            final SourcePosition last,
            final List<TargetQuotaDelta.Update> updates,
            final Map<TargetQuotaIdentity, TargetQuotaCounter> counters) {
        return TargetQuotaDelta.prepare(
                total,
                sequence,
                last,
                at(((KafkaSourcePosition) last).offset() + 1),
                repeat(32, 0x66),
                updates,
                4,
                counters::get);
    }

    private KafkaSourcePosition at(final long offset) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                offset,
                null,
                100 + offset - 10);
    }

    private TargetQuotaUsage changedUsage(final long messages) {
        final long[] values = usage.resources().amounts();
        values[0] = messages;
        return new TargetQuotaUsage(new CapacityVector(values), 1, 1, 0, 1);
    }

    private TargetQuotaUsage changedMirrorUsage(final long messages) {
        return new TargetQuotaUsage(changedUsage(messages).resources(), 1, 0, 0, 0);
    }

    private static TargetQuotaUsage resource(final long messages, final long incarnations) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[0] = messages;
        return new TargetQuotaUsage(new CapacityVector(values), 0, 0, 0, incarnations);
    }

    private java.util.function.Consumer<byte[]> decoderFor(final byte[] value) {
        if (Arrays.equals(value, hex("counter"))) {
            return TargetQuotaCounter::decode;
        }
        if (Arrays.equals(value, hex("aggregate"))) {
            return TargetQuotaAggregate::decode;
        }
        return Arrays.equals(value, hex("usage")) ? TargetQuotaUsage::decode : TargetQuotaIdentity::decode;
    }

    private void maximum(final String name, final byte[] raw, final int bound) {
        assertTrue(raw.length <= bound);
        assertEquals(Integer.parseInt(vectors.getProperty("maximum." + name + ".length")), raw.length);
        assertArrayEquals(hex("maximum." + name + ".sha256"), Bytes.sha256(raw));
    }

    private byte[] hex(final String key) {
        return HexFormat.of().parseHex(vectors.getProperty(key));
    }

    private static Properties load() {
        final var result = new Properties();
        try (var input = TargetQuotaContractTest.class.getResourceAsStream("/ndip3/target-quota-vectors.properties")) {
            result.load(input);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        return result;
    }

    private static byte[] repeat(final int length, final int value) {
        final byte[] raw = new byte[length];
        Arrays.fill(raw, (byte) value);
        return raw;
    }
}
