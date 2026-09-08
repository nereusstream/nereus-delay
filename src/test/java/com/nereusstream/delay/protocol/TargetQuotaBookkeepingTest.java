package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetQuotaBookkeepingTest {
    private final Properties vectors = properties("target-quota-bookkeeping-vectors.properties");
    private final Properties prior = properties("target-quota-vectors.properties");
    private final Properties attempts = properties("target-quota-accounting-vectors.properties");
    private final Properties grants = properties("target-quota-grant-vectors.properties");
    private final TargetQuotaIdentity owner = TargetQuotaIdentity.decode(raw(prior, "shard.identity"));
    private final TargetQuotaAccounting accounting = TargetQuotaAccounting.decode(raw(attempts, "accounting"));
    private final byte[] tenant = bytes(32, 0x44);

    @Test
    void independentInventorySourceAndChargeVectorsMatchEveryBranch() {
        for (String name : List.of("initial", "populated", "epoch", "pulsar")) {
            final SourcePosition source = TargetSourcePosition.decode(raw(vectors, name + ".source"));
            final var inventory =
                    switch (name) {
                        case "populated" -> new TargetQuotaBookkeeping.Inventory(6, 2, 3);
                        case "pulsar" -> new TargetQuotaBookkeeping.Inventory(4, 1, 2);
                        default -> new TargetQuotaBookkeeping.Inventory(2, 0, 0);
                    };
            final long revision = name.equals("epoch") ? -1L : 1;
            final var value = new TargetQuotaBookkeeping(
                    owner,
                    tenant,
                    accounting,
                    inventory,
                    revision,
                    new TargetQuotaMutation(revision, source, bytes(32, 0x66)));
            assertArrayEquals(raw(vectors, name + ".anchor"), value.canonicalBytes());
            assertArrayEquals(
                    value.canonicalBytes(),
                    TargetQuotaBookkeeping.decode(value.canonicalBytes()).canonicalBytes());
            assertEquals(CapacityVector.decode(raw(vectors, name + ".charge")), value.charge());
            assertEquals(number(name + ".sourceBound"), TargetQuotaBookkeeping.maximumSourceBytes(source));
            assertEquals(
                    number(name + ".fee.counter"), value.projectionBytes(TargetQuotaBookkeeping.Projection.COUNTER));
            assertEquals(number(name + ".fee.total"), value.projectionBytes(TargetQuotaBookkeeping.Projection.TOTAL));
            assertEquals(
                    number(name + ".fee.activation"),
                    value.projectionBytes(TargetQuotaBookkeeping.Projection.GRANT_ACTIVATION));
            assertArrayEquals(raw(vectors, "key"), value.key());
        }
    }

    @Test
    void anchorAndAggregateAndBothCounterRecordsAreChargedExactlyOnce() {
        final var value = initial();
        final long expected =
                number("initial.fee.anchor") + number("initial.fee.aggregate") + 2 * number("initial.fee.counter");
        assertEquals(expected, value.charge().amount(CapacityDimension.LOGICAL_STATE_BYTES));
        for (var dimension : CapacityDimension.values()) {
            if (dimension != CapacityDimension.LOGICAL_STATE_BYTES) {
                assertEquals(0, value.charge().amount(dimension));
            }
        }
        assertEquals(owner, value.owner());
        assertEquals(owner, value.tenantOwner().primary());
        assertArrayEquals(tenant, value.tenantOwner().tenantScope());
    }

    @Test
    void changingUsageRevisionOrKafkaEpochDoesNotRepriceProjectionSlots() {
        final var value = initial();
        final var next =
                value.advance(value.inventory().change(TargetQuotaBookkeeping.Projection.TOTAL, 0, 1), stamp(2));
        assertEquals(
                value.projectionBytes(TargetQuotaBookkeeping.Projection.COUNTER),
                next.projectionBytes(TargetQuotaBookkeeping.Projection.COUNTER));
        assertEquals(
                value.charge().amount(CapacityDimension.LOGICAL_STATE_BYTES) + number("initial.fee.total"),
                next.charge().amount(CapacityDimension.LOGICAL_STATE_BYTES));
        assertEquals(2, next.revision());
        assertArrayEquals(value.accounting().canonicalBytes(), next.accounting().canonicalBytes());
        assertEquals(
                value.charge(),
                TargetQuotaBookkeeping.decode(raw(vectors, "epoch.anchor")).charge());
        assertThrows(IllegalStateException.class, () -> value.advance(value.inventory(), stamp(2)));
    }

    @Test
    void retiredCounterRecordsKeepTheirProjectionCommitmentUntilActualDeletion() {
        final var value = new TargetQuotaBookkeeping(
                owner, tenant, accounting, new TargetQuotaBookkeeping.Inventory(4, 0, 0), 1, stamp(1));
        final var target = TargetQuotaIdentity.decode(raw(prior, "target.identity"));
        final var mirror = TargetQuotaIdentity.decode(raw(prior, "tenantTarget.identity"));
        final var primaryRoot = rootCounter(value, false, value.charge());
        final var tenantRoot = rootCounter(value, true, value.charge());
        final var retired = new TargetQuotaCounter(target, TargetQuotaUsage.empty(), 1, stamp(1));
        final var retiredMirror = new TargetQuotaCounter(mirror, TargetQuotaUsage.empty(), 1, stamp(1));
        value.auditInventory(
                aggregate(value), List.of(primaryRoot, tenantRoot, retired, retiredMirror), List.of(), List.of());
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), List.of(primaryRoot, tenantRoot), List.of(), List.of()));
        final var removed =
                value.advance(value.inventory().change(TargetQuotaBookkeeping.Projection.COUNTER, 2, 0), stamp(2));
        assertEquals(initial().charge(), removed.charge());
        assertEquals(
                value.charge().amount(CapacityDimension.LOGICAL_STATE_BYTES) - 2 * number("initial.fee.counter"),
                removed.charge().amount(CapacityDimension.LOGICAL_STATE_BYTES));
    }

    @Test
    void inventoryAuditRejectsDuplicatesMissingRootsAndUnderchargedMirrors() {
        final var value = initial();
        final var primary = rootCounter(value, false, value.charge());
        final var mirror = rootCounter(value, true, value.charge());
        value.auditInventory(aggregate(value), List.of(primary, mirror), List.of(), List.of());
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), List.of(primary, primary), List.of(), List.of()));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), List.of(primary), List.of(), List.of()));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(
                        aggregate(value),
                        List.of(primary, rootCounter(value, true, CapacityVector.empty())),
                        List.of(),
                        List.of()));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(
                        aggregate(value),
                        List.of(rootCounter(value, false, CapacityVector.empty()), mirror),
                        List.of(),
                        List.of()));
    }

    @Test
    void inventoryAuditCountsTotalsAndGrantActivationsIncludingZeroTotals() {
        final var value = new TargetQuotaBookkeeping(
                owner, tenant, accounting, new TargetQuotaBookkeeping.Inventory(2, 1, 1), 1, stamp(1));
        final var grant = TargetQuotaGrantActivation.decode(raw(grants, "target.initial.activation"));
        final var total = new TargetQuotaTotal(grant.grant().scope(), TargetQuotaUsage.empty(), 1, stamp(1));
        final var roots = List.of(rootCounter(value, false, value.charge()), rootCounter(value, true, value.charge()));
        value.auditInventory(aggregate(value), roots, List.of(total), List.of(grant));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), roots, List.of(total, total), List.of(grant)));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), roots, List.of(total), List.of(grant, grant)));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), roots, List.of(), List.of(grant)));
    }

    @Test
    void inventoryAuditRejectsForeignPhysicalSourcesAndTenantScopes() {
        final var value = initial();
        final var primary = rootCounter(value, false, value.charge());
        final var foreignTenant = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_SHARD,
                owner.shard(),
                owner.accountingIncarnation(),
                null,
                bytes(32, 0x45));
        final var foreignMirror =
                new TargetQuotaCounter(foreignTenant, new TargetQuotaUsage(value.charge(), 0, 0, 0, 0), 1, stamp(1));
        assertThrows(
                IllegalStateException.class,
                () -> value.auditInventory(aggregate(value), List.of(primary, foreignMirror), List.of(), List.of()));
        final var kafka = (KafkaSourcePosition) stamp(1).source();
        final var other =
                new KafkaSourcePosition(owner.shard(), kafka.authenticatedClusterId(), new UUID(1, 2), 10, null, 100);
        final var otherCounter = new TargetQuotaCounter(
                value.tenantOwner(),
                new TargetQuotaUsage(value.charge(), 0, 0, 0, 0),
                1,
                new TargetQuotaMutation(1, other, bytes(32, 0x66)));
        assertThrows(
                IllegalArgumentException.class,
                () -> value.auditInventory(aggregate(value), List.of(primary, otherCounter), List.of(), List.of()));
    }

    @Test
    void budgetRecordFeeSurvivesEveryPhaseAndUsesItsOwnFrozenAccounting() {
        final var value = initial();
        for (String phase : List.of("admitted", "unknown", "resolved", "retained", "released")) {
            final var budget = TargetQuotaAttemptBudget.decode(raw(attempts, phase + ".budget"));
            assertEquals(
                    number("initial.fee.budget"),
                    value.attemptRecordCharge(budget).amount(CapacityDimension.LOGICAL_STATE_BYTES));
            assertTrue(value.attemptRecordCharge(budget).amount(CapacityDimension.LOGICAL_STATE_BYTES)
                    >= budget.accounting().storedRecordBytes(budget.key().length, budget.canonicalBytes().length));
            assertEquals(
                    TargetQuotaIdentity.Kind.TARGET, budget.primaryIdentity().kind());
        }
        final var changedRoot = new TargetQuotaBookkeeping(
                owner,
                tenant,
                new TargetQuotaAccounting(bytes(32, 0xbb), 128, 24, 40, 64),
                value.inventory(),
                1,
                stamp(1));
        final var released = TargetQuotaAttemptBudget.decode(raw(attempts, "released.budget"));
        assertTrue(released.effectiveCharge().isZero());
        assertEquals(value.attemptRecordCharge(released), changedRoot.attemptRecordCharge(released));
        assertNotEquals(value.charge(), changedRoot.charge());
    }

    @Test
    void budgetRecordCannotBeMovedToAnotherTenantOrPhysicalRoute() {
        final var budget = TargetQuotaAttemptBudget.decode(raw(attempts, "admitted.budget"));
        final var foreign = new TargetQuotaBookkeeping(
                owner, bytes(32, 0x45), accounting, initial().inventory(), 1, stamp(1));
        assertThrows(IllegalArgumentException.class, () -> foreign.attemptRecordCharge(budget));
        final var pulsar = TargetQuotaBookkeeping.decode(raw(vectors, "pulsar.anchor"));
        assertThrows(IllegalArgumentException.class, () -> pulsar.attemptRecordCharge(budget));
    }

    @Test
    void netInventoryChangesSubtractBeforeAddingAndRespectRootMinimum() {
        final var huge = new TargetQuotaBookkeeping.Inventory(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(huge, huge.change(TargetQuotaBookkeeping.Projection.COUNTER, Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(
                2,
                huge.change(TargetQuotaBookkeeping.Projection.COUNTER, Long.MAX_VALUE - 2, 0)
                        .counters());
        assertThrows(ArithmeticException.class, () -> huge.change(TargetQuotaBookkeeping.Projection.TOTAL, 0, 1));
        assertThrows(
                IllegalStateException.class,
                () -> initial().inventory().change(TargetQuotaBookkeeping.Projection.TOTAL, 1, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> initial().inventory().change(TargetQuotaBookkeeping.Projection.COUNTER, 1, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> initial().inventory().change(TargetQuotaBookkeeping.Projection.TOTAL, -1, 0));
    }

    @Test
    void finiteCapacityRejectsMultiplicationAndTotalChargeOverflow() {
        assertThrows(
                ArithmeticException.class,
                () -> new TargetQuotaBookkeeping(
                        owner,
                        tenant,
                        accounting,
                        new TargetQuotaBookkeeping.Inventory(Long.MAX_VALUE, 0, 0),
                        1,
                        stamp(1)));
        assertThrows(
                ArithmeticException.class,
                () -> new TargetQuotaBookkeeping(
                        owner,
                        tenant,
                        new TargetQuotaAccounting(bytes(32, 0xbb), Long.MAX_VALUE, 0, 0, 1),
                        initial().inventory(),
                        1,
                        stamp(1)));
        final long count = Long.MAX_VALUE / initial().projectionBytes(TargetQuotaBookkeeping.Projection.COUNTER);
        assertThrows(
                ArithmeticException.class,
                () -> new TargetQuotaBookkeeping(
                        owner, tenant, accounting, new TargetQuotaBookkeeping.Inventory(count, 1, 1), 1, stamp(1)));
    }

    @Test
    void inventoryAdvancementRejectsReplaySourceReplacementAndRevisionExhaustion() {
        final var value = initial();
        final var next = value.inventory().change(TargetQuotaBookkeeping.Projection.TOTAL, 0, 1);
        assertThrows(IllegalStateException.class, () -> value.advance(next, stamp(1)));
        final var kafka = (KafkaSourcePosition) stamp(2).source();
        final var other =
                new KafkaSourcePosition(owner.shard(), kafka.authenticatedClusterId(), new UUID(1, 2), 11, null, 101);
        assertThrows(
                IllegalArgumentException.class,
                () -> value.advance(next, new TargetQuotaMutation(2, other, bytes(32, 0x66))));
        final var exhausted = TargetQuotaBookkeeping.decode(raw(vectors, "epoch.anchor"));
        assertThrows(IllegalStateException.class, () -> exhausted.advance(next, stamp(2)));
    }

    @Test
    void maximumSourceBoundUsesUtf8BytesAndReservesKafkaEpochPresence() {
        final var kafka = (KafkaSourcePosition) stamp(1).source();
        final var wide = new KafkaSourcePosition(
                owner.shard(), "é".repeat(128), kafka.nativeTopicUuid(), -1, -1, Long.MAX_VALUE);
        assertEquals(TargetSourcePosition.MAX_KAFKA_CANONICAL_BYTES, TargetQuotaBookkeeping.maximumSourceBytes(wide));
        assertEquals(number("initial.sourceBound"), TargetQuotaBookkeeping.maximumSourceBytes(kafka));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.maximumSourceBytes(
                        new KafkaSourcePosition(owner.shard(), "é".repeat(129), kafka.nativeTopicUuid(), 0, null, 0)));
    }

    @Test
    void maximumPulsarSourceAndUnsignedRevisionMatchIndependentDigest() {
        final var source = new PulsarSourcePosition(
                owner.shard(),
                bytes(32, 0x77),
                "x".repeat(1 << 20),
                -1,
                -1,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                Long.MAX_VALUE);
        final var value = new TargetQuotaBookkeeping(
                owner,
                tenant,
                accounting,
                initial().inventory(),
                -1,
                new TargetQuotaMutation(-1, source, bytes(32, 0x66)));
        final byte[] raw = value.canonicalBytes();
        assertEquals(number("maximum.length"), raw.length);
        assertEquals(vectors.getProperty("maximum.sha256"), Bytes.hex(Bytes.sha256(raw)));
        assertTrue(raw.length <= TargetQuotaBookkeeping.MAX_CANONICAL_BYTES);
        assertEquals(number("maximum.sourceBound"), TargetQuotaBookkeeping.maximumSourceBytes(source));
        assertEquals(number("maximum.fee.counter"), value.projectionBytes(TargetQuotaBookkeeping.Projection.COUNTER));
        assertArrayEquals(raw, TargetQuotaBookkeeping.decode(raw).canonicalBytes());
    }

    @Test
    void projectionCommitmentsCoverRealEncodingsAtLargeUsageAndRevisions() {
        final var value = initial();
        final var stamp =
                new TargetQuotaMutation(-1, TargetSourcePosition.decode(raw(vectors, "epoch.source")), bytes(32, 0x66));
        final long[] resources = new long[CapacityDimension.COUNT];
        for (int i = 0; i < 15; i++) {
            resources[i] = Long.MAX_VALUE;
        }
        final var identity = TargetQuotaIdentity.decode(raw(prior, "tenantTarget.identity"));
        final var counter = new TargetQuotaCounter(
                identity, new TargetQuotaUsage(new CapacityVector(resources), 1, 64, Long.MAX_VALUE, 0), -1, stamp);
        assertTrue(value.projectionBytes(TargetQuotaBookkeeping.Projection.COUNTER)
                >= accounting.storedRecordBytes(counter.identity().key().length, counter.canonicalBytes().length));
        assertTrue(number("initial.fee.anchor")
                >= accounting.storedRecordBytes(value.key().length, value.canonicalBytes().length));
        final var aggregate = new TargetQuotaAggregate(
                owner.shard(),
                owner.accountingIncarnation(),
                new TargetQuotaUsage(value.charge(), 0, 0, 0, 1),
                -1,
                stamp);
        assertTrue(number("initial.fee.aggregate")
                >= accounting.storedRecordBytes(aggregate.key().length, aggregate.canonicalBytes().length));
        for (String branch : List.of(
                "target.initial", "shard.initialTransfer", "target.replaceTransfer", "shard.replace", "target.zero")) {
            final var grant = TargetQuotaGrantActivation.decode(raw(grants, branch + ".activation"));
            assertTrue(value.projectionBytes(TargetQuotaBookkeeping.Projection.GRANT_ACTIVATION)
                    >= accounting.storedRecordBytes(grant.key().length, grant.canonicalBytes().length));
        }
        final var total = new TargetQuotaTotal(
                new TargetQuotaScope(owner.shard(), tenant, identity.target()),
                new TargetQuotaUsage(new CapacityVector(resources), 1, 64, Long.MAX_VALUE, Long.MAX_VALUE),
                -1,
                stamp);
        assertTrue(value.projectionBytes(TargetQuotaBookkeeping.Projection.TOTAL)
                >= accounting.storedRecordBytes(total.key().length, total.canonicalBytes().length));
    }

    @Test
    void storeDecodeBindsTheFullRootIncarnationKeyAndTenant() {
        final var value = initial();
        assertEquals(22, value.key().length);
        assertArrayEquals(
                value.canonicalBytes(),
                TargetQuotaBookkeeping.decodeForStore(value.key(), value.canonicalBytes(), owner, tenant)
                        .canonicalBytes());
        final var other =
                new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, owner.shard(), bytes(16, 0x23), null, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.decodeForStore(value.key(), value.canonicalBytes(), other, tenant));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.decodeForStore(
                        value.key(), value.canonicalBytes(), owner, bytes(32, 0x45)));
        final byte[] key = value.key();
        key[0]++;
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.decodeForStore(key, value.canonicalBytes(), owner, tenant));
        final byte[] copy = value.tenantScope();
        copy[0]++;
        assertArrayEquals(tenant, value.tenantScope());
    }

    @Test
    void closedWireAndInvalidOwnerOrInventoryCannotConstructAnAnchor() {
        final var value = initial();
        final byte[] encoded = value.canonicalBytes();
        final byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaBookkeeping.decode(corrupt));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.decode(Arrays.copyOf(encoded, encoded.length - 34)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.decode(Bytes.concat(encoded, new byte[] {88, 1})));
        final byte[] version = encoded.clone();
        version[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaBookkeeping.decode(version));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaBookkeeping.decode(new byte[TargetQuotaBookkeeping.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaBookkeeping(
                        value.tenantOwner(), tenant, accounting, value.inventory(), 1, stamp(1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaBookkeeping(owner, tenant, accounting, value.inventory(), 2, stamp(1)));
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaBookkeeping.Inventory(1, 0, 0));
    }

    @Test
    void oldNvReaderRejectsValidBookkeepingEnvelope() {
        final byte[] body = initial().canonicalBytes();
        final byte[] prefix = ByteBuffer.allocate(8)
                .putShort((short) 0x4e56)
                .put((byte) 31)
                .put((byte) 1)
                .putInt(body.length)
                .array();
        final byte[] raw = Bytes.concat(prefix, body);
        assertThrows(
                IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(Bytes.concat(raw, Bytes.crc32cbe(raw))));
    }

    @Test
    void rootRequiresActualMatchingAggregateAndConsistentSourceStamp() {
        final var value = initial();
        value.requireRoot(aggregate(value));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireRoot(TargetQuotaAggregate.genesis(owner.shard(), owner.accountingIncarnation())));
        final var usage = aggregate(value).usage();
        assertThrows(
                IllegalStateException.class,
                () -> value.requireRoot(new TargetQuotaAggregate(owner.shard(), bytes(16, 0x23), usage, 1, stamp(1))));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireRoot(new TargetQuotaAggregate(
                        owner.shard(),
                        owner.accountingIncarnation(),
                        usage,
                        1,
                        new TargetQuotaMutation(1, stamp(1).source(), bytes(32, 0x67)))));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireRoot(new TargetQuotaAggregate(
                        owner.shard(),
                        owner.accountingIncarnation(),
                        usage,
                        1,
                        new TargetQuotaMutation(2, stamp(1).source(), bytes(32, 0x66)))));
        value.requireRoot(new TargetQuotaAggregate(owner.shard(), owner.accountingIncarnation(), usage, 2, stamp(2)));
    }

    private TargetQuotaAggregate aggregate(final TargetQuotaBookkeeping value) {
        return new TargetQuotaAggregate(
                owner.shard(),
                owner.accountingIncarnation(),
                new TargetQuotaUsage(value.charge(), 0, 0, 0, 1),
                1,
                value.mutation());
    }

    private TargetQuotaBookkeeping initial() {
        return new TargetQuotaBookkeeping(
                owner, tenant, accounting, new TargetQuotaBookkeeping.Inventory(2, 0, 0), 1, stamp(1));
    }

    private TargetQuotaMutation stamp(final long n) {
        final var source = (KafkaSourcePosition) TargetSourcePosition.decode(raw(vectors, "initial.source"));
        return new TargetQuotaMutation(
                n,
                new KafkaSourcePosition(
                        source.shardId(),
                        source.authenticatedClusterId(),
                        source.nativeTopicUuid(),
                        9 + n,
                        null,
                        99 + n),
                bytes(32, 0x66));
    }

    private TargetQuotaCounter rootCounter(
            final TargetQuotaBookkeeping value, final boolean mirror, final CapacityVector charge) {
        return new TargetQuotaCounter(
                mirror ? value.tenantOwner() : owner,
                new TargetQuotaUsage(charge, 0, 0, 0, mirror ? 0 : 1),
                1,
                stamp(1));
    }

    private long number(final String name) {
        return Long.parseLong(vectors.getProperty(name));
    }

    private static byte[] raw(final Properties properties, final String name) {
        return HexFormat.of().parseHex(properties.getProperty(name));
    }

    private static byte[] bytes(final int size, final int value) {
        final byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static Properties properties(final String name) {
        try (var input = TargetQuotaBookkeepingTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var result = new Properties();
            result.load(input);
            return result;
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
