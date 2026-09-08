package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.TargetOrderState;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaIncarnationTest {
    private final Properties vectors = properties("target-quota-incarnation-vectors.properties");
    private final Properties scopes = properties("target-quota-scope-vectors.properties");
    private final TargetQuotaScope scope = TargetQuotaScope.decode(raw(scopes, "scope.target"));
    private final TargetQuotaAccounting accounting = TargetQuotaAccounting.decode(raw(scopes, "accounting"));
    private final byte[] lineage = bytes(16, 0xcc);
    private final TargetQuotaIncarnation.StateAuthority allow = (prior, next) -> {};
    private final TargetQuotaIncarnation.RetirementAuthority release = (i, p, m, r, a, f, d) -> {};
    private final TargetQuotaBookkeeping root = TargetQuotaBookkeeping.decode(
            raw(properties("target-quota-bookkeeping-vectors.properties"), "initial.anchor"));

    @Test
    void targetAndShardOriginsMatchIndependentFullBytesKeysAndFixedCharges() {
        for (boolean target : List.of(true, false)) {
            final String name = target ? "target" : "shard";
            final var open = allocate(target ? scope : scope.shardScope());
            final var draining = open.drain(stamp(2), allow);
            for (var entry : List.of(open, draining)) {
                final String prefix = name + (entry.draining() ? ".draining" : ".open");
                assertArrayEquals(raw(vectors, prefix), entry.canonicalBytes());
                assertArrayEquals(raw(vectors, prefix + ".key"), entry.key());
                assertArrayEquals(
                        raw(vectors, prefix + ".identity"), entry.identity().canonicalBytes());
                assertEquals(TargetQuotaUsage.decode(raw(vectors, prefix + ".primary")), entry.ownContribution());
                assertEquals(TargetQuotaUsage.decode(raw(vectors, prefix + ".mirror")), entry.tenantContribution());
                assertArrayEquals(
                        entry.canonicalBytes(),
                        TargetQuotaIncarnation.decode(entry.canonicalBytes()).canonicalBytes());
                assertTrue(entry.ownContribution().resources().amount(CapacityDimension.LOGICAL_STATE_BYTES)
                        >= entry.key().length + entry.canonicalBytes().length + 12 + 32);
            }
            assertEquals(open.ownContribution(), draining.ownContribution());
            assertEquals(target ? 71 : 39, open.key().length);
        }
    }

    @Test
    void everyOriginComponentParticipatesInDeterministicAllocation() {
        final var initial = allocate(scope);
        assertArrayEquals(
                initial.identity().accountingIncarnation(),
                allocate(scope).identity().accountingIncarnation());
        final var otherAccount = new TargetQuotaAccounting(bytes(32, 0xab), 32, 24, 40, 64);
        final var changes = List.of(
                allocate(scope.shardScope()),
                allocate(new TargetQuotaScope(scope.shard(), bytes(32, 1), scope.target())),
                allocate(scope.forTarget(new TargetPartitionId(bytes(32, 2)))),
                TargetQuotaIncarnation.allocate(scope, otherAccount, lineage, stamp(1), allow),
                TargetQuotaIncarnation.allocate(scope, accounting, bytes(16, 1), stamp(1), allow),
                TargetQuotaIncarnation.allocate(scope, accounting, lineage, stamp(2), allow),
                TargetQuotaIncarnation.allocate(
                        scope,
                        accounting,
                        lineage,
                        new TargetQuotaMutation(1, stamp(1).source(), bytes(32, 1)),
                        allow));
        for (var changed : changes) {
            assertNotEquals(
                    Bytes.hex(initial.identity().accountingIncarnation()),
                    Bytes.hex(changed.identity().accountingIncarnation()));
        }
    }

    @Test
    void stateChangesRequireActualAuthorityAndPropagateFailures() {
        final var calls = new AtomicInteger();
        final var open = TargetQuotaIncarnation.allocate(scope, accounting, lineage, stamp(1), (prior, next) -> {
            assertEquals(null, prior);
            calls.incrementAndGet();
        });
        open.drain(stamp(2), (prior, next) -> {
            assertArrayEquals(open.canonicalBytes(), prior.canonicalBytes());
            assertTrue(next.draining());
            calls.incrementAndGet();
        });
        assertEquals(2, calls.get());
        assertThrows(
                NullPointerException.class,
                () -> TargetQuotaIncarnation.allocate(scope, accounting, lineage, stamp(1), null));
        assertThrows(
                IllegalStateException.class,
                () -> open.drain(stamp(2), (prior, next) -> {
                    throw new IllegalStateException("unavailable source authority");
                }));
        assertThrows(
                AssertionError.class,
                () -> open.drain(stamp(2), (prior, next) -> {
                    throw new AssertionError("fatal");
                }));
        assertFalse(open.draining());
    }

    @Test
    void drainIsOneWayAndDoesNotReleaseItsIncarnationOrMetadata() {
        final var open = allocate(scope);
        open.requireNewIngress(accounting);
        final var drained = open.drain(stamp(2), allow);
        assertThrows(IllegalStateException.class, () -> drained.requireNewIngress(accounting));
        assertThrows(IllegalStateException.class, () -> drained.drain(stamp(3), allow));
        assertThrows(IllegalStateException.class, () -> open.drain(stamp(1), allow));
        assertEquals(1, drained.ownContribution().accountingIncarnations());
        assertEquals(0, drained.tenantContribution().accountingIncarnations());
        assertEquals(drained.identity(), drained.tenantIdentity().primary());
        assertThrows(
                IllegalStateException.class,
                () -> open.requireNewIngress(new TargetQuotaAccounting(bytes(32, 1), 32, 24, 40, 64)));
    }

    @Test
    void queueRecordsOwnOneTargetAndOnlyActiveOrDrainingExecutionDomains() {
        final var physical = CanonicalTargetPartition.decode(
                raw(properties("target-identity-vectors.properties"), "pulsar.canonical"));
        final var origin = allocate(scope.forTarget(physical.id()));
        final var domains = List.of(
                domain(0, TargetDomainState.Lifecycle.ACTIVE),
                domain(1, TargetDomainState.Lifecycle.DRAINING),
                domain(2, TargetDomainState.Lifecycle.VACANT));
        final var queue = new TargetQueueState(
                physical.id(),
                1,
                1,
                TargetQueueState.AdmissionState.CLOSED,
                origin.identity().accountingIncarnation(),
                0,
                domains);
        final var contribution =
                origin.queueContribution(TargetKeyCodec.state(physical.id()), queue.canonicalBytes(), physical, 3);
        assertEquals(1, contribution.targets());
        assertEquals(2, contribution.executionDomains());
        assertEquals(0, contribution.accountingIncarnations());
        assertEquals(0, contribution.strictOrderDomains());
        assertEquals(
                accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, 34, queue.canonicalBytes().length),
                contribution.resources());
        assertThrows(
                IllegalArgumentException.class,
                () -> origin.queueContribution(
                        TargetKeyCodec.state(physical.id()), queue.canonicalBytes(), physical, 2));
        assertThrows(IllegalArgumentException.class, () -> allocate(scope)
                .queueContribution(TargetKeyCodec.state(physical.id()), queue.canonicalBytes(), physical, 3));
    }

    @Test
    void closedStrictDomainStillOwnsItsUniqueIdentityAndRecordBytes() {
        final var origin = allocate(scope);
        final var state = new TargetOrderState(
                scope.target(),
                bytes(32, 9),
                scope.shard(),
                new TargetKeyCodec.Domain(0, 1),
                origin.identity().accountingIncarnation(),
                TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO,
                1,
                1,
                TargetOrderState.Gate.CLOSED,
                null,
                null,
                null);
        final var contribution = origin.strictDomainContribution(state.encodedKey(), state.canonicalBytes());
        assertEquals(1, contribution.strictOrderDomains());
        assertEquals(0, contribution.executionDomains());
        assertEquals(0, contribution.targets());
        assertEquals(0, contribution.accountingIncarnations());
        final byte[] bad = state.encodedKey();
        bad[0]++;
        assertThrows(
                IllegalArgumentException.class, () -> origin.strictDomainContribution(bad, state.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> allocate(scope.shardScope())
                .strictDomainContribution(state.encodedKey(), state.canonicalBytes()));
    }

    @Test
    void retirementRequiresTheExactRemainingDescriptorAndInvokesFullAuthority() {
        final var origin = allocate(scope).drain(stamp(2), allow);
        final var calls = new AtomicInteger();
        origin.requireRetirable(
                primary(origin, origin.ownContribution(), 3),
                mirror(origin, 3),
                root,
                aggregate(origin, 3),
                floor(3),
                stamp(4),
                (i, p, m, r, a, f, d) -> {
                    assertArrayEquals(origin.canonicalBytes(), i.canonicalBytes());
                    assertEquals(3, p.mutation().sequence());
                    assertEquals(4, d.sequence());
                    calls.incrementAndGet();
                });
        assertEquals(1, calls.get());
        assertThrows(NullPointerException.class, () -> retire(origin, floor(3), null));
        assertThrows(
                IllegalStateException.class,
                () -> retire(origin, floor(3), (i, p, m, r, a, f, d) -> {
                    throw new IllegalStateException("still pinned by actual ledger");
                }));
    }

    @Test
    void resourcesQueueAndStrictIdentitiesMustDrainBeforeRetirement() {
        final var origin = allocate(scope).drain(stamp(2), allow);
        for (var extra : List.of(
                new TargetQuotaUsage(CapacityVector.empty(), 1, 0, 0, 0),
                new TargetQuotaUsage(CapacityVector.empty(), 0, 1, 0, 0),
                new TargetQuotaUsage(CapacityVector.empty(), 0, 0, 1, 0),
                new TargetQuotaUsage(TargetQuotaAccounting.retainedPayload(1), 0, 0, 0, 0))) {
            assertThrows(
                    IllegalStateException.class,
                    () -> origin.requireRetirable(
                            primary(origin, origin.ownContribution().add(extra), 3),
                            mirror(origin, 3),
                            root,
                            aggregate(origin, 3),
                            floor(3),
                            stamp(4),
                            release));
        }
        assertThrows(IllegalStateException.class, () -> retire(allocate(scope), floor(3), release));
    }

    @Test
    void floorMustCoverLatestCounterUpdateNotOnlyEarlierDrain() {
        final var origin = allocate(scope).drain(stamp(2), allow);
        assertThrows(IllegalStateException.class, () -> retire(origin, floor(2), release));
        final var f = floor(3);
        final var wrong = new RecoveryFloorRef(
                bytes(16, 1),
                f.checkpointId(),
                f.manifestSha256(),
                f.catalogGeneration(),
                f.appliedSourcePosition(),
                f.includedMutationSequence(),
                List.of());
        assertThrows(IllegalStateException.class, () -> retire(origin, wrong, release));
        assertThrows(
                IllegalStateException.class,
                () -> origin.requireRetirable(
                        primary(origin, origin.ownContribution(), 3),
                        mirror(origin, 3),
                        root,
                        aggregate(origin, 3),
                        floor(3),
                        stamp(3),
                        release));
    }

    @Test
    void floorSourceMetadataAndSequenceMustBothMatchTheCoveredPosition() {
        final var origin = allocate(scope).drain(stamp(2), allow);
        final var s = (KafkaSourcePosition) stamp(3).source();
        final var changed = new KafkaSourcePosition(
                s.shardId(), s.authenticatedClusterId(), s.nativeTopicUuid(), s.offset(), s.leaderEpoch(), 101);
        final var f = floor(3);
        for (var bad : List.of(
                new RecoveryFloorRef(lineage, f.checkpointId(), f.manifestSha256(), 3, changed, 3, List.of()),
                new RecoveryFloorRef(lineage, f.checkpointId(), f.manifestSha256(), 3, s, 2, List.of()))) {
            assertThrows(IllegalStateException.class, () -> retire(origin, bad, release));
        }
    }

    @Test
    void zeroOrForeignCounterDoesNotAuthorizeDescriptorDeletion() {
        final var origin = allocate(scope).drain(stamp(2), allow);
        assertThrows(
                IllegalStateException.class,
                () -> origin.requireRetirable(
                        primary(origin, TargetQuotaUsage.empty(), 3),
                        mirror(origin, 3),
                        root,
                        aggregate(origin, 3),
                        floor(3),
                        stamp(4),
                        release));
        final var foreign = allocate(scope.shardScope());
        assertThrows(
                IllegalStateException.class,
                () -> origin.requireRetirable(
                        primary(foreign, foreign.ownContribution(), 3),
                        mirror(origin, 3),
                        root,
                        aggregate(origin, 3),
                        floor(3),
                        stamp(4),
                        release));
    }

    @Test
    void aggregateMustCoverTheRootAndRetiringIncarnation() {
        final var origin = allocate(scope).drain(stamp(2), allow);
        final var tooSmall = new TargetQuotaAggregate(
                scope.shard(),
                root.owner().accountingIncarnation(),
                new TargetQuotaUsage(root.charge(), 0, 0, 0, 1),
                3,
                stamp(3));
        assertThrows(
                IllegalStateException.class,
                () -> origin.requireRetirable(
                        primary(origin, origin.ownContribution(), 3),
                        mirror(origin, 3),
                        root,
                        tooSmall,
                        floor(3),
                        stamp(4),
                        release));
    }

    @Test
    void currentStoreRootCannotBeRetired() {
        final var origin = allocate(scope.shardScope()).drain(stamp(2), allow);
        final var currentRoot = new TargetQuotaBookkeeping(
                origin.identity(),
                scope.tenantScope(),
                accounting,
                new TargetQuotaBookkeeping.Inventory(2, 0, 0),
                1,
                stamp(1));
        final var usage = new TargetQuotaUsage(currentRoot.charge(), 0, 0, 0, 1).add(origin.ownContribution());
        final var aggregate =
                new TargetQuotaAggregate(scope.shard(), origin.identity().accountingIncarnation(), usage, 3, stamp(3));
        assertThrows(
                IllegalStateException.class,
                () -> origin.requireRetirable(
                        primary(origin, origin.ownContribution(), 3),
                        mirror(origin, 3),
                        currentRoot,
                        aggregate,
                        floor(3),
                        stamp(4),
                        release));
    }

    @Test
    void frozenPayloadAndAttemptAttributionSurvivesDrainButCannotMoveToAnotherOrigin() {
        final var old = TargetScheduleBinding.decode(
                raw(properties("target-binding-channel-vectors.properties"), "binding.best"));
        final var allocation = new TargetQuotaMutation(1, old.bindingSource(), bytes(32, 0x66));
        final var origin = TargetQuotaIncarnation.allocate(
                new TargetQuotaScope(old.messageId().routingId().shardId(), scope.tenantScope(), old.target()),
                accounting,
                lineage,
                allocation,
                allow);
        final var binding = new TargetScheduleBinding(
                old.messageId(),
                old.commandType(),
                old.canonicalBody(),
                old.bindingSource(),
                old.target(),
                old.domain(),
                origin.identity().accountingIncarnation(),
                old.requiredDispatchRef(),
                old.offeredDispatchRef(),
                old.controlScopeRef(),
                old.membershipGrantRef(),
                old.nativePolicyScopeRef(),
                old.orderingDomain());
        final var payload =
                TargetQuotaPayloadOwner.scheduled(binding, scope.tenantScope(), accounting, lineage, allocation);
        origin.requirePayloadOwner(payload);
        final var locator = new TargetMessageLocator(
                binding.messageId(),
                1,
                binding.target(),
                binding.domain(),
                origin.identity().accountingIncarnation(),
                OrderingMode.BEST_EFFORT,
                null,
                binding.digest());
        final var budget = TargetQuotaAttemptBudget.admit(
                locator,
                scope.tenantScope(),
                bytes(32, 7),
                bytes(32, 8),
                accounting,
                1,
                accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, 1, 1),
                CapacityVector.empty(),
                allocation,
                lineage);
        origin.requireAttemptBudget(budget);
        final var s = (KafkaSourcePosition) allocation.source();
        final var next = new TargetQuotaMutation(
                2,
                new KafkaSourcePosition(
                        s.shardId(),
                        s.authenticatedClusterId(),
                        s.nativeTopicUuid(),
                        s.offset() + 1,
                        s.leaderEpoch(),
                        110),
                bytes(32, 0x66));
        origin.drain(next, allow).requirePayloadOwner(payload);
        origin.drain(next, allow).requireAttemptBudget(budget);
        final var replacement = TargetQuotaIncarnation.allocate(origin.scope(), accounting, lineage, next, allow);
        assertThrows(IllegalArgumentException.class, () -> replacement.requirePayloadOwner(payload));
        assertThrows(IllegalArgumentException.class, () -> replacement.requireAttemptBudget(budget));
    }

    @Test
    void maximumTwoSourceRecordMatchesIndependentBoundsAndDigest() {
        final var allocation = new TargetQuotaMutation(-2L, maximumSource(-2L), bytes(32, 0x66));
        final var open = TargetQuotaIncarnation.allocate(scope, accounting, lineage, allocation, allow);
        final var value = open.drain(new TargetQuotaMutation(-1L, maximumSource(-1L), bytes(32, 0x66)), allow);
        assertEquals(Integer.parseInt(vectors.getProperty("maximum.length")), value.canonicalBytes().length);
        assertEquals(vectors.getProperty("maximum.sha256"), Bytes.hex(Bytes.sha256(value.canonicalBytes())));
        assertTrue(value.canonicalBytes().length <= TargetQuotaIncarnation.MAX_CANONICAL_BYTES);
        assertEquals(TargetQuotaUsage.decode(raw(vectors, "maximum.primary")), value.ownContribution());
        assertArrayEquals(
                value.canonicalBytes(),
                TargetQuotaIncarnation.decode(value.canonicalBytes()).canonicalBytes());
    }

    @Test
    void keyTenantIntegrityUnknownFieldsAndLegacyEnvelopeFailClosed() {
        final var value = allocate(scope);
        assertArrayEquals(
                value.canonicalBytes(),
                TargetQuotaIncarnation.decodeForStore(
                                value.key(), value.canonicalBytes(), scope.shard(), scope.tenantScope())
                        .canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaIncarnation.decodeForStore(
                        value.key(), value.canonicalBytes(), scope.shard(), bytes(32, 1)));
        final byte[] corrupt = value.canonicalBytes();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaIncarnation.decode(corrupt));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaIncarnation.decode(Bytes.concat(value.canonicalBytes(), new byte[] {72, 1})));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaIncarnation.decode(new byte[TargetQuotaIncarnation.MAX_CANONICAL_BYTES + 1]));
        final byte[] prefix = ByteBuffer.allocate(8)
                .putShort((short) 0x4e56)
                .put((byte) 33)
                .put((byte) 1)
                .putInt(value.canonicalBytes().length)
                .array();
        final byte[] framed = Bytes.concat(prefix, value.canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(framed, Bytes.crc32cbe(framed))));
    }

    @Test
    void validIntegrityCannotSubstituteAnotherAllocatedIdentity() {
        final var value = allocate(scope);
        final var wrong = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET, scope.shard(), bytes(16, 1), scope.target(), null);
        final var reader = new CanonicalProtobuf.Reader(value.canonicalBytes());
        final byte[] fields = CanonicalProtobuf.message(out -> {
            while (reader.hasRemaining()) {
                final var field = reader.next();
                if (field.number() == 8) {
                    continue;
                }
                if (field.number() == 1) {
                    CanonicalProtobuf.uint32(out, 1, QueryCodecSupport.uint32(field, 1));
                } else {
                    CanonicalProtobuf.bytes(
                            out,
                            field.number(),
                            field.number() == 2
                                    ? wrong.canonicalBytes()
                                    : QueryCodecSupport.bytes(field, field.number()));
                }
            }
        });
        final byte[] changed = CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(
                    out, 8, Bytes.sha256(Bytes.utf8("nereus-delay-target-quota-incarnation\0"), fields));
        });
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaIncarnation.decode(changed));
    }

    @Test
    void counterCannotPredateItsSourceDerivedAllocation() {
        final var value = TargetQuotaIncarnation.allocate(scope, accounting, lineage, stamp(2), allow)
                .drain(stamp(3), allow);
        assertThrows(
                IllegalStateException.class,
                () -> value.requireRetirable(
                        primary(value, value.ownContribution(), 1),
                        mirror(value, 3),
                        root,
                        aggregate(value, 3),
                        floor(3),
                        stamp(4),
                        release));
    }

    @Test
    void fixedDescriptorFeeRejectsOverflowBeforeAnyQuotaCanBePublished() {
        final var called = new AtomicInteger();
        assertThrows(
                ArithmeticException.class,
                () -> TargetQuotaIncarnation.allocate(
                        scope,
                        new TargetQuotaAccounting(accounting.schemaBundleHash(), Long.MAX_VALUE, 24, 40, 64),
                        lineage,
                        stamp(1),
                        (prior, next) -> called.incrementAndGet()));
        assertEquals(0, called.get());
    }

    private TargetQuotaIncarnation allocate(final TargetQuotaScope scope) {
        return TargetQuotaIncarnation.allocate(scope, accounting, lineage, stamp(1), allow);
    }

    private void retire(
            final TargetQuotaIncarnation value,
            final RecoveryFloorRef floor,
            final TargetQuotaIncarnation.RetirementAuthority authority) {
        value.requireRetirable(
                primary(value, value.ownContribution(), 3),
                mirror(value, 3),
                root,
                aggregate(value, 3),
                floor,
                stamp(4),
                authority);
    }

    private TargetQuotaCounter primary(final TargetQuotaIncarnation value, final TargetQuotaUsage usage, final long n) {
        return new TargetQuotaCounter(value.identity(), usage, 1, stamp(n));
    }

    private TargetQuotaCounter mirror(final TargetQuotaIncarnation value, final long n) {
        return new TargetQuotaCounter(value.tenantIdentity(), value.tenantContribution(), 1, stamp(n));
    }

    private TargetQuotaAggregate aggregate(final TargetQuotaIncarnation value, final long n) {
        return new TargetQuotaAggregate(
                scope.shard(),
                root.owner().accountingIncarnation(),
                new TargetQuotaUsage(root.charge(), 0, 0, 0, 1).add(value.ownContribution()),
                n,
                stamp(n));
    }

    private TargetQuotaMutation stamp(final long n) {
        final var source = (KafkaSourcePosition) TargetSourcePosition.decode(raw(scopes, "source"));
        return new TargetQuotaMutation(
                n,
                new KafkaSourcePosition(
                        source.shardId(), source.authenticatedClusterId(), source.nativeTopicUuid(), 9 + n, null, 100),
                bytes(32, 0x66));
    }

    private SourcePosition maximumSource(final long entry) {
        return new PulsarSourcePosition(
                scope.shard(),
                bytes(32, 0x77),
                "x".repeat(1 << 20),
                -1L,
                entry,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                Long.MAX_VALUE);
    }

    private RecoveryFloorRef floor(final long n) {
        return new RecoveryFloorRef(lineage, bytes(16, 0xdd), bytes(32, 0xee), n, stamp(n).source(), n, List.of());
    }

    private static TargetDomainState domain(final int slot, final TargetDomainState.Lifecycle lifecycle) {
        final boolean vacant = lifecycle == TargetDomainState.Lifecycle.VACANT;
        return new TargetDomainState(
                new TargetKeyCodec.Domain(slot, 1),
                lifecycle,
                vacant ? null : bytes(32, 1),
                vacant ? null : bytes(32, 2),
                null,
                null,
                null);
    }

    private static byte[] raw(final Properties properties, final String key) {
        return HexFormat.of().parseHex(properties.getProperty(key));
    }

    private static byte[] bytes(final int n, final int v) {
        final byte[] bytes = new byte[n];
        Arrays.fill(bytes, (byte) v);
        return bytes;
    }

    private static Properties properties(final String name) {
        try (var in = TargetQuotaIncarnationTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var properties = new Properties();
            properties.load(in);
            return properties;
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
