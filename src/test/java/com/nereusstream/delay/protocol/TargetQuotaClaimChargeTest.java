package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.store.ValueEnvelope;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaClaimChargeTest {
    private final Properties vectors = properties("target-quota-claim");
    private final Properties works = properties("target-identity");
    private final TargetQuotaAccounting accounting =
            TargetQuotaAccounting.decode(raw(properties("target-quota-accounting"), "accounting"));
    private final TargetQuotaIdentity identity = TargetQuotaIdentity.decode(raw(vectors, "identity"));
    private final OwnerIdentity owner = OwnerIdentity.decode(raw(vectors, "owner"));
    private final TargetQuotaMutation creation = TargetQuotaMutation.decode(raw(vectors, "creation"));
    private final TargetQuotaClaimCharge.Authority allow = (charge, kind, operation) -> {};

    @Test
    void ordinaryNativeAndOrderedRecordsMatchIndependentBytesAndCharges() {
        final var names = List.of("ordinary", "native", "ordered");
        final var workNames = List.of("initial", "native", "fifo.initial");
        final long[] costs = {64, 128, 96};
        for (int i = 0; i < 3; i++) {
            final var value = charge(work(workNames.get(i)), identity, owner, i + 1, costs[i], creation);
            assertArrayEquals(raw(vectors, names.get(i)), value.canonicalBytes());
            assertArrayEquals(
                    value.canonicalBytes(),
                    TargetQuotaClaimCharge.decode(value.canonicalBytes()).canonicalBytes());
            assertArrayEquals(raw(vectors, "key"), value.key());
            assertEquals(CapacityVector.decode(raw(vectors, names.get(i) + ".execution")), value.executionCharge());
            assertEquals(CapacityVector.decode(raw(vectors, names.get(i) + ".record")), value.recordCharge());
            assertEquals(value.recordCharge().add(value.executionCharge()), value.contribution());
            assertTrue(value.canonicalBytes().length <= TargetQuotaClaimCharge.MAX_CANONICAL_BYTES);
            assertEquals(identity, value.tenantIdentity().primary());
        }
    }

    @Test
    void sourceAllocatedDescriptorAndActualAuthorityAreRequiredForCreation() {
        final var descriptor = descriptor(accounting, bytes(32, 0x44), bytes(16, 0xcc));
        final var bound = rebind(work("native"), descriptor.identity());
        final var calls = new AtomicInteger();
        final var value = TargetQuotaClaimCharge.create(
                descriptor,
                bound,
                bytes(32, 0x77),
                owner,
                bytes(16, 0x88),
                1,
                1000,
                64,
                bytes(32, 0x99),
                creation,
                (charge, kind, operation) -> {
                    calls.incrementAndGet();
                    assertNull(kind);
                    assertEquals(creation, operation);
                    assertEquals(bound, charge.work());
                });
        assertEquals(1, calls.get());
        value.requireDescriptor(descriptor);
        final var draining = descriptor.drain(sourceStamp(3), (p, n) -> {});
        value.requireDescriptor(draining);
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaClaimCharge.create(
                        draining,
                        bound,
                        bytes(32, 0x77),
                        owner,
                        bytes(16, 0x88),
                        1,
                        1000,
                        64,
                        bytes(32, 0x99),
                        creation,
                        allow));
        TargetQuotaClaimCharge.create(
                draining,
                bound,
                bytes(32, 0x77),
                owner,
                bytes(16, 0x88),
                1,
                1000,
                64,
                bytes(32, 0x99),
                localAt(3, 1),
                allow);
        assertThrows(
                NullPointerException.class,
                () -> TargetQuotaClaimCharge.create(
                        descriptor,
                        bound,
                        bytes(32, 0x77),
                        owner,
                        bytes(16, 0x88),
                        1,
                        1000,
                        64,
                        bytes(32, 0x99),
                        creation,
                        null));
    }

    @Test
    void ownerAccountingTargetAndShardCannotBeReattributed() {
        final var other = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET, identity.shard(), bytes(16, 0x44), identity.target(), null);
        assertThrows(IllegalArgumentException.class, () -> charge(work("initial"), other, owner, 1, 64, creation));
        final var target = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                identity.shard(),
                identity.accountingIncarnation(),
                new TargetPartitionId(bytes(32, 0x33)),
                null);
        assertThrows(IllegalArgumentException.class, () -> charge(work("initial"), target, owner, 1, 64, creation));
        assertThrows(
                IllegalArgumentException.class,
                () -> charge(
                        work("initial"),
                        new TargetQuotaIdentity(
                                TargetQuotaIdentity.Kind.SHARD,
                                identity.shard(),
                                identity.accountingIncarnation(),
                                null,
                                null),
                        owner,
                        1,
                        64,
                        creation));
        assertThrows(
                IllegalArgumentException.class, () -> charge(work("initial"), identity, owner, 1, 64, sourceStamp(2)));
        final var otherDescriptor = descriptor(accounting, bytes(32, 0x45), bytes(16, 0xcc));
        assertThrows(IllegalStateException.class, () -> ordinary().requireDescriptor(otherDescriptor));
    }

    @Test
    void localRevokeRequiresExactOwnerStoreAndLaterLocalOrdinal() {
        final var value = ordinary();
        final var calls = new AtomicInteger();
        value.requireLocalRevoke(owner, bytes(16, 0x88), localAt(2, 2), (charge, kind, operation) -> {
            assertSame(value, charge);
            assertEquals(TargetQuotaClaimCharge.RemovalKind.REVOKE, kind);
            calls.incrementAndGet();
        });
        assertEquals(1, calls.get());
        assertThrows(
                IllegalStateException.class, () -> value.requireLocalRevoke(owner, bytes(16, 0x88), creation, allow));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireLocalRevoke(owner, bytes(16, 0x87), localAt(2, 2), allow));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireLocalRevoke(
                        new OwnerIdentity(bytes(1, 1), bytes(1, 2), 7, bytes(32, 3)),
                        bytes(16, 0x88),
                        localAt(2, 2),
                        allow));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireLocalRevoke(owner, bytes(16, 0x88), sourceStamp(3), allow));
    }

    @Test
    void admissionAndSourceResultCannotUseTheLocalShortcut() {
        final var value = ordinary();
        final var calls = new AtomicInteger();
        for (var kind : TargetQuotaClaimCharge.RemovalKind.values()) {
            value.requireSourceConsumption(kind, sourceStamp(3), (charge, k, operation) -> {
                assertEquals(kind, k);
                calls.incrementAndGet();
            });
            assertThrows(IllegalStateException.class, () -> value.requireSourceConsumption(kind, localAt(2, 2), allow));
            assertThrows(
                    IllegalStateException.class, () -> value.requireSourceConsumption(kind, sourceStamp(2), allow));
        }
        assertEquals(3, calls.get());
        assertArrayEquals(raw(vectors, "ordinary"), value.canonicalBytes());
    }

    @Test
    void authorityFailuresAndFatalErrorsPropagateWithoutChargeRelease() {
        final var value = ordinary();
        final byte[] before = value.canonicalBytes();
        assertThrows(
                IllegalStateException.class,
                () -> value.requireLocalRevoke(owner, bytes(16, 0x88), localAt(2, 2), (c, k, m) -> {
                    throw new IllegalStateException("denied");
                }));
        assertThrows(
                AssertionError.class,
                () -> value.requireSourceConsumption(
                        TargetQuotaClaimCharge.RemovalKind.ADMISSION, sourceStamp(3), (c, k, m) -> {
                            throw new AssertionError("fatal");
                        }));
        assertThrows(
                NullPointerException.class,
                () -> value.requireSourceConsumption(
                        TargetQuotaClaimCharge.RemovalKind.ADMISSION, sourceStamp(3), null));
        assertArrayEquals(before, value.canonicalBytes());
    }

    @Test
    void actualStoredKeyTypeAndCompleteValueAreGuarded() {
        final var value = ordinary();
        value.requireStored(value.key(), 34, value.canonicalBytes());
        final byte[] key = value.key();
        key[key.length - 1] ^= 1;
        assertThrows(IllegalStateException.class, () -> value.requireStored(key, 34, value.canonicalBytes()));
        assertThrows(IllegalStateException.class, () -> value.requireStored(value.key(), 9, value.canonicalBytes()));
        assertThrows(IllegalStateException.class, () -> value.requireStored(value.key(), 34, null));
        assertThrows(
                IllegalStateException.class,
                () -> value.requireStored(
                        value.key(),
                        34,
                        charge(work("initial").withRuntimeRevision(2), identity, owner, 1, 64, creation)
                                .canonicalBytes()));
    }

    @Test
    void mutationAndKeyArraysAreDefensivelyCopied() {
        final var value = ordinary();
        final byte[] before = value.canonicalBytes();
        for (byte[] a : List.of(
                value.claimId(),
                value.storeIncarnation(),
                value.claimRecordDigest(),
                value.tenantScope(),
                value.recoveryLineage(),
                value.digest(),
                value.key())) {
            a[0] ^= 1;
        }
        value.creation().mutationDigest()[0] ^= 1;
        assertArrayEquals(before, value.canonicalBytes());
    }

    @Test
    void malformedCanonicalAndLegacyReaderInputsFailClosed() {
        final var value = ordinary();
        final byte[] raw = value.canonicalBytes();
        final byte[] corrupt = raw.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaClaimCharge.decode(corrupt));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaClaimCharge.decode(Arrays.copyOf(raw, raw.length - 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaClaimCharge.decode(new byte[TargetQuotaClaimCharge.MAX_CANONICAL_BYTES + 1]));
        final byte[] unknown = raw.clone();
        unknown[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaClaimCharge.decode(unknown));
        final byte[] framed = Bytes.concat(new byte[] {'N', 'V', 34, 1}, Bytes.u32be(raw.length), raw);
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(framed, Bytes.crc32cbe(framed))));
    }

    @Test
    void finiteOwnerAndChargeBoundsDoNotWrapRawUnsignedClaimSequence() {
        final var value = charge(work("initial"), identity, owner, -1, Long.MAX_VALUE, creation);
        assertEquals(-1, TargetQuotaClaimCharge.decode(value.canonicalBytes()).claimSequence());
        assertEquals(Long.MAX_VALUE, value.executionBytes());
        assertThrows(IllegalArgumentException.class, () -> charge(work("initial"), identity, owner, 0, 64, creation));
        assertThrows(IllegalArgumentException.class, () -> charge(work("initial"), identity, owner, 1, 0, creation));
        assertThrows(IllegalArgumentException.class, () -> charge(work("initial"), identity, owner, 1, -1, creation));
        assertThrows(
                IllegalArgumentException.class,
                () -> charge(
                        work("initial"),
                        identity,
                        new OwnerIdentity(bytes(4096, 1), bytes(1, 2), 7, bytes(32, 3)),
                        1,
                        64,
                        creation));
    }

    @Test
    void exactFrozenProjectionFeedsLocalClaimAndRevokeDeltaWithoutPayloadCharges() {
        final var value = ordinary();
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[2] = 1000;
        final var prior = new TargetQuotaUsage(new CapacityVector(amounts), 1, 1, 0, 1);
        final var mirrorUsage = new TargetQuotaUsage(prior.resources(), 1, 0, 0, 0);
        final var primary = new TargetQuotaCounter(identity, prior, 1, sourceStamp(2));
        final var mirror = new TargetQuotaCounter(value.tenantIdentity(), mirrorUsage, 1, sourceStamp(2));
        final var aggregate = new TargetQuotaAggregate(identity.shard(), bytes(16, 0x22), prior, 1, sourceStamp(2));
        final var counters = Map.of(identity, primary, mirror.identity(), mirror);
        final var nextPrimary = new TargetQuotaUsage(prior.resources().add(value.contribution()), 1, 1, 0, 1);
        final var nextMirror = new TargetQuotaUsage(mirrorUsage.resources().add(value.contribution()), 1, 0, 0, 0);
        final var claim = TargetQuotaDelta.prepareLocalClaim(
                aggregate,
                2,
                creation.source(),
                creation.mutationDigest(),
                TargetQuotaDelta.LocalClaimKind.CLAIM,
                List.of(
                        new TargetQuotaDelta.Update(identity, nextPrimary),
                        new TargetQuotaDelta.Update(mirror.identity(), nextMirror)),
                4,
                counters::get,
                (k, d) -> assertEquals(value.creation(), d.mutation()));
        final var after = Map.of(
                identity,
                claim.changes().get(0).next(),
                mirror.identity(),
                claim.changes().get(1).next());
        final var revoke = TargetQuotaDelta.prepareLocalClaim(
                claim.nextAggregate(),
                2,
                creation.source(),
                bytes(32, 0x67),
                TargetQuotaDelta.LocalClaimKind.REVOKE,
                List.of(
                        new TargetQuotaDelta.Update(identity, prior),
                        new TargetQuotaDelta.Update(mirror.identity(), mirrorUsage)),
                4,
                after::get,
                (k, d) -> value.requireLocalRevoke(owner, bytes(16, 0x88), d.mutation(), allow));
        assertEquals(prior, revoke.nextAggregate().usage());
        assertEquals(0, value.contribution().amount(CapacityDimension.PENDING_PAYLOAD_BYTES));
    }

    private TargetQuotaClaimCharge ordinary() {
        return charge(work("initial"), identity, owner, 1, 64, creation);
    }

    private TargetQuotaClaimCharge charge(
            TargetTimelineWorkRef work,
            TargetQuotaIdentity id,
            OwnerIdentity own,
            long sequence,
            long cost,
            TargetQuotaMutation stamp) {
        return new TargetQuotaClaimCharge(
                bytes(32, 0x77),
                work,
                id,
                bytes(32, 0x44),
                accounting,
                own,
                bytes(16, 0x88),
                sequence,
                1000,
                cost,
                bytes(32, 0x99),
                stamp,
                bytes(16, 0xcc));
    }

    private TargetQuotaIncarnation descriptor(TargetQuotaAccounting rules, byte[] tenant, byte[] lineage) {
        return TargetQuotaIncarnation.allocate(
                new TargetQuotaScope(identity.shard(), tenant, identity.target()),
                rules,
                lineage,
                sourceStamp(1),
                (p, n) -> {});
    }

    private TargetTimelineWorkRef rebind(TargetTimelineWorkRef w, TargetQuotaIdentity id) {
        final var l = w.locator();
        return new TargetTimelineWorkRef(
                new TargetMessageLocator(
                        l.messageId(),
                        l.generation(),
                        l.target(),
                        l.domain(),
                        id.accountingIncarnation(),
                        l.orderingMode(),
                        l.orderingDomain(),
                        l.scheduleBindingDigest()),
                w.workKind(),
                w.deliverAtEpochMs(),
                w.retryEligibilityAtEpochMs(),
                w.sourceOrderToken(),
                w.candidateAttemptNo(),
                w.runtimeRevision(),
                w.uncertainRetryAuthority(),
                w.uncertainRetryControl(),
                w.uncertainRetryControlPosition(),
                w.nativeCandidate());
    }

    private TargetTimelineWorkRef work(String name) {
        return TargetTimelineWorkRef.decode(raw(works, "work." + name));
    }

    private TargetQuotaMutation sourceStamp(long n) {
        return new TargetQuotaMutation(n, source(n), bytes(32, 0x66));
    }

    private TargetQuotaMutation localAt(long n, long ordinal) {
        return new TargetQuotaMutation(n, source(n), bytes(32, 0x66), ordinal);
    }

    private SourcePosition source(long n) {
        final var s = (KafkaSourcePosition) creation.source();
        return new KafkaSourcePosition(
                s.shardId(),
                s.authenticatedClusterId(),
                s.nativeTopicUuid(),
                s.offset() + n - 2,
                s.leaderEpoch(),
                s.brokerLogAppendTimeEpochMs());
    }

    private static byte[] bytes(int size, int value) {
        final byte[] raw = new byte[size];
        Arrays.fill(raw, (byte) value);
        return raw;
    }

    private static byte[] raw(Properties p, String key) {
        return HexFormat.of().parseHex(p.getProperty(key));
    }

    private static Properties properties(String name) {
        final var p = new Properties();
        try (var in = TargetQuotaClaimChargeTest.class.getResourceAsStream("/ndip3/" + name + "-vectors.properties")) {
            p.load(in);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return p;
    }
}
