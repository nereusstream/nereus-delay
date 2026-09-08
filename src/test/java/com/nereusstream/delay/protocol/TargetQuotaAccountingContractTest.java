package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaAccountingContractTest {
    private final Properties vectors = load();
    private final TargetQuotaAccounting accounting = new TargetQuotaAccounting(repeat(32, 0xbb), 32, 24, 40, 64);
    private final ShardId shard = new ShardId(new RouteIncarnation(repeat(16, 0x11)), 3);
    private final TargetMessageLocator locator = new TargetMessageLocator(
            new DelayMessageId(
                    SelfRoutingId.fromLogicalUuid(shard, UUID.fromString("00000000-0064-7000-8000-000000000001"))
                            .bytes()),
            1,
            new TargetPartitionId(repeat(32, 0x33)),
            new TargetKeyCodec.Domain(0, 1),
            repeat(16, 0x22),
            OrderingMode.BEST_EFFORT,
            null,
            repeat(32, 0x88));
    private final CapacityVector commitment = vector("commitment");
    private final CapacityVector initial = vector("initial");
    private final CapacityVector unknownAllocation = vector("unknownAllocation");
    private final CapacityVector resolvedAllocation = vector("resolvedAllocation");

    @Test
    void independentlyDecodedArtifactsHaveCompleteValueEquality() {
        final var one = accounting;
        final var two = TargetQuotaAccounting.decode(one.canonicalBytes());
        assertEquals(one, two);
        assertEquals(one.hashCode(), two.hashCode());
        assertEquals(1, new java.util.HashSet<>(List.of(one, two)).size());
        assertNotEquals(one, new TargetQuotaAccounting(one.schemaBundleHash(), 33, 24, 40, 64));
    }

    @Test
    void artifactAndFixedMeasurementsMatchIndependentVectors() {
        assertArrayEquals(hex("accounting"), accounting.canonicalBytes());
        assertArrayEquals(
                hex("accounting.maximum"),
                new TargetQuotaAccounting(
                                repeat(32, 0xbb), Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)
                        .canonicalBytes());
        assertArrayEquals(hex("locator"), locator.canonicalBytes());
        assertArrayEquals(hex("message"), locator.messageId().bytes());
        assertEquals(vector("fee.state"), accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, 8, 100));
        assertEquals(vector("fee.result"), accounting.recordCharge(TargetQuotaAccounting.RecordClass.RESULT, 8, 100));
        assertEquals(
                vector("fee.system"),
                accounting.recordCharge(TargetQuotaAccounting.RecordClass.SYSTEM_MUTATION, 8, 100));
        assertEquals(
                vector("fee.evidence"), accounting.recordCharge(TargetQuotaAccounting.RecordClass.EVIDENCE, 8, 100));
        assertEquals(vector("fee.wal"), accounting.outcomeWalCharge(100));
        assertEquals(152, accounting.storedRecordBytes(8, 100));
        assertEquals(52, accounting.accountedPublishBytes(AdapterKind.KAFKA, 20, 8));
        assertEquals(68, accounting.accountedPublishBytes(AdapterKind.PULSAR, 20, 8));
        assertEquals(64, accounting.schedulingCost(AdapterKind.KAFKA, 20, 8));
        assertEquals(68, accounting.schedulingCost(AdapterKind.PULSAR, 20, 8));
    }

    @Test
    void payloadAndExecutionChargeSourcesRemainDisjoint() {
        assertEquals(vector("fee.reservation"), TargetQuotaAccounting.reservedPayload(20));
        assertEquals(vector("fee.active"), TargetQuotaAccounting.activePayload(20));
        assertEquals(vector("fee.retainedPayload"), TargetQuotaAccounting.retainedPayload(20));
        assertEquals(vector("fee.execution"), accounting.executionCharge(AdapterKind.KAFKA, 20, 8));
        final var combined =
                TargetQuotaAccounting.activePayload(20).add(accounting.executionCharge(AdapterKind.KAFKA, 20, 8));
        assertEquals(1, combined.amount(CapacityDimension.ACTIVE_MESSAGES));
        assertEquals(20, combined.amount(CapacityDimension.PENDING_PAYLOAD_BYTES));
        assertEquals(1, combined.amount(CapacityDimension.INFLIGHT_MESSAGES));
        assertEquals(52, combined.amount(CapacityDimension.INFLIGHT_BYTES));
    }

    @Test
    void everyAttemptBudgetPhaseMatchesIndependentBytesAndCharge() {
        final var admitted = admitted();
        final var unknown = admitted.unknown(unknownAllocation, stamp(2));
        final var resolved =
                unknown.resolve(EvidenceVerificationStatus.VERIFIED_PUBLISHED, resolvedAllocation, stamp(3));
        final var retained = resolved.retainAfterCheckpoint(floor(3), stamp(4), allow());
        final var released = retained.releaseRetained(CapacityVector.empty(), floor(4), stamp(5), allow());
        int index = 0;
        for (var budget : List.of(admitted, unknown, resolved, retained, released)) {
            final String name = List.of("admitted", "unknown", "resolved", "retained", "released")
                    .get(index++);
            assertArrayEquals(hex(name + ".budget"), budget.canonicalBytes());
            assertEquals(vector(name + ".charge"), budget.effectiveCharge());
            assertArrayEquals(
                    budget.canonicalBytes(),
                    TargetQuotaAttemptBudget.decode(budget.canonicalBytes()).canonicalBytes());
        }
        assertArrayEquals(hex("budget.key"), admitted.key());
        assertArrayEquals(hex("floor3"), floor(3).canonicalBytes());
        assertArrayEquals(hex("floor4"), floor(4).canonicalBytes());
    }

    @Test
    void unknownCannotReleaseExecutionReserveOrUseUnresolvedEvidenceToResolve() {
        final var admitted = admitted();
        final var unknown = admitted.unknown(unknownAllocation, stamp(2));
        assertEquals(admitted.effectiveCharge(), unknown.effectiveCharge());
        assertEquals(1, unknown.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
        final var shrunkActual = unknown.unknown(CapacityVector.empty(), stamp(3));
        assertEquals(admitted.effectiveCharge(), shrunkActual.effectiveCharge());
        assertThrows(
                IllegalArgumentException.class,
                () -> unknown.resolve(EvidenceVerificationStatus.UNRESOLVED, resolvedAllocation, stamp(3)));
        final var calls = new AtomicInteger();
        assertThrows(
                IllegalStateException.class,
                () -> unknown.retainAfterCheckpoint(floor(2), stamp(3), (a, b, c, d, e) -> calls.incrementAndGet()));
        assertEquals(0, calls.get());
    }

    @Test
    void definitiveOutcomeReleasesOnlyExecutionAndKeepsUncheckpointedCommitment() {
        for (var status : List.of(
                EvidenceVerificationStatus.VERIFIED_PUBLISHED, EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED)) {
            final var resolved = admitted().resolve(status, resolvedAllocation, stamp(2));
            assertEquals(commitment, resolved.effectiveCharge());
            assertEquals(0, resolved.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
            assertFalse(resolved.effectiveCharge().equals(resolved.allocated()));
            assertThrows(IllegalStateException.class, () -> resolved.unknown(initial, stamp(3)));
            assertThrows(IllegalStateException.class, () -> resolved.resolve(status, initial, stamp(3)));
        }
    }

    @Test
    void reserveToRetainedTransferNeedsExactAuthorityAndKeepsActualRecordsCharged() {
        final var resolved = resolved();
        final var calls = new AtomicInteger();
        final var floor = floor(3);
        final var stamp = stamp(4);
        final var retained =
                resolved.retainAfterCheckpoint(floor, stamp, (before, kind, seenFloor, next, seenStamp) -> {
                    calls.incrementAndGet();
                    assertSame(resolved, before);
                    assertEquals(TargetQuotaAttemptBudget.ReleaseKind.UNUSED_RESERVATION, kind);
                    assertSame(floor, seenFloor);
                    assertEquals(resolvedAllocation, next);
                    assertSame(stamp, seenStamp);
                });
        assertEquals(1, calls.get());
        assertEquals(resolvedAllocation, retained.effectiveCharge());
        assertEquals(
                commitment.subtract(resolvedAllocation),
                resolved.effectiveCharge().subtract(retained.effectiveCharge()));
        assertArrayEquals(floor.floorDigest(), retained.floorDigest());
    }

    @Test
    void scalarFloorCannotBypassCatalogPinOrDeletionAuthorityFailure() {
        final var resolved = resolved();
        final byte[] before = resolved.canonicalBytes();
        assertThrows(
                IllegalStateException.class,
                () -> resolved.retainAfterCheckpoint(floor(3), stamp(4), (a, b, c, d, e) -> {
                    throw new IllegalStateException("recovery pin protects reserved writer");
                }));
        assertThrows(
                AssertionError.class,
                () -> resolved.retainAfterCheckpoint(floor(3), stamp(4), (a, b, c, d, e) -> {
                    throw new AssertionError("authority unavailable");
                }));
        assertArrayEquals(before, resolved.canonicalBytes());
        final var retained = resolved.retainAfterCheckpoint(floor(3), stamp(4), allow());
        assertThrows(
                IllegalStateException.class,
                () -> retained.releaseRetained(CapacityVector.empty(), floor(4), stamp(5), (a, b, c, d, e) -> {
                    throw new IllegalStateException("provider deletion unconfirmed");
                }));
        assertEquals(resolvedAllocation, retained.effectiveCharge());
    }

    @Test
    void floorMustCoverExactLineageSourceSequenceAndPrecedeReleaseMutation() {
        final var resolved = resolved();
        final var calls = new AtomicInteger();
        final TargetQuotaAttemptBudget.ReleaseAuthority authority = (a, b, c, d, e) -> calls.incrementAndGet();
        assertThrows(IllegalStateException.class, () -> resolved.retainAfterCheckpoint(floor(2), stamp(4), authority));
        final var shortSequence =
                new RecoveryFloorRef(repeat(16, 0xcc), repeat(16, 1), repeat(32, 1), 1, source(3), 2, List.of());
        assertThrows(
                IllegalStateException.class, () -> resolved.retainAfterCheckpoint(shortSequence, stamp(4), authority));
        final var wrongLineage =
                new RecoveryFloorRef(repeat(16, 0xcd), repeat(16, 1), repeat(32, 1), 1, source(3), 3, List.of());
        assertThrows(
                IllegalStateException.class, () -> resolved.retainAfterCheckpoint(wrongLineage, stamp(4), authority));
        assertThrows(IllegalStateException.class, () -> resolved.retainAfterCheckpoint(floor(4), stamp(4), authority));
        final var exact = source(3);
        final var metadataDrift = new KafkaSourcePosition(
                shard, "kfk", exact.nativeTopicUuid(), exact.offset(), 1, exact.brokerLogAppendTimeEpochMs());
        final var badMetadata =
                new RecoveryFloorRef(repeat(16, 0xcc), repeat(16, 1), repeat(32, 1), 1, metadataDrift, 3, List.of());
        assertThrows(
                IllegalStateException.class, () -> resolved.retainAfterCheckpoint(badMetadata, stamp(4), authority));
        assertEquals(0, calls.get());
    }

    @Test
    void floorMustIncludeTheLastReservedWriterAllocationNotOnlyTheResolution() {
        final var resolved = resolved();
        final long[] values = resolvedAllocation.amounts();
        values[2] = 500;
        final var latest = resolved.recordResolvedAllocation(new CapacityVector(values), stamp(4));
        assertEquals(resolved.resolvedAt(), latest.resolvedAt());
        assertEquals(commitment, latest.effectiveCharge());
        assertThrows(IllegalStateException.class, () -> latest.retainAfterCheckpoint(floor(3), stamp(5), allow()));
        final var retained = latest.retainAfterCheckpoint(floor(4), stamp(5), allow());
        assertEquals(500, retained.effectiveCharge().amount(CapacityDimension.LOGICAL_STATE_BYTES));
        assertThrows(
                IllegalStateException.class, () -> retained.recordResolvedAllocation(resolvedAllocation, stamp(6)));
        assertArrayEquals(
                latest.canonicalBytes(),
                TargetQuotaAttemptBudget.decode(latest.canonicalBytes()).canonicalBytes());
    }

    @Test
    void partialRetainedReleaseKeepsAllOtherClassesAndCannotGrowOrRevive() {
        final var retained = resolved().retainAfterCheckpoint(floor(3), stamp(4), allow());
        final long[] actual = resolvedAllocation.amounts();
        actual[CapacityDimension.OUTCOME_WAL_BYTES.wireValue() - 1] = 0;
        final var nextAllocated = new CapacityVector(actual);
        final var partial = retained.releaseRetained(nextAllocated, floor(4), stamp(5), allow());
        assertEquals(TargetQuotaAttemptBudget.Phase.RETAINED, partial.phase());
        assertEquals(0, partial.effectiveCharge().amount(CapacityDimension.OUTCOME_WAL_BYTES));
        assertEquals(400, partial.effectiveCharge().amount(CapacityDimension.LOGICAL_STATE_BYTES));
        assertThrows(
                IllegalStateException.class,
                () -> partial.releaseRetained(resolvedAllocation, floor(5), stamp(6), allow()));
        assertThrows(
                IllegalStateException.class, () -> partial.releaseRetained(nextAllocated, floor(5), stamp(6), allow()));
        final var released = partial.releaseRetained(CapacityVector.empty(), floor(5), stamp(6), allow());
        assertTrue(released.effectiveCharge().isZero());
        assertThrows(IllegalStateException.class, () -> released.unknown(initial, stamp(7)));
        assertThrows(
                IllegalStateException.class,
                () -> released.releaseRetained(CapacityVector.empty(), floor(6), stamp(7), allow()));
    }

    @Test
    void noResultAllocationCanExceedOrReclassifyTheFrozenCommitment() {
        final var admitted = admitted();
        final long[] tooLarge = initial.amounts();
        tooLarge[2] = commitment.amount(CapacityDimension.LOGICAL_STATE_BYTES) + 1;
        assertThrows(IllegalArgumentException.class, () -> admitted.unknown(new CapacityVector(tooLarge), stamp(2)));
        final var wrongClass = TargetQuotaAccounting.activePayload(20);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.admit(
                        locator,
                        repeat(32, 0x44),
                        repeat(32, 0x99),
                        repeat(32, 0xaa),
                        accounting,
                        52,
                        wrongClass,
                        CapacityVector.empty(),
                        stamp(1),
                        repeat(16, 0xcc)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.admit(
                        locator,
                        repeat(32, 0x44),
                        repeat(32, 0x99),
                        repeat(32, 0xaa),
                        accounting,
                        52,
                        CapacityVector.empty(),
                        CapacityVector.empty(),
                        stamp(1),
                        repeat(16, 0xcc)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.admit(
                        locator,
                        repeat(32, 0x44),
                        repeat(32, 0x99),
                        repeat(32, 0xaa),
                        accounting,
                        52,
                        commitment.add(TargetQuotaAccounting.retainedPayload(20)),
                        initial,
                        stamp(1),
                        repeat(16, 0xcc)));
        assertEquals(commitment, admitted.commitment());
    }

    @Test
    void duplicateMutationAndForeignSourceCannotAdvanceBudget() {
        final var admitted = admitted();
        assertThrows(IllegalStateException.class, () -> admitted.unknown(initial, stamp(1)));
        final var pos = source(2);
        final var foreign = new KafkaSourcePosition(
                shard, "other", pos.nativeTopicUuid(), pos.offset(), null, pos.brokerLogAppendTimeEpochMs());
        assertThrows(
                IllegalArgumentException.class,
                () -> admitted.unknown(initial, new TargetQuotaMutation(2, foreign, repeat(32, 0x66))));
        final var moved = new TargetMessageLocator(
                locator.messageId(),
                2,
                new TargetPartitionId(repeat(32, 0x34)),
                locator.domain(),
                repeat(16, 0x23),
                locator.orderingMode(),
                null,
                repeat(32, 0x87));
        final var newer = TargetQuotaAttemptBudget.admit(
                moved,
                repeat(32, 0x44),
                repeat(32, 0x98),
                repeat(32, 0xaa),
                accounting,
                52,
                commitment,
                initial,
                stamp(2),
                repeat(16, 0xcc));
        assertNotEquals(admitted.primaryIdentity(), newer.primaryIdentity());
        assertArrayEquals(repeat(16, 0x22), admitted.primaryIdentity().accountingIncarnation());
        assertEquals(
                2, admitted.effectiveCharge().add(newer.effectiveCharge()).amount(CapacityDimension.INFLIGHT_MESSAGES));
    }

    @Test
    void terminalOrNewGenerationDoesNotDrainAnOlderUnknownAttempt() {
        final var unknown = admitted().unknown(unknownAllocation, stamp(2));
        final var payload = TargetQuotaAccounting.activePayload(20);
        final var beforeTerminal = payload.add(unknown.effectiveCharge());
        final var afterTerminal = beforeTerminal.subtract(payload).add(TargetQuotaAccounting.retainedPayload(20));
        assertEquals(0, afterTerminal.amount(CapacityDimension.ACTIVE_MESSAGES));
        assertEquals(1, afterTerminal.amount(CapacityDimension.INFLIGHT_MESSAGES));
        assertEquals(
                20,
                afterTerminal.amount(CapacityDimension.RETAINED_BYTES)
                        - commitment.amount(CapacityDimension.RETAINED_BYTES));
        final var nextGeneration = afterTerminal
                .subtract(TargetQuotaAccounting.retainedPayload(20))
                .add(payload);
        assertEquals(beforeTerminal, nextGeneration);
        assertEquals(unknown.effectiveCharge(), admitted().effectiveCharge());
    }

    @Test
    void reservationCommitRescheduleAndRevokeConservePayloadOwnership() {
        final var reserved = TargetQuotaAccounting.reservedPayload(20);
        final var active = reserved.subtract(reserved).add(TargetQuotaAccounting.activePayload(20));
        assertEquals(0, active.amount(CapacityDimension.RESERVATION_MESSAGES));
        assertEquals(1, active.amount(CapacityDimension.ACTIVE_MESSAGES));
        assertEquals(20, active.amount(CapacityDimension.PENDING_PAYLOAD_BYTES));
        final var claim = accounting.executionCharge(AdapterKind.KAFKA, 20, 8);
        final var claimed = active.add(claim);
        assertEquals(active, claimed.subtract(claim));
        final var admitted = claimed.subtract(claim).add(admitted().effectiveCharge());
        assertEquals(1, admitted.amount(CapacityDimension.INFLIGHT_MESSAGES));
        assertEquals(20, admitted.amount(CapacityDimension.PENDING_PAYLOAD_BYTES));
        assertTrue(reserved.subtract(reserved).isZero());
    }

    @Test
    void codecRejectsUnknownVersionPhaseFieldsDigestAndInvalidInitialRevision() {
        final byte[] raw = admitted().canonicalBytes();
        final byte[] corrupt = raw.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaAttemptBudget.decode(corrupt));
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaAttemptBudget.decode(replaceUint(raw, 1, 2)));
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaAttemptBudget.decode(replaceUint(raw, 10, 6)));
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaAttemptBudget.decode(replaceUint(raw, 10, 5)));
        final var unknown = admitted().unknown(initial, stamp(2));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.decode(replaceUint(unknown.canonicalBytes(), 10, 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.decode(Arrays.copyOf(raw, raw.length - 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.decode(Bytes.concat(raw, new byte[] {(byte) 0x88, 1, 0})));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.decode(new byte[TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void storeLookupAndLegacyEnvelopeRejectWrongIdentityOrType() {
        final var admitted = admitted();
        assertArrayEquals(
                admitted.canonicalBytes(),
                TargetQuotaAttemptBudget.decodeForStore(admitted.key(), admitted.canonicalBytes(), shard)
                        .canonicalBytes());
        final byte[] badKey = admitted.key();
        badKey[badKey.length - 1] ^= 1;
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.decodeForStore(badKey, admitted.canonicalBytes(), shard));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAttemptBudget.decodeForStore(
                        admitted.key(), admitted.canonicalBytes(), new ShardId(shard.routeIncarnation(), 4)));
        final byte[] prefix = ByteBuffer.allocate(8)
                .putShort((short) 0x4e56)
                .put((byte) 28)
                .put((byte) 1)
                .putInt(admitted.canonicalBytes().length)
                .array();
        final byte[] content = Bytes.concat(prefix, admitted.canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(content, Bytes.crc32cbe(content))));
    }

    @Test
    void measurementLengthsConstantsAndOverflowAreChecked() {
        assertThrows(IllegalArgumentException.class, () -> accounting.storedRecordBytes(0, 1));
        assertThrows(IllegalArgumentException.class, () -> accounting.storedRecordBytes(1, -1));
        assertThrows(IllegalArgumentException.class, () -> accounting.outcomeWalCharge(0));
        assertThrows(IllegalArgumentException.class, () -> accounting.accountedPublishBytes(AdapterKind.KAFKA, -1, 0));
        assertThrows(
                ArithmeticException.class,
                () -> accounting.accountedPublishBytes(AdapterKind.PULSAR, Long.MAX_VALUE, 0));
        assertThrows(ArithmeticException.class, () -> accounting.storedRecordBytes(1, Long.MAX_VALUE));
        assertThrows(ArithmeticException.class, () -> accounting.outcomeWalCharge(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaAccounting(repeat(32, 0xbb), 1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new TargetQuotaAccounting(new byte[32], 1, 1, 1, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaAccounting.decode(new byte[TargetQuotaAccounting.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void maximumFullSourceAndRawSequenceFitTheDeclaredBudgetBound() {
        final var largeAccounting = new TargetQuotaAccounting(
                repeat(32, 0xbb), Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        final long[] values = new long[CapacityDimension.COUNT];
        for (int n : new int[] {3, 9, 10, 11, 12, 13, 14, 15}) {
            values[n - 1] = Long.MAX_VALUE;
        }
        final var max = new CapacityVector(values);
        final String topic = "x".repeat(TargetSourcePosition.MAX_TOPIC_UTF8_BYTES);
        final var first = new TargetQuotaMutation(1, pulsar(topic, 1), repeat(32, 0x66));
        final var closing = new TargetQuotaMutation(-2, pulsar(topic, -2), repeat(32, 0x66));
        final var last = new TargetQuotaMutation(-1, pulsar(topic, -1), repeat(32, 0x66));
        final var admitted = TargetQuotaAttemptBudget.admit(
                locator,
                repeat(32, 0x44),
                repeat(32, 0x99),
                repeat(32, 0xaa),
                largeAccounting,
                Long.MAX_VALUE,
                max,
                max,
                first,
                repeat(16, 0xcc));
        final var resolved = admitted.resolve(EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, max, closing);
        final var floor = new RecoveryFloorRef(
                repeat(16, 0xcc), repeat(16, 1), repeat(32, 1), -1, closing.source(), -2, List.of());
        final var retained = resolved.retainAfterCheckpoint(floor, last, allow());
        final byte[] raw = replaceUint(retained.canonicalBytes(), 11, -1);
        assertTrue(raw.length <= TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES);
        assertEquals(-1, TargetQuotaAttemptBudget.decode(raw).revision());
        assertThrows(
                IllegalStateException.class,
                () -> retained.releaseRetained(CapacityVector.empty(), floor, last, allow()));
    }

    @Test
    void frozenArtifactScopeAndArraysCannotBeChangedThroughAccessors() {
        final var admitted = admitted();
        final byte[] original = admitted.canonicalBytes();
        admitted.tenantScope()[0] ^= 1;
        admitted.publishAttemptId()[0] ^= 1;
        admitted.admissionDigest()[0] ^= 1;
        admitted.accounting().schemaBundleHash()[0] ^= 1;
        admitted.accounting().digest()[0] ^= 1;
        admitted.recoveryLineage()[0] ^= 1;
        admitted.commitment().amounts()[2] = 0;
        assertArrayEquals(original, admitted.canonicalBytes());
        assertEquals(admitted.primaryIdentity(), admitted.tenantIdentity().primary());
        final var changedRules = new TargetQuotaAccounting(repeat(32, 0xbb), 33, 24, 40, 64);
        assertFalse(Arrays.equals(accounting.digest(), changedRules.digest()));
        assertEquals(32, admitted.unknown(initial, stamp(2)).accounting().recordOverheadBytes());
    }

    private TargetQuotaAttemptBudget admitted() {
        return TargetQuotaAttemptBudget.admit(
                locator,
                repeat(32, 0x44),
                repeat(32, 0x99),
                repeat(32, 0xaa),
                accounting,
                52,
                commitment,
                initial,
                stamp(1),
                repeat(16, 0xcc));
    }

    private TargetQuotaAttemptBudget resolved() {
        return admitted()
                .unknown(unknownAllocation, stamp(2))
                .resolve(EvidenceVerificationStatus.VERIFIED_PUBLISHED, resolvedAllocation, stamp(3));
    }

    private KafkaSourcePosition source(final long n) {
        return new KafkaSourcePosition(
                shard, "kfk", UUID.fromString("55555555-5555-5555-5555-555555555555"), n + 9, null, n + 99);
    }

    private TargetQuotaMutation stamp(final long n) {
        return new TargetQuotaMutation(n, source(n), repeat(32, 0x66));
    }

    private RecoveryFloorRef floor(final int n) {
        return new RecoveryFloorRef(
                repeat(16, 0xcc), repeat(16, 0xd0 + n), repeat(32, 0xdd), n, source(n), n, List.of());
    }

    private PulsarSourcePosition pulsar(final String topic, final long entry) {
        return new PulsarSourcePosition(
                shard,
                repeat(32, 0x77),
                topic,
                -1,
                entry,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                Long.MAX_VALUE);
    }

    private static TargetQuotaAttemptBudget.ReleaseAuthority allow() {
        return (a, b, c, d, e) -> {};
    }

    private static byte[] replaceUint(final byte[] encoded, final int number, final long value) {
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final byte[] fields = CanonicalProtobuf.message(out -> {
            while (reader.hasRemaining()) {
                final var field = reader.next();
                if (field.number() == 16) {
                    continue;
                }
                if (field.number() == number) {
                    CanonicalProtobuf.uint64Bits(out, number, value);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(out, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(out, field.number(), field.rawValue());
                }
            }
        });
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(
                    out, 16, Bytes.sha256(Bytes.utf8("nereus-delay-target-quota-attempt-budget\0"), fields));
        });
    }

    private CapacityVector vector(final String key) {
        return CapacityVector.decode(hex(key));
    }

    private byte[] hex(final String key) {
        return HexFormat.of().parseHex(vectors.getProperty(key));
    }

    private static byte[] repeat(final int count, final int value) {
        final byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static Properties load() {
        final var properties = new Properties();
        try (var input = TargetQuotaAccountingContractTest.class.getResourceAsStream(
                "/ndip3/target-quota-accounting-vectors.properties")) {
            properties.load(input);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        return properties;
    }
}
