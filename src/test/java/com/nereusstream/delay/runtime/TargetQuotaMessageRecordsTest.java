package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetQuotaMessageRecordsTest {
    private final Properties vectors = properties("target-quota-message-record-vectors.properties");
    private final Properties identity = properties("target-identity-vectors.properties");
    private final Properties payload = properties("target-quota-payload-vectors.properties");
    private final Properties binding = properties("target-binding-channel-vectors.properties");
    private final TargetQuotaPayloadOwner owner = owner("inline.active");
    private final TargetMessageRecord message = message("message.initial");
    private final TargetScheduleBinding initial = TargetScheduleBinding.decode(raw(binding, "binding.best"));

    @Test
    void allEightRolesMatchIndependentKeysPayloadDigestsAndRecordCharges() {
        final var fifo = message("order.message.initial");
        final var records = Map.of(
                "owner", ownerRecord(owner),
                "binding", bindingRecord(owner),
                "message", messageRecord(owner, message),
                "due", timeline(owner, message, "due"),
                "native", timeline(owner, message, "native"),
                "expiry", expiry(owner, message),
                "ordered", timeline(owner, fifo, "ordered"),
                "orderHead",
                        TargetQuotaMessageRecords.orderHead(
                                owner,
                                fifo,
                                state("order.head"),
                                raw(vectors, "orderHead.key"),
                                fifo.runtime().timeline().canonicalBytes()));
        for (var entry : records.entrySet()) {
            final String name = entry.getKey();
            final var record = entry.getValue();
            assertArrayEquals(raw(vectors, name + ".key"), record.key());
            assertEquals(
                    vectors.getProperty(name + ".payload.sha256"), Bytes.hex(Bytes.sha256(record.canonicalPayload())));
            assertEquals(CapacityVector.decode(raw(vectors, name + ".charge")), record.charge());
            record.requireStored(record.family(), record.valueType(), record.key(), record.canonicalPayload());
        }
        assertEquals(
                8,
                records.values().stream()
                        .map(TargetQuotaMessageRecords.Record::role)
                        .distinct()
                        .count());
        assertEquals(owner.recordCharge(), records.get("owner").charge());
    }

    @Test
    void nativeCopiesHaveTwoPhysicalFeesButNoDuplicatedPayloadOrExecutionUsage() {
        final var records = nativeRecords();
        final var total = TargetQuotaMessageRecords.total(owner, records, 6);
        assertEquals(CapacityVector.decode(raw(vectors, "nativeSubset.charge")), total);
        for (CapacityDimension dimension : CapacityDimension.values()) {
            if (dimension != CapacityDimension.LOGICAL_STATE_BYTES) {
                assertEquals(0, total.amount(dimension));
            }
        }
        assertArrayEquals(records.get(3).canonicalPayload(), records.get(4).canonicalPayload());
        assertEquals(records.get(3).charge(), records.get(4).charge());
    }

    @Test
    void exactStorageIdentityCannotBeCountedTwice() {
        final var record = messageRecord(owner, message);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.total(owner, List.of(record, record), 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.total(owner, List.of(record, messageRecord(owner, message)), 2));
    }

    @Test
    void subsetBoundIsCheckedBeforeInspectionAndDoesNotInventMissingRows() {
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaMessageRecords.total(owner, nativeRecords(), 5));
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaMessageRecords.total(owner, List.of(), 0));
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaMessageRecords.total(owner, List.of(), 7));
        assertTrue(TargetQuotaMessageRecords.total(owner, List.of(), 1).isZero());
        assertEquals(
                messageRecord(owner, message).charge(),
                TargetQuotaMessageRecords.total(owner, List.of(messageRecord(owner, message)), 1));
    }

    @Test
    void subsetCannotMixOwnerPhasesOrBeforeAndAfterMessageVersions() {
        final var retained = owner("inline.retained");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.total(owner, List.of(ownerRecord(retained)), 1));
        final var changed = copyMessage(message, message.scheduleSource(), message.stateVersion() + 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.total(
                        owner, List.of(messageRecord(owner, changed), timeline(owner, message, "due")), 2));
    }

    @Test
    void messageRecordKeepsOriginalAccountingDespiteCurrentLocatorIncarnation() {
        final var record = messageRecord(owner, message);
        assertTrue(!Arrays.equals(
                message.locator().accountingIncarnation(),
                owner.primaryIdentity().accountingIncarnation()));
        assertEquals(owner.primaryIdentity(), record.owner().primaryIdentity());
        assertArrayEquals(
                owner.accounting().canonicalBytes(), record.owner().accounting().canonicalBytes());
    }

    @Test
    void releasedOwnerAndRetainedInitialBindingStillHaveRecordFees() {
        final var released = owner("inline.released");
        assertTrue(released.payloadCharge().isZero());
        final var total =
                TargetQuotaMessageRecords.total(released, List.of(ownerRecord(released), bindingRecord(released)), 2);
        assertTrue(total.amount(CapacityDimension.LOGICAL_STATE_BYTES) > 0);
        assertThrows(IllegalStateException.class, () -> messageRecord(released, message));
        assertThrows(IllegalStateException.class, () -> timeline(released, message, "due"));
    }

    @Test
    void sourceResourceReplacementCannotHideBehindMatchingMessageAndPayload() {
        final var source = (KafkaSourcePosition) message.scheduleSource();
        final var foreign = new KafkaSourcePosition(
                source.shardId(),
                "another-cluster",
                source.nativeTopicUuid(),
                source.offset(),
                source.leaderEpoch(),
                source.brokerPersistenceTimeEpochMs());
        final var changed = copyMessage(message, foreign, message.stateVersion());
        assertThrows(IllegalArgumentException.class, () -> messageRecord(owner, changed));
    }

    @Test
    void sameSourcePositionAlsoRequiresExactMetadata() {
        final var source = (KafkaSourcePosition) message.scheduleSource();
        final var changed = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                source.offset(),
                source.leaderEpoch(),
                91);
        assertThrows(
                IllegalArgumentException.class,
                () -> messageRecord(owner, copyMessage(message, changed, message.stateVersion())));
    }

    @Test
    void wrongKeysCorruptionAndStaleOwnerBytesFailBeforeProducingCharges() {
        final byte[] wrong = message.encodedKey();
        wrong[0]++;
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.message(owner, wrong, message.canonicalBytes()));
        final byte[] corrupt = message.canonicalBytes();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.message(owner, message.encodedKey(), corrupt));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.payloadOwner(
                        owner, owner.key(), owner("inline.retained").canonicalBytes()));
        final var different = TargetScheduleBinding.decode(raw(binding, "binding.native"));
        assertThrows(
                IllegalStateException.class,
                () -> TargetQuotaMessageRecords.initialBinding(
                        owner, different.encodedKey(), different.canonicalBytes()));
    }

    @Test
    void claimedOrTerminalMessagesCannotSupplyStaleTimelineAndExpiry() {
        final var claimed = message("message.claimed");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.timeline(
                        owner,
                        claimed,
                        raw(vectors, "due.key"),
                        message.runtime().timeline().canonicalBytes()));
        final var terminal = message("message.terminal");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.expiry(
                        owner("inline.retained"),
                        terminal,
                        raw(identity, "message.expiry.key"),
                        raw(identity, "message.expiry")));
    }

    @Test
    void orderHeadNeedsTheActualOpenServiceableStateAndFullWork() {
        final var fifo = message("order.message.initial");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.orderHead(
                        owner,
                        fifo,
                        state("order.closed"),
                        raw(vectors, "orderHead.key"),
                        fifo.runtime().timeline().canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMessageRecords.timeline(
                        owner,
                        fifo,
                        raw(vectors, "orderHead.key"),
                        fifo.runtime().timeline().canonicalBytes()));
    }

    @Test
    void fullRecordRecheckClosesFamilyTypeKeyAndValueAndCopiesAreDefensive() {
        final var record = messageRecord(owner, message);
        final byte[] key = record.key();
        key[0]++;
        final byte[] bytes = record.canonicalPayload();
        bytes[bytes.length - 1] ^= 1;
        assertThrows(
                IllegalStateException.class,
                () -> record.requireStored(
                        ColumnFamily.META, record.valueType(), record.key(), record.canonicalPayload()));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireStored(record.family(), 14, record.key(), record.canonicalPayload()));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireStored(record.family(), record.valueType(), key, record.canonicalPayload()));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireStored(record.family(), record.valueType(), record.key(), bytes));
        assertArrayEquals(message.encodedKey(), record.key());
        assertArrayEquals(message.canonicalBytes(), record.canonicalPayload());
    }

    @Test
    void overflowFailsWithoutReturningAClampedOrPartialTotal() {
        final var accounting =
                new TargetQuotaAccounting(owner.accounting().schemaBundleHash(), Long.MAX_VALUE / 2, 24, 40, 64);
        final var large = TargetQuotaPayloadOwner.scheduled(
                initial, owner.tenantScope(), accounting, owner.recoveryLineage(), owner.mutation());
        final var one = messageRecord(large, message);
        final var two = timeline(large, message, "due");
        assertThrows(ArithmeticException.class, () -> TargetQuotaMessageRecords.total(large, List.of(one, two), 2));
        assertTrue(one.charge().amount(CapacityDimension.LOGICAL_STATE_BYTES) > 0);
    }

    private List<TargetQuotaMessageRecords.Record> nativeRecords() {
        return List.of(
                ownerRecord(owner),
                bindingRecord(owner),
                messageRecord(owner, message),
                timeline(owner, message, "due"),
                timeline(owner, message, "native"),
                expiry(owner, message));
    }

    private TargetQuotaMessageRecords.Record ownerRecord(final TargetQuotaPayloadOwner value) {
        return TargetQuotaMessageRecords.payloadOwner(value, value.key(), value.canonicalBytes());
    }

    private TargetQuotaMessageRecords.Record bindingRecord(final TargetQuotaPayloadOwner value) {
        return TargetQuotaMessageRecords.initialBinding(value, initial.encodedKey(), initial.canonicalBytes());
    }

    private TargetQuotaMessageRecords.Record messageRecord(
            final TargetQuotaPayloadOwner value, final TargetMessageRecord current) {
        return TargetQuotaMessageRecords.message(value, current.encodedKey(), current.canonicalBytes());
    }

    private TargetQuotaMessageRecords.Record timeline(
            final TargetQuotaPayloadOwner value, final TargetMessageRecord current, final String kind) {
        return TargetQuotaMessageRecords.timeline(
                value,
                current,
                raw(vectors, kind + ".key"),
                current.runtime().timeline().canonicalBytes());
    }

    private TargetQuotaMessageRecords.Record expiry(
            final TargetQuotaPayloadOwner value, final TargetMessageRecord current) {
        final var ref = new TargetExpiryRef(current.locator(), current.expireAtEpochMs());
        return TargetQuotaMessageRecords.expiry(value, current, ref.encodedKey(), ref.canonicalBytes());
    }

    private TargetQuotaPayloadOwner owner(final String key) {
        return TargetQuotaPayloadOwner.decode(raw(payload, key));
    }

    private TargetMessageRecord message(final String key) {
        return TargetMessageRecord.decode(raw(identity, key));
    }

    private TargetOrderState state(final String key) {
        return TargetOrderState.decode(raw(identity, key));
    }

    private static TargetMessageRecord copyMessage(
            final TargetMessageRecord value,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final long revision) {
        return new TargetMessageRecord(
                value.locator(),
                revision,
                value.deliverAtEpochMs(),
                value.expireAtEpochMs(),
                value.retryEligibilityAtEpochMs(),
                value.nativeDeliveryPolicy(),
                source,
                value.inlinePayload(),
                value.payloadReference(),
                value.runtime());
    }

    private static byte[] raw(final Properties properties, final String name) {
        return HexFormat.of().parseHex(properties.getProperty(name));
    }

    private static Properties properties(final String name) {
        try (var in = TargetQuotaMessageRecordsTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var properties = new Properties();
            properties.load(in);
            return properties;
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
