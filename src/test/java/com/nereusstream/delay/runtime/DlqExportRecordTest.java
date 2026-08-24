package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportStateV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DlqExportRecordTest {
    @Test
    void notConfiguredRecordUsesDeterministicIdentityAndCanonicalRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] source =
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, null, 1_000).canonicalBytes();

        final DlqExportRecord record = DlqExportRecord.notConfigured(messageId, 2, 17, source);

        assertEquals(DlqExportStateV1.NOT_CONFIGURED, record.state());
        assertEquals(0, record.physicalAttemptNo());
        assertEquals(record, DlqExportRecord.decode(record.encode()));
        assertArrayEquals(record.dlqExportId(), DlqExportRecord.deriveId(messageId, 2, 17));
    }

    @Test
    void preservesUnsignedTerminalRevisionBitsAcrossIdentityAndRecordRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] source =
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, null, 1_000).canonicalBytes();

        final DlqExportRecord record = DlqExportRecord.notConfigured(messageId, 2, Long.MIN_VALUE, source);

        assertEquals(Long.MIN_VALUE, DlqExportRecord.decode(record.encode()).terminalRevision());
        assertArrayEquals(record.dlqExportId(), DlqExportRecord.deriveId(messageId, 2, Long.MIN_VALUE));
    }

    @Test
    void preservesUnsignedGenerationAndPhysicalAttemptBitsAcrossRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] source =
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, null, 1_000).canonicalBytes();
        final int highBit = (int) 0x8000_0000L;
        final byte[] envelope = Bytes.sha256(Bytes.utf8("high-bit-dlq-envelope"));
        final byte[] emptyCharge = new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
        final DlqExportRecord record = new DlqExportRecord(
                DlqExportRecord.deriveId(messageId, highBit, 1),
                messageId,
                highBit,
                1,
                envelope,
                emptyCharge,
                DlqExportStateV1.PENDING,
                highBit,
                source);

        assertEquals(record, DlqExportRecord.decode(record.encode()));
    }

    @Test
    void rejectsIdentityAndStateDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] source =
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, null, 1_000).canonicalBytes();
        final DlqExportRecord record = DlqExportRecord.notConfigured(messageId, 0, 1, source);
        final byte[] encoded = record.encode();
        encoded[4] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> DlqExportRecord.decode(encoded));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DlqExportRecord(
                        Bytes.sha256(Bytes.utf8("wrong")),
                        messageId,
                        0,
                        1,
                        Bytes.sha256(Bytes.utf8("envelope")),
                        DlqExportStateV1.NOT_CONFIGURED,
                        1,
                        source));
    }

    @Test
    void configuredRecordRetainsCanonicalChargeAndSupportsLegacyZeroChargeDecode() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] source =
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, null, 1_000).canonicalBytes();
        final byte[] envelope = Bytes.sha256(Bytes.utf8("configured-envelope"));
        final byte[] charge = new PublishAdmissionBody.ChargeVector(2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
        final DlqExportRecord record = DlqExportRecord.pending(messageId, 0, 1, envelope, charge, source);

        assertArrayEquals(charge, record.retainedCharge());
        assertEquals(record, DlqExportRecord.decode(record.encode()));

        final byte[] legacy = Bytes.concat(
                Bytes.u32be(1),
                record.dlqExportId(),
                messageId.bytes(),
                Bytes.u32be(record.generation()),
                Bytes.u64be(record.terminalRevision()),
                record.exportEnvelopeHash(),
                Bytes.u8(record.state().wireValue()),
                Bytes.u32be(record.physicalAttemptNo()),
                Bytes.lp32(record.appliedSourcePosition()));
        final DlqExportRecord decodedLegacy = DlqExportRecord.decode(legacy);
        assertArrayEquals(
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                        .canonicalBytes(),
                decodedLegacy.retainedCharge());
    }

    @Test
    void sourcePositionMustBelongToExportMessageShard() {
        final ShardId messageShard = new ShardId(RouteIncarnation.random(), 11);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 12);
        assertThrows(
                IllegalArgumentException.class,
                () -> DlqExportRecord.notConfigured(
                        DelayMessageId.random(messageShard),
                        0,
                        1,
                        new KafkaSourcePosition(foreignShard, "cluster", UUID.randomUUID(), 9, null, 1_000)
                                .canonicalBytes()));
    }
}
