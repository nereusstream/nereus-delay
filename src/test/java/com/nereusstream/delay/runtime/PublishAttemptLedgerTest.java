package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PublishAttemptLedgerTest {
    @Test
    void decodeRejectsTruncatedNumericSuffixesAsValidationErrors() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final PublishAttemptLedger ledger = PublishAttemptLedger.publishing(
                DelayMessageId.random(shardId),
                0,
                Bytes.sha256(Bytes.utf8("publish-attempt")),
                Bytes.sha256(Bytes.utf8("claim")),
                Long.MIN_VALUE,
                1,
                DestinationLaneId.derive(Bytes.utf8("publish-attempt-lane")),
                new byte[16],
                new byte[] {1},
                new byte[16],
                Bytes.sha256(Bytes.utf8("prepared")),
                new byte[] {2},
                sourcePosition(shardId));
        final byte[] encoded = ledger.encode();

        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> PublishAttemptLedger.decode(truncated),
                    "truncated publish attempt ledger length=" + length);
        }
        assertEquals(ledger, PublishAttemptLedger.decode(encoded));
    }

    @Test
    void sourcePositionMustBeCanonicalBeforeAttemptValueConstruction() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> PublishAttemptLedger.publishing(
                        DelayMessageId.random(shardId),
                        0,
                        Bytes.sha256(Bytes.utf8("source-fence-attempt")),
                        Bytes.sha256(Bytes.utf8("source-fence-claim")),
                        1,
                        1,
                        DestinationLaneId.derive(Bytes.utf8("source-fence-lane")),
                        new byte[16],
                        new byte[] {1},
                        new byte[16],
                        Bytes.sha256(Bytes.utf8("source-fence-prepared")),
                        canonicalAdmissionBytes(),
                        Bytes.utf8("not-a-source-position")));
    }

    @Test
    void sourcePositionMustBelongToAttemptMessageShard() {
        final ShardId messageShard = new ShardId(RouteIncarnation.random(), 1);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> PublishAttemptLedger.publishing(
                        DelayMessageId.random(messageShard),
                        0,
                        Bytes.sha256(Bytes.utf8("foreign-source-attempt")),
                        Bytes.sha256(Bytes.utf8("foreign-source-claim")),
                        1,
                        1,
                        DestinationLaneId.derive(Bytes.utf8("foreign-source-lane")),
                        new byte[16],
                        new byte[] {1},
                        new byte[16],
                        Bytes.sha256(Bytes.utf8("foreign-source-prepared")),
                        canonicalAdmissionBytes(),
                        sourcePosition(foreignShard)));
    }

    @Test
    void v2LedgerRoundTripsAnIndependentRetryWindowAndKeepsV1Compatibility() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final PublishAttemptLedger ledger = PublishAttemptLedger.publishingWithRetryWindow(
                DelayMessageId.random(shardId),
                0,
                Bytes.sha256(Bytes.utf8("publish-attempt-v2")),
                Bytes.sha256(Bytes.utf8("claim-v2")),
                1,
                1,
                DestinationLaneId.derive(Bytes.utf8("publish-attempt-lane-v2")),
                new byte[16],
                new byte[] {1},
                new byte[16],
                Bytes.sha256(Bytes.utf8("prepared-v2")),
                canonicalAdmissionBytes(),
                2_001,
                5_000,
                sourcePosition(shardId));

        assertTrue(ledger.hasRetryWindow());
        assertEquals(2_001, ledger.firstAttemptAtEpochMs());
        assertEquals(5_000, ledger.retryDeadlineEpochMs());
        assertEquals(ledger, PublishAttemptLedger.decode(ledger.encode()));
        assertEquals(2, java.nio.ByteBuffer.wrap(ledger.encode()).getInt());

        final PublishAttemptLedger legacy = PublishAttemptLedger.publishing(
                ledger.delayMessageId(),
                0,
                Bytes.sha256(Bytes.utf8("publish-attempt-v1")),
                Bytes.sha256(Bytes.utf8("claim-v1")),
                1,
                1,
                ledger.laneId(),
                new byte[16],
                new byte[] {1},
                new byte[16],
                Bytes.sha256(Bytes.utf8("prepared-v1")),
                canonicalAdmissionBytes(),
                sourcePosition(shardId));
        assertFalse(legacy.hasRetryWindow());
        assertEquals(legacy, PublishAttemptLedger.decode(legacy.encode()));
        assertEquals(1, java.nio.ByteBuffer.wrap(legacy.encode()).getInt());
        assertThrows(IllegalStateException.class, legacy::firstAttemptAtEpochMs);
    }

    @Test
    void v1LedgerPreservesHighBitGenerationAndAttemptBits() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final int highBit = (int) 0x8000_0000L;
        final PublishAttemptLedger ledger = PublishAttemptLedger.publishing(
                DelayMessageId.random(shardId),
                highBit,
                Bytes.sha256(Bytes.utf8("high-bit-attempt")),
                Bytes.sha256(Bytes.utf8("high-bit-claim")),
                1,
                highBit,
                DestinationLaneId.derive(Bytes.utf8("high-bit-lane")),
                new byte[16],
                new byte[] {1},
                new byte[16],
                Bytes.sha256(Bytes.utf8("high-bit-prepared")),
                canonicalAdmissionBytes(),
                sourcePosition(shardId));

        final PublishAttemptLedger decoded = PublishAttemptLedger.decode(ledger.encode());
        assertEquals(highBit, decoded.generation());
        assertEquals(0x8000_0000L, Integer.toUnsignedLong(decoded.generation()));
        assertEquals(highBit, decoded.attemptNo());
        assertEquals(0x8000_0000L, Integer.toUnsignedLong(decoded.attemptNo()));
    }

    @Test
    void v3LedgerPersistsJournalMappingAndRetirementLifecycle() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final PublishAttemptLedger base = PublishAttemptLedger.publishingWithRetryWindow(
                DelayMessageId.random(shardId),
                0,
                Bytes.sha256(Bytes.utf8("journal-attempt")),
                Bytes.sha256(Bytes.utf8("journal-claim")),
                1,
                1,
                DestinationLaneId.derive(Bytes.utf8("journal-lane")),
                new byte[16],
                new byte[] {1},
                new byte[16],
                Bytes.sha256(Bytes.utf8("journal-prepared")),
                canonicalAdmissionBytes(),
                2_001,
                5_000,
                sourcePosition(shardId));

        final PublishAttemptLedger allocated = base.withAllocatedJournalSequence(0);
        assertTrue(allocated.hasAllocatedJournalSequence());
        assertEquals(0, allocated.journalSequenceId());
        assertFalse(allocated.mappingDurable());
        assertEquals(3, java.nio.ByteBuffer.wrap(allocated.encode()).getInt());

        final byte[] mappedPosition = Bytes.sha256(Bytes.utf8("mapped-position"));
        final PublishAttemptLedger mapped = allocated.withDurableJournalMapping(0, mappedPosition);
        assertTrue(mapped.mappingDurable());
        assertArrayEquals(mappedPosition, mapped.journalPosition());
        assertEquals(mapped, mapped.withDurableJournalMapping(0, mappedPosition));

        final PublishAttemptLedger pending = mapped.withRetirementPending();
        assertTrue(pending.retirementPending());
        assertThrows(
                IllegalStateException.class,
                () -> pending.withUnknownOutcome(Bytes.utf8("unknown"), new byte[0], sourcePosition(shardId)));

        final byte[] retiredPosition = Bytes.sha256(Bytes.utf8("retired-position"));
        final PublishAttemptLedger retired = pending.withDurableRetirement(retiredPosition);
        assertFalse(retired.retirementPending());
        assertArrayEquals(retiredPosition, retired.journalPosition());
        assertEquals(retired, PublishAttemptLedger.decode(retired.encode()));
    }

    @Test
    void journalLifecycleRejectsSequenceAndEvidenceDrift() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final PublishAttemptLedger base = PublishAttemptLedger.publishing(
                DelayMessageId.random(shardId),
                0,
                Bytes.sha256(Bytes.utf8("journal-drift-attempt")),
                Bytes.sha256(Bytes.utf8("journal-drift-claim")),
                1,
                1,
                DestinationLaneId.derive(Bytes.utf8("journal-drift-lane")),
                new byte[16],
                new byte[] {1},
                new byte[16],
                Bytes.sha256(Bytes.utf8("journal-drift-prepared")),
                canonicalAdmissionBytes(),
                sourcePosition(shardId));
        final PublishAttemptLedger allocated = base.withAllocatedJournalSequence(7);
        assertThrows(IllegalStateException.class, () -> allocated.withAllocatedJournalSequence(8));
        final byte[] position = Bytes.sha256(Bytes.utf8("journal-drift-position"));
        final PublishAttemptLedger mapped = allocated.withDurableJournalMapping(7, position);
        assertThrows(
                IllegalStateException.class,
                () -> mapped.withDurableJournalMapping(7, Bytes.sha256(Bytes.utf8("other-position"))));
        assertThrows(IllegalStateException.class, () -> base.withRetirementPending());
        assertThrows(IllegalStateException.class, () -> mapped.withDurableRetirement(position));
        assertThrows(IllegalArgumentException.class, () -> allocated.withDurableJournalMapping(7, new byte[129]));
    }

    private static byte[] canonicalAdmissionBytes() {
        return Bytes.utf8("canonical-admission-placeholder");
    }

    private static byte[] sourcePosition(final ShardId shardId) {
        return new KafkaSourcePosition(shardId, "test", UUID.randomUUID(), 0, null, 1_000).canonicalBytes();
    }
}
