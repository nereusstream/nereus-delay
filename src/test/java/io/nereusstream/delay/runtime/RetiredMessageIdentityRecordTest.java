package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetiredMessageIdentityRecordTest {
    @Test
    void roundTripsUnsignedRetirementSequenceAndCopiesSourcePosition() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] source = Bytes.sha256(Bytes.utf8("retired-source"));
        final RetiredMessageIdentityRecord record = new RetiredMessageIdentityRecord(
                messageId, 9_876_543_210L, -1L, source);

        final byte[] encoded = record.encode();
        source[0] ^= 1;
        final RetiredMessageIdentityRecord decoded = RetiredMessageIdentityRecord.decode(encoded);

        assertTrue(RetiredMessageIdentityRecord.isEncoded(encoded));
        assertEquals(messageId, decoded.messageId());
        assertEquals(9_876_543_210L, decoded.messageIdentityReuseUntilEpochMs());
        assertEquals(-1L, decoded.retirementMutationSequence());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("retired-source")), decoded.appliedSourcePosition());
        assertArrayEquals(encoded, decoded.encode());
    }

    @Test
    void rejectsNonCanonicalOrMalformedPayloads() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final RetiredMessageIdentityRecord record = new RetiredMessageIdentityRecord(
                DelayMessageId.random(shard), 42, 1, Bytes.utf8("source"));
        final byte[] encoded = record.encode();

        final byte[] trailing = Bytes.concat(encoded, new byte[]{1});
        assertThrows(IllegalArgumentException.class,
                () -> RetiredMessageIdentityRecord.decode(trailing));

        final byte[] altered = encoded.clone();
        final int sourceLengthOffset = Integer.BYTES + DelayMessageId.LENGTH + Long.BYTES + Long.BYTES;
        altered[sourceLengthOffset + 3] = 5;
        assertThrows(IllegalArgumentException.class,
                () -> RetiredMessageIdentityRecord.decode(altered));
        assertThrows(IllegalArgumentException.class,
                () -> RetiredMessageIdentityRecord.decode(Bytes.u32be(4)));
    }
}
