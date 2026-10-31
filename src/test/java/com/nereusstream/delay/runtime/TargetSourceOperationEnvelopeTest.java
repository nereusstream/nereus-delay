package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardLogFrame;
import com.nereusstream.delay.protocol.TargetMigrationArchiveSourceFrame;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetSourceOperationEnvelopeTest {
    private static final Duration CALCULATION_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void basicCalculationTerminatesButFixedTermsDoNotQualifySourceService() {
        final var fixed = assertTimeoutPreemptively(CALCULATION_TIMEOUT, TargetSourceOperationEnvelope::basic);

        assertTrue(fixed.readRecords() > 0);
        assertTrue(fixed.writeRecords() > 0);
        assertTrue(fixed.fixedReadBytes() > 0);
        assertTrue(fixed.fixedWriteBytes() > 0);
        assertEquals(fixed.fixedReadBytes(), fixed.readBytes());
        assertEquals(fixed.fixedWriteBytes(), fixed.writeBytes());
        assertTrue(fixed.requiresSourceFrameQualification());
        assertFalse(fixed.includes(fixed));
        assertThrows(TargetSourceAuthorityPreparation.InputUnavailable.class, fixed::allocatedBytes);
        assertThrows(TargetSourceAuthorityPreparation.InputUnavailable.class, fixed::retainedBytes);
        assertThrows(TargetSourceAuthorityPreparation.InputUnavailable.class, fixed::maximumFrameBytes);

        final var covering = fixed.covering(fixed);
        assertEquals(fixed.readRecords(), covering.readRecords());
        assertEquals(fixed.fixedReadBytes(), covering.fixedReadBytes());
        assertEquals(fixed.writeRecords(), covering.writeRecords());
        assertEquals(fixed.fixedWriteBytes(), covering.fixedWriteBytes());
        assertEquals(fixed.fixedRetainedBytes(), covering.fixedRetainedBytes());
        assertEquals(fixed.fixedMaximumFrameBytes(), covering.fixedMaximumFrameBytes());
        assertTrue(covering.requiresSourceFrameQualification());
        assertFalse(covering.includes(fixed));
        assertFalse(fixed.includes(covering));
        assertThrows(TargetSourceAuthorityPreparation.InputUnavailable.class, covering::retainedBytes);
    }

    @Test
    void actualFrameDecodeIsBoundedMonotonicAndCoversTheRealFramingCopies() {
        // decodeFrame accepts the exact opaque body; the typed managed-body decoder is a later step.
        final byte[] body = new byte[4096];
        final var shard = new ShardId(new RouteIncarnation(new byte[RouteIncarnation.LENGTH]), 1);
        final var commandId = new CommandId(
                SelfRoutingId.fromLogicalUuid(shard, UUID.fromString("00000000-0064-7000-8000-000000000001"))
                        .bytes());
        final var messageId = new DelayMessageId(
                SelfRoutingId.fromLogicalUuid(shard, UUID.fromString("00000000-0064-7000-8000-000000000002"))
                        .bytes());
        final var command = PreparedCommand.create(shard, commandId, messageId, CommandType.SCHEDULE, 100, body);
        final byte[] actualFrame = CommandCodec.encodeFrame(command);
        assertArrayEquals(body, CommandCodec.decodeFrame(actualFrame).canonicalBody());

        assertTimeoutPreemptively(CALCULATION_TIMEOUT, () -> {
            TargetSourceOperationEnvelope.FrameDecodeBounds previous = null;
            for (long frame :
                    new long[] {actualFrame.length, 64L * 1024 * 1024, TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES
                    }) {
                final var current = TargetSourceOperationEnvelope.frameDecode(frame);
                final long envelope = frame - ShardLogFrame.HEADER_LENGTH - ShardLogFrame.TRAILER_LENGTH;
                // The original input F remains owned. ShardLogFrame.decode makes copyOfRange,
                // Decoded's owned copy, and canonicalEnvelope()'s accessor copy of the envelope.
                final long framingAllocated = frame + 3 * envelope;
                final long framingRetained = frame + 2 * envelope;
                assertEquals(frame, current.maximumFrameBytes());
                assertTrue(current.allocatedBytes() >= framingAllocated);
                assertTrue(current.retainedBytes() >= framingRetained);
                assertTrue(current.allocatedBytes() >= current.retainedBytes());
                // CommandCodec reads the two outer fields and all eight ClientCommand fields.
                assertTrue(current.fieldVisits() >= 10);
                assertTrue(current.arrayObjects() >= 4);
                if (previous != null) {
                    assertTrue(current.allocatedBytes() >= previous.allocatedBytes());
                    assertTrue(current.retainedBytes() >= previous.retainedBytes());
                    assertTrue(current.fieldVisits() >= previous.fieldVisits());
                    assertTrue(current.arrayObjects() >= previous.arrayObjects());
                }
                previous = current;
            }
        });
    }

    @Test
    void actualFrameDecodeRejectsLengthsOutsideTheCompleteRepresentation() {
        for (long invalid : new long[] {
            -1,
            0,
            ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH - 1L,
            (long) TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES + 1,
            Long.MAX_VALUE
        }) {
            assertThrows(IllegalArgumentException.class, () -> TargetSourceOperationEnvelope.frameDecode(invalid));
        }
    }
}
