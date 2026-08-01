package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.store.KeyCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GenerationRuntimeIndexTest {
    @Test
    void timelineWorkRoundTripsAndSeparatesSemanticFromInstanceRevision() {
        final byte[] key = timelineKey(100, 0);
        final TimelineWorkRef first = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE, key, 100, 100,
                1, 7, false, UncertainRetryAuthority.NONE, null, null);
        final TimelineWorkRef second = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE, key, 100, 100,
                1, 8, false, UncertainRetryAuthority.NONE, null, null);

        assertEquals(first, TimelineWorkRef.decode(first.canonicalBytes()));
        assertArrayEquals(first.semanticWorkDigest(), second.semanticWorkDigest());
        org.junit.jupiter.api.Assertions.assertFalse(Bytes.constantTimeEquals(first.workInstanceDigest(),
                second.workInstanceDigest()));
        assertThrows(IllegalArgumentException.class,
                () -> new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE, key, 100, 99, 1, 7,
                        false, UncertainRetryAuthority.NONE, null, null));
    }

    @Test
    void runtimeIndexRoundTripsRepeatedObligationsInCanonicalOrder() {
        final byte[] firstId = Bytes.sha256(Bytes.utf8("attempt-1"));
        final byte[] secondId = Bytes.sha256(Bytes.utf8("attempt-2"));
        final AttemptObligationRef first = new AttemptObligationRef(firstId, 0, AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, 1, firstId));
        final AttemptObligationRef second = new AttemptObligationRef(secondId, 0, AttemptLedgerState.UNCERTAIN,
                KeyCodec.inflight((byte) 3, 2, secondId));
        final List<AttemptObligationRef> obligations = ((first.publishAttemptId()[0] & 0xff)
                < (second.publishAttemptId()[0] & 0xff)) ? List.of(first, second) : List.of(second, first);
        final GenerationRuntimeIndex index = GenerationRuntimeIndex.publishing(firstId, obligations, 2, 1,
                true, 9);

        assertEquals(index, GenerationRuntimeIndex.decode(index.canonicalBytes()));
        assertArrayEquals(GenerationRuntimeIndex.obligationSetDigest(obligations),
                GenerationRuntimeIndex.obligationSetDigest(index.attemptObligations()));
        assertThrows(IllegalArgumentException.class,
                () -> GenerationRuntimeIndex.publishing(firstId, List.of(first, first), 2, 1, false, 9));
    }

    private static byte[] timelineKey(final long eligibleAt, final int generation) {
        return KeyCodec.timelineDue(new DestinationLaneId(new byte[32]), eligibleAt,
                Bytes.concat(new byte[]{1}, new byte[8]),
                io.nereusstream.delay.protocol.DelayMessageId.random(
                        new io.nereusstream.delay.protocol.ShardId(
                                new io.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0)), generation);
    }
}
