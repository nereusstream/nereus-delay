package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.store.KeyCodec;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class GenerationRuntimeIndexTest {
    @Test
    void timelineWorkRoundTripsAndSeparatesSemanticFromInstanceRevision() {
        final byte[] key = timelineKey(100, 0);
        final TimelineWorkRef first = new TimelineWorkRef(
                TimelineWorkKind.INITIAL_SCHEDULE,
                key,
                100,
                100,
                1,
                7,
                false,
                UncertainRetryAuthority.NONE,
                null,
                null);
        final TimelineWorkRef second = new TimelineWorkRef(
                TimelineWorkKind.INITIAL_SCHEDULE,
                key,
                100,
                100,
                1,
                8,
                false,
                UncertainRetryAuthority.NONE,
                null,
                null);

        assertEquals(first, TimelineWorkRef.decode(first.canonicalBytes()));
        assertArrayEquals(first.semanticWorkDigest(), second.semanticWorkDigest());
        org.junit.jupiter.api.Assertions.assertFalse(
                Bytes.constantTimeEquals(first.workInstanceDigest(), second.workInstanceDigest()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        key,
                        100,
                        99,
                        1,
                        7,
                        false,
                        UncertainRetryAuthority.NONE,
                        null,
                        null));
    }

    @Test
    void timelineWorkFencesPhysicalEligibilityAndOrderedUncertainRetry() {
        final byte[] dueAtTwoHundred = timelineKey(200, 0);
        final TimelineWorkRef due = new TimelineWorkRef(
                TimelineWorkKind.DEFINITIVE_RETRY,
                dueAtTwoHundred,
                100,
                200,
                2,
                7,
                false,
                UncertainRetryAuthority.NONE,
                null,
                null);
        assertEquals(due, TimelineWorkRef.decode(due.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.DEFINITIVE_RETRY,
                        timelineKey(100, 0),
                        100,
                        200,
                        2,
                        7,
                        false,
                        UncertainRetryAuthority.NONE,
                        null,
                        null));

        final byte[] orderedAtOneHundred = orderedTimelineKey(100, 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.DEFINITIVE_RETRY,
                        orderedAtOneHundred,
                        200,
                        200,
                        2,
                        7,
                        true,
                        UncertainRetryAuthority.NONE,
                        null,
                        null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        orderedAtOneHundred,
                        100,
                        100,
                        2,
                        7,
                        true,
                        UncertainRetryAuthority.PINNED_POLICY,
                        null,
                        null));
    }

    @Test
    void controlOverrideTimelineRequiresCanonicalTypedNestedValues() {
        final var shard = new com.nereusstream.delay.protocol.ShardId(
                new com.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 4, null, 1_000);
        final ControlRef control =
                new ControlRef(Bytes.sha256(Bytes.utf8("operation")), Bytes.sha256(Bytes.utf8("request")), 3);
        final byte[] key = timelineKey(1_000, 0);
        final TimelineWorkRef valid = new TimelineWorkRef(
                TimelineWorkKind.UNCERTAIN_RETRY,
                key,
                900,
                1_000,
                2,
                7,
                false,
                UncertainRetryAuthority.CONTROL_OVERRIDE,
                control.canonicalBytes(),
                source.canonicalBytes());
        assertEquals(valid, TimelineWorkRef.decode(valid.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        key,
                        900,
                        1_000,
                        2,
                        7,
                        false,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        Bytes.utf8("not-control"),
                        source.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        key,
                        900,
                        1_000,
                        2,
                        7,
                        false,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        control.canonicalBytes(),
                        Bytes.concat(source.canonicalBytes(), new byte[] {0})));
    }

    @Test
    void controlOverrideTimelineRejectsSourcePositionFromAnotherShard() {
        final var timelineShard = new com.nereusstream.delay.protocol.ShardId(
                new com.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0);
        final var foreignShard = new com.nereusstream.delay.protocol.ShardId(
                new com.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 1);
        final KafkaSourcePosition foreignSource =
                new KafkaSourcePosition(foreignShard, "cluster", UUID.randomUUID(), 4, null, 1_000);
        final ControlRef control = new ControlRef(
                Bytes.sha256(Bytes.utf8("operation-foreign")), Bytes.sha256(Bytes.utf8("request-foreign")), 3);
        final byte[] key = KeyCodec.timelineDue(
                new DestinationLaneId(new byte[32]),
                1_000,
                Bytes.concat(new byte[] {1}, new byte[8]),
                com.nereusstream.delay.protocol.DelayMessageId.random(timelineShard),
                2);

        assertThrows(
                IllegalArgumentException.class,
                () -> new TimelineWorkRef(
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        key,
                        900,
                        1_000,
                        2,
                        7,
                        false,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        control.canonicalBytes(),
                        foreignSource.canonicalBytes()));
    }

    @Test
    void runtimeIndexRoundTripsRepeatedObligationsInCanonicalOrder() {
        final byte[] firstId = Bytes.sha256(Bytes.utf8("attempt-1"));
        final byte[] secondId = Bytes.sha256(Bytes.utf8("attempt-2"));
        final AttemptObligationRef first = new AttemptObligationRef(
                firstId, 0, AttemptLedgerState.PUBLISHING, KeyCodec.inflight((byte) 2, 1, firstId));
        final AttemptObligationRef second = new AttemptObligationRef(
                secondId, 0, AttemptLedgerState.UNCERTAIN, KeyCodec.inflight((byte) 3, 2, secondId));
        final List<AttemptObligationRef> obligations =
                ((first.publishAttemptId()[0] & 0xff) < (second.publishAttemptId()[0] & 0xff))
                        ? List.of(first, second)
                        : List.of(second, first);
        final GenerationRuntimeIndex index = GenerationRuntimeIndex.publishing(firstId, obligations, 2, 1, true, 9);

        assertEquals(index, GenerationRuntimeIndex.decode(index.canonicalBytes()));
        assertArrayEquals(
                GenerationRuntimeIndex.obligationSetDigest(obligations),
                GenerationRuntimeIndex.obligationSetDigest(index.attemptObligations()));
        assertThrows(
                IllegalArgumentException.class,
                () -> GenerationRuntimeIndex.publishing(firstId, List.of(first, first), 2, 1, false, 9));
    }

    @Test
    void attemptObligationPreservesUnsignedGenerationBits() {
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("attempt-high-bit-generation"));
        final AttemptObligationRef reference = new AttemptObligationRef(
                attemptId, Integer.MIN_VALUE, AttemptLedgerState.PUBLISHING, KeyCodec.inflight((byte) 2, 1, attemptId));

        assertEquals(reference, AttemptObligationRef.decode(reference.canonicalBytes()));
    }

    @Test
    void runtimeIndexFencesAggregateAndCurrentWorkProjectionDrift() {
        final byte[] key = timelineKey(100, 0);
        final TimelineWorkRef initial = TimelineWorkRef.initial(key, 100, 7);
        assertThrows(
                IllegalArgumentException.class,
                () -> new GenerationRuntimeIndex(
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.NONE,
                        null,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        7));
        final GenerationRuntimeIndex scheduled = new GenerationRuntimeIndex(
                GenerationAggregateState.SCHEDULED,
                CurrentSendWorkKind.TIMELINE,
                initial,
                null,
                null,
                List.of(),
                0,
                0,
                false,
                7);
        assertEquals(GenerationAggregateState.SCHEDULED, scheduled.aggregateState());
        assertThrows(
                IllegalArgumentException.class,
                () -> new GenerationRuntimeIndex(
                        GenerationAggregateState.RETRY_WAIT,
                        CurrentSendWorkKind.TIMELINE,
                        initial,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        7));

        final byte[] uncertainId = Bytes.sha256(Bytes.utf8("uncertain"));
        final AttemptObligationRef uncertain = new AttemptObligationRef(
                uncertainId, 0, AttemptLedgerState.UNCERTAIN, KeyCodec.inflight((byte) 3, 1, uncertainId));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GenerationRuntimeIndex(
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.TIMELINE,
                        initial,
                        null,
                        null,
                        List.of(uncertain),
                        1,
                        0,
                        false,
                        7));
        assertThrows(
                IllegalArgumentException.class,
                () -> GenerationRuntimeIndex.timeline(
                        GenerationAggregateState.UNCERTAIN, initial, List.of(uncertain), 1, 0, false, 7));
        final GenerationRuntimeIndex noCurrentWork =
                GenerationRuntimeIndex.none(GenerationAggregateState.UNCERTAIN, List.of(uncertain), 1, 0, false, 7);
        assertEquals(GenerationAggregateState.UNCERTAIN, noCurrentWork.aggregateState());
    }

    private static byte[] timelineKey(final long eligibleAt, final int generation) {
        return KeyCodec.timelineDue(
                new DestinationLaneId(new byte[32]),
                eligibleAt,
                Bytes.concat(new byte[] {1}, new byte[8]),
                com.nereusstream.delay.protocol.DelayMessageId.random(new com.nereusstream.delay.protocol.ShardId(
                        new com.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0)),
                generation);
    }

    private static byte[] orderedTimelineKey(final long deliverAt, final int generation) {
        return KeyCodec.timelineOrdered(
                new DestinationLaneId(new byte[32]),
                deliverAt,
                Bytes.concat(new byte[] {1}, new byte[8]),
                com.nereusstream.delay.protocol.DelayMessageId.random(new com.nereusstream.delay.protocol.ShardId(
                        new com.nereusstream.delay.protocol.RouteIncarnation(new byte[16]), 0)),
                generation);
    }
}
