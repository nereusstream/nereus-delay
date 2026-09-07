package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetOrderContractTest {
    private final Properties vectors = vectors();
    private final TargetMessageLocator locator = TargetMessageLocator.decode(bytes("locator.fifo"));

    @Test
    void emptyHeadAndAllBarrierBranchesMatchIndependentVectors() {
        final var empty = create(1, 1, TargetOrderState.Gate.OPEN, null, null, null);
        assertArrayEquals(bytes("order.empty"), empty.canonicalBytes());
        assertArrayEquals(bytes("order.state.key"), empty.encodedKey());
        final var head = head(90, locator.messageId(), 2);
        assertArrayEquals(
                bytes("order.head"),
                create(2, 1, TargetOrderState.Gate.OPEN, null, head, null).canonicalBytes());
        for (String name : List.of("claimed", "publishing", "hold", "terminal")) {
            final var message = message(name);
            final var barrier = TargetOrderBarrier.fromMessage(message);
            assertArrayEquals(bytes("order.barrier." + name), barrier.canonicalBytes(), name);
            final var actual =
                    create(message.runtime().runtimeRevision(), 1, TargetOrderState.Gate.OPEN, null, null, barrier);
            assertArrayEquals(bytes("order." + name), actual.canonicalBytes(), name);
            assertDoesNotThrow(() -> actual.requireBarrierProjection(message));
            assertEquals(actual, TargetOrderState.decode(actual.canonicalBytes()));
        }
        final var watermark = state("watermark");
        assertArrayEquals(
                bytes("order.watermark"),
                create(
                                8,
                                1,
                                TargetOrderState.Gate.OPEN,
                                bytes("order.key"),
                                null,
                                TargetOrderBarrier.fromMessage(message("hold")))
                        .canonicalBytes());
        assertDoesNotThrow(() -> watermark.requireBarrierProjection(message("hold")));
        assertDoesNotThrow(() -> state("closed").requireBarrierProjection(message("hold")));
    }

    @Test
    void claimedPublishingUncertainAndTerminalRetainedHeadsBlockEverySuccessor() {
        for (String name : List.of("claimed", "publishing", "hold", "terminal", "closed")) {
            final var state = state(name);
            assertNull(state.serviceableHead());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> state.requireServiceableProjection(
                            bytes("order.serviceable.key"), bytes("work.fifo.initial"), message("initial")),
                    name);
        }
        final var terminal = message("terminal");
        assertTrue(terminal.runtime().terminal());
        assertEquals(1, terminal.runtime().attemptObligations().size());
        assertDoesNotThrow(() -> TargetOrderBarrier.fromMessage(terminal));
        final var released = new TargetGenerationRuntimeIndex(
                2,
                GenerationAggregateState.EXPIRED,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                List.of(),
                1,
                0,
                true,
                10);
        assertThrows(
                IllegalArgumentException.class, () -> TargetOrderBarrier.fromMessage(withRuntime(terminal, released)));
        assertThrows(IllegalArgumentException.class, () -> TargetOrderBarrier.fromMessage(message("initial")));
    }

    @Test
    void serviceableHeadRequiresExactMessageWorkAndUsesEligibilityRatherThanDeliveryOrder() {
        final var state = state("head");
        assertEquals(
                TargetTimelineWorkRef.decode(bytes("work.fifo.initial")),
                state.requireServiceableProjection(
                        bytes("order.serviceable.key"), bytes("work.fifo.initial"), message("initial")));
        final var retry = TargetTimelineWorkRef.decode(bytes("work.fifo.retry"));
        final var runtime = new TargetGenerationRuntimeIndex(
                2,
                GenerationAggregateState.RETRY_WAIT,
                CurrentSendWorkKind.TIMELINE,
                retry,
                null,
                null,
                List.of(),
                1,
                0,
                false,
                6);
        final var initial = message("initial");
        final var retryMessage = new TargetMessageRecord(
                locator,
                6,
                90,
                200,
                150,
                NativeDeliveryPolicy.FORBID,
                initial.scheduleSource(),
                initial.inlinePayload(),
                null,
                runtime);
        final var retryHead = head(150, locator.messageId(), 2);
        final var retryState = create(3, 1, TargetOrderState.Gate.OPEN, null, retryHead, null);
        assertArrayEquals(bytes("order.key"), retry.ordinaryKey());
        assertEquals(
                retry, retryState.requireServiceableProjection(retryHead.key(), retry.canonicalBytes(), retryMessage));
        assertThrows(
                IllegalArgumentException.class,
                () -> state.requireServiceableProjection(retryHead.key(), retry.canonicalBytes(), retryMessage));
        assertThrows(
                IllegalArgumentException.class,
                () -> retryState.requireServiceableProjection(
                        retryHead.key(), bytes("work.fifo.initial"), retryMessage));
        assertThrows(
                IllegalArgumentException.class,
                () -> retryState.requireServiceableProjection(
                        retryHead.key(), retry.withRuntimeRevision(7).canonicalBytes(), retryMessage));
        assertThrows(
                IllegalArgumentException.class,
                () -> state.requireServiceableProjection(
                        bytes("order.serviceable.key"), bytes("work.fifo.initial"), message("claimed")));
    }

    @Test
    void barrierCannotBeClearedByTerminalStatusOrAChangedRuntimeDigest() {
        final var barrier = TargetOrderBarrier.fromMessage(message("publishing"));
        assertThrows(IllegalArgumentException.class, () -> barrier.requireMessageProjection(message("hold")));
        assertThrows(IllegalArgumentException.class, () -> barrier.requireMessageProjection(message("terminal")));
        final var prior = message("claimed");
        final var changed = new TargetGenerationRuntimeIndex(
                2,
                GenerationAggregateState.CLAIMED,
                CurrentSendWorkKind.CLAIMED,
                null,
                repeated(32, 0x67),
                null,
                List.of(),
                0,
                0,
                false,
                6);
        assertThrows(IllegalArgumentException.class, () -> TargetOrderBarrier.fromMessage(prior)
                .requireMessageProjection(withRuntime(prior, changed)));
        final var fabricated = new TargetOrderBarrier(locator, bytes("order.key"), 6, repeated(32, 0x67));
        assertThrows(IllegalArgumentException.class, () -> fabricated.requireMessageProjection(prior));
    }

    @Test
    void headAndBarrierAreExclusiveAndNonOpenGatesNeverExposeAHead() {
        final var head = state("head").serviceableHead();
        final var barrier = state("hold").barrier();
        assertThrows(
                IllegalArgumentException.class, () -> create(2, 1, TargetOrderState.Gate.OPEN, null, head, barrier));
        for (var gate : List.of(TargetOrderState.Gate.ORDERING_BROKEN, TargetOrderState.Gate.CLOSED)) {
            assertThrows(IllegalArgumentException.class, () -> create(2, 1, gate, null, head, null));
            assertDoesNotThrow(() -> create(2, 1, gate, null, null, barrier).requireBarrierProjection(message("hold")));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> create(
                        2,
                        1,
                        TargetOrderState.Gate.OPEN,
                        null,
                        new TargetHeadRef(bytes("due.kafka.key"), locator.messageId(), 2, 100),
                        null));
    }

    @Test
    void foreignOrderingDomainTargetShardAndGenerationCannotMasqueradeAsTheHead() {
        final var initial = message("initial");
        for (TargetHeadRef wrong : List.of(
                new TargetHeadRef(
                        TargetKeyCodec.orderedHead(locator.target(), locator.domain(), 90, repeated(32, 0x12)),
                        locator.messageId(),
                        2,
                        90),
                new TargetHeadRef(
                        TargetKeyCodec.orderedHead(
                                new TargetPartitionId(repeated(32, 0x12)),
                                locator.domain(),
                                90,
                                locator.orderingDomain()),
                        locator.messageId(),
                        2,
                        90),
                new TargetHeadRef(
                        TargetKeyCodec.orderedHead(
                                locator.target(), new TargetKeyCodec.Domain(0, 2), 90, locator.orderingDomain()),
                        locator.messageId(),
                        2,
                        90))) {
            assertThrows(
                    IllegalArgumentException.class, () -> create(2, 1, TargetOrderState.Gate.OPEN, null, wrong, null));
        }
        final var wrongGeneration =
                create(2, 1, TargetOrderState.Gate.OPEN, null, head(90, locator.messageId(), 3), null);
        assertThrows(
                IllegalArgumentException.class,
                () -> wrongGeneration.requireServiceableProjection(
                        bytes("order.serviceable.key"), bytes("work.fifo.initial"), initial));
        final var key = TargetKeyCodec.decodeOrdered(bytes("order.key"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetOrderBarrier(
                        locator,
                        TargetKeyCodec.ordered(
                                key.target(), key.orderingDomain(), 90, key.sourceOrderToken(), key.messageId(), 3),
                        6,
                        repeated(32, 0x77)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetOrderBarrier(
                        TargetMessageLocator.decode(bytes("locator.best")), bytes("order.key"), 6, repeated(32, 0x77)));
        final var wrongShard =
                new ShardId(locator.messageId().routingId().shardId().routeIncarnation(), 4);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetOrderState(
                        locator.target(),
                        locator.orderingDomain(),
                        wrongShard,
                        locator.domain(),
                        locator.accountingIncarnation(),
                        TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO,
                        2,
                        1,
                        TargetOrderState.Gate.OPEN,
                        null,
                        head(90, locator.messageId(), 2),
                        null));
    }

    @Test
    void legacyContractCannotAcquireWatermarkOrSilentlySwitchToNewContract() {
        final var prior = state("empty");
        assertNull(prior.lastAdmittedOrder());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetOrderState(
                        prior.target(),
                        prior.orderingDomain(),
                        prior.sourceShard(),
                        prior.executionDomain(),
                        prior.accountingIncarnation(),
                        prior.orderingContract(),
                        2,
                        1,
                        TargetOrderState.Gate.OPEN,
                        bytes("order.key"),
                        null,
                        null));
        final var replacement = create(2, 1, TargetOrderState.Gate.OPEN, bytes("order.key"), null, null);
        assertThrows(IllegalArgumentException.class, () -> replacement.requireSuccessorOf(prior));
        final var claimed = state("claimed");
        assertDoesNotThrow(() -> claimed.requireBarrierProjection(message("claimed")));
    }

    @Test
    void admittedBarrierRequiresExactWatermarkButAReversibleClaimDoesNotCreateOne() {
        final var hold = state("watermark");
        final var noWatermark = new TargetOrderState(
                hold.target(),
                hold.orderingDomain(),
                hold.sourceShard(),
                hold.executionDomain(),
                hold.accountingIncarnation(),
                hold.orderingContract(),
                8,
                1,
                hold.gate(),
                null,
                null,
                hold.barrier());
        assertThrows(IllegalArgumentException.class, () -> noWatermark.requireBarrierProjection(message("hold")));
        final var key = hold.lastAdmittedOrder();
        final var laterKey = TargetKeyCodec.ordered(
                key.target(), key.orderingDomain(), 91, key.sourceOrderToken(), key.messageId(), key.generation());
        final var advanced = create(9, 1, hold.gate(), laterKey, null, hold.barrier());
        assertThrows(IllegalArgumentException.class, () -> advanced.requireBarrierProjection(message("hold")));
        final var claim = new TargetOrderState(
                hold.target(),
                hold.orderingDomain(),
                hold.sourceShard(),
                hold.executionDomain(),
                hold.accountingIncarnation(),
                hold.orderingContract(),
                6,
                1,
                hold.gate(),
                null,
                null,
                state("claimed").barrier());
        assertDoesNotThrow(() -> claim.requireBarrierProjection(message("claimed")));
    }

    @Test
    void successorUsesUnsignedFullOrderAndRevisionWithoutWrapOrReopening() {
        final var watermark = state("watermark");
        final var order = watermark.lastAdmittedOrder();
        final var earlier = TargetKeyCodec.ordered(
                order.target(),
                order.orderingDomain(),
                89,
                order.sourceOrderToken(),
                order.messageId(),
                order.generation());
        final var rewind = create(9, 1, TargetOrderState.Gate.OPEN, earlier, null, null);
        assertThrows(IllegalArgumentException.class, () -> rewind.requireSuccessorOf(watermark));
        final var equal = create(9, 1, TargetOrderState.Gate.OPEN, order.encodedKey(), null, null);
        assertDoesNotThrow(() -> equal.requireSuccessorOf(watermark));
        final var laterGeneration = TargetKeyCodec.ordered(
                order.target(),
                order.orderingDomain(),
                order.deliverAtEpochMs(),
                order.sourceOrderToken(),
                order.messageId(),
                -1);
        final var later = create(9, 1, TargetOrderState.Gate.OPEN, laterGeneration, null, null);
        assertDoesNotThrow(() -> later.requireSuccessorOf(watermark));
        final var closed = state("closed");
        assertDoesNotThrow(() -> closed.requireSuccessorOf(watermark));
        assertThrows(IllegalArgumentException.class, () -> create(
                        10, 3, TargetOrderState.Gate.OPEN, bytes("order.key"), null, null)
                .requireSuccessorOf(closed));
        assertThrows(IllegalArgumentException.class, () -> create(
                        9, 1, TargetOrderState.Gate.CLOSED, bytes("order.key"), null, null)
                .requireSuccessorOf(watermark));
        final var unsigned = create(Long.MAX_VALUE, 1, TargetOrderState.Gate.OPEN, null, null, null);
        assertDoesNotThrow(() -> create(Long.MIN_VALUE, 1, TargetOrderState.Gate.OPEN, null, null, null)
                .requireSuccessorOf(unsigned));
        assertThrows(IllegalArgumentException.class, () -> create(3, 1, TargetOrderState.Gate.OPEN, null, null, null)
                .requireSuccessorOf(state("empty")));
        final var exhausted = create(-1, 1, TargetOrderState.Gate.OPEN, null, null, null);
        assertThrows(IllegalStateException.class, () -> state("empty").requireSuccessorOf(exhausted));
    }

    @Test
    void storeProjectionRejectsReusedVacantAndForeignAccountingDomains() {
        final var state = state("hold");
        final var queue = queue(locator.domain(), TargetDomainState.Lifecycle.ACTIVE, locator.accountingIncarnation());
        assertEquals(
                state,
                TargetOrderState.decodeForStore(
                        state.encodedKey(), state.canonicalBytes(), state.sourceShard(), queue));
        assertDoesNotThrow(() -> TargetOrderState.decodeForStore(
                state.encodedKey(),
                state.canonicalBytes(),
                state.sourceShard(),
                queue(locator.domain(), TargetDomainState.Lifecycle.DRAINING, locator.accountingIncarnation())));
        for (TargetQueueState wrong : List.of(
                queue(
                        new TargetKeyCodec.Domain(0, 2),
                        TargetDomainState.Lifecycle.ACTIVE,
                        locator.accountingIncarnation()),
                queue(locator.domain(), TargetDomainState.Lifecycle.VACANT, locator.accountingIncarnation()),
                queue(locator.domain(), TargetDomainState.Lifecycle.ACTIVE, repeated(16, 0x12)))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetOrderState.decodeForStore(
                            state.encodedKey(), state.canonicalBytes(), state.sourceShard(), wrong));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderState.decodeForStore(
                        state.encodedKey(),
                        state.canonicalBytes(),
                        new ShardId(state.sourceShard().routeIncarnation(), 4),
                        queue));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderState.decodeForStore(
                        TargetKeyCodec.orderState(state.target(), repeated(32, 0x12)),
                        state.canonicalBytes(),
                        state.sourceShard(),
                        queue));
    }

    @Test
    void maximumFullWidthOrderAndBarrierStayWithinSmallRecordBound() {
        final var target = locator.target();
        final var maxLocator = new TargetMessageLocator(
                locator.messageId(),
                -1,
                target,
                new TargetKeyCodec.Domain(63, -1),
                locator.accountingIncarnation(),
                OrderingMode.DELIVERY_TIME_FIFO,
                locator.orderingDomain(),
                locator.scheduleBindingDigest());
        final var key = TargetKeyCodec.ordered(
                target,
                locator.orderingDomain(),
                Long.MAX_VALUE,
                Bytes.concat(new byte[] {2}, Bytes.u64beBits(-1), Bytes.u64beBits(-1), Bytes.u32be(0xffff_ffffL)),
                locator.messageId(),
                -1);
        final var barrier = new TargetOrderBarrier(maxLocator, key, -1, repeated(32, 0x77));
        final var maximum = new TargetOrderState(
                target,
                locator.orderingDomain(),
                locator.messageId().routingId().shardId(),
                maxLocator.domain(),
                locator.accountingIncarnation(),
                TargetOrderState.OrderingContract.ADMISSION_WATERMARK,
                -1,
                -1,
                TargetOrderState.Gate.CLOSED,
                key,
                null,
                barrier);
        assertArrayEquals(bytes("order.maximum"), maximum.canonicalBytes());
        assertArrayEquals(bytes("order.maximum.sha256"), Bytes.sha256(maximum.canonicalBytes()));
        assertEquals(Integer.parseInt(vectors.getProperty("order.maximum.length")), maximum.canonicalBytes().length);
        assertTrue(maximum.canonicalBytes().length <= TargetOrderState.MAX_CANONICAL_BYTES);
        assertTrue(barrier.canonicalBytes().length <= TargetOrderBarrier.MAX_CANONICAL_BYTES);
        assertEquals(maximum, TargetOrderState.decode(maximum.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderState.decode(new byte[TargetOrderState.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderBarrier.decode(new byte[TargetOrderBarrier.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void malformedPresenceUnknownFieldsAndEnumsFailEvenWithFreshDigest() {
        final byte[] original = bytes("order.watermark");
        for (int number : List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetOrderState.decode(
                            rewrite(original, number, null, 15, "nereus-delay-target-order-state\0")),
                    "missing " + number);
        }
        for (int number : List.of(1, 6, 8, 9, 10, 11)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetOrderState.decode(
                            rewrite(original, number, uint(number, 0), 15, "nereus-delay-target-order-state\0")),
                    "zero " + number);
        }
        for (long value : List.of(3L, 0x1_0000_0001L)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetOrderState.decode(
                            rewrite(original, 8, uint(8, value), 15, "nereus-delay-target-order-state\0")));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderState.decode(
                        rewrite(original, 5, uint(5, 64), 15, "nereus-delay-target-order-state\0")));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderState.decode(rewrite(
                        original,
                        13,
                        field(13, state("head").serviceableHead().canonicalBytes()),
                        15,
                        "nereus-delay-target-order-state\0")));
        assertThrows(
                IllegalArgumentException.class, () -> TargetOrderState.decode(Bytes.concat(original, uint(16, 1))));
        assertThrows(IllegalArgumentException.class, () -> TargetOrderState.decode(Bytes.concat(uint(1, 1), original)));
        final byte[] corrupt = original.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetOrderState.decode(corrupt));
        final byte[] barrier = bytes("order.barrier.hold");
        for (int number : List.of(1, 2, 3, 4, 5)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetOrderBarrier.decode(
                            rewrite(barrier, number, null, 5, "nereus-delay-target-order-barrier\0")));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderBarrier.decode(
                        rewrite(barrier, 3, uint(3, 0), 5, "nereus-delay-target-order-barrier\0")));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetOrderBarrier.decode(
                        rewrite(barrier, 4, field(4, new byte[32]), 5, "nereus-delay-target-order-barrier\0")));
    }

    @Test
    void fullOrderBreaksTiesBySourceMessageAndUnsignedGeneration() {
        final var order = TargetKeyCodec.decodeOrdered(bytes("order.key"));
        final byte[] nextSource = TargetKeyCodec.ordered(
                order.target(),
                order.orderingDomain(),
                90,
                Bytes.concat(new byte[] {1}, Bytes.u64be(8)),
                order.messageId(),
                0);
        assertTrue(Arrays.compareUnsigned(order.encodedKey(), nextSource) < 0);
        final byte[] nextGeneration = TargetKeyCodec.ordered(
                order.target(), order.orderingDomain(), 90, order.sourceOrderToken(), order.messageId(), -1);
        assertTrue(Arrays.compareUnsigned(order.encodedKey(), nextGeneration) < 0);
        // Generate another valid Message ID in the same Shard, with CRC recalculated independently of key encoding.
        final byte[] id = order.messageId().bytes();
        id[36]++;
        final byte[] prefix = Arrays.copyOf(id, 37);
        final var nextMessage = new DelayMessageId(Bytes.concat(prefix, Bytes.u32be(Bytes.crc32c(prefix))));
        final byte[] nextId = TargetKeyCodec.ordered(
                order.target(), order.orderingDomain(), 90, order.sourceOrderToken(), nextMessage, 0);
        assertTrue(Arrays.compareUnsigned(nextGeneration, nextId) < 0);
    }

    @Test
    void validReservedEnvelopeIsRejectedByTheUnchangedLaneReader() {
        final var value = bytes("order.watermark.value");
        assertEquals(17, TargetOrderState.VALUE_TYPE);
        assertEquals(Bytes.readU32be(value, value.length - 4), Bytes.crc32c(Arrays.copyOf(value, value.length - 4)));
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
    }

    @Test
    void stateAndBarrierArraysAreDefensive() {
        final var original = state("watermark");
        final var domain = original.orderingDomain();
        final var account = original.accountingIncarnation();
        final var runtimeDigest = original.barrier().runtimeDigest();
        final var order = original.barrier().order().encodedKey();
        final var barrier = new TargetOrderBarrier(locator, order, 8, runtimeDigest);
        final var state = new TargetOrderState(
                original.target(),
                domain,
                original.sourceShard(),
                original.executionDomain(),
                account,
                original.orderingContract(),
                8,
                1,
                original.gate(),
                order,
                null,
                barrier);
        for (byte[] value : List.of(
                domain,
                account,
                runtimeDigest,
                order,
                state.digest(),
                barrier.digest(),
                barrier.runtimeDigest(),
                state.orderingDomain(),
                state.lastAdmittedOrder().sourceOrderToken())) {
            Arrays.fill(value, (byte) 0);
        }
        assertArrayEquals(original.canonicalBytes(), state.canonicalBytes());
    }

    private TargetOrderState create(
            final long revision,
            final long control,
            final TargetOrderState.Gate gate,
            final byte[] watermark,
            final TargetHeadRef head,
            final TargetOrderBarrier barrier) {
        return new TargetOrderState(
                locator.target(),
                locator.orderingDomain(),
                locator.messageId().routingId().shardId(),
                locator.domain(),
                locator.accountingIncarnation(),
                watermark == null
                        ? TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO
                        : TargetOrderState.OrderingContract.ADMISSION_WATERMARK,
                revision,
                control,
                gate,
                watermark,
                head,
                barrier);
    }

    private TargetHeadRef head(final long time, final DelayMessageId message, final int generation) {
        return new TargetHeadRef(
                TargetKeyCodec.orderedHead(locator.target(), locator.domain(), time, locator.orderingDomain()),
                message,
                generation,
                time);
    }

    private TargetQueueState queue(
            final TargetKeyCodec.Domain domain, final TargetDomainState.Lifecycle lifecycle, final byte[] account) {
        final boolean vacant = lifecycle == TargetDomainState.Lifecycle.VACANT;
        return new TargetQueueState(
                locator.target(),
                1,
                1,
                TargetQueueState.AdmissionState.OPEN,
                account,
                0,
                List.of(new TargetDomainState(
                        domain,
                        lifecycle,
                        vacant ? null : repeated(32, 1),
                        vacant ? null : repeated(32, 2),
                        null,
                        null,
                        null)));
    }

    private static TargetMessageRecord withRuntime(
            final TargetMessageRecord message, final TargetGenerationRuntimeIndex runtime) {
        return new TargetMessageRecord(
                message.locator(),
                message.stateVersion(),
                message.deliverAtEpochMs(),
                message.expireAtEpochMs(),
                message.retryEligibilityAtEpochMs(),
                message.nativeDeliveryPolicy(),
                message.scheduleSource(),
                message.inlinePayload(),
                message.payloadReference(),
                runtime);
    }

    private TargetMessageRecord message(final String name) {
        return TargetMessageRecord.decode(bytes("order.message." + name));
    }

    private TargetOrderState state(final String name) {
        return TargetOrderState.decode(bytes("order." + name));
    }

    private byte[] bytes(final String name) {
        return HexFormat.of().parseHex(vectors.getProperty(name));
    }

    private static byte[] repeated(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static byte[] field(final int number, final byte[] value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, number, value));
    }

    private static byte[] uint(final int number, final long value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.uint64Bits(out, number, value));
    }

    private static byte[] rewrite(
            final byte[] original,
            final int number,
            final byte[] replacement,
            final int digestField,
            final String digestDomain) {
        final var fields = QueryCodecSupport.read(original, "fixture");
        final byte[] changed = CanonicalProtobuf.message(out -> {
            for (int n = 1; n < digestField; n++) {
                if (n == number) {
                    if (replacement != null) {
                        out.writeBytes(replacement);
                    }
                } else {
                    final var field = QueryCodecSupport.optional(fields, n);
                    if (field != null) {
                        out.writeBytes(
                                field.wireType() == 2 ? field(n, field.rawValue()) : uint(n, field.unsignedValue()));
                    }
                }
            }
        });
        return number == digestField
                ? changed
                : Bytes.concat(changed, field(digestField, Bytes.sha256(Bytes.utf8(digestDomain), changed)));
    }

    private static Properties vectors() {
        try (var stream =
                TargetOrderContractTest.class.getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            final var result = new Properties();
            result.load(stream);
            return result;
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }
}
