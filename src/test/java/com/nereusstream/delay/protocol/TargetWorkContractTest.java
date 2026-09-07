package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetWorkContractTest {
    private final Properties vectors = vectors();
    private final DelayMessageId message = new DelayMessageId(bytes("message.id"));
    private final CanonicalTargetPartition target = CanonicalTargetPartition.decode(bytes("pulsar.canonical"));
    private final ShardId shard = message.routingId().shardId();
    private final byte[] account = HexFormat.of().parseHex("0102030405060708090a0b0c0d0e0f10");
    private final byte[] binding = repeated(32, 0xdd);
    private final byte[] order = repeated(32, 0x11);
    private final byte[] token = Bytes.concat(new byte[] {1}, Bytes.u64be(7));
    private final SourcePosition source = SourcePositionCodec.decode(bytes("work.schedule.source"));
    private final SourcePosition controlSource = SourcePositionCodec.decode(bytes("work.control.source"));
    private final ControlRef control = ControlRef.decode(bytes("work.control.ref"));

    @Test
    void independentGoldenWorkFormsRoundTripEveryBranch() {
        final var best = locator(false);
        final var fifo = locator(true);
        assertArrayEquals(bytes("locator.best"), best.canonicalBytes());
        assertArrayEquals(bytes("locator.fifo"), fifo.canonicalBytes());
        assertEquals(best, TargetMessageLocator.decode(best.canonicalBytes()));
        assertEquals(fifo, TargetMessageLocator.decode(fifo.canonicalBytes()));
        final Map<String, TargetTimelineWorkRef> cases = new LinkedHashMap<>();
        cases.put(
                "initial",
                work(
                        best,
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        100,
                        100,
                        1,
                        5,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        cases.put(
                "native",
                work(
                        best,
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        100,
                        100,
                        1,
                        5,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        true));
        cases.put(
                "definitive",
                work(
                        best,
                        TimelineWorkKind.DEFINITIVE_RETRY,
                        100,
                        150,
                        2,
                        6,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        cases.put(
                "uncertain.pinned",
                work(
                        best,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        7,
                        UncertainRetryAuthority.PINNED_POLICY,
                        null,
                        null,
                        false));
        cases.put(
                "uncertain.control",
                work(
                        best,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        8,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        control,
                        controlSource,
                        false));
        cases.put(
                "fifo.initial",
                work(
                        fifo,
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        90,
                        90,
                        1,
                        5,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        cases.put(
                "fifo.retry",
                work(
                        fifo,
                        TimelineWorkKind.DEFINITIVE_RETRY,
                        90,
                        150,
                        2,
                        6,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        cases.forEach((name, expected) -> {
            assertArrayEquals(bytes("work." + name), expected.canonicalBytes(), name);
            assertEquals(expected, TargetTimelineWorkRef.decode(expected.canonicalBytes()));
            assertEquals(
                    expected,
                    TargetTimelineWorkRef.decodeForIndex(expected.ordinaryKey(), expected.canonicalBytes(), source));
        });
    }

    @Test
    void nativeAndOrdinaryIndexesReferToOneInitialWorkAtBusinessDeliveryTime() {
        final var work = decode("native");
        assertArrayEquals(bytes("queue.due.key"), work.ordinaryKey());
        assertArrayEquals(bytes("native.kafka.key"), work.nativeKey());
        assertEquals(work, TargetTimelineWorkRef.decodeForIndex(work.nativeKey(), work.canonicalBytes(), source));
        assertDoesNotThrow(() -> work.requireHeadProjection(new TargetHeadRef(work.nativeKey(), message, 2, 100)));
        assertDoesNotThrow(() -> work.requireHeadProjection(new TargetHeadRef(work.ordinaryKey(), message, 2, 100)));
        final byte[] early = work.nativeKey();
        ByteBuffer.wrap(early).putLong(44, 75);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimelineWorkRef.decodeForIndex(early, work.canonicalBytes(), source));
        assertThrows(IllegalStateException.class, () -> decode("initial").nativeKey());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimelineWorkRef.decodeForIndex(
                        work.nativeKey(), decode("initial").canonicalBytes(), source));
    }

    @Test
    void strictRetryPreservesBusinessOrderWhileItsHeadUsesRetryEligibility() {
        final var initial = decode("fifo.initial");
        final var retry = decode("fifo.retry");
        assertArrayEquals(initial.ordinaryKey(), retry.ordinaryKey());
        assertEquals(90, TargetKeyCodec.decodeOrdered(retry.ordinaryKey()).deliverAtEpochMs());
        final byte[] headKey =
                TargetKeyCodec.orderedHead(target.id(), locator(true).domain(), 150, order);
        assertDoesNotThrow(() -> retry.requireHeadProjection(new TargetHeadRef(headKey, message, 2, 150)));
        assertThrows(
                IllegalArgumentException.class,
                () -> retry.requireHeadProjection(new TargetHeadRef(bytes("order.head.key"), message, 2, 100)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimelineWorkRef.decodeForIndex(headKey, retry.canonicalBytes(), source));
        final byte[] later = TargetKeyCodec.ordered(target.id(), order, 100, token, message, 3);
        assertTrue(Arrays.compareUnsigned(retry.ordinaryKey(), later) < 0);
    }

    @Test
    void runtimeRevisionsChangeOnlyInstanceDigestWhileSemanticChangesFenceTheWork() {
        final var original = decode("native");
        final var next = original.withRuntimeRevision(Long.MIN_VALUE);
        assertArrayEquals(original.semanticWorkDigest(), next.semanticWorkDigest());
        assertFalse(Arrays.equals(original.workInstanceDigest(), next.workInstanceDigest()));
        assertEquals(next, TargetTimelineWorkRef.decode(next.canonicalBytes()));
        assertFalse(
                Arrays.equals(original.semanticWorkDigest(), decode("initial").semanticWorkDigest()));
        assertFalse(Arrays.equals(
                decode("initial").semanticWorkDigest(), decode("definitive").semanticWorkDigest()));
        final var reused = new TargetMessageLocator(
                message,
                2,
                target.id(),
                new TargetKeyCodec.Domain(0, 2),
                account,
                OrderingMode.BEST_EFFORT,
                null,
                binding);
        final var changed = work(
                reused,
                TimelineWorkKind.INITIAL_SCHEDULE,
                100,
                100,
                1,
                5,
                UncertainRetryAuthority.NONE,
                null,
                null,
                true);
        assertFalse(Arrays.equals(original.semanticWorkDigest(), changed.semanticWorkDigest()));
        assertThrows(IllegalArgumentException.class, () -> original.withRuntimeRevision(0));
    }

    @Test
    void workKindOrderingNativeAndControlPresenceMustAgree() {
        final var best = locator(false);
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        100,
                        101,
                        1,
                        1,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        100,
                        100,
                        2,
                        1,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.DEFINITIVE_RETRY,
                        100,
                        150,
                        2,
                        1,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        true));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        locator(true),
                        TimelineWorkKind.INITIAL_SCHEDULE,
                        100,
                        100,
                        1,
                        1,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        true));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        1,
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        false));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        locator(true),
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        1,
                        UncertainRetryAuthority.PINNED_POLICY,
                        null,
                        null,
                        false));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        1,
                        UncertainRetryAuthority.PINNED_POLICY,
                        control,
                        controlSource,
                        false));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        1,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        control,
                        null,
                        false));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        best,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        1,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        null,
                        controlSource,
                        false));
    }

    @Test
    void scheduleAndRetryControlRequireExactShardOrderAndPhysicalSource() {
        final var retry = decode("uncertain.control");
        final KafkaSourcePosition kafka = (KafkaSourcePosition) source;
        assertThrows(IllegalArgumentException.class, () -> retry.requireScheduleProjection(controlSource));
        final var wrongResource = new KafkaSourcePosition(
                shard,
                kafka.authenticatedClusterId(),
                UUID.fromString("00000000-0000-4000-8000-000000000001"),
                7,
                4,
                90);
        assertThrows(IllegalArgumentException.class, () -> retry.requireScheduleProjection(wrongResource));
        final var wrongShard = new KafkaSourcePosition(
                new ShardId(shard.routeIncarnation(), 4),
                kafka.authenticatedClusterId(),
                kafka.nativeTopicUuid(),
                7,
                4,
                90);
        assertThrows(IllegalArgumentException.class, () -> retry.requireScheduleProjection(wrongShard));
        assertThrows(
                IllegalArgumentException.class,
                () -> work(
                        locator(false),
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        100,
                        150,
                        2,
                        8,
                        UncertainRetryAuthority.CONTROL_OVERRIDE,
                        control,
                        wrongShard,
                        false));
        final var sameOffset = work(
                locator(false),
                TimelineWorkKind.UNCERTAIN_RETRY,
                100,
                150,
                2,
                8,
                UncertainRetryAuthority.CONTROL_OVERRIDE,
                control,
                source,
                false);
        assertThrows(IllegalArgumentException.class, () -> sameOffset.requireScheduleProjection(source));
        final byte[] wrongKey = retry.ordinaryKey();
        ByteBuffer.wrap(wrongKey).putLong(36, 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimelineWorkRef.decodeForIndex(wrongKey, retry.canonicalBytes(), source));
    }

    @Test
    void queueProjectionRejectsReleasedReusedOrForeignAccountingAndNativeResources() {
        final var work = decode("native");
        assertDoesNotThrow(() -> work.requireQueueProjection(queue(target, 1, account, true, false), target));
        assertThrows(
                IllegalArgumentException.class,
                () -> work.requireQueueProjection(queue(target, 2, account, true, false), target));
        assertThrows(
                IllegalArgumentException.class,
                () -> work.requireQueueProjection(queue(target, 1, repeated(16, 3), true, false), target));
        assertThrows(
                IllegalArgumentException.class,
                () -> work.requireQueueProjection(queue(target, 1, account, true, true), target));
        assertThrows(
                IllegalArgumentException.class,
                () -> work.requireQueueProjection(queue(target, 1, account, false, false), target));
        final var kafka = CanonicalTargetPartition.decode(bytes("kafka.canonical"));
        final var loc = new TargetMessageLocator(
                message,
                2,
                kafka.id(),
                new TargetKeyCodec.Domain(0, 1),
                account,
                OrderingMode.BEST_EFFORT,
                null,
                binding);
        final var invalid = work(
                loc, TimelineWorkKind.INITIAL_SCHEDULE, 100, 100, 1, 5, UncertainRetryAuthority.NONE, null, null, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> invalid.requireQueueProjection(queue(kafka, 1, account, true, false), kafka));
    }

    @Test
    void locatorEnforcesOrderingPresenceIdentityGenerationAndBoundedSlots() {
        final var domain = new TargetKeyCodec.Domain(0, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageLocator(
                        message, 2, target.id(), domain, account, OrderingMode.BEST_EFFORT, order, binding));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageLocator(
                        message, 2, target.id(), domain, account, OrderingMode.DELIVERY_TIME_FIFO, null, binding));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageLocator(
                        message, 2, target.id(), domain, new byte[16], OrderingMode.BEST_EFFORT, null, binding));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageLocator(
                        message, 2, target.id(), domain, account, OrderingMode.BEST_EFFORT, null, new byte[32]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageLocator(
                        message,
                        2,
                        target.id(),
                        new TargetKeyCodec.Domain(64, 1),
                        account,
                        OrderingMode.BEST_EFFORT,
                        null,
                        binding));
        assertThrows(IllegalArgumentException.class, () -> locator(false).requireMessageProjection(message, 3));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMessageLocator.decode(new byte[TargetMessageLocator.MAX_CANONICAL_BYTES + 1]));
        final byte[] invalid = bytes("locator.best");
        invalid[invalid.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetMessageLocator.decode(invalid));
    }

    @Test
    void closedWorkDecoderRejectsUnknownDuplicateMissingMalformedAndOversizedFields() {
        final byte[] good = bytes("work.uncertain.control");
        final byte[] corrupt = good.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(corrupt));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimelineWorkRef.decode(Arrays.copyOf(good, good.length - 1)));
        final byte[] unknown =
                Bytes.concat(good, CanonicalProtobuf.message(out -> CanonicalProtobuf.uint32(out, 15, 1)));
        assertThrows(IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(unknown));
        final byte[] duplicate = Bytes.concat(new byte[] {8, 1}, good);
        assertThrows(IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(duplicate));
        final byte[] unknownVersion = good.clone();
        unknownVersion[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(unknownVersion));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimelineWorkRef.decode(new byte[TargetTimelineWorkRef.MAX_CANONICAL_BYTES + 1]));
        final byte[] invalidBool = replaceUint(good, 12, 2);
        assertThrows(IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(invalidBool));
        assertThrows(IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(replaceUint(good, 8, 0)));
        assertThrows(
                IllegalArgumentException.class, () -> TargetTimelineWorkRef.decode(replaceUint(good, 7, 0x8000_0000L)));
    }

    @Test
    void maximumFullSourceAndWorkMatchIndependentBoundsAndDigests() {
        final byte[] resource = new byte[32];
        for (int i = 0; i < resource.length; i++) {
            resource[i] = (byte) i;
        }
        final var position = new PulsarSourcePosition(
                shard,
                resource,
                "x".repeat(1 << 20),
                -1,
                -1,
                -2,
                -1,
                PulsarSourcePosition.EntryKind.BATCH,
                Long.MAX_VALUE);
        assertEquals(TargetSourcePosition.MAX_PULSAR_CANONICAL_BYTES, position.canonicalBytes().length);
        assertEquals(
                vectors.getProperty("work.maximum.source.sha256"), Bytes.hex(Bytes.sha256(position.canonicalBytes())));
        assertEquals(position, TargetSourcePosition.decode(position.canonicalBytes()));
        final var loc = new TargetMessageLocator(
                message,
                -1,
                target.id(),
                new TargetKeyCodec.Domain(63, -1),
                account,
                OrderingMode.BEST_EFFORT,
                null,
                binding);
        final var maxControl = new ControlRef(repeated(32, 0x44), repeated(32, 0x55), 0xffff_ffffL);
        final byte[] maxToken =
                Bytes.concat(new byte[] {2}, Bytes.u64beBits(-1), Bytes.u64beBits(-2), Bytes.u32beBits(-2));
        final var maximum = new TargetTimelineWorkRef(
                loc,
                TimelineWorkKind.UNCERTAIN_RETRY,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                maxToken,
                Integer.MAX_VALUE,
                -1,
                UncertainRetryAuthority.CONTROL_OVERRIDE,
                maxControl,
                position,
                false);
        final byte[] encoded = maximum.canonicalBytes();
        assertTrue(encoded.length <= TargetTimelineWorkRef.MAX_CANONICAL_BYTES);
        assertEquals(Integer.parseInt(vectors.getProperty("work.maximum.length")), encoded.length);
        assertEquals(vectors.getProperty("work.maximum.sha256"), Bytes.hex(Bytes.sha256(encoded)));
        assertEquals(maximum, TargetTimelineWorkRef.decode(encoded));
        final var schedule = new PulsarSourcePosition(
                shard,
                resource,
                position.physicalTopic(),
                -1,
                -2,
                -2,
                -1,
                PulsarSourcePosition.EntryKind.BATCH,
                Long.MAX_VALUE);
        assertDoesNotThrow(() -> maximum.requireScheduleProjection(schedule));
    }

    @Test
    void newSourceLimitsPreserveCompleteIdentityAndRejectOutsizeOrUnassignedResources() {
        final UUID uuid = ((KafkaSourcePosition) source).nativeTopicUuid();
        final var maximum = new KafkaSourcePosition(shard, "é".repeat(128), uuid, -1, -1, Long.MAX_VALUE);
        assertEquals(TargetSourcePosition.MAX_KAFKA_CANONICAL_BYTES, maximum.canonicalBytes().length);
        assertEquals(maximum, TargetSourcePosition.decode(maximum.canonicalBytes()));
        final var oversized = new KafkaSourcePosition(shard, "é".repeat(129), uuid, 0, null, 0);
        assertEquals(oversized, SourcePositionCodec.decode(oversized.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> TargetSourcePosition.decode(oversized.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetSourcePosition.requireBounded(
                        new KafkaSourcePosition(shard, "cluster", new UUID(0, 0), 0, null, 0)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetSourcePosition.requireBounded(new PulsarSourcePosition(
                        shard, new byte[32], "topic", 0, 0, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 0)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetSourcePosition.requireBounded(new PulsarSourcePosition(
                        shard,
                        repeated(32, 1),
                        "x".repeat((1 << 20) + 1),
                        0,
                        0,
                        0,
                        1,
                        PulsarSourcePosition.EntryKind.NON_BATCH,
                        0)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetSourcePosition.decode(new byte[TargetSourcePosition.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void reservedWorkValueTypeRemainsRejectedByTheLaneReaderWithValidCrc() {
        final byte[] value = bytes("work.native.value");
        final int crc = ByteBuffer.wrap(value).getInt(value.length - 4);
        assertEquals(crc, Bytes.crc32c(Arrays.copyOf(value, value.length - 4)));
        assertEquals(TargetTimelineWorkRef.VALUE_TYPE, Byte.toUnsignedInt(value[2]));
        assertThrows(
                IllegalArgumentException.class, () -> ValueEnvelope.decode(value, TargetTimelineWorkRef.VALUE_TYPE));
    }

    @Test
    void callersCannotMutateRetainedLocatorWorkTokensOrDigests() {
        final var loc = locator(true);
        final byte[] locBytes = loc.canonicalBytes();
        Arrays.fill(account, (byte) 9);
        Arrays.fill(binding, (byte) 9);
        Arrays.fill(order, (byte) 9);
        Arrays.fill(loc.accountingIncarnation(), (byte) 9);
        Arrays.fill(loc.orderingDomain(), (byte) 9);
        Arrays.fill(loc.scheduleBindingDigest(), (byte) 9);
        Arrays.fill(loc.digest(), (byte) 9);
        assertArrayEquals(locBytes, loc.canonicalBytes());
        final var work = work(
                loc,
                TimelineWorkKind.INITIAL_SCHEDULE,
                100,
                100,
                1,
                1,
                UncertainRetryAuthority.NONE,
                null,
                null,
                false);
        final byte[] workBytes = work.canonicalBytes();
        Arrays.fill(token, (byte) 0);
        Arrays.fill(work.sourceOrderToken(), (byte) 0);
        Arrays.fill(work.semanticWorkDigest(), (byte) 0);
        Arrays.fill(work.workInstanceDigest(), (byte) 0);
        assertArrayEquals(workBytes, work.canonicalBytes());
    }

    private TargetMessageLocator locator(final boolean ordered) {
        return new TargetMessageLocator(
                message,
                2,
                target.id(),
                new TargetKeyCodec.Domain(0, 1),
                account,
                ordered ? OrderingMode.DELIVERY_TIME_FIFO : OrderingMode.BEST_EFFORT,
                ordered ? order : null,
                binding);
    }

    private TargetQueueState queue(
            final CanonicalTargetPartition identity,
            final long generation,
            final byte[] incarnation,
            final boolean nativeScope,
            final boolean vacant) {
        final var domain = new TargetDomainState(
                new TargetKeyCodec.Domain(0, generation),
                vacant ? TargetDomainState.Lifecycle.VACANT : TargetDomainState.Lifecycle.ACTIVE,
                vacant ? null : repeated(32, 0xaa),
                vacant ? null : repeated(32, 0xbb),
                vacant || !nativeScope ? null : repeated(32, 0xcc),
                null,
                null);
        return new TargetQueueState(
                identity.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, incarnation, 60000, List.of(domain));
    }

    private TargetTimelineWorkRef work(
            final TargetMessageLocator loc,
            final TimelineWorkKind kind,
            final long deliver,
            final long retry,
            final int attempt,
            final long revision,
            final UncertainRetryAuthority authority,
            final ControlRef ref,
            final SourcePosition position,
            final boolean nativeCandidate) {
        return new TargetTimelineWorkRef(
                loc, kind, deliver, retry, token, attempt, revision, authority, ref, position, nativeCandidate);
    }

    private TargetTimelineWorkRef decode(final String name) {
        return TargetTimelineWorkRef.decode(bytes("work." + name));
    }

    private byte[] bytes(final String key) {
        return HexFormat.of().parseHex(vectors.getProperty(key));
    }

    private static byte[] repeated(final int size, final int value) {
        final byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static Properties vectors() {
        try (var stream =
                TargetWorkContractTest.class.getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            final var props = new Properties();
            props.load(stream);
            return props;
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }

    private static byte[] replaceUint(final byte[] bytes, final int number, final long replacement) {
        return CanonicalProtobuf.message(out -> {
            for (var field : QueryCodecSupport.read(bytes, "test")) {
                if (field.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, field.number(), field.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(
                            out, field.number(), field.number() == number ? replacement : field.unsignedValue());
                }
            }
        });
    }
}
