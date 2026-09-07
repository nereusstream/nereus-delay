package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPayloadReference;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetMessageContractTest {
    private final Properties vectors = vectors();
    private final TargetMessageLocator locator = TargetMessageLocator.decode(bytes("locator.best"));
    private final SourcePosition source = TargetSourcePosition.decode(bytes("work.schedule.source"));
    private final byte[] claim = repeated(32, 0x66);
    private final byte[] attempt = repeated(32, 0x77);

    @Test
    void allRuntimeBranchesMatchIndependentCanonicalVectors() {
        final var publishing = ref(0x77, 2, AttemptLedgerState.PUBLISHING);
        final var uncertain = ref(0x77, 2, AttemptLedgerState.UNCERTAIN);
        assertArrayEquals(bytes("obligation.publishing"), publishing.canonicalBytes());
        assertArrayEquals(bytes("obligation.uncertain"), uncertain.canonicalBytes());
        final Map<String, TargetGenerationRuntimeIndex> actual = Map.of(
                "initial",
                        index(
                                GenerationAggregateState.SCHEDULED,
                                CurrentSendWorkKind.TIMELINE,
                                work("native"),
                                null,
                                null,
                                List.of(),
                                0,
                                0,
                                false,
                                5),
                "claimed",
                        index(
                                GenerationAggregateState.CLAIMED,
                                CurrentSendWorkKind.CLAIMED,
                                null,
                                claim,
                                null,
                                List.of(),
                                0,
                                0,
                                false,
                                6),
                "publishing",
                        index(
                                GenerationAggregateState.PUBLISHING,
                                CurrentSendWorkKind.PUBLISHING,
                                null,
                                null,
                                attempt,
                                List.of(publishing),
                                1,
                                0,
                                false,
                                7),
                "hold",
                        index(
                                GenerationAggregateState.UNCERTAIN,
                                CurrentSendWorkKind.NONE,
                                null,
                                null,
                                null,
                                List.of(uncertain),
                                1,
                                0,
                                false,
                                8),
                "retry",
                        index(
                                GenerationAggregateState.UNCERTAIN,
                                CurrentSendWorkKind.TIMELINE,
                                work("uncertain.pinned"),
                                null,
                                null,
                                List.of(uncertain),
                                1,
                                0,
                                true,
                                7),
                "terminal",
                        index(
                                GenerationAggregateState.EXPIRED,
                                CurrentSendWorkKind.NONE,
                                null,
                                null,
                                null,
                                List.of(uncertain),
                                1,
                                0,
                                true,
                                9));
        actual.forEach((name, expected) -> {
            assertArrayEquals(bytes("runtime." + name), expected.canonicalBytes(), name);
            assertEquals(expected, TargetGenerationRuntimeIndex.decode(expected.canonicalBytes()));
            assertArrayEquals(
                    GenerationRuntimeIndex.obligationSetDigest(expected.attemptObligations()),
                    expected.obligationSetDigest());
        });
    }

    @Test
    void completeMessagesAndStableExpiryMatchIndependentVectors() {
        for (String name : List.of("initial", "claimed", "terminal")) {
            final var expected = message(runtime(name));
            assertArrayEquals(bytes("message." + name), expected.canonicalBytes(), name);
            assertArrayEquals(bytes("message.key"), expected.encodedKey());
            assertEquals(
                    expected,
                    TargetMessageRecord.decodeForStore(
                            expected.encodedKey(), expected.canonicalBytes(), source.shardId()));
        }
        final var object = new TargetMessageRecord(
                locator,
                5,
                100,
                200,
                100,
                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                source,
                null,
                TargetPayloadReference.decode(bytes("message.object.ref")),
                runtime("initial"));
        assertArrayEquals(bytes("message.object"), object.canonicalBytes());
        assertEquals(12, object.payloadLength());
        final var expiry = new TargetExpiryRef(locator, 200);
        assertArrayEquals(bytes("message.expiry"), expiry.canonicalBytes());
        assertArrayEquals(bytes("message.expiry.key"), expiry.encodedKey());
        assertEquals(
                expiry,
                TargetExpiryRef.decodeForMessage(
                        expiry.encodedKey(), expiry.canonicalBytes(), message(runtime("initial"))));
    }

    @Test
    void terminalAggregateRetainsEveryUnresolvedAttemptWithoutCreatingNewWork() {
        final var terminal = TargetMessageRecord.decode(bytes("message.terminal"));
        assertEquals(GenerationAggregateState.EXPIRED, terminal.aggregateState());
        assertEquals(CurrentSendWorkKind.NONE, terminal.runtime().currentWorkKind());
        assertEquals(1, terminal.runtime().attemptObligations().size());
        assertEquals(
                AttemptLedgerState.UNCERTAIN,
                terminal.runtime().attemptObligations().getFirst().ledgerState());
        assertArrayEquals(
                bytes("obligation.uncertain"),
                terminal.runtime().attemptObligations().getFirst().canonicalBytes());
        assertTrue(terminal.runtime().possibleDestinationDuplicate());
        final var publishedButRetained = index(
                GenerationAggregateState.PUBLISHED,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                List.of(ref(0x77, 2, AttemptLedgerState.PUBLISHING)),
                1,
                0,
                false,
                10);
        assertEquals(publishedButRetained, TargetGenerationRuntimeIndex.decode(publishedButRetained.canonicalBytes()));
    }

    @Test
    void aNewClaimOrPublisherCannotHideTheEarlierUncertainAttempt() {
        final var unresolved = ref(0x77, 2, AttemptLedgerState.UNCERTAIN);
        final var publisher = ref(0x88, 2, AttemptLedgerState.PUBLISHING);
        final var claimed = index(
                GenerationAggregateState.UNCERTAIN,
                CurrentSendWorkKind.CLAIMED,
                null,
                claim,
                null,
                List.of(unresolved),
                1,
                0,
                true,
                9);
        assertEquals(claimed, TargetGenerationRuntimeIndex.decode(claimed.canonicalBytes()));
        final var publishing = index(
                GenerationAggregateState.UNCERTAIN,
                CurrentSendWorkKind.PUBLISHING,
                null,
                null,
                repeated(32, 0x88),
                List.of(unresolved, publisher),
                2,
                1,
                true,
                10);
        assertEquals(publishing, TargetGenerationRuntimeIndex.decode(publishing.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.PUBLISHING,
                        CurrentSendWorkKind.PUBLISHING,
                        null,
                        null,
                        repeated(32, 0x88),
                        List.of(unresolved, publisher),
                        2,
                        1,
                        true,
                        10));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.RETRY_WAIT,
                        CurrentSendWorkKind.TIMELINE,
                        work("definitive"),
                        null,
                        null,
                        List.of(unresolved),
                        1,
                        0,
                        false,
                        6));
    }

    @Test
    void branchAndAggregateMismatchesCannotDecodeAsCurrentSendWork() {
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.NONE,
                        null,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        5));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.TIMELINE,
                        null,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        5));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.TIMELINE,
                        work("native"),
                        claim,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        5));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.PUBLISHING,
                        CurrentSendWorkKind.PUBLISHING,
                        null,
                        null,
                        attempt,
                        List.of(),
                        1,
                        0,
                        false,
                        7));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.PUBLISHING,
                        CurrentSendWorkKind.PUBLISHING,
                        null,
                        null,
                        attempt,
                        List.of(ref(0x88, 2, AttemptLedgerState.PUBLISHING)),
                        1,
                        0,
                        false,
                        7));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.CLAIMED,
                        CurrentSendWorkKind.CLAIMED,
                        null,
                        claim,
                        null,
                        List.of(ref(0x77, 2, AttemptLedgerState.PUBLISHING)),
                        1,
                        0,
                        false,
                        7));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.EXPIRED,
                        CurrentSendWorkKind.TIMELINE,
                        work("native"),
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        5));
    }

    @Test
    void nextAttemptAndRevisionCannotResetAdmissionHistoryOrOverflow() {
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.TIMELINE,
                        work("native"),
                        null,
                        null,
                        List.of(),
                        1,
                        0,
                        false,
                        5));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.RETRY_WAIT,
                        CurrentSendWorkKind.TIMELINE,
                        work("definitive"),
                        null,
                        null,
                        List.of(),
                        1,
                        0,
                        false,
                        7));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.RETRY_WAIT,
                        CurrentSendWorkKind.TIMELINE,
                        work("definitive"),
                        null,
                        null,
                        List.of(),
                        Integer.MAX_VALUE,
                        0,
                        false,
                        6));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.UNCERTAIN,
                        CurrentSendWorkKind.NONE,
                        null,
                        null,
                        null,
                        List.of(ref(0x77, 2, AttemptLedgerState.UNCERTAIN)),
                        0,
                        0,
                        false,
                        8));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.EXPIRED,
                        CurrentSendWorkKind.NONE,
                        null,
                        null,
                        null,
                        List.of(),
                        1,
                        2,
                        false,
                        8));
        assertThrows(
                IllegalArgumentException.class,
                () -> index(
                        GenerationAggregateState.EXPIRED,
                        CurrentSendWorkKind.NONE,
                        null,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        0));
    }

    @Test
    void exactAttemptGenerationIdLengthAndOrderingAreRequired() {
        assertThrows(
                IllegalArgumentException.class, () -> terminal(List.of(ref(0x77, 3, AttemptLedgerState.UNCERTAIN)), 1));
        final var good = ref(0x77, 2, AttemptLedgerState.UNCERTAIN);
        assertThrows(IllegalArgumentException.class, () -> terminal(List.of(good, good), 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> terminal(List.of(ref(0x88, 2, AttemptLedgerState.UNCERTAIN), good), 2));
        final byte[] wrongKey = good.encodedInflightKey();
        wrongKey[45] ^= 1;
        final var mismatched = new AttemptObligationRef(attempt, 2, AttemptLedgerState.UNCERTAIN, wrongKey);
        assertThrows(IllegalArgumentException.class, () -> terminal(List.of(mismatched), 1));
        final byte[] wrongLength = good.encodedInflightKey();
        ByteBuffer.wrap(wrongLength).putInt(10, 31);
        assertThrows(
                IllegalArgumentException.class,
                () -> terminal(
                        List.of(new AttemptObligationRef(attempt, 2, AttemptLedgerState.UNCERTAIN, wrongLength)), 1));
    }

    @Test
    void obligationAndFieldBoundsRejectExcessBeforeConstructingAnUnboundedProjection() {
        final var ref = ref(0x77, 2, AttemptLedgerState.UNCERTAIN);
        assertThrows(IllegalArgumentException.class, () -> terminal(java.util.Collections.nCopies(1025, ref), 1025));
        final byte[] excessive = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, 2);
            CanonicalProtobuf.uint32(out, 3, 9);
            CanonicalProtobuf.uint32(out, 4, 1);
            for (int i = 0; i < 1025; i++) {
                CanonicalProtobuf.bytes(out, 8, ref.canonicalBytes());
            }
            CanonicalProtobuf.uint32(out, 9, 1025);
            CanonicalProtobuf.uint32(out, 10, 0);
            CanonicalProtobuf.uint32(out, 11, 0);
            CanonicalProtobuf.uint32(out, 12, 9);
            CanonicalProtobuf.bytes(out, 13, new byte[32]);
        });
        assertTrue(assertThrows(IllegalArgumentException.class, () -> TargetGenerationRuntimeIndex.decode(excessive))
                .getMessage()
                .contains("count"));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetGenerationRuntimeIndex.decode(
                        new byte[TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void completeMessageChecksTimingSourcePermissionPayloadAndSelectedWork() {
        final var initial = message(runtime("initial"));
        assertEquals(
                work("native"),
                initial.requireTimelineProjection(
                        work("native").nativeKey(), work("native").canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> message(runtime("claimed"))
                .requireTimelineProjection(
                        work("native").ordinaryKey(), work("native").canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> initial.requireTimelineProjection(
                        work("initial").ordinaryKey(), work("initial").canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageRecord(
                        locator,
                        5,
                        101,
                        200,
                        100,
                        NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                        source,
                        new byte[0],
                        null,
                        runtime("initial")));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageRecord(
                        locator,
                        5,
                        100,
                        200,
                        100,
                        NativeDeliveryPolicy.FORBID,
                        source,
                        new byte[0],
                        null,
                        runtime("initial")));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageRecord(
                        locator,
                        5,
                        100,
                        200,
                        100,
                        NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                        source,
                        null,
                        null,
                        runtime("initial")));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageRecord(
                        locator,
                        5,
                        100,
                        200,
                        100,
                        NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                        source,
                        new byte[0],
                        payload(),
                        runtime("initial")));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMessageRecord(
                        locator,
                        5,
                        100,
                        200,
                        100,
                        NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                        TargetSourcePosition.decode(bytes("work.control.source")),
                        new byte[0],
                        null,
                        runtime("initial")));
        final var empty = new TargetMessageRecord(
                locator,
                5,
                100,
                200,
                100,
                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                source,
                new byte[0],
                null,
                runtime("initial"));
        assertEquals(0, TargetMessageRecord.decode(empty.canonicalBytes()).payloadLength());
    }

    @Test
    void storeKeyAndExpiryCannotCrossShardGenerationOrTerminalState() {
        final var initial = message(runtime("initial"));
        final byte[] value = initial.canonicalBytes();
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMessageRecord.decodeForStore(
                        KeyCodec.idMessage(locator.messageId()), value, source.shardId()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMessageRecord.decodeForStore(
                        initial.encodedKey(),
                        value,
                        new ShardId(source.shardId().routeIncarnation(), 4)));
        final var expiry = new TargetExpiryRef(locator, 200);
        assertDoesNotThrow(() -> TargetExpiryRef.decodeForMessage(
                expiry.encodedKey(), expiry.canonicalBytes(), message(runtime("claimed"))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetExpiryRef.decodeForMessage(
                        expiry.encodedKey(), expiry.canonicalBytes(), message(runtime("terminal"))));
        final byte[] nextGenKey = TargetKeyCodec.expiry(200, locator.target(), locator.messageId(), 3);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetExpiryRef.decodeForMessage(nextGenKey, expiry.canonicalBytes(), initial));
        final var wrongTime = new TargetExpiryRef(locator, 201);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetExpiryRef.decodeForMessage(wrongTime.encodedKey(), wrongTime.canonicalBytes(), initial));
    }

    @Test
    void objectPayloadRequiresBoundedExactCommittedIdentityAndKeepsAbsentEtag() {
        final var p = payload();
        assertEquals(null, p.etag());
        assertArrayEquals(
                bytes("message.object.ref"),
                TargetPayloadReference.decode(p.encode()).encode());
        final var old = new PayloadReference(
                p.objectStoreProfileHash(),
                p.container(),
                p.objectKey(),
                p.immutableObjectVersion(),
                null,
                p.length(),
                p.payloadSha256());
        assertEquals(old, PayloadReference.decode(old.encode()));
        assertThrows(IllegalArgumentException.class, () -> TargetPayloadReference.decode(old.encode()));
        final byte[] maximum = new byte[TargetPayloadReference.MAX_COMPONENT_BYTES];
        final var big = new PayloadReference(
                p.objectStoreProfileHash(),
                maximum,
                maximum,
                maximum,
                maximum,
                Long.MAX_VALUE,
                p.payloadSha256(),
                p.reservationId(),
                p.proofId());
        assertEquals(TargetPayloadReference.MAX_CANONICAL_BYTES, big.encode().length);
        assertEquals(big, TargetPayloadReference.decode(big.encode()));
        final var over = new PayloadReference(
                p.objectStoreProfileHash(),
                new byte[maximum.length + 1],
                p.objectKey(),
                p.immutableObjectVersion(),
                null,
                12,
                p.payloadSha256(),
                p.reservationId(),
                p.proofId());
        assertEquals(maximum.length + 1, over.maximumIdentityComponentBytes());
        assertThrows(IllegalArgumentException.class, () -> TargetPayloadReference.requireBounded(over));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetPayloadReference.decode(new byte[TargetPayloadReference.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void maximumMessageAndRuntimeMatchIndependentFullWidthDigests() {
        final byte[] resource = new byte[32];
        for (int i = 0; i < 32; i++) {
            resource[i] = (byte) i;
        }
        final var schedule = new PulsarSourcePosition(
                source.shardId(),
                resource,
                "x".repeat(1 << 20),
                -1,
                -2,
                -2,
                -1,
                PulsarSourcePosition.EntryKind.BATCH,
                Long.MAX_VALUE);
        final var controlSource = new PulsarSourcePosition(
                source.shardId(),
                resource,
                schedule.physicalTopic(),
                -1,
                -1,
                -2,
                -1,
                PulsarSourcePosition.EntryKind.BATCH,
                Long.MAX_VALUE);
        final var loc = new TargetMessageLocator(
                locator.messageId(),
                -1,
                locator.target(),
                new TargetKeyCodec.Domain(63, -1),
                locator.accountingIncarnation(),
                OrderingMode.BEST_EFFORT,
                null,
                locator.scheduleBindingDigest());
        final var work = new TargetTimelineWorkRef(
                loc,
                TimelineWorkKind.UNCERTAIN_RETRY,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                schedule.sourceOrderToken(),
                Integer.MAX_VALUE,
                -1,
                UncertainRetryAuthority.CONTROL_OVERRIDE,
                new ControlRef(repeated(32, 0x44), repeated(32, 0x55), 0xffff_ffffL),
                controlSource,
                false);
        final List<AttemptObligationRef> refs = new ArrayList<>();
        for (int i = 1; i <= 1024; i++) {
            final byte[] id = new byte[32];
            ByteBuffer.wrap(id).putInt(28, i);
            refs.add(new AttemptObligationRef(
                    id, -1, AttemptLedgerState.UNCERTAIN, KeyCodec.inflight((byte) 3, Long.MAX_VALUE, id)));
        }
        final var runtime = new TargetGenerationRuntimeIndex(
                -1,
                GenerationAggregateState.UNCERTAIN,
                CurrentSendWorkKind.TIMELINE,
                work,
                null,
                null,
                refs,
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE - 2,
                true,
                -1);
        final byte[] runtimeBytes = runtime.canonicalBytes();
        assertTrue(runtimeBytes.length <= TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES);
        assertEquals(Integer.parseInt(vectors.getProperty("runtime.maximum.length")), runtimeBytes.length);
        assertEquals(vectors.getProperty("runtime.maximum.sha256"), Bytes.hex(Bytes.sha256(runtimeBytes)));
        assertEquals(runtime, TargetGenerationRuntimeIndex.decode(runtimeBytes));
        final var message = new TargetMessageRecord(
                loc,
                -1,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                NativeDeliveryPolicy.FORBID,
                schedule,
                new byte[TargetMessageRecord.MAX_INLINE_BYTES],
                null,
                runtime);
        final byte[] messageBytes = message.canonicalBytes();
        assertTrue(messageBytes.length <= TargetMessageRecord.MAX_CANONICAL_BYTES);
        assertEquals(Integer.parseInt(vectors.getProperty("message.maximum.length")), messageBytes.length);
        assertEquals(vectors.getProperty("message.maximum.sha256"), Bytes.hex(Bytes.sha256(messageBytes)));
        assertEquals(message, TargetMessageRecord.decode(messageBytes));
    }

    @Test
    void corruptMissingDuplicateAndUnknownFieldsFailClosed() {
        for (String name : List.of("runtime.initial", "message.initial", "message.expiry")) {
            final byte[] valid = bytes(name);
            final byte[] corrupt = valid.clone();
            corrupt[corrupt.length - 1] ^= 1;
            assertThrows(IllegalArgumentException.class, () -> decode(name, corrupt));
            assertThrows(IllegalArgumentException.class, () -> decode(name, Arrays.copyOf(valid, valid.length - 1)));
            assertThrows(IllegalArgumentException.class, () -> decode(name, Bytes.concat(new byte[] {8, 1}, valid)));
            assertThrows(IllegalArgumentException.class, () -> decode(name, Bytes.concat(valid, new byte[] {0x78, 1})));
        }
        final byte[] missing = CanonicalProtobuf.message(out -> {
            for (var field : QueryCodecSupport.read(bytes("runtime.initial"), "test", true)) {
                if (field.number() != 12) {
                    if (field.wireType() == 2) {
                        CanonicalProtobuf.bytes(out, field.number(), field.rawValue());
                    } else {
                        CanonicalProtobuf.uint64Bits(out, field.number(), field.unsignedValue());
                    }
                }
            }
        });
        assertThrows(IllegalArgumentException.class, () -> TargetGenerationRuntimeIndex.decode(missing));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetExpiryRef.decode(new byte[TargetExpiryRef.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMessageRecord.decode(new byte[TargetMessageRecord.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void reservedMessageAndExpiryEnvelopesHaveValidCrcAndRemainRejectedByLaneReader() {
        for (String name : List.of("message.initial.value", "message.expiry.value")) {
            final byte[] value = bytes(name);
            assertEquals(
                    Bytes.readU32be(value, value.length - 4), Bytes.crc32c(Arrays.copyOf(value, value.length - 4)));
            assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        }
    }

    @Test
    void retainedListsAndPayloadsCannotBeChangedByCallers() {
        final List<AttemptObligationRef> refs = new ArrayList<>(List.of(ref(0x77, 2, AttemptLedgerState.UNCERTAIN)));
        final var runtime = terminal(refs, 1);
        final byte[] saved = runtime.canonicalBytes();
        refs.clear();
        assertThrows(UnsupportedOperationException.class, () -> runtime.attemptObligations()
                .clear());
        assertArrayEquals(saved, runtime.canonicalBytes());
        final byte[] payload = new byte[] {1, 2, 3};
        final var record = new TargetMessageRecord(
                locator, 9, 100, 200, 100, NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF, source, payload, null, runtime);
        final byte[] encoded = record.canonicalBytes();
        Arrays.fill(payload, (byte) 0);
        Arrays.fill(record.inlinePayload(), (byte) 0);
        Arrays.fill(record.digest(), (byte) 0);
        assertArrayEquals(encoded, record.canonicalBytes());
    }

    private TargetMessageRecord message(final TargetGenerationRuntimeIndex runtime) {
        return new TargetMessageRecord(
                locator,
                runtime.runtimeRevision(),
                100,
                200,
                100,
                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                source,
                Bytes.utf8("payload"),
                null,
                runtime);
    }

    private TargetGenerationRuntimeIndex terminal(final List<AttemptObligationRef> refs, final int admissions) {
        return index(
                GenerationAggregateState.EXPIRED,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                refs,
                admissions,
                0,
                true,
                9);
    }

    private TargetGenerationRuntimeIndex index(
            final GenerationAggregateState aggregate,
            final CurrentSendWorkKind current,
            final TargetTimelineWorkRef work,
            final byte[] claimId,
            final byte[] attemptId,
            final List<AttemptObligationRef> refs,
            final int admissions,
            final int uncertain,
            final boolean duplicate,
            final long revision) {
        return new TargetGenerationRuntimeIndex(
                2, aggregate, current, work, claimId, attemptId, refs, admissions, uncertain, duplicate, revision);
    }

    private static AttemptObligationRef ref(final int idByte, final int generation, final AttemptLedgerState state) {
        final byte[] id = repeated(32, idByte);
        return new AttemptObligationRef(
                id,
                generation,
                state,
                KeyCodec.inflight((byte) (state == AttemptLedgerState.PUBLISHING ? 2 : 3), 7, id));
    }

    private TargetGenerationRuntimeIndex runtime(final String name) {
        return TargetGenerationRuntimeIndex.decode(bytes("runtime." + name));
    }

    private TargetTimelineWorkRef work(final String name) {
        return TargetTimelineWorkRef.decode(bytes("work." + name));
    }

    private PayloadReference payload() {
        return TargetPayloadReference.decode(bytes("message.object.ref"));
    }

    private byte[] bytes(final String name) {
        return HexFormat.of().parseHex(vectors.getProperty(name));
    }

    private static byte[] repeated(final int n, final int value) {
        final byte[] bytes = new byte[n];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static void decode(final String name, final byte[] value) {
        if (name.startsWith("runtime.")) {
            TargetGenerationRuntimeIndex.decode(value);
        } else if (name.endsWith("expiry")) {
            TargetExpiryRef.decode(value);
        } else {
            TargetMessageRecord.decode(value);
        }
    }

    private static Properties vectors() {
        try (var stream =
                TargetMessageContractTest.class.getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            final var props = new Properties();
            props.load(stream);
            return props;
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }
}
