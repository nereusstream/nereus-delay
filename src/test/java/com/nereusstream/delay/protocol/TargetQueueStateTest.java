package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetQueueStateTest {
    private static final byte[] DISPATCH = repeated(32, 0xaa);
    private static final byte[] CONTROL = repeated(32, 0xbb);
    private static final byte[] NATIVE = repeated(32, 0xcc);
    private static final byte[] ACCOUNT = HexFormat.of().parseHex("0102030405060708090a0b0c0d0e0f10");
    private final Properties vectors = loadVectors();
    private final CanonicalTargetPartition target = CanonicalTargetPartition.decode(bytes("pulsar.canonical"));
    private final DelayMessageId message = new DelayMessageId(bytes("message.id"));
    private final ShardId shard = message.routingId().shardId();

    @Test
    void kafkaCannotExposeNativeHeadAtTheStoreBoundary() {
        final CanonicalTargetPartition kafka = CanonicalTargetPartition.decode(bytes("kafka.canonical"));
        final var domain = new TargetKeyCodec.Domain(0, 1);
        final byte[] token = Bytes.concat(new byte[] {1}, Bytes.u64be(7));
        final TargetHeadRef due = new TargetHeadRef(
                TargetKeyCodec.candidate(TargetKeyCodec.CandidateKind.DUE, kafka.id(), domain, 100, token, message, 2),
                message,
                2,
                100);
        final TargetHeadRef nativeHead = new TargetHeadRef(
                TargetKeyCodec.candidate(
                        TargetKeyCodec.CandidateKind.NATIVE, kafka.id(), domain, 100, token, message, 2),
                message,
                2,
                100);
        final TargetQueueState invalid = new TargetQueueState(
                kafka.id(),
                1,
                1,
                TargetQueueState.AdmissionState.OPEN,
                ACCOUNT,
                60000,
                List.of(active(domain, due, nativeHead)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decodeForStore(
                        TargetKeyCodec.state(kafka.id()), invalid.canonicalBytes(), kafka, shard, 1));
    }

    @Test
    void strictBusinessOrderAndTheEligibleHeadUseSeparateValidatedKeys() {
        final byte[] ordering = repeated(32, 0x11);
        final var domain = new TargetKeyCodec.Domain(0, 1);
        final byte[] token = Bytes.concat(new byte[] {1}, Bytes.u64be(7));
        final byte[] ordered = TargetKeyCodec.ordered(target.id(), ordering, 90, token, message, 2);
        final byte[] head = TargetKeyCodec.orderedHead(target.id(), domain, 100, ordering);
        assertArrayEquals(bytes("ordered.key"), ordered);
        assertArrayEquals(bytes("order.head.key"), head);
        assertArrayEquals(bytes("order.state.key"), TargetKeyCodec.orderState(target.id(), ordering));
        assertEquals(128, ordered.length);
        assertEquals(84, head.length);
        assertEquals(90, TargetKeyCodec.decodeOrdered(ordered).deliverAtEpochMs());
        assertEquals(100, TargetKeyCodec.decodeOrderedHead(head).eligibleAtEpochMs());
        assertArrayEquals(ordered, TargetKeyCodec.decodeOrdered(ordered).encodedKey());
        assertArrayEquals(head, TargetKeyCodec.decodeOrderedHead(head).encodedKey());
        assertEquals(TargetKeyCodec.decodeOrdered(ordered), TargetKeyCodec.decodeOrdered(ordered));
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeCandidate(ordered));
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeOrderedHead(ordered));
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeOrdered(head));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetKeyCodec.orderedHead(target.id(), domain, 100, new byte[32]));
        final byte[] unknown = ordered.clone();
        unknown[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeOrdered(unknown));
        final byte[] negative = head.clone();
        ByteBuffer.wrap(negative).putLong(44, -1);
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeOrderedHead(negative));
    }

    @Test
    void closedStateHeadAndDomainEncodingsMatchIndependentVectors() {
        final TargetHeadRef ordinary = head("queue.due.key");
        final TargetHeadRef nativeHead = head("native.kafka.key");
        final TargetHeadRef ordered = head("order.head.key");
        assertArrayEquals(bytes("head.ordinary"), ordinary.canonicalBytes());
        assertArrayEquals(bytes("head.native"), nativeHead.canonicalBytes());
        assertArrayEquals(bytes("head.ordered"), ordered.canonicalBytes());
        final TargetDomainState active = active(new TargetKeyCodec.Domain(0, 1), ordinary, nativeHead);
        final TargetDomainState draining = domain(0, 1, TargetDomainState.Lifecycle.DRAINING, null, null, NATIVE);
        final TargetDomainState vacant = vacant(0, 1);
        final TargetDomainState orderedDomain = domain(0, 1, TargetDomainState.Lifecycle.ACTIVE, ordered, null, null);
        assertArrayEquals(bytes("domain.active"), active.canonicalBytes());
        assertArrayEquals(bytes("domain.draining"), draining.canonicalBytes());
        assertArrayEquals(bytes("domain.vacant"), vacant.canonicalBytes());
        assertArrayEquals(bytes("domain.ordered"), orderedDomain.canonicalBytes());
        final List<TargetQueueState> states = List.of(
                state(1, 1, TargetQueueState.AdmissionState.OPEN, List.of()),
                state(2, 1, TargetQueueState.AdmissionState.OPEN, List.of(active)),
                state(3, 1, TargetQueueState.AdmissionState.OPEN, List.of(draining)),
                state(4, 1, TargetQueueState.AdmissionState.OPEN, List.of(vacant)),
                state(2, 2, TargetQueueState.AdmissionState.PAUSED, List.of(orderedDomain)));
        final String[] names = {"empty", "active", "draining", "vacant", "ordered"};
        for (int index = 0; index < names.length; index++) {
            final TargetQueueState expected = states.get(index);
            assertArrayEquals(bytes("queue." + names[index]), expected.canonicalBytes());
            assertEquals(expected, TargetQueueState.decode(expected.canonicalBytes()));
            assertEquals(
                    expected,
                    TargetQueueState.decodeForStore(
                            TargetKeyCodec.state(target.id()), expected.canonicalBytes(), target, shard, 1));
        }
    }

    @Test
    void maximumFieldWidthsAndAllSixtyFourSlotsFitTheDerivedBound() {
        final List<TargetDomainState> domains = new ArrayList<>();
        final byte[] token =
                Bytes.concat(new byte[] {2}, Bytes.u64beBits(-1), Bytes.u64beBits(-1), Bytes.u32beBits(-1));
        for (int slot = 0; slot < TargetQueueState.MAX_DOMAIN_SLOTS; slot++) {
            final var domain = new TargetKeyCodec.Domain(slot, -1);
            final TargetHeadRef due = new TargetHeadRef(
                    TargetKeyCodec.candidate(
                            TargetKeyCodec.CandidateKind.DUE, target.id(), domain, Long.MAX_VALUE, token, message, -1),
                    message,
                    -1,
                    Long.MAX_VALUE);
            final TargetHeadRef nativeHead = new TargetHeadRef(
                    TargetKeyCodec.candidate(
                            TargetKeyCodec.CandidateKind.NATIVE,
                            target.id(),
                            domain,
                            Long.MAX_VALUE,
                            token,
                            message,
                            -1),
                    message,
                    -1,
                    Long.MAX_VALUE);
            final TargetDomainState state = active(domain, due, nativeHead);
            assertTrue(due.canonicalBytes().length <= TargetHeadRef.MAX_CANONICAL_BYTES);
            assertTrue(state.canonicalBytes().length <= TargetDomainState.MAX_CANONICAL_BYTES);
            domains.add(state);
        }
        final TargetQueueState maximum = new TargetQueueState(
                target.id(), -1, -1, TargetQueueState.AdmissionState.PAUSED, ACCOUNT, Long.MAX_VALUE, domains);
        assertTrue(maximum.canonicalBytes().length <= TargetQueueState.MAX_CANONICAL_BYTES);
        assertEquals(Integer.parseInt(vectors.getProperty("queue.maximum.length")), maximum.canonicalBytes().length);
        assertEquals(vectors.getProperty("queue.maximum.sha256"), Bytes.hex(Bytes.sha256(maximum.canonicalBytes())));
        assertEquals(maximum, TargetQueueState.decode(maximum.canonicalBytes()));
        assertEquals(
                maximum,
                TargetQueueState.decodeForStore(
                        TargetKeyCodec.state(target.id()), maximum.canonicalBytes(), target, shard, 64));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decodeForStore(
                        TargetKeyCodec.state(target.id()), maximum.canonicalBytes(), target, shard, 1));
    }

    @Test
    void corruptUnknownMissingAndRepeatedFieldsNeverProduceAState() {
        final byte[] good = bytes("queue.active");
        final byte[] corrupt = good.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(corrupt));
        assertThrows(
                IllegalArgumentException.class, () -> TargetQueueState.decode(Arrays.copyOf(good, good.length - 1)));
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(replaceUint(good, 1, 2)));
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(replaceUint(good, 3, 0)));
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(replaceUint(good, 5, 4)));
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(replaceUint(good, 7, -1)));
        final byte[] unknown =
                Bytes.concat(good, CanonicalProtobuf.message(out -> CanonicalProtobuf.uint32(out, 10, 1)));
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(unknown));
        final byte[] duplicate =
                Bytes.concat(CanonicalProtobuf.message(out -> CanonicalProtobuf.uint32(out, 1, 1)), good);
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(duplicate));
        for (String key : new String[] {"head.ordinary", "domain.active", "domain.vacant"}) {
            final byte[] damaged = bytes(key);
            damaged[damaged.length - 1] ^= 1;
            assertThrows(IllegalArgumentException.class, () -> {
                if (key.startsWith("head")) {
                    TargetHeadRef.decode(damaged);
                } else {
                    TargetDomainState.decode(damaged);
                }
            });
        }
        final byte[] noDigest = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 0);
            CanonicalProtobuf.uint64(out, 2, 1);
            CanonicalProtobuf.uint32(out, 3, 1);
            CanonicalProtobuf.bytes(out, 4, DISPATCH);
        });
        assertThrows(IllegalArgumentException.class, () -> TargetDomainState.decode(noDigest));
    }

    @Test
    void byteAndFieldCountLimitsAreCheckedBeforeBuildingAnUnboundedFieldList() {
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decode(new byte[TargetQueueState.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDomainState.decode(new byte[TargetDomainState.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetHeadRef.decode(new byte[TargetHeadRef.MAX_CANONICAL_BYTES + 1]));
        final byte[] repeated = CanonicalProtobuf.message(out -> {
            for (int index = 0; index < 73; index++) {
                CanonicalProtobuf.uint32(out, 1, 1);
            }
        });
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.decode(repeated));
    }

    @Test
    void headIdentityTimeDomainRoleAndNativeScopeMustAgree() {
        final var domain = new TargetKeyCodec.Domain(0, 1);
        final TargetHeadRef due = head("queue.due.key");
        final TargetHeadRef nativeHead = head("native.kafka.key");
        assertThrows(IllegalArgumentException.class, () -> new TargetHeadRef(due.key(), message, 3, 100));
        assertThrows(IllegalArgumentException.class, () -> new TargetHeadRef(due.key(), message, 2, 101));
        assertThrows(IllegalArgumentException.class, () -> active(domain, nativeHead, nativeHead));
        assertThrows(IllegalArgumentException.class, () -> active(domain, null, nativeHead));
        assertThrows(
                IllegalArgumentException.class,
                () -> domain(0, 1, TargetDomainState.Lifecycle.ACTIVE, due, nativeHead, null));
        assertThrows(IllegalArgumentException.class, () -> active(new TargetKeyCodec.Domain(0, 2), due, nativeHead));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetDomainState(
                        domain, TargetDomainState.Lifecycle.VACANT, DISPATCH, null, null, null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetDomainState(
                        domain, TargetDomainState.Lifecycle.ACTIVE, new byte[32], CONTROL, null, null, null));
        final byte[] wrongSource = bytes("native.kafka.key");
        wrongSource[60] ^= 1;
        assertThrows(
                IllegalArgumentException.class,
                () -> active(domain, due, new TargetHeadRef(wrongSource, message, 2, 100)));
        final byte[] wrongGeneration = nativeHead.key();
        ByteBuffer.wrap(wrongGeneration).putInt(wrongGeneration.length - 4, 3);
        assertThrows(
                IllegalArgumentException.class,
                () -> active(domain, due, new TargetHeadRef(wrongGeneration, message, 3, 100)));
    }

    @Test
    void storeProjectionChecksExactPhysicalIdentityKeySourceAndActivatedSlotLimit() {
        final byte[] canonical = bytes("queue.active");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decodeForStore(new byte[] {9, 1}, canonical, target, shard, 1));
        final CanonicalTargetPartition different = new CanonicalTargetPartition(target.resource(), 4);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decodeForStore(
                        TargetKeyCodec.state(target.id()), canonical, different, shard, 1));
        final ShardId otherShard = new ShardId(shard.routeIncarnation(), 4);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decodeForStore(
                        TargetKeyCodec.state(target.id()), canonical, target, otherShard, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQueueState.decodeForStore(TargetKeyCodec.state(target.id()), canonical, target, shard, 0));
        assertEquals(target, CanonicalTargetPartition.decodeForStore(bytes("identity.key"), target.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalTargetPartition.decodeForStore(
                        TargetKeyCodec.identity(different.id()), target.canonicalBytes()));
    }

    @Test
    void domainHistoryIsContiguousAndClosedTargetsExposeNoHead() {
        final TargetDomainState active = active(new TargetKeyCodec.Domain(0, 1), head("queue.due.key"), null);
        assertThrows(
                IllegalArgumentException.class,
                () -> state(1, 1, TargetQueueState.AdmissionState.CLOSED, List.of(active)));
        assertThrows(
                IllegalArgumentException.class,
                () -> state(1, 1, TargetQueueState.AdmissionState.OPEN, List.of(vacant(1, 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> state(1, 1, TargetQueueState.AdmissionState.OPEN, List.of(vacant(0, 1), vacant(0, 2))));
        final List<TargetDomainState> excess = new ArrayList<>();
        for (int index = 0; index <= 64; index++) {
            excess.add(vacant(index, 1));
        }
        assertThrows(IllegalArgumentException.class, () -> state(1, 1, TargetQueueState.AdmissionState.OPEN, excess));
        final CanonicalTargetPartition different = new CanonicalTargetPartition(target.resource(), 4);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQueueState(
                        different.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, ACCOUNT, 60000, List.of(active)));
    }

    @Test
    void slotReuseRequiresDrainVacancyAndANewGeneration() {
        final TargetQueueState empty = TargetQueueState.decode(bytes("queue.empty"));
        final TargetQueueState active = TargetQueueState.decode(bytes("queue.active"));
        final TargetQueueState draining = TargetQueueState.decode(bytes("queue.draining"));
        final TargetQueueState vacant = TargetQueueState.decode(bytes("queue.vacant"));
        active.requireSuccessorOf(empty);
        draining.requireSuccessorOf(active);
        vacant.requireSuccessorOf(draining);
        final TargetDomainState rebound = domain(0, 2, TargetDomainState.Lifecycle.ACTIVE, null, null, NATIVE);
        final TargetQueueState reused = state(5, 1, TargetQueueState.AdmissionState.OPEN, List.of(rebound));
        reused.requireSuccessorOf(vacant);
        assertThrows(
                IllegalArgumentException.class,
                () -> active(new TargetKeyCodec.Domain(0, 2), head("queue.due.key"), null));
        assertThrows(IllegalArgumentException.class, () -> state(
                        3, 1, TargetQueueState.AdmissionState.OPEN, List.of(vacant(0, 1)))
                .requireSuccessorOf(active));
        assertThrows(IllegalArgumentException.class, () -> state(5, 1, TargetQueueState.AdmissionState.OPEN, List.of())
                .requireSuccessorOf(vacant));
        assertThrows(IllegalArgumentException.class, () -> state(
                        5,
                        1,
                        TargetQueueState.AdmissionState.OPEN,
                        List.of(domain(0, 1, TargetDomainState.Lifecycle.ACTIVE, null, null, NATIVE)))
                .requireSuccessorOf(vacant));
        assertThrows(IllegalArgumentException.class, () -> state(
                        4, 1, TargetQueueState.AdmissionState.OPEN, active.domains())
                .requireSuccessorOf(draining));
    }

    @Test
    void boundSlotsCannotRewriteCompatibilityControlOrPolicyReferences() {
        final TargetQueueState prior = TargetQueueState.decode(bytes("queue.active"));
        for (int field = 0; field < 3; field++) {
            final byte[][] refs = {DISPATCH.clone(), CONTROL.clone(), NATIVE.clone()};
            refs[field][0] ^= 1;
            final TargetDomainState changed = new TargetDomainState(
                    new TargetKeyCodec.Domain(0, 1),
                    TargetDomainState.Lifecycle.ACTIVE,
                    refs[0],
                    refs[1],
                    refs[2],
                    head("queue.due.key"),
                    head("native.kafka.key"));
            assertThrows(IllegalArgumentException.class, () -> state(
                            3, 1, TargetQueueState.AdmissionState.OPEN, List.of(changed))
                    .requireSuccessorOf(prior));
        }
    }

    @Test
    void unsignedRevisionsCrossTheSignedBoundaryButCannotWrap() {
        assertEquals(Long.MIN_VALUE, TargetQueueState.nextRevision(Long.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> TargetQueueState.nextRevision(-1));
        assertThrows(IllegalArgumentException.class, () -> TargetQueueState.nextRevision(0));
        final TargetQueueState prior = state(Long.MAX_VALUE, -1, TargetQueueState.AdmissionState.OPEN, List.of());
        final TargetQueueState next = state(Long.MIN_VALUE, -1, TargetQueueState.AdmissionState.OPEN, List.of());
        assertDoesNotThrow(() -> next.requireSuccessorOf(prior));
        assertEquals(next, TargetQueueState.decode(next.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> state(
                        Long.MIN_VALUE + 1, -1, TargetQueueState.AdmissionState.OPEN, List.of())
                .requireSuccessorOf(prior));
    }

    @Test
    void targetControlsAndImmutableAccountingAndCapAreCheckedAcrossSuccessors() {
        final TargetQueueState prior = TargetQueueState.decode(bytes("queue.active"));
        final TargetQueueState paused = state(3, 2, TargetQueueState.AdmissionState.PAUSED, prior.domains());
        paused.requireSuccessorOf(prior);
        assertThrows(IllegalArgumentException.class, () -> state(
                        3, 1, TargetQueueState.AdmissionState.PAUSED, prior.domains())
                .requireSuccessorOf(prior));
        final TargetQueueState closed = state(
                4,
                3,
                TargetQueueState.AdmissionState.CLOSED,
                List.of(domain(0, 1, TargetDomainState.Lifecycle.DRAINING, null, null, NATIVE)));
        closed.requireSuccessorOf(paused);
        assertThrows(IllegalArgumentException.class, () -> state(
                        5, 4, TargetQueueState.AdmissionState.OPEN, closed.domains())
                .requireSuccessorOf(closed));
        assertThrows(IllegalArgumentException.class, () -> new TargetQueueState(
                        target.id(), 3, 1, TargetQueueState.AdmissionState.OPEN, ACCOUNT, 60001, prior.domains())
                .requireSuccessorOf(prior));
        assertThrows(IllegalArgumentException.class, () -> new TargetQueueState(
                        target.id(),
                        3,
                        1,
                        TargetQueueState.AdmissionState.OPEN,
                        repeated(16, 1),
                        60000,
                        prior.domains())
                .requireSuccessorOf(prior));
    }

    @Test
    void reservedStateAndIdentityValueTypesRemainUnreadableToTheLaneEnvelopeReader() {
        for (String name : new String[] {"queue.active.value", "identity.value"}) {
            final byte[] value = bytes(name);
            assertEquals(Bytes.crc32c(value, 0, value.length - 4), Bytes.readU32be(value, value.length - 4));
            assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        }
    }

    @Test
    void retainedArraysAndListsCannotMutateAnAlreadyBoundState() {
        final TargetQueueState state = TargetQueueState.decode(bytes("queue.active"));
        final byte[] before = state.canonicalBytes();
        state.accountingIncarnation()[0] ^= 1;
        state.digest()[0] ^= 1;
        state.domains().get(0).dispatchCompatibilityRef()[0] ^= 1;
        state.domains().get(0).ordinaryHead().key()[0] ^= 1;
        assertThrows(UnsupportedOperationException.class, () -> state.domains().clear());
        assertArrayEquals(before, state.canonicalBytes());
    }

    private TargetQueueState state(
            final long revision,
            final long control,
            final TargetQueueState.AdmissionState gate,
            final List<TargetDomainState> domains) {
        return new TargetQueueState(target.id(), revision, control, gate, ACCOUNT, 60000, domains);
    }

    private TargetHeadRef head(final String key) {
        return new TargetHeadRef(bytes(key), message, 2, 100);
    }

    private static TargetDomainState active(
            final TargetKeyCodec.Domain domain, final TargetHeadRef ordinary, final TargetHeadRef nativeHead) {
        return new TargetDomainState(
                domain, TargetDomainState.Lifecycle.ACTIVE, DISPATCH, CONTROL, NATIVE, ordinary, nativeHead);
    }

    private static TargetDomainState domain(
            final int slot,
            final long generation,
            final TargetDomainState.Lifecycle lifecycle,
            final TargetHeadRef ordinary,
            final TargetHeadRef nativeHead,
            final byte[] scope) {
        return new TargetDomainState(
                new TargetKeyCodec.Domain(slot, generation), lifecycle, DISPATCH, CONTROL, scope, ordinary, nativeHead);
    }

    private static TargetDomainState vacant(final int slot, final long generation) {
        return new TargetDomainState(
                new TargetKeyCodec.Domain(slot, generation),
                TargetDomainState.Lifecycle.VACANT,
                null,
                null,
                null,
                null,
                null);
    }

    private byte[] bytes(final String name) {
        return HexFormat.of().parseHex(vectors.getProperty(name));
    }

    private static byte[] repeated(final int count, final int value) {
        final byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static byte[] replaceUint(final byte[] canonical, final int number, final long value) {
        final var fields = QueryCodecSupport.read(canonical, "test", true);
        return CanonicalProtobuf.message(out -> {
            for (var field : fields) {
                if (field.number() == number) {
                    CanonicalProtobuf.uint64Bits(out, number, value);
                } else if (field.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, field.number(), field.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(out, field.number(), field.unsignedValue());
                }
            }
        });
    }

    private static Properties loadVectors() {
        final Properties vectors = new Properties();
        try (var input = TargetQueueStateTest.class.getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            vectors.load(input);
        } catch (IOException failure) {
            throw new IllegalStateException(failure);
        }
        return vectors;
    }
}
