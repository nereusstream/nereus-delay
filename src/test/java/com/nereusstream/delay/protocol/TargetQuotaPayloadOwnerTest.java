package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaPayloadOwnerTest {
    private final Properties vectors = properties("target-quota-payload-vectors.properties");
    private final Properties bindings = properties("target-binding-channel-vectors.properties");
    private final Properties account = properties("target-quota-accounting-vectors.properties");
    private final TargetQuotaAccounting accounting = TargetQuotaAccounting.decode(raw(account, "accounting"));
    private final byte[] tenant = bytes(32, 0x44);
    private final byte[] lineage = bytes(16, 0xcc);
    private final TargetQuotaPayloadOwner.TransitionAuthority allow = (before, after, floor) -> {};

    @Test
    void allLifecycleBranchesMatchIndependentEncodingAndCharges() {
        final var inline = scheduled("best");
        final var inlineRetained = inline.retain(stamp(2), allow);
        final var reserved = reserved();
        final var committed = reserved.commit(payload(), stamp(2), allow);
        final var retained = committed.retain(stamp(3), allow);
        final var expired = reserved.retain(stamp(2), allow);
        final var values = List.of(
                inline,
                inlineRetained,
                inlineRetained.release(floor(2), stamp(3), allow),
                scheduled("object"),
                reserved,
                committed,
                retained,
                retained.release(floor(3), stamp(4), allow),
                expired,
                expired.release(floor(2), stamp(3), allow));
        final var names = List.of(
                "inline.active",
                "inline.retained",
                "inline.released",
                "object.direct",
                "object.reserved",
                "object.active",
                "object.retained",
                "object.released",
                "object.expired",
                "object.abandoned");
        for (int i = 0; i < values.size(); i++) {
            final var value = values.get(i);
            final String name = names.get(i);
            assertArrayEquals(raw(vectors, name), value.canonicalBytes());
            assertArrayEquals(
                    value.canonicalBytes(),
                    TargetQuotaPayloadOwner.decode(value.canonicalBytes()).canonicalBytes());
            assertEquals(CapacityVector.decode(raw(vectors, name + ".payloadCharge")), value.payloadCharge());
            assertEquals(CapacityVector.decode(raw(vectors, name + ".recordCharge")), value.recordCharge());
            assertArrayEquals(raw(vectors, "owner"), value.primaryIdentity().canonicalBytes());
            assertArrayEquals(raw(vectors, "key"), value.key());
        }
    }

    @Test
    void oneOwnerMovesBetweenBucketsWithoutCopyingPayloadOrAttemptCharges() {
        final var before = reserved();
        final var active = before.commit(payload(), stamp(2), allow);
        final var retained = active.retain(stamp(3), allow);
        final var replay = retained.replay(stamp(4), allow);
        assertEquals(TargetQuotaAccounting.reservedPayload(7), before.payloadCharge());
        assertEquals(TargetQuotaAccounting.activePayload(7), active.payloadCharge());
        assertEquals(TargetQuotaAccounting.retainedPayload(7), retained.payloadCharge());
        assertEquals(active.payloadCharge(), replay.payloadCharge());
        for (var value : List.of(before, active, retained, replay)) {
            assertEquals(0, value.payloadCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
            assertEquals(0, value.payloadCharge().amount(CapacityDimension.INFLIGHT_BYTES));
            assertEquals(before.primaryIdentity(), value.primaryIdentity());
            assertEquals(before.primaryIdentity(), value.tenantIdentity().primary());
            assertArrayEquals(
                    before.accounting().canonicalBytes(), value.accounting().canonicalBytes());
            assertArrayEquals(before.initialBindingDigest(), value.initialBindingDigest());
        }
    }

    @Test
    void allTransitionsRequireTheCompleteAuthorityAndPropagateItsFailures() {
        final var calls = new AtomicInteger();
        final TargetQuotaPayloadOwner.TransitionAuthority authority = (before, after, floor) -> {
            assertEquals(before.messageId(), after.messageId());
            assertTrue(after.mutation().sequence() > before.mutation().sequence());
            calls.incrementAndGet();
        };
        final var active = reserved().commit(payload(), stamp(2), authority);
        final var retained = active.retain(stamp(3), authority);
        retained.release(floor(3), stamp(4), authority);
        assertEquals(3, calls.get());
        for (RuntimeException failure :
                List.of(new IllegalArgumentException("bad proof"), new IllegalStateException("unavailable"))) {
            assertThrows(failure.getClass(), () -> reserved().commit(payload(), stamp(2), (a, b, f) -> {
                throw failure;
            }));
            assertThrows(
                    failure.getClass(),
                    () -> retained.release(floor(3), stamp(4), (a, b, f) -> {
                        throw failure;
                    }));
        }
        assertThrows(
                AssertionError.class,
                () -> active.retain(stamp(3), (a, b, f) -> {
                    throw new AssertionError("fatal");
                }));
        assertThrows(NullPointerException.class, () -> retained.replay(stamp(4), null));
        assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, retained.phase());
    }

    @Test
    void commitMustMatchFrozenLengthHashReservationAndProfile() {
        final var p = payload();
        for (var wrong : List.of(
                reference(p, 8, p.payloadSha256(), p.reservationId(), p.objectStoreProfileHash()),
                reference(p, 7, bytes(32, 9), p.reservationId(), p.objectStoreProfileHash()),
                reference(p, 7, p.payloadSha256(), bytes(32, 9), p.objectStoreProfileHash()),
                reference(p, 7, p.payloadSha256(), p.reservationId(), bytes(32, 9)))) {
            assertThrows(IllegalArgumentException.class, () -> reserved().commit(wrong, stamp(2), allow));
        }
        assertThrows(IllegalStateException.class, () -> scheduled("object").commit(p, stamp(2), allow));
    }

    @Test
    void uncommittedExpiredReservationCannotReplayOrMatchAnInlineMessage() {
        final var expired = reserved().retain(stamp(2), allow);
        assertEquals(TargetQuotaAccounting.retainedPayload(7), expired.payloadCharge());
        assertThrows(IllegalStateException.class, () -> expired.replay(stamp(3), allow));
        assertThrows(
                IllegalStateException.class,
                () -> expired.requireMessagePayload(message("payload".getBytes(), null, 2, bytes(16, 1))));
    }

    @Test
    void releasedPayloadStillHasRecordFeesAndCannotBeRevived() {
        final var released = scheduled("best").retain(stamp(2), allow).release(floor(2), stamp(3), allow);
        assertTrue(released.payloadCharge().isZero());
        assertTrue(released.recordCharge().amount(CapacityDimension.LOGICAL_STATE_BYTES) > 0);
        assertArrayEquals(floor(2).floorDigest(), released.releaseFloorDigest());
        assertThrows(IllegalStateException.class, () -> released.replay(stamp(4), allow));
        assertThrows(IllegalStateException.class, () -> released.retain(stamp(4), allow));
        assertThrows(IllegalStateException.class, () -> released.release(floor(3), stamp(4), allow));
    }

    @Test
    void releaseRequiresFloorCoveringLatestOwnerAndExactSourceSequenceLineage() {
        final var retained = reserved().commit(payload(), stamp(2), allow).retain(stamp(3), allow);
        assertThrows(IllegalStateException.class, () -> retained.release(floor(2), stamp(4), allow));
        final var f = floor(3);
        final var wrongLineage = new RecoveryFloorRef(
                bytes(16, 1),
                f.checkpointId(),
                f.manifestSha256(),
                f.catalogGeneration(),
                f.appliedSourcePosition(),
                f.includedMutationSequence(),
                f.evidenceCursors());
        assertThrows(IllegalStateException.class, () -> retained.release(wrongLineage, stamp(4), allow));
        final var wrongSequence = new RecoveryFloorRef(
                lineage,
                f.checkpointId(),
                f.manifestSha256(),
                f.catalogGeneration(),
                f.appliedSourcePosition(),
                4,
                f.evidenceCursors());
        assertThrows(IllegalStateException.class, () -> retained.release(wrongSequence, stamp(5), allow));
        final var position = (KafkaSourcePosition) f.appliedSourcePosition();
        final var changed = new KafkaSourcePosition(
                position.shardId(),
                position.authenticatedClusterId(),
                position.nativeTopicUuid(),
                position.offset(),
                position.leaderEpoch(),
                111);
        final var wrongMetadata = new RecoveryFloorRef(
                lineage, f.checkpointId(), f.manifestSha256(), f.catalogGeneration(), changed, 3, f.evidenceCursors());
        assertThrows(IllegalStateException.class, () -> retained.release(wrongMetadata, stamp(4), allow));
        assertThrows(IllegalStateException.class, () -> retained.release(f, stamp(3), allow));
    }

    @Test
    void sourceReplayAndForeignResourceCannotAdvanceOwnership() {
        final var value = scheduled("best");
        assertThrows(IllegalStateException.class, () -> value.retain(stamp(1), allow));
        final var source = (KafkaSourcePosition) stamp(2).source();
        final var foreign = new KafkaSourcePosition(
                source.shardId(), "foreign", source.nativeTopicUuid(), source.offset(), source.leaderEpoch(), 110);
        assertThrows(
                IllegalArgumentException.class,
                () -> value.retain(new TargetQuotaMutation(2, foreign, bytes(32, 0x66)), allow));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.scheduled(binding("best"), tenant, accounting, lineage, stamp(2)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.scheduled(binding("prepare"), tenant, accounting, lineage, stamp(1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.reserved(
                        binding("best"), bytes(32, 0x63), tenant, accounting, lineage, stamp(1)));
    }

    @Test
    void fullInitialBindingAndMessagePayloadAreCheckedWithoutMovingTheFrozenOwner() {
        final var value = scheduled("best");
        value.requireInitialBinding(binding("best"));
        assertThrows(IllegalStateException.class, () -> value.requireInitialBinding(binding("native")));
        final var object = reserved().commit(payload(), stamp(2), allow);
        object.requireInitialBinding(binding("prepare"));
        value.requireMessagePayload(message("payload".getBytes(), null, 2, bytes(16, 1)));
        value.requireMessagePayload(message("payload".getBytes(), null, 3, bytes(16, 2)));
        assertArrayEquals(bytes(16, 1), value.primaryIdentity().accountingIncarnation());
        assertThrows(
                IllegalStateException.class,
                () -> value.requireMessagePayload(message("changed".getBytes(), null, 2, bytes(16, 1))));
        object.requireMessagePayload(message(null, payload(), 2, bytes(16, 1)));
    }

    @Test
    void sourceOwnerKeyAndTenantAreClosedAndDefensivelyCopied() {
        final var value = scheduled("best");
        assertEquals(43, value.key().length);
        assertArrayEquals(
                value.canonicalBytes(),
                TargetQuotaPayloadOwner.decodeForStore(
                                value.key(),
                                value.canonicalBytes(),
                                value.primaryIdentity().shard(),
                                tenant)
                        .canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decodeForStore(
                        value.key(),
                        value.canonicalBytes(),
                        value.primaryIdentity().shard(),
                        bytes(32, 1)));
        final byte[] key = value.key();
        key[0]++;
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decodeForStore(
                        key, value.canonicalBytes(), value.primaryIdentity().shard(), tenant));
        final byte[] copy = value.payloadSha256();
        copy[0]++;
        assertNotEquals(HexFormat.of().formatHex(copy), HexFormat.of().formatHex(value.payloadSha256()));
    }

    @Test
    void wireRejectsUnknownMissingDuplicateFieldsAndBadIntegrity() {
        final byte[] value = scheduled("best").canonicalBytes();
        final byte[] bad = value.clone();
        bad[bad.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaPayloadOwner.decode(bad));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decode(Arrays.copyOf(value, value.length - 35)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decode(Bytes.concat(value, new byte[] {(byte) 152, 1, 1})));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decode(Bytes.concat(value, new byte[] {8, 1})));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decode(new byte[TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES + 1]));
        final byte[] prefix = ByteBuffer.allocate(8)
                .putShort((short) 0x4e56)
                .put((byte) 32)
                .put((byte) 1)
                .putInt(value.length)
                .array();
        final byte[] framed = Bytes.concat(prefix, value);
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(framed, Bytes.crc32cbe(framed))));
    }

    @Test
    void maximumObjectAndSourceIdentitiesHaveIndependentSizeAndDigest() {
        final var original = scheduled("object");
        final byte[] component = bytes(TargetPayloadReference.MAX_COMPONENT_BYTES, 0x79);
        final var p = payload();
        final var maximum = new PayloadReference(
                p.objectStoreProfileHash(),
                component,
                component,
                component,
                component,
                Long.MAX_VALUE,
                p.payloadSha256(),
                p.reservationId(),
                p.proofId());
        final var source = new PulsarSourcePosition(
                original.primaryIdentity().shard(),
                bytes(32, 0x77),
                "x".repeat(TargetSourcePosition.MAX_TOPIC_UTF8_BYTES),
                -1L,
                -1L,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                Long.MAX_VALUE);
        byte[] encoded = replace(original.canonicalBytes(), 8, Long.MAX_VALUE);
        encoded = replace(encoded, 12, maximum.encode());
        encoded = replace(encoded, 14, -1L);
        encoded = replace(encoded, 15, new TargetQuotaMutation(-1L, source, bytes(32, 0x66)).canonicalBytes());
        final var value = TargetQuotaPayloadOwner.decode(encoded);
        assertEquals(Integer.parseInt(vectors.getProperty("maximum.length")), encoded.length);
        assertEquals(vectors.getProperty("maximum.sha256"), Bytes.hex(Bytes.sha256(encoded)));
        assertTrue(encoded.length <= TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES);
        assertEquals(CapacityVector.decode(raw(vectors, "maximum.recordCharge")), value.recordCharge());
        assertEquals(-1L, value.revision());
        assertEquals(Long.MAX_VALUE, value.payloadCharge().amount(CapacityDimension.PENDING_PAYLOAD_BYTES));
        assertThrows(IllegalStateException.class, () -> value.retain(stamp(2), allow));
    }

    @Test
    void integrityValidWireStillRejectsImpossibleBranchesAndRevision() {
        final byte[] inline = scheduled("best").canonicalBytes();
        for (long kind : new long[] {0, 3, 0xffff_ffffL}) {
            assertThrows(
                    IllegalArgumentException.class, () -> TargetQuotaPayloadOwner.decode(replace(inline, 7, kind)));
        }
        for (long phase : new long[] {0, 1, 4, 5, 0xffff_ffffL}) {
            assertThrows(
                    IllegalArgumentException.class, () -> TargetQuotaPayloadOwner.decode(replace(inline, 13, phase)));
        }
        for (long revision : new long[] {0, 2, -1L}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetQuotaPayloadOwner.decode(replace(inline, 14, revision)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaPayloadOwner.decode(replace(inline, 8, TargetScheduleBody.MAX_INLINE_BYTES + 1L)));
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaPayloadOwner.decode(replace(inline, 8, -1L)));
        final byte[] reserved = reserved().canonicalBytes();
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaPayloadOwner.decode(replace(reserved, 13, 2L)));
        final byte[] committed = scheduled("object").canonicalBytes();
        assertThrows(IllegalArgumentException.class, () -> TargetQuotaPayloadOwner.decode(replace(committed, 13, 1L)));
    }

    @Test
    void emptyPayloadKeepsMessageCountAndRequiresTheFullBinding() {
        byte[] encoded = replace(scheduled("best").canonicalBytes(), 8, 0L);
        encoded = replace(encoded, 9, Bytes.sha256(new byte[0]));
        final var value = TargetQuotaPayloadOwner.decode(encoded);
        assertEquals(TargetQuotaAccounting.activePayload(0), value.payloadCharge());
        assertThrows(IllegalStateException.class, () -> value.requireInitialBinding(binding("best")));
        assertTrue(value.retain(stamp(2), allow).payloadCharge().isZero());
    }

    @Test
    void samePositionBindingRequiresExactSourceMetadata() {
        final var source = (KafkaSourcePosition) stamp(1).source();
        final var changed = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                source.offset(),
                source.leaderEpoch(),
                91);
        final var mutation = new TargetQuotaMutation(1, changed, bytes(32, 0x66));
        final var decoded = TargetQuotaPayloadOwner.decode(
                replace(scheduled("best").canonicalBytes(), 15, mutation.canonicalBytes()));
        assertThrows(IllegalStateException.class, () -> decoded.requireInitialBinding(binding("best")));
    }

    @Test
    void unsignedRevisionCrossesSignedBoundaryWithoutReset() {
        byte[] encoded = replace(scheduled("best").canonicalBytes(), 14, Long.MAX_VALUE);
        encoded = replace(
                encoded,
                15,
                new TargetQuotaMutation(Long.MAX_VALUE, stamp(1).source(), bytes(32, 0x66)).canonicalBytes());
        final var value = TargetQuotaPayloadOwner.decode(encoded);
        final var next =
                value.retain(new TargetQuotaMutation(Long.MIN_VALUE, stamp(2).source(), bytes(32, 0x66)), allow);
        assertEquals(Long.MIN_VALUE, next.revision());
        assertArrayEquals(
                next.canonicalBytes(),
                TargetQuotaPayloadOwner.decode(next.canonicalBytes()).canonicalBytes());
    }

    /** Recomputes integrity so negative cases exercise semantic validation rather than only the checksum. */
    private static byte[] replace(final byte[] encoded, final int number, final Object replacement) {
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final byte[] fields = CanonicalProtobuf.message(out -> {
            while (reader.hasRemaining()) {
                final var field = reader.next();
                if (field.number() == 18) {
                    continue;
                }
                if (field.number() == number) {
                    if (replacement instanceof byte[] bytes) {
                        CanonicalProtobuf.bytes(out, number, bytes);
                    } else {
                        CanonicalProtobuf.uint64Bits(out, number, (Long) replacement);
                    }
                } else if (field.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, field.number(), QueryCodecSupport.bytes(field, field.number()));
                } else {
                    CanonicalProtobuf.uint64Bits(
                            out, field.number(), QueryCodecSupport.uint64Bits(field, field.number()));
                }
            }
        });
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(
                    out, 18, Bytes.sha256(Bytes.utf8("nereus-delay-target-quota-payload-owner\0"), fields));
        });
    }

    private TargetMessageRecord message(
            final byte[] inline, final PayloadReference object, final int generation, final byte[] incarnation) {
        final var b = binding("best");
        final var locator = new TargetMessageLocator(
                b.messageId(),
                generation,
                b.target(),
                b.domain(),
                incarnation,
                OrderingMode.BEST_EFFORT,
                null,
                b.digest());
        final var runtime = new TargetGenerationRuntimeIndex(
                generation,
                GenerationAggregateState.DEAD_LETTER,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                List.of(),
                0,
                0,
                false,
                1);
        return new TargetMessageRecord(
                locator, 1, 100, 200, 100, NativeDeliveryPolicy.FORBID, stamp(1).source(), inline, object, runtime);
    }

    private TargetQuotaPayloadOwner scheduled(final String kind) {
        return TargetQuotaPayloadOwner.scheduled(binding(kind), tenant, accounting, lineage, stamp(1));
    }

    private TargetQuotaPayloadOwner reserved() {
        return TargetQuotaPayloadOwner.reserved(
                binding("prepare"), bytes(32, 0x63), tenant, accounting, lineage, stamp(1));
    }

    private TargetScheduleBinding binding(final String kind) {
        return TargetScheduleBinding.decode(raw(bindings, "binding." + kind));
    }

    private PayloadReference payload() {
        return TargetPayloadReference.decode(raw(vectors, "committed"));
    }

    private RecoveryFloorRef floor(final int n) {
        return RecoveryFloorRef.decode(raw(vectors, "floor" + n));
    }

    private TargetQuotaMutation stamp(final long n) {
        final var b = (KafkaSourcePosition) binding("best").bindingSource();
        return new TargetQuotaMutation(
                n,
                new KafkaSourcePosition(
                        b.shardId(),
                        b.authenticatedClusterId(),
                        b.nativeTopicUuid(),
                        6 + n,
                        b.leaderEpoch(),
                        n == 1 ? 90 : 110),
                bytes(32, 0x66));
    }

    private static PayloadReference reference(
            final PayloadReference p,
            final long length,
            final byte[] hash,
            final byte[] reservation,
            final byte[] profile) {
        return new PayloadReference(
                profile,
                p.container(),
                p.objectKey(),
                p.immutableObjectVersion(),
                p.etag(),
                length,
                hash,
                reservation,
                p.proofId());
    }

    private static byte[] raw(final Properties p, final String name) {
        return HexFormat.of().parseHex(p.getProperty(name));
    }

    private static byte[] bytes(final int n, final int v) {
        final byte[] b = new byte[n];
        Arrays.fill(b, (byte) v);
        return b;
    }

    private static Properties properties(final String name) {
        try (var in = TargetQuotaPayloadOwnerTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var p = new Properties();
            p.load(in);
            return p;
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
