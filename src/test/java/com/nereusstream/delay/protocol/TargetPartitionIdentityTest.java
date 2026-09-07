package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetPartitionIdentityTest {
    private static final UUID TOPIC = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

    @Test
    void kafkaAndPulsarMatchIndependentCanonicalAndHashVectors() throws IOException {
        final Properties vectors = vectors();
        for (String kind : new String[] {"kafka", "pulsar"}) {
            final CanonicalTargetPartition target = kind.equals("kafka")
                    ? new CanonicalTargetPartition(kafka("cluster-a", TOPIC), 3)
                    : new CanonicalTargetPartition(pulsar(token(), 1_700_000_000_000L), 5);
            assertEquals(vectors.getProperty(kind + ".canonical"), Bytes.hex(target.canonicalBytes()));
            assertEquals(vectors.getProperty(kind + ".id"), target.id().toString());
            assertEquals(target, CanonicalTargetPartition.decode(target.canonicalBytes()));
            assertNotEquals(new DestinationLaneId(target.id().bytes()), target.id());
        }
    }

    @Test
    void profileTenantOrderingKeyAndBucketDoNotSplitThePhysicalTarget() {
        final byte[] first = laneTuple(false, false, 1, "profile-a");
        final byte[] second = laneTuple(false, false, 2, "profile-b");
        final byte[] ordered = laneTuple(false, true, 3, "profile-c");
        final CanonicalTargetPartition expected = new CanonicalTargetPartition(kafka("cluster-a", TOPIC), 3);
        assertNotEquals(DestinationLaneId.derive(first), DestinationLaneId.derive(second));
        assertNotEquals(DestinationLaneId.derive(second), DestinationLaneId.derive(ordered));
        for (byte[] tuple : new byte[][] {first, second, ordered}) {
            assertEquals(expected, CanonicalTargetPartition.fromLaneTuple(tuple));
            assertEquals(
                    expected.id(), CanonicalTargetPartition.fromLaneTuple(tuple).id());
        }
    }

    @Test
    void clusterIncarnationPhysicalTopicCreationAndPartitionRemainDistinct() {
        final CanonicalTargetPartition base = new CanonicalTargetPartition(kafka("cluster-a", TOPIC), 3);
        assertNotEquals(base.id(), new CanonicalTargetPartition(kafka("cluster-b", TOPIC), 3).id());
        assertNotEquals(base.id(), new CanonicalTargetPartition(kafka("cluster-a", new UUID(1, 2)), 3).id());
        assertNotEquals(base.id(), new CanonicalTargetPartition(base.resource(), 4).id());
        final byte[] changed = token();
        changed[0] ^= 1;
        final CanonicalTargetPartition p = new CanonicalTargetPartition(pulsar(token(), 17), 5);
        assertNotEquals(p.id(), new CanonicalTargetPartition(pulsar(changed, 17), 5).id());
        assertNotEquals(p.id(), new CanonicalTargetPartition(pulsar(token(), 18), 5).id());
        assertNotEquals(
                p.id(),
                new CanonicalTargetPartition(
                                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                                        "cluster-p", token(), "another-physical-topic", 17)),
                                5)
                        .id());
    }

    @Test
    void mismatchedKafkaUuidAndPinnedPulsarTokenCannotBeMerged() {
        final byte[] kafkaLane = laneTuple(false, false, 1, "profile-a");
        final int repeatedUuid = 32 + 1 + 4 + Bytes.utf8("cluster-a").length + 1 + 16 + 4;
        kafkaLane[repeatedUuid] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CanonicalTargetPartition.fromLaneTuple(kafkaLane));
        final CanonicalTargetPartition p = CanonicalTargetPartition.fromLaneTuple(laneTuple(true, false, 1, "p"));
        p.requireResourceProjection(pulsar(token(), 17), 5);
        final byte[] changed = token();
        changed[0] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> p.requireResourceProjection(pulsar(changed, 17), 5));
        assertThrows(IllegalArgumentException.class, () -> p.requireResourceProjection(pulsar(token(), 17), 6));
    }

    @Test
    void decoderRejectsUnknownVersionsBranchesMalformedLengthsAndNoncanonicalText() {
        final byte[] good = new CanonicalTargetPartition(kafka("cluster-a", TOPIC), 3).canonicalBytes();
        final byte[] version = good.clone();
        version[0] = 2;
        assertThrows(IllegalArgumentException.class, () -> CanonicalTargetPartition.decode(version));
        assertThrows(IllegalArgumentException.class, () -> CanonicalTargetPartition.decode(Arrays.copyOf(good, 5)));
        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalTargetPartition.decode(Bytes.concat(good, new byte[] {0})));
        final byte[] length = good.clone();
        ByteBuffer.wrap(length).putInt(1, -1);
        assertThrows(IllegalArgumentException.class, () -> CanonicalTargetPartition.decode(length));
        final byte[] unknown = CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 3, new byte[] {1}));
        assertThrows(IllegalArgumentException.class, () -> CanonicalTargetPartition.decode(tuple(unknown)));
        for (byte[] cluster : new byte[][] {Bytes.utf8("e\u0301"), {(byte) 0xc3, 0x28}, {0}, {32}}) {
            final byte[] invalidResource = CanonicalProtobuf.message(
                    out -> CanonicalProtobuf.bytes(out, 1, CanonicalProtobuf.message(fields -> {
                        CanonicalProtobuf.bytes(fields, 1, cluster);
                        CanonicalProtobuf.bytes(fields, 2, uuidBytes(TOPIC));
                    })));
            assertThrows(IllegalArgumentException.class, () -> CanonicalTargetPartition.decode(tuple(invalidResource)));
        }
    }

    @Test
    void byteBoundsIncludeMultibyteTextAndRejectUnassignedResources() {
        final String cluster = "é".repeat(128);
        final String topic = "x".repeat(CanonicalTargetPartition.MAX_PHYSICAL_TOPIC_BYTES);
        final CanonicalTargetPartition largest = new CanonicalTargetPartition(
                BrokerResourceIdentity.pulsar(
                        new PulsarBrokerResourceIdentity(cluster, token(), topic, Long.MAX_VALUE)),
                0xffff_ffffL);
        assertTrue(largest.canonicalBytes().length <= CanonicalTargetPartition.MAX_CANONICAL_BYTES);
        assertEquals(largest, CanonicalTargetPartition.decode(largest.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class, () -> new CanonicalTargetPartition(kafka("é".repeat(129), TOPIC), 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CanonicalTargetPartition(
                        BrokerResourceIdentity.pulsar(
                                new PulsarBrokerResourceIdentity(cluster, token(), topic + "x", 1)),
                        0));
        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalTargetPartition.decode(new byte[CanonicalTargetPartition.MAX_CANONICAL_BYTES + 1]));
        assertThrows(IllegalArgumentException.class, () -> new CanonicalTargetPartition(kafka("a", new UUID(0, 0)), 0));
        assertThrows(IllegalArgumentException.class, () -> new CanonicalTargetPartition(pulsar(new byte[32], 1), 0));
        assertThrows(IllegalArgumentException.class, () -> new CanonicalTargetPartition(pulsar(token(), -1), 0));
        assertThrows(IllegalArgumentException.class, () -> new CanonicalTargetPartition(kafka("a", TOPIC), -1));
        assertThrows(
                IllegalArgumentException.class, () -> new CanonicalTargetPartition(kafka("a", TOPIC), 0x1_0000_0000L));
    }

    @Test
    void retainedIdentityBytesCannotBeMutatedThroughCallerArrays() {
        final byte[] token = token();
        final CanonicalTargetPartition target = new CanonicalTargetPartition(pulsar(token, 1), 0);
        final byte[] before = target.canonicalBytes();
        token[0] ^= 1;
        target.id().bytes()[0] ^= 1;
        target.resource().pulsar().resourceIncarnation()[0] ^= 1;
        assertArrayEquals(before, target.canonicalBytes());
    }

    private static byte[] laneTuple(final boolean pulsar, final boolean ordered, final int bucket, final String id) {
        final byte[] physical = pulsar
                ? Bytes.concat(
                        token(), Bytes.u64be(17), Bytes.lp32(Bytes.utf8("persistent://tenant/ns/topic-partition-5")))
                : Bytes.concat(uuidBytes(TOPIC), Bytes.lp32(uuidBytes(TOPIC)));
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8(id)),
                Bytes.u8(pulsar ? 2 : 1),
                Bytes.lp32(Bytes.utf8(pulsar ? "cluster-p" : "cluster-a")),
                Bytes.u8(pulsar ? 2 : 1),
                physical,
                Bytes.u32be(pulsar ? 5 : 3),
                Bytes.lp32(Bytes.utf8(id)),
                Bytes.u64be(bucket),
                Bytes.sha256(Bytes.utf8(id + "-dest")),
                Bytes.lp32(Bytes.utf8(id + "-cap")),
                Bytes.u64be(bucket),
                Bytes.sha256(Bytes.utf8(id + "-cap")),
                Bytes.u8(ordered ? 1 : 2),
                ordered ? Bytes.sha256(Bytes.utf8(id)) : Bytes.u32be(bucket));
    }

    private static byte[] tuple(final byte[] resource) {
        return Bytes.concat(new byte[] {1}, Bytes.lp32(resource), Bytes.u32be(3));
    }

    private static BrokerResourceIdentity kafka(final String cluster, final UUID uuid) {
        return BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(cluster, uuid));
    }

    private static BrokerResourceIdentity pulsar(final byte[] token, final long timestamp) {
        return BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "cluster-p", token, "persistent://tenant/ns/topic-partition-5", timestamp));
    }

    private static byte[] uuidBytes(final UUID uuid) {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static byte[] token() {
        return HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    }

    private static Properties vectors() throws IOException {
        final Properties properties = new Properties();
        try (var input =
                TargetPartitionIdentityTest.class.getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            properties.load(input);
        }
        return properties;
    }
}
