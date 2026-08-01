package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolCodecTest {
    @Test
    void frameZeroVectorMatchesRegistry() {
        final byte[] frame = ShardLogFrame.encode(ShardLogFrame.CLIENT_COMMAND_KIND, new byte[0]);
        assertEquals("4e444c310101000000000000519553ae", Bytes.hex(frame));
        assertArrayEquals(new byte[0], ShardLogFrame.decode(frame).canonicalEnvelope());
    }

    @Test
    void preparedCommandRoundTripsThroughCanonicalFrame() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("lane")), 1000, 5000,
                OrderingMode.BEST_EFFORT, "payload".getBytes(StandardCharsets.UTF_8));
        final PreparedCommand command = PreparedCommand.schedule(shard, intent, 10_000);

        final PreparedCommand decoded = CommandCodec.decodeFrame(CommandCodec.encodeFrame(command));

        assertEquals(command, decoded);
        assertArrayEquals(command.commandHash(), CommandHash.compute(command.type(), command.commandId(),
                command.delayMessageId(), command.retryUntilEpochMs(), command.canonicalBody()));
    }

    @Test
    void corruptIdentityAndCrcAreRejected() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final SelfRoutingId id = SelfRoutingId.random(shard);
        final byte[] corrupt = id.bytes();
        corrupt[20] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SelfRoutingId.decode(corrupt));

        final byte[] frame = ShardLogFrame.encode(ShardLogFrame.CLIENT_COMMAND_KIND, new byte[]{1});
        frame[frame.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ShardLogFrame.decode(frame));
    }

    @Test
    void scheduleBodyIsCanonicalAndDefensive() {
        final DestinationLaneId lane = new DestinationLaneId(new byte[32]);
        final byte[] payload = new byte[]{1, 2, 3};
        final ScheduleIntent intent = new ScheduleIntent(lane, 1, 2, OrderingMode.DELIVERY_TIME_FIFO, payload);
        payload[0] = 9;
        final byte[] encoded = intent.canonicalBytes();
        final ScheduleIntent decoded = CommandBodies.decodeSchedule(encoded);
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.payload());
        assertEquals(intent, decoded);
        assertThrows(IllegalArgumentException.class,
                () -> CommandBodies.decodeSchedule(Arrays.copyOf(encoded, encoded.length - 1)));
    }

    @Test
    void stableCodeRegistryIsClosedAndRoundTripsEveryValue() {
        final HashSet<Integer> values = new HashSet<>();
        for (StableCode code : StableCode.values()) {
            org.junit.jupiter.api.Assertions.assertTrue(values.add(code.wireValue()),
                    "duplicate stable code: " + code.wireValue());
            assertEquals(code, StableCode.fromWire(code.wireValue()));
        }
        assertEquals(101, values.size());
        assertThrows(IllegalArgumentException.class, () -> StableCode.fromWire(0x7fff));
    }

    @Test
    void sourceOrderTokenUsesClosedAdapterVariant() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final KafkaSourcePosition kafka = new KafkaSourcePosition(shard, "cluster", java.util.UUID.randomUUID(),
                9, null, 10);
        assertEquals("010000000000000009", Bytes.hex(kafka.sourceOrderToken()));
        final PulsarSourcePosition pulsar = new PulsarSourcePosition(shard, new byte[32], "persistent://t/topic",
                1, 2, 3, 4, PulsarSourcePosition.EntryKind.BATCH, 10);
        assertEquals(21, pulsar.sourceOrderToken().length);
        assertEquals(2, pulsar.sourceOrderToken()[0]);
    }

    @Test
    void sourcePositionsCannotCompareAcrossPhysicalResourceIncarnations() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final java.util.UUID topic = java.util.UUID.randomUUID();
        final KafkaSourcePosition kafkaA = new KafkaSourcePosition(shard, "cluster", topic, 1, null, 10);
        final KafkaSourcePosition kafkaDifferentTopic = new KafkaSourcePosition(shard, "cluster",
                java.util.UUID.randomUUID(), 2, null, 11);
        assertFalse(kafkaA.sameSourceIdentity(kafkaDifferentTopic));
        assertThrows(IllegalArgumentException.class, () -> kafkaA.compareTo(kafkaDifferentTopic));

        final byte[] resource = new byte[32];
        final PulsarSourcePosition pulsarA = new PulsarSourcePosition(shard, resource, "persistent://t/a",
                1, 1, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 10);
        final byte[] replacementResource = new byte[32];
        replacementResource[0] = 1;
        final PulsarSourcePosition pulsarReplacement = new PulsarSourcePosition(shard, replacementResource,
                "persistent://t/a", 1, 2, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 11);
        assertFalse(pulsarA.sameSourceIdentity(pulsarReplacement));
        assertThrows(IllegalArgumentException.class, () -> pulsarA.compareTo(pulsarReplacement));
    }

    @Test
    void largeScheduleAndPayloadProofAreCanonicalAndSigned() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("large-lane")), 2_000, 8_000,
                OrderingMode.BEST_EFFORT, 123_456, Bytes.sha256(Bytes.utf8("payload")), 10_000, 3);
        assertEquals(intent, CommandBodies.decodePrepareLarge(CommandBodies.prepareLarge(intent)));
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shard, intent, 20_000);
        assertEquals(prepare, CommandCodec.decodeFrame(CommandCodec.encodeFrame(prepare)));

        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final DelayMessageId messageId = prepare.delayMessageId();
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("reservation"));
        final PayloadCommitProof proof = PayloadCommitProof.signed(3, 7, shard.routeIncarnation().bytes(),
                shard.partition(), messageId, reservationId, Bytes.sha256(Bytes.utf8("profile")),
                Bytes.utf8("bucket"), Bytes.utf8("object-key"), Bytes.utf8("version-1"), new byte[0],
                intent.expectedPayloadLength(), intent.payloadSha256(), 12_000, keyPair.getPrivate());
        assertTrue(proof.verifySignature(keyPair.getPublic()));
        assertEquals(proof, PayloadCommitProof.decode(proof.canonicalBytes()));
        assertEquals(proof, CommandBodies.decodeCommitLarge(CommandBodies.commitLarge(proof)));
        final PreparedCommand commit = PreparedCommand.commitLarge(shard, messageId, proof, 20_000);
        assertEquals(commit, CommandCodec.decodeFrame(CommandCodec.encodeFrame(commit)));
        final byte[] tampered = proof.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertFalse(PayloadCommitProof.decode(tampered).verifySignature(keyPair.getPublic()));
    }
}
