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

    @Test
    void systemMutationEnvelopeIsCanonicalHashedAndSigned() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] logicalIdentity = Bytes.sha256(Bytes.utf8("publish-attempt"));
        final byte[] author = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease-fence"))).canonicalBytes();
        final byte[] body = systemBody(shard, SystemMutationType.PUBLISH_ADMISSION, 25_000);
        final SystemMutation mutation = SystemMutation.signed(shard, SystemMutationType.PUBLISH_ADMISSION, 25_000,
                logicalIdentity, body, author, 3, keyPair.getPrivate());

        final SystemMutation decoded = SystemMutation.decodeFrame(mutation.encodeFrame(), logicalIdentity);

        assertEquals(mutation, decoded);
        assertTrue(decoded.verifySignature(keyPair.getPublic()));
        assertArrayEquals(mutation.mutationHash(), SystemMutation.computeMutationHash(shard, mutation.type(),
                mutation.retryUntilEpochMs(), mutation.canonicalBody()));
    }

    @Test
    void systemMutationRejectsWrongIdentityAndTampering() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] logicalIdentity = Bytes.sha256(Bytes.utf8("control-target"));
        final byte[] author = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                Bytes.sha256(Bytes.utf8("roles")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes();
        final byte[] body = systemBody(shard, SystemMutationType.APPLY_SHARD_CONTROL, 30_000);
        final SystemMutation mutation = SystemMutation.signed(shard, SystemMutationType.APPLY_SHARD_CONTROL, 30_000,
                logicalIdentity, body, author, 1, keyPair.getPrivate());

        assertThrows(IllegalArgumentException.class,
                () -> SystemMutation.decodeFrame(mutation.encodeFrame(), Bytes.sha256(Bytes.utf8("other"))));
        final byte[] tampered = mutation.encodeFrame();
        tampered[tampered.length - 5] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SystemMutation.decodeFrame(tampered, logicalIdentity));
    }

    @Test
    void authorIdentityBranchMustMatchSystemMutationType() {
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        assertEquals(AuthorIdentity.Kind.OWNER, AuthorIdentity.decode(owner).kind());
        assertThrows(IllegalArgumentException.class,
                () -> AuthorIdentity.decode(owner).requireFor(SystemMutationType.APPLY_SHARD_CONTROL));
    }

    @Test
    void systemMutationBodyPrefixCannotDriftFromOuterEnvelope() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] author = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] mismatchedType = systemBody(shard, SystemMutationType.PUBLISH_OUTCOME, 1_000);
        assertThrows(IllegalArgumentException.class, () -> SystemMutation.signed(shard,
                SystemMutationType.PUBLISH_ADMISSION, 1_000, Bytes.sha256(Bytes.utf8("logical")), mismatchedType,
                author, 1, keyPair.getPrivate()));
    }

    @Test
    void trustedUtcEvidenceRoundTripsAndEnforcesSignedSourceShape() {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(1_000, 1_005,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("host-a"), 3, 7, 9,
                Bytes.sha256(Bytes.utf8("sample")), 0, null);
        evidence.requireEarliestAtLeast(1_000);
        evidence.requireWidthAtMost(5);
        assertArrayEquals(evidence.canonicalBytes(), TrustedUtcIntervalEvidence.decode(evidence.canonicalBytes())
                .canonicalBytes());
        assertThrows(IllegalArgumentException.class, () -> new TrustedUtcIntervalEvidence(1_000, 1_005,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("host-a"), 3, 7, 9,
                Bytes.sha256(Bytes.utf8("sample")), 2, new byte[64]));
        assertThrows(IllegalArgumentException.class, () -> evidence.requireEarliestAtLeast(1_001));
    }

    private static byte[] systemBody(final ShardId shard, final SystemMutationType type, final long retryUntil) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, type.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntil);
            switch (type) {
                case APPLY_SHARD_CONTROL -> {
                    CanonicalProtobuf.bytes(output, 10, nestedPlaceholder());
                    CanonicalProtobuf.uint32(output, 11, 1);
                    CanonicalProtobuf.uint32(output, 12, 1);
                    CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("control")));
                    CanonicalProtobuf.bytes(output, 15, nestedPlaceholder());
                }
                case PUBLISH_ADMISSION -> {
                    CanonicalProtobuf.bytes(output, 10, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 11, new byte[16]);
                    CanonicalProtobuf.bytes(output, 12, Bytes.sha256(Bytes.utf8("claim")));
                    CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("lane")));
                    CanonicalProtobuf.bytes(output, 14, new byte[16]);
                    CanonicalProtobuf.bytes(output, 15, DelayMessageId.random(shard).bytes());
                    CanonicalProtobuf.uint32(output, 16, 0);
                    CanonicalProtobuf.bytes(output, 17, Bytes.sha256(Bytes.utf8("attempt")));
                    CanonicalProtobuf.bytes(output, 18, Bytes.sha256(Bytes.utf8("prepared")));
                    CanonicalProtobuf.bytes(output, 19, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 20, Bytes.sha256(Bytes.utf8("ready")));
                    CanonicalProtobuf.bytes(output, 21, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 22, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 23, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 24, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 25, nestedPlaceholder());
                }
                case PUBLISH_OUTCOME -> {
                    CanonicalProtobuf.bytes(output, 10, Bytes.sha256(Bytes.utf8("attempt")));
                    CanonicalProtobuf.uint32(output, 11, 3);
                    CanonicalProtobuf.uint32(output, 12, 4);
                    CanonicalProtobuf.uint32(output, 13, StableCode.DESTINATION_OUTCOME_UNKNOWN.wireValue());
                    CanonicalProtobuf.bytes(output, 15, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 16, nestedPlaceholder());
                    CanonicalProtobuf.bytes(output, 17, nestedPlaceholder());
                }
                default -> CanonicalProtobuf.bytes(output, 10, nestedPlaceholder());
            }
        });
    }

    private static byte[] nestedPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, new byte[]{1}));
    }
}
