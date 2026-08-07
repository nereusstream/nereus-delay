package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

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
    void receiptFrameZeroVectorMatchesRegistry() {
        final byte[] frame = ReceiptFrame.encode(ReceiptKind.COMMAND_QUEUED, new byte[0]);
        assertEquals("4e44523101010000000000002ad79a80", Bytes.hex(frame));
        final ReceiptFrame.Decoded decoded = ReceiptFrame.decode(frame);
        assertEquals(ReceiptKind.COMMAND_QUEUED, decoded.kind());
        assertArrayEquals(new byte[0], decoded.payload());
        final ReceiptFrame.Decoded textDecoded = ReceiptFrame.decodeText(ReceiptFrame.encodeText(
                ReceiptKind.COMMAND_QUEUED, new byte[0]));
        assertEquals(ReceiptKind.COMMAND_QUEUED, textDecoded.kind());
        assertArrayEquals(new byte[0], textDecoded.payload());
    }

    @Test
    void receiptFrameRejectsFlagsLengthKindAndCrcDrift() {
        final byte[] frame = ReceiptFrame.encode(ReceiptKind.COMMAND_APPLIED, new byte[]{1, 2, 3});
        final byte[] flags = frame.clone();
        flags[6] = 1;
        assertThrows(IllegalArgumentException.class, () -> ReceiptFrame.decode(flags));
        final byte[] length = frame.clone();
        length[11] = 4;
        assertThrows(IllegalArgumentException.class, () -> ReceiptFrame.decode(length));
        final byte[] kind = frame.clone();
        kind[5] = (byte) 99;
        assertThrows(IllegalArgumentException.class, () -> ReceiptFrame.decode(kind));
        final byte[] crc = frame.clone();
        crc[crc.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ReceiptFrame.decode(crc));
        assertThrows(IllegalArgumentException.class, () -> ReceiptFrame.decodeText("ndr1_!"));
    }

    @Test
    void commandQueuedReceiptKafkaPayloadBindsPreparedCommandSourceAndAck() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        final PreparedCommand command = PreparedCommand.schedule(shard,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("receipt-lane")), 2_000, 8_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("receipt")), 9_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-a", topic, 7, 3, 1_234);
        final CommandQueuedReceiptV1.KafkaQueuedAck ack = new CommandQueuedReceiptV1.KafkaQueuedAck(
                "cluster-a", topic, 8, 7, 3, 1_234, Bytes.sha256(Bytes.utf8("broker-response")));
        final byte[] attempt = new byte[16];
        attempt[15] = 1;

        final CommandQueuedReceiptV1 receipt = CommandQueuedReceiptV1.create(command, source, ack, 9_000, attempt);
        final CommandQueuedReceiptV1 decoded = CommandQueuedReceiptV1.decodeFrame(receipt.frame());

        assertEquals(receipt, decoded);
        assertEquals(command.commandId(), decoded.command().commandId());
        assertEquals(command.delayMessageId(), decoded.command().delayMessageId());
        assertEquals(source, decoded.sourcePosition());
        assertEquals(ack, decoded.brokerAck());
        assertArrayEquals(receipt.receiptPayloadDigest(), decoded.receiptPayloadDigest());
        assertEquals(ReceiptKind.COMMAND_QUEUED, ReceiptFrame.decode(receipt.frame()).kind());

        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CommandQueuedReceiptV1.decodePayload(tampered));
        final CommandQueuedReceiptV1.KafkaQueuedAck wrongAck = new CommandQueuedReceiptV1.KafkaQueuedAck(
                "cluster-a", topic, 8, 8, 3, 1_235, ack.responseSha256());
        assertThrows(IllegalArgumentException.class,
                () -> CommandQueuedReceiptV1.create(command, source, wrongAck, 9_000, attempt));
    }

    @Test
    void commandQueuedReceiptPulsarPayloadUsesTheClosedAckBranch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final byte[] resource = new byte[32];
        resource[0] = 7;
        final PulsarSourcePosition source = new PulsarSourcePosition(shard, resource, "persistent://tenant/topic",
                4, 5, 1, 3, PulsarSourcePosition.EntryKind.BATCH, 2_345);
        final PreparedCommand command = PreparedCommand.cancel(shard, DelayMessageId.random(shard), 0, 9_000);
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
                "pulsar-cluster", resource, "persistent://tenant/topic", 1_111, 9, 4, 5, 1, 3, 2_345,
                Bytes.sha256(Bytes.utf8("send-receipt")));
        final byte[] attempt = new byte[16];
        attempt[0] = 1;

        final CommandQueuedReceiptV1 decoded = CommandQueuedReceiptV1.decodeFrame(
                CommandQueuedReceiptV1.create(command, source, ack, 3_000, attempt).frame());
        assertEquals(source, decoded.sourcePosition());
        assertEquals(ack, decoded.brokerAck());
        assertEquals(CommandType.CANCEL, decoded.command().commandType());
    }

    @Test
    void queryErrorResponsesKeepClosedResultTagsAndRetryPresence() {
        final CommandQueryResponseV1 command = CommandQueryResponseV1.error(StableCode.SHARD_TRANSITIONING,
                7_000L);
        assertEquals(command, CommandQueryResponseV1.decode(command.canonicalBytes()));
        final MessageQueryResponseV1 message = MessageQueryResponseV1.error(StableCode.INVALID_RECEIPT, null);
        assertEquals(message, MessageQueryResponseV1.decode(message.canonicalBytes()));
        assertThrows(IllegalArgumentException.class,
                () -> new PublicQueryErrorV1(StableCode.SHARD_UNAVAILABLE, 7_000L));
        assertThrows(IllegalArgumentException.class,
                () -> PublicQueryErrorV1.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint32(output, 1, StableCode.SHARD_TRANSITIONING.wireValue());
                })));
        assertThrows(IllegalArgumentException.class,
                () -> CommandQueryResponseV1.error(StableCode.OK, null));
    }

    @Test
    void fullQueryViewsRoundTripAndKeepUnionBranchesClosed() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition current = new KafkaSourcePosition(shard, "cluster-query", topic, 10, 2, 1_000);
        final KafkaSourcePosition awaited = new KafkaSourcePosition(shard, "cluster-query", topic, 11, 2, 1_001);
        final PublicDestinationBindingViewV1 binding = publicBinding();

        final PendingCommandViewV1 pendingView = new PendingCommandViewV1(awaited, current, 2_000);
        assertEquals(CommandQueryResponseV1.pending(pendingView),
                CommandQueryResponseV1.decode(CommandQueryResponseV1.pending(pendingView).canonicalBytes()));

        final PublicCommandResultV1 appliedView = new PublicCommandResultV1(CommandApplyStatusV1.APPLIED,
                StableCode.OK, awaited, 0, 1L, binding, 3_000);
        final PublicCommandResultV1 rejectedView = new PublicCommandResultV1(CommandApplyStatusV1.REJECTED,
                StableCode.INVALID_COMMAND, awaited, null, null, null, 3_000);
        assertEquals(CommandQueryResponseV1.applied(appliedView),
                CommandQueryResponseV1.decode(CommandQueryResponseV1.applied(appliedView).canonicalBytes()));
        assertEquals(CommandQueryResponseV1.rejected(rejectedView),
                CommandQueryResponseV1.decode(CommandQueryResponseV1.rejected(rejectedView).canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> new PublicCommandResultV1(
                CommandApplyStatusV1.REJECTED, StableCode.INVALID_COMMAND, awaited, 0, null, null, 3_000));
        assertThrows(IllegalArgumentException.class, () -> new PublicCommandResultV1(
                CommandApplyStatusV1.APPLIED, StableCode.OK, awaited, null, 1L, null, 3_000));

        final CompactCommandResultV1 compact = new CompactCommandResultV1(CommandApplyStatusV1.REJECTED,
                StableCode.INVALID_COMMAND, awaited, 3_000);
        assertEquals(CommandQueryResponseV1.resultExpired(compact),
                CommandQueryResponseV1.decode(CommandQueryResponseV1.resultExpired(compact).canonicalBytes()));
        assertEquals(CommandQueryResponseV1.resultEvidenceExpired(),
                CommandQueryResponseV1.decode(CommandQueryResponseV1.resultEvidenceExpired().canonicalBytes()));

        final ReservedMessageViewV1 reserved = new ReservedMessageViewV1(new byte[32], 1,
                PayloadReservationStateV1.PAYLOAD_RESERVED, 4_000, binding);
        final ActiveMessageViewV1 active = new ActiveMessageViewV1(0, 2, MessageGenerationStateV1.UNCERTAIN,
                1_000, 5_000, binding, PayloadAvailabilityV1.INLINE_RETAINED, true);
        final PublicEvidenceRefV1 evidence = new PublicEvidenceRefV1(PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                Bytes.sha256(Bytes.utf8("evidence")), EvidenceVerificationStatusV1.VERIFIED_PUBLISHED);
        final TerminalMessageViewV1 terminal = new TerminalMessageViewV1(0, 3,
                MessageGenerationStateV1.PUBLISHED, StableCode.OK, binding, PayloadAvailabilityV1.INLINE_RETAINED,
                DlqExportStateV1.NOT_CONFIGURED, false, evidence);
        assertEquals(MessageQueryResponseV1.reserved(reserved),
                MessageQueryResponseV1.decode(MessageQueryResponseV1.reserved(reserved).canonicalBytes()));
        assertEquals(MessageQueryResponseV1.active(active),
                MessageQueryResponseV1.decode(MessageQueryResponseV1.active(active).canonicalBytes()));
        assertEquals(MessageQueryResponseV1.terminal(terminal),
                MessageQueryResponseV1.decode(MessageQueryResponseV1.terminal(terminal).canonicalBytes()));
        assertEquals(MessageQueryResponseV1.identityRetired(),
                MessageQueryResponseV1.decode(MessageQueryResponseV1.identityRetired().canonicalBytes()));
        assertEquals(MessageQueryResponseV1.unknown(FirstScheduleEligibilityV1.NOT_PROVEN),
                MessageQueryResponseV1.decode(
                        MessageQueryResponseV1.unknown(FirstScheduleEligibilityV1.NOT_PROVEN).canonicalBytes()));

        assertThrows(IllegalArgumentException.class,
                () -> new CommandQueryResponseV1(CommandQueryResult.APPLIED, rejectedView));
        assertThrows(IllegalArgumentException.class,
                () -> new ActiveMessageViewV1(0, 2, MessageGenerationStateV1.PUBLISHED, 1_000, 5_000, binding,
                        PayloadAvailabilityV1.INLINE_RETAINED, false));
    }

    @Test
    void commandAppliedReceiptBindsQueuedDigestAndAppliedSourcePosition() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final UUID topic = UUID.randomUUID();
        final PreparedCommand command = PreparedCommand.schedule(shard,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("applied-lane")), 1_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 8_000);
        final KafkaSourcePosition queuedPosition = new KafkaSourcePosition(shard, "cluster-applied", topic, 10, 1,
                1_000);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(shard, "cluster-applied", topic, 11, 1,
                1_001);
        final byte[] attempt = new byte[16];
        attempt[0] = 1;
        final CommandQueuedReceiptV1 queued = CommandQueuedReceiptV1.create(command, queuedPosition,
                new CommandQueuedReceiptV1.KafkaQueuedAck("cluster-applied", topic, shard.partition(), 10, 1, 1_000,
                        Bytes.sha256(Bytes.utf8("ack"))), 2_000, attempt);
        final CommandAppliedReceiptV1 applied = CommandAppliedReceiptV1.create(queued,
                CommandApplyStatusV1.APPLIED, StableCode.OK, appliedPosition, 0, 1L, publicBinding(), 3_000);

        assertEquals(applied, CommandAppliedReceiptV1.decodeFrame(applied.frame()));
        assertEquals(ReceiptKind.COMMAND_APPLIED, ReceiptFrame.decode(applied.frame()).kind());

        final CommandAppliedReceiptV1 rejected = CommandAppliedReceiptV1.create(queued,
                CommandApplyStatusV1.REJECTED, StableCode.INVALID_COMMAND, appliedPosition, null, null, null, 3_000);
        assertEquals(rejected, CommandAppliedReceiptV1.decodePayload(rejected.payload()));

        final byte[] tampered = applied.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CommandAppliedReceiptV1.decodePayload(tampered));
        final KafkaSourcePosition beforeQueued = new KafkaSourcePosition(shard, "cluster-applied", topic, 9, 1,
                999);
        assertThrows(IllegalArgumentException.class, () -> CommandAppliedReceiptV1.create(queued,
                CommandApplyStatusV1.APPLIED, StableCode.OK, beforeQueued, 0, 1L, publicBinding(), 3_000));
        final KafkaSourcePosition conflictingSameOffset = new KafkaSourcePosition(shard, "cluster-applied", topic,
                10, 2, 1_002);
        assertThrows(IllegalArgumentException.class, () -> CommandAppliedReceiptV1.create(queued,
                CommandApplyStatusV1.APPLIED, StableCode.OK, conflictingSameOffset, 0, 1L, publicBinding(), 3_000));
        assertThrows(IllegalArgumentException.class, () -> CommandAppliedReceiptV1.create(queued,
                CommandApplyStatusV1.REJECTED, StableCode.INVALID_COMMAND, appliedPosition, 0, 1L, publicBinding(),
                3_000));
        assertThrows(IllegalArgumentException.class, () -> CommandAppliedReceiptV1.create(queued,
                CommandApplyStatusV1.APPLIED, StableCode.OK, appliedPosition, null, 1L, null, 3_000));
    }

    @Test
    void payloadReservationReceiptKeepsObjectIdentityAndTrustSetPinned() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-payload", UUID.randomUUID(), 12, 1,
                1_200);
        final ProfileRefV1 objectStore = new ProfileRefV1(Bytes.utf8("object-store"), 3,
                Bytes.sha256(Bytes.utf8("object-store-semantic")), ProfileKindV1.OBJECT_STORE);
        final PayloadProofTrustSetRefV1 trustSet = new PayloadProofTrustSetRefV1(4,
                Bytes.sha256(Bytes.utf8("trust-set")));
        final PayloadReservationReceiptV1 receipt = PayloadReservationReceiptV1.create(Bytes.sha256(
                Bytes.utf8("reservation")), messageId, shard, source, 2, objectStore, Bytes.utf8("container-a"),
                Bytes.utf8("service-owned/key"), 123, Bytes.sha256(Bytes.utf8("payload")), 9_000, trustSet);

        assertEquals(receipt, PayloadReservationReceiptV1.decodeFrame(receipt.frame()));
        assertEquals(ReceiptKind.PAYLOAD_RESERVATION, ReceiptFrame.decode(receipt.frame()).kind());
        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PayloadReservationReceiptV1.decodePayload(tampered));

        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination"), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic")), ProfileKindV1.DESTINATION);
        assertThrows(IllegalArgumentException.class, () -> PayloadReservationReceiptV1.create(new byte[32], messageId,
                shard, source, 2, destination, Bytes.utf8("container-a"), Bytes.utf8("key"), 1,
                Bytes.sha256(Bytes.utf8("payload")), 9_000, trustSet));
        assertThrows(IllegalArgumentException.class, () -> PayloadReservationReceiptV1.create(new byte[32], messageId,
                new ShardId(RouteIncarnation.random(), 6), source, 2, objectStore, Bytes.utf8("container-a"),
                Bytes.utf8("key"), 1, Bytes.sha256(Bytes.utf8("payload")), 9_000, trustSet));
    }

    @Test
    void controlOperationReceiptPinsRegisteredEvidenceAndQueryBoundary() {
        final TrustedUtcIntervalEvidence registeredAt = new TrustedUtcIntervalEvidence(1_000, 1_005,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("control-clock"), 2, 7, 9,
                Bytes.sha256(Bytes.utf8("control-sample")), 0, null);
        final ControlOperationReceiptV1 receipt = ControlOperationReceiptV1.create(
                Bytes.sha256(Bytes.utf8("operation")), Bytes.sha256(Bytes.utf8("request")),
                Bytes.sha256(Bytes.utf8("scope")), Bytes.sha256(Bytes.utf8("targets")), 3, registeredAt, 5_000);

        assertEquals(receipt, ControlOperationReceiptV1.decodeFrame(receipt.frame()));
        assertEquals(ReceiptKind.CONTROL_OPERATION, ReceiptFrame.decode(receipt.frame()).kind());
        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ControlOperationReceiptV1.decodePayload(tampered));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationReceiptV1.create(new byte[32],
                Bytes.sha256(Bytes.utf8("request")), Bytes.sha256(Bytes.utf8("scope")),
                Bytes.sha256(Bytes.utf8("targets")), 3, registeredAt, 5_000));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationReceiptV1.create(
                Bytes.sha256(Bytes.utf8("operation")), Bytes.sha256(Bytes.utf8("request")),
                Bytes.sha256(Bytes.utf8("scope")), Bytes.sha256(Bytes.utf8("targets")), 3, registeredAt, 1_004));
    }

    @Test
    void nativeDeliveryReceiptPinsPulsarTargetAndPhysicalAttempt() {
        final byte[] resource = new byte[32];
        resource[0] = 9;
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "pulsar-native", resource, "persistent://tenant/native", 1_234);
        assertEquals(target, PulsarBrokerResourceIdentityV1.decode(target.canonicalBytes()));
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("native-destination"), 2,
                Bytes.sha256(Bytes.utf8("native-destination-semantic")), ProfileKindV1.DESTINATION);
        final NativePreparedRefV1 prepared = new NativePreparedRefV1(
                nonZero(32, 1), Bytes.sha256(Bytes.utf8("submission")), destination, target, 2,
                Bytes.sha256(Bytes.utf8("capability-snapshot")), 5_000,
                Bytes.sha256(Bytes.utf8("prepared-bytes")));
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
                "pulsar-native", resource, "persistent://tenant/native", 1_234, 2, 4, 5, 0, 1, 1_250,
                Bytes.sha256(Bytes.utf8("send-receipt")));
        final byte[] attempt = nonZero(16, 2);

        final NativeDeliveryReceiptV1 receipt = NativeDeliveryReceiptV1.create(prepared, ack, attempt);
        assertEquals(receipt, NativeDeliveryReceiptV1.decodeFrame(receipt.frame()));
        assertEquals(ReceiptKind.NATIVE_DELIVERY, ReceiptFrame.decode(receipt.frame()).kind());

        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> NativeDeliveryReceiptV1.decodePayload(tampered));
        assertThrows(IllegalArgumentException.class, () -> NativeDeliveryReceiptV1.create(prepared,
                new CommandQueuedReceiptV1.PulsarQueuedAck("pulsar-native", resource,
                        "persistent://tenant/native", 1_235, 2, 4, 5, 0, 1, 1_250,
                        Bytes.sha256(Bytes.utf8("send-receipt"))), attempt));
        assertThrows(IllegalArgumentException.class, () -> new NativePreparedRefV1(new byte[32],
                Bytes.sha256(Bytes.utf8("submission")), destination, target, 2,
                Bytes.sha256(Bytes.utf8("capability-snapshot")), 5_000,
                Bytes.sha256(Bytes.utf8("prepared-bytes"))));
    }

    @Test
    void nativeCapabilitySnapshotRoundTripsAndVerifiesIssuerSignature() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resource = nonZero(32, 3);
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "pulsar-snapshot", resource, "persistent://tenant/snapshot", 2_000);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("snapshot-destination"), 4,
                Bytes.sha256(Bytes.utf8("snapshot-destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("snapshot-capability"), 5,
                Bytes.sha256(Bytes.utf8("snapshot-capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_100, 2_110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("snapshot-clock"), 7, 8, 9,
                Bytes.sha256(Bytes.utf8("snapshot-sample")), 0, null);

        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destination, capability, target,
                6, Bytes.sha256(Bytes.utf8("guard-attestation")), 11, 12,
                Bytes.sha256(Bytes.utf8("binding")), Bytes.sha256(Bytes.utf8("credential-fingerprint")),
                Bytes.sha256(Bytes.utf8("principal-scope")), issuedAt, 3_000, 13, keyPair.getPrivate());
        final NativeCapabilitySnapshotV1 decoded = NativeCapabilitySnapshotV1.decode(snapshot.canonicalBytes());

        assertEquals(snapshot, decoded);
        assertTrue(decoded.verifySignature(keyPair.getPublic()));
        assertFalse(decoded.verifySignature(keyPairGenerator.generateKeyPair().getPublic()));

        final byte[] tamperedSignature = snapshot.canonicalBytes();
        tamperedSignature[tamperedSignature.length - 1] ^= 1;
        assertFalse(NativeCapabilitySnapshotV1.decode(tamperedSignature).verifySignature(keyPair.getPublic()));
        assertThrows(IllegalArgumentException.class, () -> NativeCapabilitySnapshotV1.create(destination, capability,
                target, 6, Bytes.sha256(Bytes.utf8("guard-attestation")), 11, 12,
                Bytes.sha256(Bytes.utf8("binding")), Bytes.sha256(Bytes.utf8("credential-fingerprint")),
                Bytes.sha256(Bytes.utf8("principal-scope")), issuedAt, 2_110, 13, keyPair.getPrivate()));
    }

    @Test
    void pulsarMetadataKeepsOptionalKeysAndSortedUniqueProperties() {
        final PulsarMetadataV1 metadata = new PulsarMetadataV1(Bytes.utf8("partition-key"),
                PulsarMetadataV1.KeyEncoding.UTF8, Bytes.utf8("ordering-key"),
                java.util.List.of(new PulsarMetadataV1.Property("a", "one"),
                        new PulsarMetadataV1.Property("z", "two")));
        assertEquals(metadata, PulsarMetadataV1.decode(metadata.canonicalBytes()));
        assertEquals(new PulsarMetadataV1(null, null, null, java.util.List.of()),
                PulsarMetadataV1.decode(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new PulsarMetadataV1(Bytes.utf8("key"), null, null,
                java.util.List.of()));
        assertThrows(IllegalArgumentException.class, () -> new PulsarMetadataV1(null, null, null,
                java.util.List.of(new PulsarMetadataV1.Property("z", "two"),
                        new PulsarMetadataV1.Property("a", "one"))));
        assertThrows(IllegalArgumentException.class, () -> PulsarMetadataV1.decode(CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("key")))));
    }

    @Test
    void nativePreparedDeliveryPinsSnapshotProjectionAndSubmissionHash() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resource = nonZero(32, 5);
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "pulsar-prepared", resource, "persistent://tenant/prepared", 2_500);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("prepared-destination"), 1,
                Bytes.sha256(Bytes.utf8("prepared-destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("prepared-capability"), 1,
                Bytes.sha256(Bytes.utf8("prepared-capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_600, 2_610,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("prepared-clock"), 1, 2, 3,
                Bytes.sha256(Bytes.utf8("prepared-sample")), 0, null);
        final byte[] guard = Bytes.sha256(Bytes.utf8("prepared-guard"));
        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destination, capability, target,
                1, guard, 3, 4, Bytes.sha256(Bytes.utf8("prepared-binding")),
                Bytes.sha256(Bytes.utf8("prepared-fingerprint")), Bytes.sha256(Bytes.utf8("prepared-scope")),
                issuedAt, 4_000, 5, keyPair.getPrivate());
        final PulsarMetadataV1 metadata = new PulsarMetadataV1(Bytes.utf8("partition"),
                PulsarMetadataV1.KeyEncoding.UTF8, null,
                java.util.List.of(new PulsarMetadataV1.Property("trace", "native")));

        final NativePreparedDeliveryV1 prepared = NativePreparedDeliveryV1.create(nonZero(32, 6), destination,
                capability, target, 1, Bytes.utf8("inline-payload"), metadata, 2_650L, 2_700, 2_800, snapshot);
        final NativePreparedDeliveryV1 decoded = NativePreparedDeliveryV1.decode(prepared.canonicalBytes());
        final NativePreparedRefV1 ref = prepared.preparedRef();

        assertEquals(prepared, decoded);
        assertArrayEquals(snapshot.snapshotDigest(), ref.capabilitySnapshotDigest());
        assertArrayEquals(Bytes.sha256(prepared.canonicalBytes()), ref.preparedBytesSha256());
        assertArrayEquals(prepared.submissionHash(), ref.submissionHash());

        final byte[] tampered = prepared.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> NativePreparedDeliveryV1.decode(tampered));
        assertThrows(IllegalArgumentException.class, () -> NativePreparedDeliveryV1.create(nonZero(32, 6),
                destination, capability,
                new PulsarBrokerResourceIdentityV1("pulsar-prepared", nonZero(32, 7),
                        "persistent://tenant/prepared", 2_500),
                1, Bytes.utf8("inline-payload"), metadata, 2_650L, 2_700, 2_800, snapshot));
    }

    @Test
    void stableErrorPinsRegistryRetryabilityAndPreparedRefPresence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final PreparedCommand command = PreparedCommand.cancel(shard, DelayMessageId.random(shard), 0, 9_000);
        final CommandQueuedReceiptV1.PreparedCommandRef commandRef =
                CommandQueuedReceiptV1.PreparedCommandRef.from(command);
        final StableErrorV1 uncertain = StableErrorV1.of(FailureStageV1.ENQUEUE,
                StableCode.ENQUEUE_RESULT_UNCERTAIN, null, commandRef, null, 7);
        assertEquals(uncertain, StableErrorV1.decode(uncertain.canonicalBytes()));
        assertEquals(RetryabilityV1.RETRY_EXACT_BYTES_AFTER_RETRY_AT,
                RetryabilityV1.forCode(StableCode.SHARD_TRANSITIONING));
        final StableErrorV1 delayed = StableErrorV1.of(FailureStageV1.QUERY, StableCode.SHARD_TRANSITIONING,
                12_000L, null, null, null);
        assertEquals(delayed, StableErrorV1.decode(delayed.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> new StableErrorV1(FailureStageV1.ENQUEUE,
                StableCode.ENQUEUE_RESULT_UNCERTAIN, RetryabilityV1.NEVER, null, commandRef, null, null));
        assertThrows(IllegalArgumentException.class, () -> StableErrorV1.of(FailureStageV1.QUERY,
                StableCode.SHARD_TRANSITIONING, null, null, null, null));
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1("stable-error",
                nonZero(32, 8), "persistent://tenant/stable-error", 1_000);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("stable-error-destination"), 1,
                Bytes.sha256(Bytes.utf8("stable-error-destination-semantic")), ProfileKindV1.DESTINATION);
        final NativePreparedRefV1 nativeRef = new NativePreparedRefV1(nonZero(32, 9),
                Bytes.sha256(Bytes.utf8("native-submission")), destination, target, 0,
                Bytes.sha256(Bytes.utf8("native-snapshot")), 2_000, Bytes.sha256(Bytes.utf8("native-prepared")));
        assertThrows(IllegalArgumentException.class, () -> StableErrorV1.of(FailureStageV1.ENQUEUE,
                StableCode.ENQUEUE_RESULT_UNCERTAIN, null, commandRef, nativeRef, null));
    }

    @Test
    void nonPersistenceProofEnforcesKindSpecificBrokerEvidence() {
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-submission"));
        final byte[] attempt = nonZero(16, 10);
        final NonPersistenceProofV1 local = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, preparedHash, null, null, null);
        assertEquals(local, NonPersistenceProofV1.decode(local.canonicalBytes()));

        final PulsarBrokerResourceIdentityV1 pulsar = new PulsarBrokerResourceIdentityV1("proof-pulsar",
                nonZero(32, 11), "persistent://tenant/proof", 1_111);
        final NonPersistenceProofV1 pulsarProof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION, attempt, preparedHash,
                BrokerResourceIdentityV1.pulsar(pulsar), Bytes.sha256(Bytes.utf8("pulsar-request")),
                Bytes.sha256(Bytes.utf8("pulsar-response")));
        assertEquals(pulsarProof, NonPersistenceProofV1.decode(pulsarProof.canonicalBytes()));
        assertEquals(pulsar, BrokerResourceIdentityV1.decode(pulsar.canonicalBytes()).pulsar());

        final KafkaBrokerResourceIdentityV1 kafka = new KafkaBrokerResourceIdentityV1("proof-kafka",
                UUID.randomUUID());
        final NonPersistenceProofV1 kafkaProof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.KAFKA_DEFINITIVE_REJECTION, attempt, preparedHash,
                BrokerResourceIdentityV1.kafka(kafka), Bytes.sha256(Bytes.utf8("kafka-request")),
                Bytes.sha256(Bytes.utf8("kafka-response")));
        assertEquals(kafka, BrokerResourceIdentityV1.decode(kafka.canonicalBytes()).kafka());
        assertEquals(kafkaProof, NonPersistenceProofV1.decode(kafkaProof.canonicalBytes()));

        final byte[] tampered = pulsarProof.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> NonPersistenceProofV1.decode(tampered));
        assertThrows(IllegalArgumentException.class, () -> NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION, attempt, preparedHash, null,
                Bytes.sha256(Bytes.utf8("request")), Bytes.sha256(Bytes.utf8("response"))));
        assertThrows(IllegalArgumentException.class, () -> NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, attempt, preparedHash,
                BrokerResourceIdentityV1.pulsar(pulsar), Bytes.sha256(Bytes.utf8("request")),
                Bytes.sha256(Bytes.utf8("response"))));
    }

    @Test
    void enqueueAndSubmissionOutcomeUnionsKeepBranchIdentityClosed() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 13);
        final PreparedCommand command = PreparedCommand.cancel(shard, DelayMessageId.random(shard), 0, 9_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "outcome-cluster", UUID.randomUUID(), 4,
                1, 1_000);
        final CommandQueuedReceiptV1 queuedReceipt = CommandQueuedReceiptV1.create(command, source,
                new CommandQueuedReceiptV1.KafkaQueuedAck("outcome-cluster", source.nativeTopicUuid(), 13, 4, 1,
                        1_000, Bytes.sha256(Bytes.utf8("outcome-ack"))), 2_000, nonZero(16, 12));
        final CommandQueuedReceiptV1.PreparedCommandRef commandRef = queuedReceipt.command();
        final EnqueueOutcomeMessageV1 queued = EnqueueOutcomeMessageV1.queued(queuedReceipt);
        assertEquals(queued, EnqueueOutcomeMessageV1.decode(queued.canonicalBytes()));

        final NonPersistenceProofV1 localProof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, commandRef.frameSha256(),
                null, null, null);
        final DefinitelyNotQueuedV1 definite = new DefinitelyNotQueuedV1(commandRef, localProof,
                StableErrorV1.of(FailureStageV1.ENQUEUE, StableCode.BROKER_DEFINITIVE_NOT_PERSISTED, null,
                        commandRef, null, null));
        assertEquals(EnqueueOutcomeMessageV1.definitelyNotQueued(definite),
                EnqueueOutcomeMessageV1.decode(EnqueueOutcomeMessageV1.definitelyNotQueued(definite).canonicalBytes()));
        final EnqueueUncertainV1 uncertain = new EnqueueUncertainV1(commandRef, nonZero(16, 13),
                StableErrorV1.of(FailureStageV1.ENQUEUE, StableCode.ENQUEUE_RESULT_UNCERTAIN, null, commandRef,
                        null, null));
        assertEquals(EnqueueOutcomeMessageV1.uncertain(uncertain),
                EnqueueOutcomeMessageV1.decode(EnqueueOutcomeMessageV1.uncertain(uncertain).canonicalBytes()));

        final byte[] resource = nonZero(32, 14);
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1("outcome-pulsar", resource,
                "persistent://tenant/outcome", 1_400);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("outcome-destination"), 1,
                Bytes.sha256(Bytes.utf8("outcome-destination-semantic")), ProfileKindV1.DESTINATION);
        final NativePreparedRefV1 nativeRef = new NativePreparedRefV1(nonZero(32, 15),
                Bytes.sha256(Bytes.utf8("outcome-native-submission")), destination, target, 2,
                Bytes.sha256(Bytes.utf8("outcome-snapshot")), 5_000, Bytes.sha256(Bytes.utf8("outcome-prepared")));
        final CommandQueuedReceiptV1.PulsarQueuedAck ack = new CommandQueuedReceiptV1.PulsarQueuedAck(
                "outcome-pulsar", resource, "persistent://tenant/outcome", 1_400, 2, 3, 4, 0, 1, 1_500,
                Bytes.sha256(Bytes.utf8("native-ack")));
        final NativeDeliveryReceiptV1 nativeReceipt = NativeDeliveryReceiptV1.create(nativeRef, ack, nonZero(16, 16));
        assertEquals(SubmissionOutcomeMessageV1.nativeReceipt(nativeReceipt),
                SubmissionOutcomeMessageV1.decode(SubmissionOutcomeMessageV1.nativeReceipt(nativeReceipt)
                        .canonicalBytes()));

        final NonPersistenceProofV1 nativeProof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION, nonZero(16, 17), nativeRef.submissionHash(),
                BrokerResourceIdentityV1.pulsar(target), Bytes.sha256(Bytes.utf8("native-request")),
                Bytes.sha256(Bytes.utf8("native-response")));
        final NativeDefinitelyNotQueuedV1 nativeDefinite = new NativeDefinitelyNotQueuedV1(nativeRef, nativeProof,
                StableErrorV1.of(FailureStageV1.ENQUEUE, StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED, null, null,
                        nativeRef, null));
        assertEquals(SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(nativeDefinite),
                SubmissionOutcomeMessageV1.decode(SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(nativeDefinite)
                        .canonicalBytes()));
        final NativeEnqueueUncertainV1 nativeUncertain = new NativeEnqueueUncertainV1(nativeRef, nonZero(16, 18),
                StableErrorV1.of(FailureStageV1.ENQUEUE, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null, null,
                        nativeRef, null));
        assertEquals(SubmissionOutcomeMessageV1.nativeUncertain(nativeUncertain),
                SubmissionOutcomeMessageV1.decode(SubmissionOutcomeMessageV1.nativeUncertain(nativeUncertain)
                        .canonicalBytes()));

        final PreparedCommand managedCommand = PreparedCommand.cancelV1(shard, DelayMessageId.random(shard),
                new MessagePreconditionV1(null, null), 9_000);
        final PreparedSubmissionV1 managedPrepared = PreparedSubmissionV1.managed(
                CommandCodec.encodeFrameV1(managedCommand));
        assertEquals(managedPrepared, PreparedSubmissionV1.decode(managedPrepared.canonicalBytes()));
        final NativePreparedDeliveryV1 nativePrepared = nativePreparedForOutcomeTest();
        final PreparedSubmissionV1 nativePreparedSubmission = PreparedSubmissionV1.nativePrepared(nativePrepared);
        assertEquals(nativePreparedSubmission, PreparedSubmissionV1.decode(nativePreparedSubmission.canonicalBytes()));
    }

    @Test
    void payloadUploadAndAttestationResponsesKeepPayloadScopedBranches() throws Exception {
        final ProfileRefV1 objectStore = new ProfileRefV1(Bytes.utf8("payload-object-store"), 2,
                Bytes.sha256(Bytes.utf8("payload-object-store-semantic")), ProfileKindV1.OBJECT_STORE);
        final OpaquePayloadUploadHandleV1 handle = OpaquePayloadUploadHandleV1.create(nonZero(32, 21), objectStore,
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 9_000, Bytes.utf8("opaque-envelope"));
        final PayloadUploadHandleResponseV1 issued = PayloadUploadHandleResponseV1.issued(handle);
        assertEquals(issued, PayloadUploadHandleResponseV1.decode(issued.canonicalBytes()));
        final StableErrorV1 uploadRetry = StableErrorV1.of(FailureStageV1.PAYLOAD,
                StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE, 8_000L, null, null, null);
        final PayloadUploadHandleResponseV1 unavailable = PayloadUploadHandleResponseV1.error(
                PayloadUploadHandleOutcomeV1.OBJECT_STORE_UNAVAILABLE_RETRYABLE, uploadRetry);
        assertEquals(unavailable, PayloadUploadHandleResponseV1.decode(unavailable.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> PayloadUploadHandleResponseV1.error(
                PayloadUploadHandleOutcomeV1.INTEGRITY_ERROR, uploadRetry));

        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final PayloadCommitProofV1 proof = PayloadCommitProofV1.signed(nonZero(32, 22), nonZero(32, 23),
                shard.routeIncarnation().bytes(), shard.partition(), messageId, objectStore, 1, 1,
                Bytes.utf8("bucket"), Bytes.utf8("key"), Bytes.utf8("version"), new byte[0], 3,
                Bytes.sha256(Bytes.utf8("payload")), 7_000, keyPair.getPrivate());
        final PayloadAttestationResponseV1 attested = PayloadAttestationResponseV1.attested(proof);
        assertEquals(attested, PayloadAttestationResponseV1.decode(attested.canonicalBytes()));
        final StableErrorV1 notReadyError = StableErrorV1.of(FailureStageV1.PAYLOAD,
                StableCode.OBJECT_NOT_READY_RETRYABLE, 6_000L, null, null, null);
        final PayloadAttestationResponseV1 notReady = PayloadAttestationResponseV1.error(
                PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE, notReadyError);
        assertEquals(notReady, PayloadAttestationResponseV1.decode(notReady.canonicalBytes()));

        final byte[] tampered = handle.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> OpaquePayloadUploadHandleV1.decode(tampered));
        assertThrows(IllegalArgumentException.class, () -> PayloadAttestationResponseV1.error(
                PayloadAttestationOutcomeV1.INTEGRITY_ERROR, notReadyError));
    }

    @Test
    void publicQueryViewsRejectUnsafeBindingAndNonCanonicalBranchShape() {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination"), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("capability"), 1,
                Bytes.sha256(Bytes.utf8("capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        assertThrows(IllegalArgumentException.class,
                () -> new PublicDestinationBindingViewV1(destination, capability, AdapterKindV1.KAFKA,
                        Bytes.utf8("e\u0301"), 0, OrderingMode.BEST_EFFORT));

        final CommandQueryResponseV1 unknown = CommandQueryResponseV1.unknown();
        final byte[] nonCanonical = unknown.canonicalBytes();
        nonCanonical[nonCanonical.length - 1] = 1;
        assertThrows(IllegalArgumentException.class, () -> CommandQueryResponseV1.decode(nonCanonical));
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
        assertEquals(103, values.size());
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
    void sourcePositionDecoderRejectsNonCanonicalUtf8Bytes() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final KafkaSourcePosition kafka = new KafkaSourcePosition(shard, "cluster", java.util.UUID.randomUUID(),
                3, null, 10);
        final byte[] kafkaBytes = kafka.canonicalBytes();
        final int kafkaClusterByte = 1 + 16 + 4;
        kafkaBytes[kafkaClusterByte] = (byte) 0xc3;
        assertThrows(IllegalArgumentException.class, () -> SourcePositionCodec.decode(kafkaBytes));

        final byte[] resource = Bytes.sha256(Bytes.utf8("source-resource"));
        final PulsarSourcePosition pulsar = new PulsarSourcePosition(shard, resource, "persistent://t/topic",
                2, 4, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 11);
        final byte[] pulsarBytes = pulsar.canonicalBytes();
        final int topicByte = 1 + 16 + 4 + resource.length + 4;
        pulsarBytes[topicByte] = (byte) 0xc3;
        assertThrows(IllegalArgumentException.class, () -> SourcePositionCodec.decode(pulsarBytes));
    }

    @Test
    void sourcePositionsRejectNonCanonicalTextAtConstruction() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaSourcePosition(shard, "cluster\u0301", topic, 1, null, 10));

        final byte[] resource = Bytes.sha256(Bytes.utf8("source-resource-nfc"));
        assertThrows(IllegalArgumentException.class,
                () -> new PulsarSourcePosition(shard, resource, "persistent://t/e\u0301", 1, 1, 0, 1,
                        PulsarSourcePosition.EntryKind.NON_BATCH, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new PulsarSourcePosition(shard, resource, "persistent://t/\uD800", 1, 1, 0, 1,
                        PulsarSourcePosition.EntryKind.NON_BATCH, 10));
    }

    @Test
    void sourcePositionDecoderRejectsTruncatedLengthAndFixedFields() {
        final byte[] truncatedLength = new byte[1 + 16 + 3];
        truncatedLength[0] = (byte) SourcePositionKind.KAFKA.wireValue();
        assertThrows(IllegalArgumentException.class, () -> SourcePositionCodec.decode(truncatedLength));

        final KafkaSourcePosition source = new KafkaSourcePosition(new ShardId(RouteIncarnation.random(), 9),
                "cluster", UUID.randomUUID(), 1, null, 10);
        assertThrows(IllegalArgumentException.class,
                () -> SourcePositionCodec.decode(Arrays.copyOf(source.canonicalBytes(),
                        source.canonicalBytes().length - 1)));
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

    private static PublicDestinationBindingViewV1 publicBinding() {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination"), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("capability"), 1,
                Bytes.sha256(Bytes.utf8("capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        return new PublicDestinationBindingViewV1(destination, capability, AdapterKindV1.KAFKA,
                Bytes.utf8("safe-destination"), 2, OrderingMode.BEST_EFFORT);
    }

    private static NativePreparedDeliveryV1 nativePreparedForOutcomeTest() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resource = nonZero(32, 19);
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1("prepared-outcome", resource,
                "persistent://tenant/prepared-outcome", 1_900);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("prepared-outcome-destination"), 1,
                Bytes.sha256(Bytes.utf8("prepared-outcome-destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("prepared-outcome-capability"), 1,
                Bytes.sha256(Bytes.utf8("prepared-outcome-capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("prepared-outcome-clock"), 1, 2,
                3, Bytes.sha256(Bytes.utf8("prepared-outcome-sample")), 0, null);
        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destination, capability, target,
                0, Bytes.sha256(Bytes.utf8("prepared-outcome-guard")), 1, 1,
                Bytes.sha256(Bytes.utf8("prepared-outcome-binding")),
                Bytes.sha256(Bytes.utf8("prepared-outcome-fingerprint")),
                Bytes.sha256(Bytes.utf8("prepared-outcome-scope")), issuedAt, 3_000, 1, keyPair.getPrivate());
        return NativePreparedDeliveryV1.create(nonZero(32, 20), destination, capability, target, 0,
                Bytes.utf8("prepared-outcome-payload"), new PulsarMetadataV1(null, null, null, java.util.List.of()),
                null, 2_100, 2_200, snapshot);
    }

    private static byte[] nonZero(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }

    private static byte[] nestedPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, new byte[]{1}));
    }
}
