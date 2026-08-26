package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

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
        final ReceiptFrame.Decoded textDecoded =
                ReceiptFrame.decodeText(ReceiptFrame.encodeText(ReceiptKind.COMMAND_QUEUED, new byte[0]));
        assertEquals(ReceiptKind.COMMAND_QUEUED, textDecoded.kind());
        assertArrayEquals(new byte[0], textDecoded.payload());
    }

    @Test
    void receiptFrameRejectsFlagsLengthKindAndCrcDrift() {
        final byte[] frame = ReceiptFrame.encode(ReceiptKind.COMMAND_APPLIED, new byte[] {1, 2, 3});
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
        final PreparedCommand command = schedule(shard, "receipt-lane", 2_000, 8_000, 9_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-a", topic, 7, 3, 1_234);
        final CanonicalCommandQueuedReceipt.KafkaQueuedAck ack = new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                "cluster-a", topic, 8, 7, 3, 1_234, Bytes.sha256(Bytes.utf8("broker-response")));
        final byte[] attempt = new byte[16];
        attempt[15] = 1;

        final CanonicalCommandQueuedReceipt receipt =
                CanonicalCommandQueuedReceipt.create(command, source, ack, 9_000, attempt);
        final CanonicalCommandQueuedReceipt decoded = CanonicalCommandQueuedReceipt.decodeFrame(receipt.frame());

        assertEquals(receipt, decoded);
        assertEquals(command.commandId(), decoded.command().commandId());
        assertEquals(command.delayMessageId(), decoded.command().delayMessageId());
        assertEquals(source, decoded.sourcePosition());
        assertEquals(ack, decoded.brokerAck());
        assertArrayEquals(receipt.receiptPayloadDigest(), decoded.receiptPayloadDigest());
        assertEquals(
                ReceiptKind.COMMAND_QUEUED, ReceiptFrame.decode(receipt.frame()).kind());

        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CanonicalCommandQueuedReceipt.decodePayload(tampered));
        final CanonicalCommandQueuedReceipt.KafkaQueuedAck wrongAck = new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                "cluster-a", topic, 8, 8, 3, 1_235, ack.responseSha256());
        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalCommandQueuedReceipt.create(command, source, wrongAck, 9_000, attempt));
    }

    @Test
    void commandQueuedReceiptRejectsACommandAndSourceFromDifferentShards() {
        final ShardId commandShard = new ShardId(RouteIncarnation.random(), 8);
        final ShardId sourceShard = new ShardId(RouteIncarnation.random(), 9);
        final UUID topic = UUID.randomUUID();
        final PreparedCommand command = schedule(commandShard, "receipt-shard-fence", 2_000, 8_000, 9_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(sourceShard, "cluster-a", topic, 7, 3, 1_234);
        final CanonicalCommandQueuedReceipt.KafkaQueuedAck ack = new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                "cluster-a",
                topic,
                sourceShard.partition(),
                7,
                3,
                1_234,
                Bytes.sha256(Bytes.utf8("broker-response-shard-fence")));
        final byte[] attempt = new byte[16];
        attempt[15] = 1;

        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalCommandQueuedReceipt.create(command, source, ack, 9_000, attempt));
    }

    @Test
    void commandQueuedReceiptRejectsCompatibilityCommandBody() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        final PreparedCommand legacy = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("legacy-receipt-lane")),
                        2_000,
                        8_000,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("legacy-receipt")),
                9_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-legacy", topic, 7, 3, 1_234);
        final CanonicalCommandQueuedReceipt.KafkaQueuedAck ack = new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                "cluster-legacy", topic, shard.partition(), 7, 3, 1_234, Bytes.sha256(Bytes.utf8("legacy-response")));
        final byte[] attempt = new byte[16];
        attempt[15] = 1;

        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalCommandQueuedReceipt.create(legacy, source, ack, 9_000, attempt));
        assertThrows(
                IllegalArgumentException.class, () -> CanonicalCommandQueuedReceipt.PreparedCommandRef.from(legacy));
    }

    @Test
    void commandQueuedReceiptPulsarPayloadUsesTheClosedAckBranch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final byte[] resource = new byte[32];
        resource[0] = 7;
        final PulsarSourcePosition source = new PulsarSourcePosition(
                shard, resource, "persistent://tenant/topic", 4, 5, 1, 3, PulsarSourcePosition.EntryKind.BATCH, 2_345);
        final PreparedCommand command = cancel(shard, 9_000);
        final CanonicalCommandQueuedReceipt.PulsarQueuedAck ack = new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                "pulsar-cluster",
                resource,
                "persistent://tenant/topic",
                Long.MIN_VALUE,
                9,
                4,
                5,
                1,
                3,
                2_345,
                Bytes.sha256(Bytes.utf8("send-receipt")));
        final byte[] attempt = new byte[16];
        attempt[0] = 1;

        final CanonicalCommandQueuedReceipt decoded = CanonicalCommandQueuedReceipt.decodeFrame(
                CanonicalCommandQueuedReceipt.create(command, source, ack, 3_000, attempt)
                        .frame());
        assertEquals(source, decoded.sourcePosition());
        assertEquals(ack, decoded.brokerAck());
        assertEquals(CommandType.CANCEL, decoded.command().commandType());
    }

    @Test
    void queryErrorResponsesKeepClosedResultTagsAndRetryPresence() {
        final CommandQueryResponse command = CommandQueryResponse.error(StableCode.SHARD_TRANSITIONING, 7_000L);
        assertEquals(command, CommandQueryResponse.decode(command.canonicalBytes()));
        final MessageQueryResponse message = MessageQueryResponse.error(StableCode.INVALID_RECEIPT, null);
        assertEquals(message, MessageQueryResponse.decode(message.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> new PublicQueryError(StableCode.SHARD_UNAVAILABLE, 7_000L));
        assertThrows(
                IllegalArgumentException.class,
                () -> PublicQueryError.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint32(output, 1, StableCode.SHARD_TRANSITIONING.wireValue());
                })));
        assertThrows(
                IllegalArgumentException.class,
                () -> PublicQueryError.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint32(output, 1, StableCode.SHARD_TRANSITIONING.wireValue());
                    CanonicalProtobuf.bytes(output, 2, Bytes.utf8("not-a-varint"));
                })));
        assertThrows(IllegalArgumentException.class, () -> CommandQueryResponse.error(StableCode.OK, null));
    }

    @Test
    void fullQueryViewsRoundTripAndKeepUnionBranchesClosed() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition current = new KafkaSourcePosition(shard, "cluster-query", topic, 10, 2, 1_000);
        final KafkaSourcePosition awaited = new KafkaSourcePosition(shard, "cluster-query", topic, 11, 2, 1_001);
        final PublicDestinationBindingView binding = publicBinding();
        final int highBitGeneration = (int) 0x8000_0000L;

        final PendingCommandView pendingView = new PendingCommandView(awaited, current, 2_000);
        assertEquals(
                CommandQueryResponse.pending(pendingView),
                CommandQueryResponse.decode(
                        CommandQueryResponse.pending(pendingView).canonicalBytes()));

        final PublicCommandResult appliedView = new PublicCommandResult(
                CommandApplyStatus.APPLIED, StableCode.OK, awaited, highBitGeneration, 1L, binding, 3_000);
        final PublicCommandResult rejectedView = new PublicCommandResult(
                CommandApplyStatus.REJECTED, StableCode.INVALID_COMMAND, awaited, null, null, null, 3_000);
        assertEquals(
                CommandQueryResponse.applied(appliedView),
                CommandQueryResponse.decode(
                        CommandQueryResponse.applied(appliedView).canonicalBytes()));
        assertEquals(
                CommandQueryResponse.rejected(rejectedView),
                CommandQueryResponse.decode(
                        CommandQueryResponse.rejected(rejectedView).canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PublicCommandResult(
                        CommandApplyStatus.REJECTED, StableCode.INVALID_COMMAND, awaited, 0, null, null, 3_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PublicCommandResult(
                        CommandApplyStatus.APPLIED, StableCode.OK, awaited, null, 1L, null, 3_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PublicCommandResult(
                        CommandApplyStatus.REJECTED, StableCode.INVALID_COMMAND, awaited, null, null, null, 1_000));

        final CompactCommandResult compact =
                new CompactCommandResult(CommandApplyStatus.REJECTED, StableCode.INVALID_COMMAND, awaited, 3_000);
        assertEquals(
                CommandQueryResponse.resultExpired(compact),
                CommandQueryResponse.decode(
                        CommandQueryResponse.resultExpired(compact).canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CompactCommandResult(
                        CommandApplyStatus.REJECTED, StableCode.INVALID_COMMAND, awaited, 1_000));
        assertEquals(
                CommandQueryResponse.resultEvidenceExpired(),
                CommandQueryResponse.decode(
                        CommandQueryResponse.resultEvidenceExpired().canonicalBytes()));

        final ReservedMessageView reserved =
                new ReservedMessageView(new byte[32], 1, PayloadReservationState.PAYLOAD_RESERVED, 4_000, binding);
        final ActiveMessageView active = new ActiveMessageView(
                highBitGeneration,
                2,
                MessageGenerationState.UNCERTAIN,
                1_000,
                5_000,
                binding,
                PayloadAvailability.INLINE_RETAINED,
                true);
        final PublicEvidenceRef evidence = new PublicEvidenceRef(
                PublishEvidenceKind.KAFKA_PRODUCE_ACK,
                Bytes.sha256(Bytes.utf8("evidence")),
                EvidenceVerificationStatus.VERIFIED_PUBLISHED);
        final TerminalMessageView terminal = new TerminalMessageView(
                highBitGeneration,
                3,
                MessageGenerationState.PUBLISHED,
                StableCode.OK,
                binding,
                PayloadAvailability.INLINE_RETAINED,
                DlqExportState.NOT_CONFIGURED,
                false,
                evidence);
        assertEquals(
                MessageQueryResponse.reserved(reserved),
                MessageQueryResponse.decode(
                        MessageQueryResponse.reserved(reserved).canonicalBytes()));
        assertEquals(
                MessageQueryResponse.active(active),
                MessageQueryResponse.decode(MessageQueryResponse.active(active).canonicalBytes()));
        assertEquals(
                MessageQueryResponse.terminal(terminal),
                MessageQueryResponse.decode(
                        MessageQueryResponse.terminal(terminal).canonicalBytes()));
        assertEquals(
                MessageQueryResponse.identityRetired(),
                MessageQueryResponse.decode(
                        MessageQueryResponse.identityRetired().canonicalBytes()));
        assertEquals(
                MessageQueryResponse.unknown(FirstScheduleEligibility.NOT_PROVEN),
                MessageQueryResponse.decode(MessageQueryResponse.unknown(FirstScheduleEligibility.NOT_PROVEN)
                        .canonicalBytes()));

        assertThrows(
                IllegalArgumentException.class,
                () -> new CommandQueryResponse(CommandQueryResult.APPLIED, rejectedView));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ActiveMessageView(
                        0,
                        2,
                        MessageGenerationState.PUBLISHED,
                        1_000,
                        5_000,
                        binding,
                        PayloadAvailability.INLINE_RETAINED,
                        false));
    }

    @Test
    void commandAppliedReceiptBindsQueuedDigestAndAppliedSourcePosition() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final UUID topic = UUID.randomUUID();
        final PreparedCommand command = schedule(shard, "applied-lane", 1_000, 5_000, 8_000);
        final KafkaSourcePosition queuedPosition =
                new KafkaSourcePosition(shard, "cluster-applied", topic, 10, 1, 1_000);
        final KafkaSourcePosition appliedPosition =
                new KafkaSourcePosition(shard, "cluster-applied", topic, 11, 1, 1_001);
        final byte[] attempt = new byte[16];
        attempt[0] = 1;
        final CanonicalCommandQueuedReceipt queued = CanonicalCommandQueuedReceipt.create(
                command,
                queuedPosition,
                new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                        "cluster-applied", topic, shard.partition(), 10, 1, 1_000, Bytes.sha256(Bytes.utf8("ack"))),
                2_000,
                attempt);
        final CommandAppliedReceipt applied = CommandAppliedReceipt.create(
                queued,
                CommandApplyStatus.APPLIED,
                StableCode.OK,
                appliedPosition,
                (int) 0x8000_0000L,
                1L,
                publicBinding(),
                3_000);

        assertEquals(applied, CommandAppliedReceipt.decodeFrame(applied.frame()));
        assertEquals(
                ReceiptKind.COMMAND_APPLIED,
                ReceiptFrame.decode(applied.frame()).kind());

        final CommandAppliedReceipt rejected = CommandAppliedReceipt.create(
                queued,
                CommandApplyStatus.REJECTED,
                StableCode.INVALID_COMMAND,
                appliedPosition,
                null,
                null,
                null,
                3_000);
        assertEquals(rejected, CommandAppliedReceipt.decodePayload(rejected.payload()));

        final byte[] tampered = applied.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CommandAppliedReceipt.decodePayload(tampered));
        final KafkaSourcePosition beforeQueued = new KafkaSourcePosition(shard, "cluster-applied", topic, 9, 1, 999);
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandAppliedReceipt.create(
                        queued,
                        CommandApplyStatus.APPLIED,
                        StableCode.OK,
                        beforeQueued,
                        0,
                        1L,
                        publicBinding(),
                        3_000));
        final KafkaSourcePosition conflictingSameOffset =
                new KafkaSourcePosition(shard, "cluster-applied", topic, 10, 2, 1_002);
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandAppliedReceipt.create(
                        queued,
                        CommandApplyStatus.APPLIED,
                        StableCode.OK,
                        conflictingSameOffset,
                        0,
                        1L,
                        publicBinding(),
                        3_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandAppliedReceipt.create(
                        queued,
                        CommandApplyStatus.REJECTED,
                        StableCode.INVALID_COMMAND,
                        appliedPosition,
                        0,
                        1L,
                        publicBinding(),
                        3_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandAppliedReceipt.create(
                        queued, CommandApplyStatus.APPLIED, StableCode.OK, appliedPosition, null, 1L, null, 3_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandAppliedReceipt.create(
                        queued,
                        CommandApplyStatus.REJECTED,
                        StableCode.INVALID_COMMAND,
                        appliedPosition,
                        null,
                        null,
                        null,
                        1_000));
    }

    @Test
    void payloadReservationReceiptKeepsObjectIdentityAndTrustSetPinned() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shard, "cluster-payload", UUID.randomUUID(), 12, 1, 1_200);
        final ProfileRef objectStore = new ProfileRef(
                Bytes.utf8("object-store"),
                3,
                Bytes.sha256(Bytes.utf8("object-store-semantic")),
                ProfileKind.OBJECT_STORE);
        final PayloadProofTrustSetRef trustSet = new PayloadProofTrustSetRef(4, Bytes.sha256(Bytes.utf8("trust-set")));
        final PayloadReservationReceipt receipt = PayloadReservationReceipt.create(
                Bytes.sha256(Bytes.utf8("reservation")),
                messageId,
                shard,
                source,
                2,
                objectStore,
                Bytes.utf8("container-a"),
                Bytes.utf8("service-owned/key"),
                123,
                Bytes.sha256(Bytes.utf8("payload")),
                9_000,
                trustSet);

        assertEquals(receipt, PayloadReservationReceipt.decodeFrame(receipt.frame()));
        assertEquals(
                ReceiptKind.PAYLOAD_RESERVATION,
                ReceiptFrame.decode(receipt.frame()).kind());
        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PayloadReservationReceipt.decodePayload(tampered));

        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination"),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic")),
                ProfileKind.DESTINATION);
        assertThrows(
                IllegalArgumentException.class,
                () -> PayloadReservationReceipt.create(
                        new byte[32],
                        messageId,
                        shard,
                        source,
                        2,
                        destination,
                        Bytes.utf8("container-a"),
                        Bytes.utf8("key"),
                        1,
                        Bytes.sha256(Bytes.utf8("payload")),
                        9_000,
                        trustSet));
        assertThrows(
                IllegalArgumentException.class,
                () -> PayloadReservationReceipt.create(
                        new byte[32],
                        messageId,
                        new ShardId(RouteIncarnation.random(), 6),
                        source,
                        2,
                        objectStore,
                        Bytes.utf8("container-a"),
                        Bytes.utf8("key"),
                        1,
                        Bytes.sha256(Bytes.utf8("payload")),
                        9_000,
                        trustSet));
    }

    @Test
    void controlOperationReceiptPinsRegisteredEvidenceAndQueryBoundary() {
        final TrustedUtcIntervalEvidence registeredAt = new TrustedUtcIntervalEvidence(
                1_000,
                1_005,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("control-clock"),
                2,
                7,
                9,
                Bytes.sha256(Bytes.utf8("control-sample")),
                0,
                null);
        final ControlOperationReceipt receipt = ControlOperationReceipt.create(
                Bytes.sha256(Bytes.utf8("operation")),
                Bytes.sha256(Bytes.utf8("request")),
                Bytes.sha256(Bytes.utf8("scope")),
                Bytes.sha256(Bytes.utf8("targets")),
                3,
                registeredAt,
                5_000);

        assertEquals(receipt, ControlOperationReceipt.decodeFrame(receipt.frame()));
        assertEquals(
                ReceiptKind.CONTROL_OPERATION,
                ReceiptFrame.decode(receipt.frame()).kind());
        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ControlOperationReceipt.decodePayload(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationReceipt.create(
                        new byte[32],
                        Bytes.sha256(Bytes.utf8("request")),
                        Bytes.sha256(Bytes.utf8("scope")),
                        Bytes.sha256(Bytes.utf8("targets")),
                        3,
                        registeredAt,
                        5_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationReceipt.create(
                        Bytes.sha256(Bytes.utf8("operation")),
                        Bytes.sha256(Bytes.utf8("request")),
                        Bytes.sha256(Bytes.utf8("scope")),
                        Bytes.sha256(Bytes.utf8("targets")),
                        3,
                        registeredAt,
                        1_004));
    }

    @Test
    void nativeDeliveryReceiptPinsPulsarTargetAndPhysicalAttempt() {
        final byte[] resource = new byte[32];
        resource[0] = 9;
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "pulsar-native", resource, "persistent://tenant/native", Long.MIN_VALUE);
        assertEquals(target, PulsarBrokerResourceIdentity.decode(target.canonicalBytes()));
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("native-destination"),
                2,
                Bytes.sha256(Bytes.utf8("native-destination-semantic")),
                ProfileKind.DESTINATION);
        final NativePreparedRef prepared = new NativePreparedRef(
                nonZero(32, 1),
                Bytes.sha256(Bytes.utf8("submission")),
                destination,
                target,
                -1,
                Bytes.sha256(Bytes.utf8("capability-snapshot")),
                5_000,
                Bytes.sha256(Bytes.utf8("prepared-bytes")));
        final CanonicalCommandQueuedReceipt.PulsarQueuedAck ack = new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                "pulsar-native",
                resource,
                "persistent://tenant/native",
                Long.MIN_VALUE,
                -1,
                4,
                5,
                0,
                1,
                1_250,
                Bytes.sha256(Bytes.utf8("send-receipt")));
        final byte[] attempt = nonZero(16, 2);

        final NativeDeliveryReceipt receipt = NativeDeliveryReceipt.create(prepared, ack, attempt);
        assertEquals(receipt, NativeDeliveryReceipt.decodeFrame(receipt.frame()));
        assertEquals(
                ReceiptKind.NATIVE_DELIVERY,
                ReceiptFrame.decode(receipt.frame()).kind());

        final byte[] tampered = receipt.payload();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> NativeDeliveryReceipt.decodePayload(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> NativeDeliveryReceipt.create(
                        prepared,
                        new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                                "pulsar-native",
                                resource,
                                "persistent://tenant/native",
                                1_235,
                                -1,
                                4,
                                5,
                                0,
                                1,
                                1_250,
                                Bytes.sha256(Bytes.utf8("send-receipt"))),
                        attempt));
        assertThrows(
                IllegalArgumentException.class,
                () -> new NativePreparedRef(
                        new byte[32],
                        Bytes.sha256(Bytes.utf8("submission")),
                        destination,
                        target,
                        2,
                        Bytes.sha256(Bytes.utf8("capability-snapshot")),
                        5_000,
                        Bytes.sha256(Bytes.utf8("prepared-bytes"))));
    }

    @Test
    void nativeCapabilitySnapshotRoundTripsAndVerifiesIssuerSignature() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resource = nonZero(32, 3);
        final PulsarBrokerResourceIdentity target =
                new PulsarBrokerResourceIdentity("pulsar-snapshot", resource, "persistent://tenant/snapshot", 2_000);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("snapshot-destination"),
                4,
                Bytes.sha256(Bytes.utf8("snapshot-destination-semantic")),
                ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("snapshot-capability"),
                5,
                Bytes.sha256(Bytes.utf8("snapshot-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_100,
                2_110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("snapshot-clock"),
                7,
                8,
                9,
                Bytes.sha256(Bytes.utf8("snapshot-sample")),
                0,
                null);

        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destination,
                capability,
                target,
                -1,
                Bytes.sha256(Bytes.utf8("guard-attestation")),
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("binding")),
                Bytes.sha256(Bytes.utf8("credential-fingerprint")),
                Bytes.sha256(Bytes.utf8("principal-scope")),
                issuedAt,
                3_000,
                Integer.MIN_VALUE,
                keyPair.getPrivate());
        final NativeCapabilitySnapshot decoded = NativeCapabilitySnapshot.decode(snapshot.canonicalBytes());

        assertEquals(snapshot, decoded);
        assertEquals(Long.MIN_VALUE, decoded.resourceGuardConfigGeneration());
        assertEquals(Long.MIN_VALUE, decoded.credentialBindingGeneration());
        assertEquals(Integer.MIN_VALUE, decoded.issuerSigningKeyVersion());
        assertTrue(decoded.verifySignature(keyPair.getPublic()));
        assertFalse(decoded.verifySignature(keyPairGenerator.generateKeyPair().getPublic()));

        final byte[] tamperedSignature = snapshot.canonicalBytes();
        tamperedSignature[tamperedSignature.length - 1] ^= 1;
        assertFalse(NativeCapabilitySnapshot.decode(tamperedSignature).verifySignature(keyPair.getPublic()));
        assertThrows(
                IllegalArgumentException.class,
                () -> NativeCapabilitySnapshot.create(
                        destination,
                        capability,
                        target,
                        -1,
                        Bytes.sha256(Bytes.utf8("guard-attestation")),
                        11,
                        12,
                        Bytes.sha256(Bytes.utf8("binding")),
                        Bytes.sha256(Bytes.utf8("credential-fingerprint")),
                        Bytes.sha256(Bytes.utf8("principal-scope")),
                        issuedAt,
                        2_110,
                        13,
                        keyPair.getPrivate()));
    }

    @Test
    void pulsarMetadataKeepsOptionalKeysAndSortedUniqueProperties() {
        final PulsarMetadata metadata = new PulsarMetadata(
                Bytes.utf8("partition-key"),
                PulsarMetadata.KeyEncoding.UTF8,
                Bytes.utf8("ordering-key"),
                java.util.List.of(new PulsarMetadata.Property("a", "one"), new PulsarMetadata.Property("z", "two")));
        assertEquals(metadata, PulsarMetadata.decode(metadata.canonicalBytes()));
        assertEquals(new PulsarMetadata(null, null, null, java.util.List.of()), PulsarMetadata.decode(new byte[0]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarMetadata(Bytes.utf8("key"), null, null, java.util.List.of()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarMetadata(
                        null,
                        null,
                        null,
                        java.util.List.of(
                                new PulsarMetadata.Property("z", "two"), new PulsarMetadata.Property("a", "one"))));
        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarMetadata.decode(
                        CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("key")))));
    }

    @Test
    void nativePreparedDeliveryPinsSnapshotProjectionAndSubmissionHash() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resource = nonZero(32, 5);
        final PulsarBrokerResourceIdentity target =
                new PulsarBrokerResourceIdentity("pulsar-prepared", resource, "persistent://tenant/prepared", 2_500);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("prepared-destination"),
                1,
                Bytes.sha256(Bytes.utf8("prepared-destination-semantic")),
                ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("prepared-capability"),
                1,
                Bytes.sha256(Bytes.utf8("prepared-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_600,
                2_610,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("prepared-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("prepared-sample")),
                0,
                null);
        final byte[] guard = Bytes.sha256(Bytes.utf8("prepared-guard"));
        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destination,
                capability,
                target,
                1,
                guard,
                3,
                4,
                Bytes.sha256(Bytes.utf8("prepared-binding")),
                Bytes.sha256(Bytes.utf8("prepared-fingerprint")),
                Bytes.sha256(Bytes.utf8("prepared-scope")),
                issuedAt,
                4_000,
                5,
                keyPair.getPrivate());
        final PulsarMetadata metadata = new PulsarMetadata(
                Bytes.utf8("partition"),
                PulsarMetadata.KeyEncoding.UTF8,
                null,
                java.util.List.of(new PulsarMetadata.Property("trace", "native")));

        final NativePreparedDelivery prepared = NativePreparedDelivery.create(
                nonZero(32, 6),
                destination,
                capability,
                target,
                1,
                Bytes.utf8("inline-payload"),
                metadata,
                2_650L,
                2_700,
                2_800,
                snapshot);
        final NativePreparedDelivery decoded = NativePreparedDelivery.decode(prepared.canonicalBytes());
        final NativePreparedRef ref = prepared.preparedRef();

        assertEquals(prepared, decoded);
        assertArrayEquals(snapshot.snapshotDigest(), ref.capabilitySnapshotDigest());
        assertArrayEquals(Bytes.sha256(prepared.canonicalBytes()), ref.preparedBytesSha256());
        assertArrayEquals(prepared.submissionHash(), ref.submissionHash());

        final byte[] tampered = prepared.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> NativePreparedDelivery.decode(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> NativePreparedDelivery.create(
                        nonZero(32, 6),
                        destination,
                        capability,
                        new PulsarBrokerResourceIdentity(
                                "pulsar-prepared", nonZero(32, 7), "persistent://tenant/prepared", 2_500),
                        1,
                        Bytes.utf8("inline-payload"),
                        metadata,
                        2_650L,
                        2_700,
                        2_800,
                        snapshot));
    }

    @Test
    void stableErrorPinsRegistryRetryabilityAndPreparedRefPresence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final PreparedCommand command = cancel(shard, 9_000);
        final CanonicalCommandQueuedReceipt.PreparedCommandRef commandRef =
                CanonicalCommandQueuedReceipt.PreparedCommandRef.from(command);
        final StableError uncertain =
                StableError.of(FailureStage.ENQUEUE, StableCode.ENQUEUE_RESULT_UNCERTAIN, null, commandRef, null, 7);
        assertEquals(uncertain, StableError.decode(uncertain.canonicalBytes()));
        assertEquals(
                Retryability.RETRY_EXACT_BYTES_AFTER_RETRY_AT, Retryability.forCode(StableCode.SHARD_TRANSITIONING));
        final StableError delayed =
                StableError.of(FailureStage.QUERY, StableCode.SHARD_TRANSITIONING, 12_000L, null, null, null);
        assertEquals(delayed, StableError.decode(delayed.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new StableError(
                        FailureStage.ENQUEUE,
                        StableCode.ENQUEUE_RESULT_UNCERTAIN,
                        Retryability.NEVER,
                        null,
                        commandRef,
                        null,
                        null));
        assertThrows(
                IllegalArgumentException.class,
                () -> StableError.of(FailureStage.QUERY, StableCode.SHARD_TRANSITIONING, null, null, null, null));
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "stable-error", nonZero(32, 8), "persistent://tenant/stable-error", 1_000);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("stable-error-destination"),
                1,
                Bytes.sha256(Bytes.utf8("stable-error-destination-semantic")),
                ProfileKind.DESTINATION);
        final NativePreparedRef nativeRef = new NativePreparedRef(
                nonZero(32, 9),
                Bytes.sha256(Bytes.utf8("native-submission")),
                destination,
                target,
                0,
                Bytes.sha256(Bytes.utf8("native-snapshot")),
                2_000,
                Bytes.sha256(Bytes.utf8("native-prepared")));
        assertThrows(
                IllegalArgumentException.class,
                () -> StableError.of(
                        FailureStage.ENQUEUE, StableCode.ENQUEUE_RESULT_UNCERTAIN, null, commandRef, nativeRef, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> StableError.of(FailureStage.QUERY, StableCode.OK, null, null, null, null));

        final NonPersistenceProof managedProof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                null,
                commandRef.frameSha256(),
                null,
                null,
                null);
        final NonPersistenceProof nativeProof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                null,
                nativeRef.submissionHash(),
                null,
                null,
                null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefinitelyNotQueued(
                        commandRef,
                        managedProof,
                        StableError.of(FailureStage.QUERY, StableCode.INVALID_COMMAND, null, commandRef, null, null)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new EnqueueUncertain(
                        commandRef,
                        nonZero(16, 11),
                        StableError.of(FailureStage.QUERY, StableCode.CLIENT_CLOSED, null, commandRef, null, null)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new NativeDefinitelyNotQueued(
                        nativeRef,
                        nativeProof,
                        StableError.of(
                                FailureStage.QUERY,
                                StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                                null,
                                null,
                                nativeRef,
                                null)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new NativeEnqueueUncertain(
                        nativeRef,
                        nonZero(16, 12),
                        StableError.of(FailureStage.QUERY, StableCode.CLIENT_CLOSED, null, null, nativeRef, null)));
    }

    @Test
    void nonPersistenceProofEnforcesKindSpecificBrokerEvidence() {
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-submission"));
        final byte[] attempt = nonZero(16, 10);
        final NonPersistenceProof local = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, preparedHash, null, null, null);
        assertEquals(local, NonPersistenceProof.decode(local.canonicalBytes()));

        final PulsarBrokerResourceIdentity pulsar =
                new PulsarBrokerResourceIdentity("proof-pulsar", nonZero(32, 11), "persistent://tenant/proof", 1_111);
        final NonPersistenceProof pulsarProof = NonPersistenceProof.create(
                NonPersistenceProofKind.PULSAR_GUARD_REJECTION,
                attempt,
                preparedHash,
                BrokerResourceIdentity.pulsar(pulsar),
                Bytes.sha256(Bytes.utf8("pulsar-request")),
                Bytes.sha256(Bytes.utf8("pulsar-response")));
        assertEquals(pulsarProof, NonPersistenceProof.decode(pulsarProof.canonicalBytes()));
        assertEquals(
                pulsar, BrokerResourceIdentity.decode(pulsar.canonicalBytes()).pulsar());

        final KafkaBrokerResourceIdentity kafka = new KafkaBrokerResourceIdentity("proof-kafka", UUID.randomUUID());
        final NonPersistenceProof kafkaProof = NonPersistenceProof.create(
                NonPersistenceProofKind.KAFKA_DEFINITIVE_REJECTION,
                attempt,
                preparedHash,
                BrokerResourceIdentity.kafka(kafka),
                Bytes.sha256(Bytes.utf8("kafka-request")),
                Bytes.sha256(Bytes.utf8("kafka-response")));
        assertEquals(
                kafka, BrokerResourceIdentity.decode(kafka.canonicalBytes()).kafka());
        assertEquals(kafkaProof, NonPersistenceProof.decode(kafkaProof.canonicalBytes()));

        final byte[] tampered = pulsarProof.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> NonPersistenceProof.decode(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> NonPersistenceProof.create(
                        NonPersistenceProofKind.PULSAR_GUARD_REJECTION,
                        attempt,
                        preparedHash,
                        null,
                        Bytes.sha256(Bytes.utf8("request")),
                        Bytes.sha256(Bytes.utf8("response"))));
        assertThrows(
                IllegalArgumentException.class,
                () -> NonPersistenceProof.create(
                        NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                        attempt,
                        preparedHash,
                        BrokerResourceIdentity.pulsar(pulsar),
                        Bytes.sha256(Bytes.utf8("request")),
                        Bytes.sha256(Bytes.utf8("response"))));
    }

    @Test
    void enqueueAndSubmissionOutcomeUnionsKeepBranchIdentityClosed() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 13);
        final PreparedCommand command = cancel(shard, 9_000);
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shard, "outcome-cluster", UUID.randomUUID(), 4, 1, 1_000);
        final CanonicalCommandQueuedReceipt queuedReceipt = CanonicalCommandQueuedReceipt.create(
                command,
                source,
                new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                        "outcome-cluster",
                        source.nativeTopicUuid(),
                        13,
                        4,
                        1,
                        1_000,
                        Bytes.sha256(Bytes.utf8("outcome-ack"))),
                2_000,
                nonZero(16, 12));
        final CanonicalCommandQueuedReceipt.PreparedCommandRef commandRef = queuedReceipt.command();
        final EnqueueOutcomeMessage queued = EnqueueOutcomeMessage.queued(queuedReceipt);
        assertEquals(queued, EnqueueOutcomeMessage.decode(queued.canonicalBytes()));

        final NonPersistenceProof localProof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                null,
                commandRef.frameSha256(),
                null,
                null,
                null);
        final DefinitelyNotQueued definite = new DefinitelyNotQueued(
                commandRef,
                localProof,
                StableError.of(
                        FailureStage.ENQUEUE,
                        StableCode.BROKER_DEFINITIVE_NOT_PERSISTED,
                        null,
                        commandRef,
                        null,
                        null));
        assertEquals(
                EnqueueOutcomeMessage.definitelyNotQueued(definite),
                EnqueueOutcomeMessage.decode(
                        EnqueueOutcomeMessage.definitelyNotQueued(definite).canonicalBytes()));
        final EnqueueUncertain uncertain = new EnqueueUncertain(
                commandRef,
                nonZero(16, 13),
                StableError.of(
                        FailureStage.ENQUEUE, StableCode.ENQUEUE_RESULT_UNCERTAIN, null, commandRef, null, null));
        assertEquals(
                EnqueueOutcomeMessage.uncertain(uncertain),
                EnqueueOutcomeMessage.decode(
                        EnqueueOutcomeMessage.uncertain(uncertain).canonicalBytes()));

        final byte[] resource = nonZero(32, 14);
        final PulsarBrokerResourceIdentity target =
                new PulsarBrokerResourceIdentity("outcome-pulsar", resource, "persistent://tenant/outcome", 1_400);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("outcome-destination"),
                1,
                Bytes.sha256(Bytes.utf8("outcome-destination-semantic")),
                ProfileKind.DESTINATION);
        final NativePreparedRef nativeRef = new NativePreparedRef(
                nonZero(32, 15),
                Bytes.sha256(Bytes.utf8("outcome-native-submission")),
                destination,
                target,
                2,
                Bytes.sha256(Bytes.utf8("outcome-snapshot")),
                5_000,
                Bytes.sha256(Bytes.utf8("outcome-prepared")));
        final CanonicalCommandQueuedReceipt.PulsarQueuedAck ack = new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                "outcome-pulsar",
                resource,
                "persistent://tenant/outcome",
                1_400,
                2,
                3,
                4,
                0,
                1,
                1_500,
                Bytes.sha256(Bytes.utf8("native-ack")));
        final NativeDeliveryReceipt nativeReceipt = NativeDeliveryReceipt.create(nativeRef, ack, nonZero(16, 16));
        assertEquals(
                SubmissionOutcomeMessage.nativeReceipt(nativeReceipt),
                SubmissionOutcomeMessage.decode(
                        SubmissionOutcomeMessage.nativeReceipt(nativeReceipt).canonicalBytes()));

        final NonPersistenceProof nativeProof = NonPersistenceProof.create(
                NonPersistenceProofKind.PULSAR_GUARD_REJECTION,
                nonZero(16, 17),
                nativeRef.submissionHash(),
                BrokerResourceIdentity.pulsar(target),
                Bytes.sha256(Bytes.utf8("native-request")),
                Bytes.sha256(Bytes.utf8("native-response")));
        final NativeDefinitelyNotQueued nativeDefinite = new NativeDefinitelyNotQueued(
                nativeRef,
                nativeProof,
                StableError.of(
                        FailureStage.ENQUEUE,
                        StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED,
                        null,
                        null,
                        nativeRef,
                        null));
        assertEquals(
                SubmissionOutcomeMessage.nativeDefinitelyNotQueued(nativeDefinite),
                SubmissionOutcomeMessage.decode(SubmissionOutcomeMessage.nativeDefinitelyNotQueued(nativeDefinite)
                        .canonicalBytes()));
        final NativeEnqueueUncertain nativeUncertain = new NativeEnqueueUncertain(
                nativeRef,
                nonZero(16, 18),
                StableError.of(
                        FailureStage.ENQUEUE, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null, null, nativeRef, null));
        assertEquals(
                SubmissionOutcomeMessage.nativeUncertain(nativeUncertain),
                SubmissionOutcomeMessage.decode(SubmissionOutcomeMessage.nativeUncertain(nativeUncertain)
                        .canonicalBytes()));

        final PreparedCommand managedCommand =
                PreparedCommand.cancel(shard, DelayMessageId.random(shard), new MessagePrecondition(null, null), 9_000);
        final PreparedSubmission managedPrepared =
                PreparedSubmission.managed(CommandCodec.encodeManagedFrame(managedCommand));
        assertEquals(managedPrepared, PreparedSubmission.decode(managedPrepared.canonicalBytes()));
        final NativePreparedDelivery nativePrepared = nativePreparedForOutcomeTest();
        final PreparedSubmission nativePreparedSubmission = PreparedSubmission.nativePrepared(nativePrepared);
        assertEquals(nativePreparedSubmission, PreparedSubmission.decode(nativePreparedSubmission.canonicalBytes()));
    }

    @Test
    void payloadUploadAndAttestationResponsesKeepPayloadScopedBranches() throws Exception {
        final ProfileRef objectStore = new ProfileRef(
                Bytes.utf8("payload-object-store"),
                2,
                Bytes.sha256(Bytes.utf8("payload-object-store-semantic")),
                ProfileKind.OBJECT_STORE);
        final OpaquePayloadUploadHandle handle = OpaquePayloadUploadHandle.create(
                nonZero(32, 21), objectStore, UploadHandleKind.OPAQUE_SINGLE_PUT, 9_000, Bytes.utf8("opaque-envelope"));
        final PayloadUploadHandleResponse issued = PayloadUploadHandleResponse.issued(handle);
        assertEquals(issued, PayloadUploadHandleResponse.decode(issued.canonicalBytes()));
        final StableError uploadRetry = StableError.of(
                FailureStage.PAYLOAD, StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE, 8_000L, null, null, null);
        final PayloadUploadHandleResponse unavailable = PayloadUploadHandleResponse.error(
                PayloadUploadHandleOutcome.OBJECT_STORE_UNAVAILABLE_RETRYABLE, uploadRetry);
        assertEquals(unavailable, PayloadUploadHandleResponse.decode(unavailable.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> PayloadUploadHandleResponse.error(PayloadUploadHandleOutcome.INTEGRITY_ERROR, uploadRetry));

        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final CanonicalPayloadCommitProof proof = CanonicalPayloadCommitProof.signed(
                nonZero(32, 22),
                nonZero(32, 23),
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                objectStore,
                1,
                Integer.MIN_VALUE,
                Bytes.utf8("bucket"),
                Bytes.utf8("key"),
                Bytes.utf8("version"),
                new byte[0],
                3,
                Bytes.sha256(Bytes.utf8("payload")),
                7_000,
                keyPair.getPrivate());
        final PayloadAttestationResponse attested = PayloadAttestationResponse.attested(proof);
        assertEquals(attested, PayloadAttestationResponse.decode(attested.canonicalBytes()));
        final StableError notReadyError =
                StableError.of(FailureStage.PAYLOAD, StableCode.OBJECT_NOT_READY_RETRYABLE, 6_000L, null, null, null);
        final PayloadAttestationResponse notReady =
                PayloadAttestationResponse.error(PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE, notReadyError);
        assertEquals(notReady, PayloadAttestationResponse.decode(notReady.canonicalBytes()));

        final byte[] tampered = handle.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> OpaquePayloadUploadHandle.decode(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> PayloadAttestationResponse.error(PayloadAttestationOutcome.INTEGRITY_ERROR, notReadyError));
    }

    @Test
    void publicQueryViewsRejectUnsafeBindingAndNonCanonicalBranchShape() {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination"),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic")),
                ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("capability"),
                1,
                Bytes.sha256(Bytes.utf8("capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
        assertThrows(
                IllegalArgumentException.class,
                () -> new PublicDestinationBindingView(
                        destination,
                        capability,
                        AdapterKind.KAFKA,
                        Bytes.utf8("e\u0301"),
                        0,
                        OrderingMode.BEST_EFFORT));

        final CommandQueryResponse unknown = CommandQueryResponse.unknown();
        final byte[] nonCanonical = unknown.canonicalBytes();
        nonCanonical[nonCanonical.length - 1] = 1;
        assertThrows(IllegalArgumentException.class, () -> CommandQueryResponse.decode(nonCanonical));
    }

    @Test
    void publicClosedUnionTagsRejectHighBitUint32AsInvalidInput() {
        final byte[] highBitError =
                CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 0x8000_0000L));
        assertThrows(IllegalArgumentException.class, () -> PublicQueryError.decode(highBitError));

        final byte[] highBitCommandResult = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, 0x8000_0000L);
            CanonicalProtobuf.bytes(output, 10, new byte[0]);
        });
        assertThrows(IllegalArgumentException.class, () -> CommandQueryResponse.decode(highBitCommandResult));

        final byte[] highBitMessageResult = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, 0x8000_0000L);
            CanonicalProtobuf.bytes(output, 10, new byte[0]);
        });
        assertThrows(IllegalArgumentException.class, () -> MessageQueryResponse.decode(highBitMessageResult));
    }

    @Test
    void preparedCommandRoundTripsThroughCanonicalFrame() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("lane")),
                1000,
                5000,
                OrderingMode.BEST_EFFORT,
                "payload".getBytes(StandardCharsets.UTF_8));
        final PreparedCommand command = PreparedCommand.schedule(shard, intent, 10_000);

        final PreparedCommand decoded = CommandCodec.decodeFrame(CommandCodec.encodeFrame(command));

        assertEquals(command, decoded);
        assertArrayEquals(
                command.commandHash(),
                CommandHash.compute(
                        command.type(),
                        command.commandId(),
                        command.delayMessageId(),
                        command.retryUntilEpochMs(),
                        command.canonicalBody()));
    }

    @Test
    void corruptIdentityAndCrcAreRejected() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final SelfRoutingId id = SelfRoutingId.random(shard);
        final byte[] corrupt = id.bytes();
        corrupt[20] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SelfRoutingId.decode(corrupt));

        final byte[] frame = ShardLogFrame.encode(ShardLogFrame.CLIENT_COMMAND_KIND, new byte[] {1});
        frame[frame.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ShardLogFrame.decode(frame));
    }

    @Test
    void selfRoutingIdRejectsNonUuidV7LogicalLocatorsEvenWithValidCrc() {
        final SelfRoutingId id = SelfRoutingId.random(new ShardId(RouteIncarnation.random(), 0));

        final byte[] wrongVersion = id.bytes();
        wrongVersion[27] = (byte) ((wrongVersion[27] & 0x0f) | 0x60);
        rewriteCrc(wrongVersion);
        assertThrows(IllegalArgumentException.class, () -> SelfRoutingId.decode(wrongVersion));

        final byte[] wrongVariant = id.bytes();
        wrongVariant[29] = (byte) (wrongVariant[29] & 0x3f);
        rewriteCrc(wrongVariant);
        assertThrows(IllegalArgumentException.class, () -> SelfRoutingId.decode(wrongVariant));
    }

    private static void rewriteCrc(final byte[] encoded) {
        System.arraycopy(Bytes.crc32cbe(Arrays.copyOf(encoded, 37)), 0, encoded, 37, 4);
    }

    @Test
    void scheduleBodyIsCanonicalAndDefensive() {
        final DestinationLaneId lane = new DestinationLaneId(new byte[32]);
        final byte[] payload = new byte[] {1, 2, 3};
        final ScheduleIntent intent = new ScheduleIntent(lane, 1, 2, OrderingMode.DELIVERY_TIME_FIFO, payload);
        payload[0] = 9;
        final byte[] encoded = intent.canonicalBytes();
        final ScheduleIntent decoded = CommandBodies.decodeDirectSchedule(encoded);
        assertArrayEquals(new byte[] {1, 2, 3}, decoded.payload());
        assertEquals(intent, decoded);
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandBodies.decodeDirectSchedule(Arrays.copyOf(encoded, encoded.length - 1)));
    }

    @Test
    void stableCodeRegistryIsClosedAndRoundTripsEveryValue() {
        final HashSet<Integer> values = new HashSet<>();
        for (StableCode code : StableCode.values()) {
            org.junit.jupiter.api.Assertions.assertTrue(
                    values.add(code.wireValue()), "duplicate stable code: " + code.wireValue());
            assertEquals(code, StableCode.fromWire(code.wireValue()));
        }
        assertEquals(103, values.size());
        assertThrows(IllegalArgumentException.class, () -> StableCode.fromWire(0x7fff));
    }

    @Test
    void sourceOrderTokenUsesClosedAdapterVariant() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final KafkaSourcePosition kafka =
                new KafkaSourcePosition(shard, "cluster", java.util.UUID.randomUUID(), 9, null, 10);
        assertEquals("010000000000000009", Bytes.hex(kafka.sourceOrderToken()));
        final PulsarSourcePosition pulsar = new PulsarSourcePosition(
                shard, new byte[32], "persistent://t/topic", 1, 2, 3, 4, PulsarSourcePosition.EntryKind.BATCH, 10);
        assertEquals(21, pulsar.sourceOrderToken().length);
        assertEquals(2, pulsar.sourceOrderToken()[0]);
    }

    @Test
    void sourcePositionsRoundTripUnsignedHighBitOffsetsThroughReceiptAndEvidenceCodecs() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition kafka = new KafkaSourcePosition(shard, "cluster", topic, Long.MIN_VALUE, 3, 10);
        assertEquals(kafka, SourcePositionCodec.decode(kafka.canonicalBytes()));
        assertEquals(
                1, kafka.compareWithinShard(new KafkaSourcePosition(shard, "cluster", topic, Long.MAX_VALUE, 3, 10)));

        final PreparedCommand command = schedule(shard, "unsigned-source", 2_000, 8_000, 9_000);
        final byte[] response = Bytes.sha256(Bytes.utf8("unsigned-response"));
        final byte[] attempt = new byte[16];
        attempt[0] = 1;
        final CanonicalCommandQueuedReceipt receipt = CanonicalCommandQueuedReceipt.create(
                command,
                kafka,
                new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                        "cluster", topic, shard.partition(), Long.MIN_VALUE, 3, 10, response),
                9_000,
                attempt);
        assertEquals(receipt, CanonicalCommandQueuedReceipt.decodeFrame(receipt.frame()));

        final EvidenceCursor cursor = EvidenceCursor.kafka(
                new byte[32], new byte[16], uuidBytes(topic), shard.partition(), 1, 10, Long.MIN_VALUE, -1L);
        assertEquals(cursor, EvidenceCursor.decode(cursor.canonicalBytes()));
        assertTrue(cursor.dominates(EvidenceCursor.kafka(
                new byte[32],
                new byte[16],
                uuidBytes(topic),
                shard.partition(),
                1,
                10,
                Long.MAX_VALUE,
                Long.MAX_VALUE)));
    }

    @Test
    void sourcePositionsPreserveUnsignedPartitionLeaderAndBatchFields() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), -1);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition kafka =
                new KafkaSourcePosition(shard, "cluster", topic, Long.MIN_VALUE, Integer.MIN_VALUE, 10);
        assertEquals(kafka, SourcePositionCodec.decode(kafka.canonicalBytes()));
        assertEquals(kafka, QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.encodeSourcePosition(kafka)));

        final PulsarSourcePosition pulsar = new PulsarSourcePosition(
                shard,
                new byte[32],
                "persistent://t/topic",
                Long.MIN_VALUE,
                -1L,
                Integer.MIN_VALUE,
                Integer.MIN_VALUE + 1,
                PulsarSourcePosition.EntryKind.BATCH,
                10);
        assertEquals(pulsar, SourcePositionCodec.decode(pulsar.canonicalBytes()));
        assertEquals(pulsar, QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.encodeSourcePosition(pulsar)));
    }

    @Test
    void sourcePositionsCannotCompareAcrossPhysicalResourceIncarnations() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final java.util.UUID topic = java.util.UUID.randomUUID();
        final KafkaSourcePosition kafkaA = new KafkaSourcePosition(shard, "cluster", topic, 1, null, 10);
        final KafkaSourcePosition kafkaDifferentTopic =
                new KafkaSourcePosition(shard, "cluster", java.util.UUID.randomUUID(), 2, null, 11);
        assertFalse(kafkaA.sameSourceIdentity(kafkaDifferentTopic));
        assertThrows(IllegalArgumentException.class, () -> kafkaA.compareTo(kafkaDifferentTopic));

        final byte[] resource = new byte[32];
        final PulsarSourcePosition pulsarA = new PulsarSourcePosition(
                shard, resource, "persistent://t/a", 1, 1, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 10);
        final byte[] replacementResource = new byte[32];
        replacementResource[0] = 1;
        final PulsarSourcePosition pulsarReplacement = new PulsarSourcePosition(
                shard,
                replacementResource,
                "persistent://t/a",
                1,
                2,
                0,
                1,
                PulsarSourcePosition.EntryKind.NON_BATCH,
                11);
        assertFalse(pulsarA.sameSourceIdentity(pulsarReplacement));
        assertThrows(IllegalArgumentException.class, () -> pulsarA.compareTo(pulsarReplacement));
    }

    @Test
    void sourcePositionDecoderRejectsNonCanonicalUtf8Bytes() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final KafkaSourcePosition kafka =
                new KafkaSourcePosition(shard, "cluster", java.util.UUID.randomUUID(), 3, null, 10);
        final byte[] kafkaBytes = kafka.canonicalBytes();
        final int kafkaClusterByte = 1 + 16 + 4;
        kafkaBytes[kafkaClusterByte] = (byte) 0xc3;
        assertThrows(IllegalArgumentException.class, () -> SourcePositionCodec.decode(kafkaBytes));

        final byte[] resource = Bytes.sha256(Bytes.utf8("source-resource"));
        final PulsarSourcePosition pulsar = new PulsarSourcePosition(
                shard, resource, "persistent://t/topic", 2, 4, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 11);
        final byte[] pulsarBytes = pulsar.canonicalBytes();
        final int topicByte = 1 + 16 + 4 + resource.length + 4;
        pulsarBytes[topicByte] = (byte) 0xc3;
        assertThrows(IllegalArgumentException.class, () -> SourcePositionCodec.decode(pulsarBytes));
    }

    @Test
    void sourcePositionsRejectNonCanonicalTextAtConstruction() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaSourcePosition(shard, "cluster\u0301", topic, 1, null, 10));

        final byte[] resource = Bytes.sha256(Bytes.utf8("source-resource-nfc"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarSourcePosition(
                        shard,
                        resource,
                        "persistent://t/e\u0301",
                        1,
                        1,
                        0,
                        1,
                        PulsarSourcePosition.EntryKind.NON_BATCH,
                        10));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarSourcePosition(
                        shard,
                        resource,
                        "persistent://t/\uD800",
                        1,
                        1,
                        0,
                        1,
                        PulsarSourcePosition.EntryKind.NON_BATCH,
                        10));
    }

    @Test
    void brokerEvidenceAndQueuedAckIdentitiesRejectNonCanonicalUtf8AtConstruction() {
        final UUID topic = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () -> new KafkaBrokerResourceIdentity("cluster\uD800", topic));

        final byte[] resource = Bytes.sha256(Bytes.utf8("broker-resource"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarBrokerResourceIdentity("cluster\uD800", resource, "topic", 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarBrokerResourceIdentity("cluster", resource, "topic\uD800", 1));

        final byte[] lane = Bytes.sha256(Bytes.utf8("evidence-lane"));
        final byte[] incarnation = Bytes.sha256(Bytes.utf8("lane-incarnation"));
        assertThrows(
                IllegalArgumentException.class,
                () -> EvidenceCursor.pulsar(lane, incarnation, resource, 0, 1, 1, "topic\uD800", 1, 1, 1, 0, 1));

        final byte[] response = Bytes.sha256(Bytes.utf8("response"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                        "cluster\uD800", topic, 0, 1, null, 1, response));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                        "cluster", resource, "topic\uD800", 1, 0, 1, 1, 0, 1, 1, response));
    }

    @Test
    void sourcePositionDecoderRejectsTruncatedLengthAndFixedFields() {
        final byte[] truncatedLength = new byte[1 + 16 + 3];
        truncatedLength[0] = (byte) SourcePositionKind.KAFKA.wireValue();
        assertThrows(IllegalArgumentException.class, () -> SourcePositionCodec.decode(truncatedLength));

        final KafkaSourcePosition source = new KafkaSourcePosition(
                new ShardId(RouteIncarnation.random(), 9), "cluster", UUID.randomUUID(), 1, null, 10);
        assertThrows(
                IllegalArgumentException.class,
                () -> SourcePositionCodec.decode(
                        Arrays.copyOf(source.canonicalBytes(), source.canonicalBytes().length - 1)));
    }

    @Test
    void largeScheduleAndPayloadProofAreCanonicalAndSigned() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("large-lane")),
                2_000,
                8_000,
                OrderingMode.BEST_EFFORT,
                123_456,
                Bytes.sha256(Bytes.utf8("payload")),
                10_000,
                Long.MIN_VALUE);
        assertEquals(intent, CommandBodies.decodeDirectPrepareLarge(CommandBodies.prepareLarge(intent)));
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shard, intent, 20_000);
        assertEquals(prepare, CommandCodec.decodeFrame(CommandCodec.encodeFrame(prepare)));

        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final DelayMessageId messageId = prepare.delayMessageId();
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("reservation"));
        final PayloadCommitProof proof = PayloadCommitProof.signed(
                Long.MIN_VALUE,
                Integer.MIN_VALUE,
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                reservationId,
                Bytes.sha256(Bytes.utf8("profile")),
                Bytes.utf8("bucket"),
                Bytes.utf8("object-key"),
                Bytes.utf8("version-1"),
                new byte[0],
                intent.expectedPayloadLength(),
                intent.payloadSha256(),
                12_000,
                keyPair.getPrivate());
        assertTrue(proof.verifySignature(keyPair.getPublic()));
        assertEquals(proof, PayloadCommitProof.decode(proof.canonicalBytes()));
        assertEquals(proof, CommandBodies.decodeDirectCommitLarge(CommandBodies.commitLarge(proof)));
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
        final byte[] author = AuthorIdentity.owner(
                        Bytes.utf8("deployment"), Bytes.utf8("worker"), 42, Bytes.sha256(Bytes.utf8("lease-fence")))
                .canonicalBytes();
        final byte[] body = systemBody(shard, SystemMutationType.PUBLISH_ADMISSION, 25_000);
        final SystemMutation mutation = SystemMutation.signed(
                shard,
                SystemMutationType.PUBLISH_ADMISSION,
                25_000,
                logicalIdentity,
                body,
                author,
                Integer.MIN_VALUE,
                keyPair.getPrivate());

        final SystemMutation decoded = SystemMutation.decodeFrame(mutation.encodeFrame(), logicalIdentity);

        assertEquals(mutation, decoded);
        assertTrue(decoded.verifySignature(keyPair.getPublic()));
        assertArrayEquals(
                mutation.mutationHash(),
                SystemMutation.computeMutationHash(
                        shard, mutation.type(), mutation.retryUntilEpochMs(), mutation.canonicalBody()));
    }

    @Test
    void systemMutationReplayDecoderDerivesTimeFenceIdentityFromBody() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                2_000,
                2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("time-fence-evidence")),
                0,
                null);
        final int keyVersion = 1;
        final long closeThrough = 1_000;
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(keyVersion),
                Bytes.lp32(evidence.canonicalBytes()));
        final byte[] subject = new ShardSubject(shard).canonicalBytes();
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, closeThrough);
            CanonicalProtobuf.uint32Bits(output, 11, keyVersion);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, evidence.canonicalBytes());
        });
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final SystemMutation mutation = SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                9_000,
                proofId,
                body,
                AuthorIdentity.fence(Bytes.utf8("fence"), keyVersion).canonicalBytes(),
                keyVersion,
                keyPair.getPrivate());

        assertEquals(mutation, SystemMutation.decodeFrame(mutation.encodeFrame()));
    }

    @Test
    void systemMutationRejectsWrongIdentityAndTampering() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] logicalIdentity = Bytes.sha256(Bytes.utf8("control-target"));
        final byte[] author = AuthorIdentity.control(
                        Bytes.sha256(Bytes.utf8("actor")),
                        Bytes.sha256(Bytes.utf8("roles")),
                        Bytes.sha256(Bytes.utf8("scope")))
                .canonicalBytes();
        final byte[] body = systemBody(shard, SystemMutationType.APPLY_SHARD_CONTROL, 30_000);
        final SystemMutation mutation = SystemMutation.signed(
                shard,
                SystemMutationType.APPLY_SHARD_CONTROL,
                30_000,
                logicalIdentity,
                body,
                author,
                1,
                keyPair.getPrivate());

        assertThrows(
                IllegalArgumentException.class,
                () -> SystemMutation.decodeFrame(mutation.encodeFrame(), Bytes.sha256(Bytes.utf8("other"))));
        final byte[] tampered = mutation.encodeFrame();
        tampered[tampered.length - 5] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> SystemMutation.decodeFrame(tampered, logicalIdentity));
    }

    @Test
    void authorIdentityBranchMustMatchSystemMutationType() {
        final byte[] owner = AuthorIdentity.owner(
                        Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, Bytes.sha256(Bytes.utf8("lease")))
                .canonicalBytes();
        assertEquals(AuthorIdentity.Kind.OWNER, AuthorIdentity.decode(owner).kind());
        assertThrows(IllegalArgumentException.class, () -> AuthorIdentity.decode(owner)
                .requireFor(SystemMutationType.APPLY_SHARD_CONTROL));
    }

    @Test
    void ownerAndWriterGenerationsPreserveUnsignedBitPatterns() {
        final long highBit = Long.MIN_VALUE;
        final AuthorIdentity owner = AuthorIdentity.decode(AuthorIdentity.owner(
                        Bytes.utf8("deployment"),
                        Bytes.utf8("worker"),
                        highBit,
                        Bytes.sha256(Bytes.utf8("owner-lease")))
                .canonicalBytes());
        final AuthorIdentity fence = AuthorIdentity.decode(
                AuthorIdentity.fence(Bytes.utf8("fence"), highBit).canonicalBytes());
        final AuthorIdentity service =
                AuthorIdentity.decode(AuthorIdentity.service(Bytes.utf8("service"), Bytes.utf8("service-run"), highBit)
                        .canonicalBytes());
        final OwnerIdentity typedOwner = OwnerIdentity.decode(new OwnerIdentity(
                        Bytes.utf8("deployment"),
                        Bytes.utf8("worker"),
                        highBit,
                        Bytes.sha256(Bytes.utf8("typed-owner-lease")))
                .canonicalBytes());

        assertEquals(highBit, owner.generation());
        assertEquals(highBit, fence.generation());
        assertEquals(highBit, service.generation());
        assertEquals(highBit, typedOwner.ownerEpoch());
    }

    @Test
    void systemMutationBodyPrefixCannotDriftFromOuterEnvelope() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] author = AuthorIdentity.owner(
                        Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, Bytes.sha256(Bytes.utf8("lease")))
                .canonicalBytes();
        final byte[] mismatchedType = systemBody(shard, SystemMutationType.PUBLISH_OUTCOME, 1_000);
        assertThrows(
                IllegalArgumentException.class,
                () -> SystemMutation.signed(
                        shard,
                        SystemMutationType.PUBLISH_ADMISSION,
                        1_000,
                        Bytes.sha256(Bytes.utf8("logical")),
                        mismatchedType,
                        author,
                        1,
                        keyPair.getPrivate()));
    }

    @Test
    void trustedUtcEvidenceRoundTripsAndEnforcesSignedSourceShape() {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                1_000,
                1_005,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("host-a"),
                3,
                7,
                9,
                Bytes.sha256(Bytes.utf8("sample")),
                0,
                null);
        evidence.requireEarliestAtLeast(1_000);
        evidence.requireWidthAtMost(5);
        assertArrayEquals(
                evidence.canonicalBytes(),
                TrustedUtcIntervalEvidence.decode(evidence.canonicalBytes()).canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TrustedUtcIntervalEvidence(
                        1_000,
                        1_005,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("host-a"),
                        3,
                        7,
                        9,
                        Bytes.sha256(Bytes.utf8("sample")),
                        2,
                        new byte[64]));
        assertThrows(IllegalArgumentException.class, () -> evidence.requireEarliestAtLeast(1_001));
    }

    @Test
    void trustedUtcEvidencePreservesUnsignedCounterBitPatterns() {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                1_000,
                1_005,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("host-high-bit"),
                Long.MIN_VALUE,
                -1L,
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("sample-high-bit")),
                0,
                null);
        final TrustedUtcIntervalEvidence decoded = TrustedUtcIntervalEvidence.decode(evidence.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.sourceConfigGeneration());
        assertEquals(-1L, decoded.sampleSequence());
        assertEquals(Long.MIN_VALUE, decoded.monotonicAnchorNs());
        assertArrayEquals(evidence.canonicalBytes(), decoded.canonicalBytes());
    }

    @Test
    void trustedUtcEvidencePreservesUnsignedSourceKeyVersionBits() {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                1_000,
                1_005,
                TrustedUtcIntervalEvidence.Source.SIGNED_TIME_SERVICE,
                Bytes.utf8("signed-time-service"),
                3,
                7,
                9,
                Bytes.sha256(Bytes.utf8("signed-time-evidence")),
                Integer.MIN_VALUE,
                new byte[64]);
        final TrustedUtcIntervalEvidence decoded = TrustedUtcIntervalEvidence.decode(evidence.canonicalBytes());
        assertEquals(Integer.MIN_VALUE, decoded.sourceKeyVersion());
        assertArrayEquals(evidence.canonicalBytes(), decoded.canonicalBytes());
    }

    @Test
    void uint32DecodersRejectAUint64VarintOutsideTheUint32Domain() {
        final byte[] encoded =
                CanonicalProtobuf.message(output -> CanonicalProtobuf.uint64Bits(output, 1, Long.MIN_VALUE));
        final CanonicalProtobuf.Reader.Field field = new CanonicalProtobuf.Reader(encoded).next();

        assertThrows(IllegalArgumentException.class, () -> QueryCodecSupport.uint32(field, 1));
        assertThrows(IllegalArgumentException.class, () -> QueryCodecSupport.uint32Bits(field, 1));
        assertThrows(IllegalArgumentException.class, () -> QueryCodecSupport.bool(field, 1));
    }

    @Test
    void canonicalReaderRejectsHighBitLengthPrefixes() {
        final byte[] malformed = new byte[] {
            0x0a,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            0x01
        };
        assertThrows(IllegalArgumentException.class, () -> new CanonicalProtobuf.Reader(malformed).next());
    }

    @Test
    void queuedReceiptRejectsHighBitInt64TimingFields() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final PreparedCommand command = schedule(shard, "queued-timing", 2_000, 8_000, 9_000);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-a", topic, 7, 3, 1_234);
        final byte[] response = Bytes.sha256(Bytes.utf8("queued-timing-response"));
        final CanonicalCommandQueuedReceipt receipt = CanonicalCommandQueuedReceipt.create(
                command,
                source,
                new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                        "cluster-a", topic, shard.partition(), 7, 3, 1_234, response),
                9_000,
                new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
        final var fields = QueryCodecSupport.read(receipt.payload(), "CanonicalCommandQueuedReceipt");
        final byte[] malformed = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, fields.get(1).rawValue());
            CanonicalProtobuf.bytes(output, 3, fields.get(2).rawValue());
            CanonicalProtobuf.bytes(output, 4, fields.get(3).rawValue());
            CanonicalProtobuf.uint64Bits(output, 5, Long.MIN_VALUE);
            CanonicalProtobuf.uint64Bits(output, 6, fields.get(5).unsignedValue());
            CanonicalProtobuf.bytes(output, 7, fields.get(6).rawValue());
            CanonicalProtobuf.bytes(output, 8, fields.get(7).rawValue());
        });

        assertThrows(IllegalArgumentException.class, () -> CanonicalCommandQueuedReceipt.decodePayload(malformed));
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
                    CanonicalProtobuf.bytes(
                            output, 15, DelayMessageId.random(shard).bytes());
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

    private static PublicDestinationBindingView publicBinding() {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination"),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic")),
                ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("capability"),
                1,
                Bytes.sha256(Bytes.utf8("capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
        return new PublicDestinationBindingView(
                destination,
                capability,
                AdapterKind.KAFKA,
                Bytes.utf8("safe-destination"),
                2,
                OrderingMode.BEST_EFFORT);
    }

    private static NativePreparedDelivery nativePreparedForOutcomeTest() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resource = nonZero(32, 19);
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "prepared-outcome", resource, "persistent://tenant/prepared-outcome", 1_900);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("prepared-outcome-destination"),
                1,
                Bytes.sha256(Bytes.utf8("prepared-outcome-destination-semantic")),
                ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("prepared-outcome-capability"),
                1,
                Bytes.sha256(Bytes.utf8("prepared-outcome-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_000,
                2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("prepared-outcome-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("prepared-outcome-sample")),
                0,
                null);
        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destination,
                capability,
                target,
                0,
                Bytes.sha256(Bytes.utf8("prepared-outcome-guard")),
                1,
                1,
                Bytes.sha256(Bytes.utf8("prepared-outcome-binding")),
                Bytes.sha256(Bytes.utf8("prepared-outcome-fingerprint")),
                Bytes.sha256(Bytes.utf8("prepared-outcome-scope")),
                issuedAt,
                3_000,
                1,
                keyPair.getPrivate());
        return NativePreparedDelivery.create(
                nonZero(32, 20),
                destination,
                capability,
                target,
                0,
                Bytes.utf8("prepared-outcome-payload"),
                new PulsarMetadata(null, null, null, java.util.List.of()),
                null,
                2_100,
                2_200,
                snapshot);
    }

    private static PreparedCommand schedule(
            final ShardId shard, final String lane, final long deliverAt, final long expireAt, final long retryUntil) {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination-" + lane),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + lane)),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy =
                new RetryPolicyRef(Bytes.utf8("retry-" + lane), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + lane)));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                deliverAt,
                expireAt,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, java.util.List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, retryUntil);
    }

    private static PreparedCommand cancel(final ShardId shard, final long retryUntil) {
        return PreparedCommand.cancel(
                shard, DelayMessageId.random(shard), new MessagePrecondition(0L, null), retryUntil);
    }

    private static byte[] nonZero(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }

    private static byte[] nestedPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, new byte[] {1}));
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }
}
