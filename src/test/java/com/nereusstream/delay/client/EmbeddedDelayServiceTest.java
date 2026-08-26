package com.nereusstream.delay.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import com.nereusstream.delay.adapter.ControlOperationQueryPolicy;
import com.nereusstream.delay.adapter.InMemoryPayloadObjectStore;
import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandAppliedReceipt;
import com.nereusstream.delay.protocol.CommandBodies;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandQueryResult;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlOperationQueryResult;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationRequest;
import com.nereusstream.delay.protocol.ControlOperationState;
import com.nereusstream.delay.protocol.ControlReason;
import com.nereusstream.delay.protocol.ControlReasonKind;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.EnqueueOutcomeKind;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessage;
import com.nereusstream.delay.protocol.ForceCheckpointRequest;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.MessageQueryResult;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleOutcome;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PublicDestinationBindingView;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.MessageQuerySnapshot;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.PayloadAvailability;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EmbeddedDelayServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void delayClientPreparesStrictCommandsWithoutIo() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 25);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("prepare")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final DelayClient client = service;
            final PreparedCommand schedule =
                    client.prepareSchedule(scheduleIntent("prepare-lane", 2_000, 5_000, "payload"), 10_000);
            assertEquals(
                    schedule.delayMessageId(),
                    CommandBodies.decodeSchedule(schedule.canonicalBody()).delayMessageId());

            final PreparedCommand cancel =
                    client.prepareCancel(schedule.delayMessageId(), new MessagePrecondition(0L, null), 10_000);
            assertEquals(
                    schedule.delayMessageId(),
                    CommandBodies.decodeCancel(cancel.canonicalBody()).delayMessageId());

            final PreparedCommand reschedule = client.prepareReschedule(
                    schedule.delayMessageId(), new MessagePrecondition(0L, null), 2_500, 5_500, 10_000);
            assertEquals(
                    schedule.delayMessageId(),
                    CommandBodies.decodeReschedule(reschedule.canonicalBody()).delayMessageId());
        }
    }

    @Test
    void directIngressAcceptsBothCurrentPreparationShapes() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 26);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("ingress")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final DelayClient client = service;
            final PreparedCommand initial =
                    client.prepareSchedule(scheduleIntent("ingress-lane", 2_000, 5_000, "payload"), 10_000);
            final PreparedCommand direct = client.prepareSchedule(
                    new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("direct-ingress-lane")),
                            2_000,
                            5_000,
                            OrderingMode.BEST_EFFORT,
                            Bytes.utf8("payload")),
                    10_000);

            final EnqueueOutcome queued =
                    client.enqueue(initial).toCompletableFuture().join();
            final EnqueueOutcome directQueued =
                    client.enqueue(direct).toCompletableFuture().join();

            assertEquals(EnqueueStatus.QUEUED, queued.status());
            assertEquals(EnqueueStatus.QUEUED, directQueued.status());
            assertEquals(2, service.pendingCommandCount());
        }
    }

    @Test
    void managedPreparedSubmissionKeepsStrictBranchAndAttemptFence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 27);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("managed-submission")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final DelayClient client = service;
            final PreparedCommand command =
                    client.prepareCancel(DelayMessageId.random(shard), new MessagePrecondition(0L, null), 10_000);
            final PreparedSubmission submission = client.prepareManagedSubmission(command);
            assertEquals(submission, PreparedSubmission.decode(submission.canonicalBytes()));

            final var invalidAttempt = client.submit(submission, 10_000, new byte[16])
                    .toCompletableFuture()
                    .join();
            assertEquals(SubmissionOutcomeKind.MANAGED, invalidAttempt.kind());
            assertEquals(
                    StableCode.INVALID_PREPARED_COMMAND,
                    invalidAttempt.managed().definitelyNotQueued().error().code());

            final byte[] attempt = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("managed-submit-attempt")), 16);
            final var queued = client.submit(submission, 10_000, attempt)
                    .toCompletableFuture()
                    .join();
            assertEquals(SubmissionOutcomeKind.MANAGED, queued.kind());
            assertEquals(EnqueueOutcomeKind.QUEUED, queued.managed().kind());
            assertEquals(
                    command.commandId(), queued.managed().queued().command().commandId());
            assertArrayEquals(attempt, queued.managed().queued().physicalEnqueueAttemptId());
        }
    }

    @Test
    void queuedReceiptIsNotAppliedReceipt() {
        final long now = 1_000;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC);
        try (EmbeddedDelayService service =
                new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir), shard, clock)) {
            final var command = service.prepareSchedule(
                    new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("embedded-lane")),
                            2_000,
                            5_000,
                            OrderingMode.BEST_EFFORT,
                            Bytes.utf8("payload")),
                    10_000);
            final EnqueueOutcome outcome =
                    service.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, outcome.status());
            assertNull(service.shard().getCommandResult(command.commandId()));

            final CommandResultView result = new CommandResultView(service.awaitApplied(outcome.receipt())
                    .toCompletableFuture()
                    .join()
                    .stableCode());
            assertEquals(StableCode.SCHEDULED, result.code());
        }
    }

    @Test
    void prepareLargePayloadCommitBindsReceiptAndProof() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 22);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] payload = Bytes.utf8("payload");
        final byte[] payloadHash = Bytes.sha256(payload);
        final ProfileRef objectStoreProfile = new ProfileRef(
                Bytes.utf8("object-store"),
                1,
                Bytes.sha256(Bytes.utf8("object-store-semantic")),
                ProfileKind.OBJECT_STORE);
        final PayloadProofTrustSetRef trustSet =
                new PayloadProofTrustSetRef(1, Bytes.sha256(Bytes.utf8("payload-trust-set")));
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shard, "embedded", UUID.randomUUID(), 3, null, 1_000);
        final PayloadReservationReceipt receipt = PayloadReservationReceipt.create(
                Bytes.sha256(Bytes.utf8("reservation")),
                messageId,
                shard,
                source,
                1,
                objectStoreProfile,
                Bytes.utf8("container"),
                Bytes.utf8("object-key"),
                payload.length,
                payloadHash,
                5_000,
                trustSet);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final CanonicalPayloadCommitProof proof = CanonicalPayloadCommitProof.signed(
                receipt.reservationId(),
                Bytes.sha256(Bytes.utf8("tenant-scope")),
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                objectStoreProfile,
                trustSet.version(),
                1,
                receipt.container(),
                receipt.objectKey(),
                Bytes.utf8("sha256-version"),
                payloadHash,
                payload.length,
                payloadHash,
                4_500,
                keyPair.getPrivate());

        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("prepare-large-commit")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand prepared = service.prepareLargePayloadCommit(receipt, proof, 10_000);
            assertEquals(messageId, prepared.delayMessageId());
            assertEquals(
                    proof,
                    CommandBodies.decodeCommitLarge(prepared.canonicalBody()).proof());

            final CanonicalPayloadCommitProof drifted = CanonicalPayloadCommitProof.signed(
                    receipt.reservationId(),
                    Bytes.sha256(Bytes.utf8("tenant-scope")),
                    shard.routeIncarnation().bytes(),
                    shard.partition(),
                    messageId,
                    objectStoreProfile,
                    trustSet.version(),
                    1,
                    receipt.container(),
                    Bytes.utf8("different-object-key"),
                    Bytes.utf8("sha256-version"),
                    payloadHash,
                    payload.length,
                    payloadHash,
                    4_500,
                    keyPair.getPrivate());
            assertThrows(
                    IllegalArgumentException.class, () -> service.prepareLargePayloadCommit(receipt, drifted, 10_000));
        }
    }

    @Test
    void payloadClientWithoutLocalObjectStoreReturnsTypedRetryableOutcome() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 23);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("payload-store-unavailable")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PayloadUploadHandleResponse handle = service.issuePayloadUploadHandle(
                            null, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadUploadHandleOutcome.OBJECT_STORE_UNAVAILABLE_RETRYABLE, handle.outcome());
            assertEquals(
                    StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE,
                    handle.error().code());
            final PayloadAttestationResponse attestation = service.attestPayloadUpload(null, null, 1_000)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadAttestationOutcome.OBJECT_STORE_UNAVAILABLE_RETRYABLE, attestation.outcome());
            assertEquals(
                    StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE,
                    attestation.error().code());
            final PayloadUploadHandleResponse negativeHandle = service.issuePayloadUploadHandle(
                            null, UploadHandleKind.OPAQUE_SINGLE_PUT, -1)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadUploadHandleOutcome.INTEGRITY_ERROR, negativeHandle.outcome());
            assertEquals(StableCode.INTEGRITY_ERROR, negativeHandle.error().code());
            final PayloadAttestationResponse negativeAttestation = service.attestPayloadUpload(null, null, -1)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadAttestationOutcome.INTEGRITY_ERROR, negativeAttestation.outcome());
            assertEquals(StableCode.INTEGRITY_ERROR, negativeAttestation.error().code());
        }
    }

    @Test
    void receiptBoundPayloadFacadeRereadsTheShardReservation() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PayloadProofTrustSetSemantic trustSet = payloadTrustSet(keyPair);
        final ProfileSemanticEnvelope profile = payloadObjectStoreProfile();
        final byte[] payload = new byte[(1 << 20) + 1];
        payload[0] = 1;
        final InMemoryPayloadObjectStore objectStore = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), trustSet, 7, 500, keyPair.getPrivate());
        final ShardId shard = new ShardId(RouteIncarnation.random(), 24);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("payload-facade-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                payload.length,
                Bytes.sha256(payload),
                4_000,
                trustSet.version());
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("payload-facade")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
                EmbeddedDelayServiceConfig.defaults(),
                objectStore)) {
            final PreparedCommand prepare = service.prepareLargeSchedule(intent, 10_000);
            final EnqueueOutcome queued =
                    service.enqueue(prepare).toCompletableFuture().join();
            final CommandResult applied =
                    service.awaitApplied(queued.receipt()).toCompletableFuture().join();
            assertEquals(StableCode.OK, applied.stableCode());
            final byte[] reservationId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-reservation-id\0"),
                    prepare.commandId().bytes(),
                    prepare.delayMessageId().bytes(),
                    prepare.commandHash());
            final var reservation = service.shard().getReservation(reservationId);
            objectStore.register(reservation);
            final PayloadReservationReceipt receipt = objectStore.reservationReceipt(reservation);

            final PayloadUploadHandleResponse handle = service.issuePayloadUploadHandle(
                            receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadUploadHandleOutcome.ISSUED, handle.outcome());
            objectStore.upload(receipt, handle.issued(), payload, 1_101);
            final PayloadAttestationResponse attestation = service.attestPayloadUpload(receipt, handle.issued(), 1_102)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadAttestationOutcome.ATTESTED, attestation.outcome());
            assertEquals(EnqueueStatus.QUEUED, queued.status());
        }
    }

    @Test
    void payloadFacadeMapsSourceOrderedReservationCloseToTypedOutcome() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PayloadProofTrustSetSemantic trustSet = payloadTrustSet(keyPair);
        final ProfileSemanticEnvelope profile = payloadObjectStoreProfile();
        final byte[] payload = new byte[(1 << 20) + 1];
        final InMemoryPayloadObjectStore objectStore = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), trustSet, 7, keyPair.getPrivate());
        final ShardId shard = new ShardId(RouteIncarnation.random(), 29);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("payload-facade-close-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                payload.length,
                Bytes.sha256(payload),
                4_000,
                trustSet.version());
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("payload-facade-close")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
                EmbeddedDelayServiceConfig.defaults(),
                objectStore)) {
            final PreparedCommand prepare = service.prepareLargeSchedule(intent, 10_000);
            final EnqueueOutcome queuedPrepare =
                    service.enqueue(prepare).toCompletableFuture().join();
            assertEquals(
                    StableCode.OK,
                    service.awaitApplied(queuedPrepare.receipt())
                            .toCompletableFuture()
                            .join()
                            .stableCode());
            final var reservation = service.shard()
                    .getReservation(Bytes.sha256(
                            Bytes.utf8("nereus-delay-reservation-id\0"),
                                    prepare.commandId().bytes(),
                            prepare.delayMessageId().bytes(), prepare.commandHash()));
            objectStore.register(reservation);
            final PayloadReservationReceipt receipt = objectStore.reservationReceipt(reservation);

            final PreparedCommand cancel = service.prepareCancel(prepare.delayMessageId(), 0, 10_000);
            final EnqueueOutcome queuedCancel =
                    service.enqueue(cancel).toCompletableFuture().join();
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_ABANDONED,
                    service.awaitApplied(queuedCancel.receipt())
                            .toCompletableFuture()
                            .join()
                            .stableCode());

            final PayloadUploadHandleResponse result = service.issuePayloadUploadHandle(
                            receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadUploadHandleOutcome.RESERVATION_ABANDONED, result.outcome());
            assertEquals(StableCode.RESERVATION_ABANDONED, result.error().code());
        }
    }

    @Test
    void payloadFacadeMapsLocalReservationBindingFailureAsIntegrityError() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PayloadProofTrustSetSemantic reservationTrustSet = payloadTrustSet(keyPair);
        final PayloadProofTrustSetSemantic adapterTrustSet = new PayloadProofTrustSetSemantic(
                10, List.of(PayloadProofVerifierKey.fromPublicKey(7, keyPair.getPublic(), 0, 9_000)));
        final ProfileSemanticEnvelope profile = payloadObjectStoreProfile();
        final InMemoryPayloadObjectStore mismatchedAdapter = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), adapterTrustSet, 7, keyPair.getPrivate());
        final InMemoryPayloadObjectStore receiptProjector = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), reservationTrustSet, 7, keyPair.getPrivate());
        final byte[] payload = new byte[(1 << 20) + 1];
        payload[0] = 1;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 28);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("payload-binding-failure-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                payload.length,
                Bytes.sha256(payload),
                4_000,
                reservationTrustSet.version());
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("payload-binding-failure")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
                EmbeddedDelayServiceConfig.defaults(),
                mismatchedAdapter)) {
            final PreparedCommand prepare = service.prepareLargeSchedule(intent, 10_000);
            final EnqueueOutcome queued =
                    service.enqueue(prepare).toCompletableFuture().join();
            assertEquals(
                    StableCode.OK,
                    service.awaitApplied(queued.receipt())
                            .toCompletableFuture()
                            .join()
                            .stableCode());
            final byte[] reservationId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-reservation-id\0"),
                    prepare.commandId().bytes(),
                    prepare.delayMessageId().bytes(),
                    prepare.commandHash());
            final var reservation = service.shard().getReservation(reservationId);
            receiptProjector.register(reservation);
            final PayloadReservationReceipt receipt = receiptProjector.reservationReceipt(reservation);

            final PayloadUploadHandleResponse handle = service.issuePayloadUploadHandle(
                            receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadUploadHandleOutcome.INTEGRITY_ERROR, handle.outcome());
            assertEquals(StableCode.INTEGRITY_ERROR, handle.error().code());
            final PayloadAttestationResponse attestation = service.attestPayloadUpload(receipt, null, 1_100)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, attestation.outcome());

            final PayloadAttestationResponse attestationWithHandle = service.attestPayloadUpload(
                            receipt,
                            OpaquePayloadUploadHandle.create(
                                    receipt.reservationId(),
                                    profile.ref(),
                                    UploadHandleKind.OPAQUE_SINGLE_PUT,
                                    2_000,
                                    Bytes.utf8("handle")),
                            1_100)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadAttestationOutcome.INTEGRITY_ERROR, attestationWithHandle.outcome());
            assertEquals(
                    StableCode.INTEGRITY_ERROR, attestationWithHandle.error().code());
        }
    }

    @Test
    void payloadFacadeRejectsAdapterSemanticDriftFromDurablePrepareBinding() throws Exception {
        final KeyPair pinnedKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final KeyPair foreignKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PayloadProofTrustSetSemantic pinnedTrustSet = payloadTrustSet(pinnedKey);
        final PayloadProofTrustSetSemantic foreignTrustSet = payloadTrustSet(foreignKey);
        final ProfileSemanticEnvelope profile = payloadObjectStoreProfile();
        final byte[] payload = new byte[(1 << 20) + 1];
        payload[0] = 1;
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 46);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("payload-semantic-drift"));
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("payload-semantic-drift-lane"));
        final ScheduleResolver resolver = new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final DelayMessageId messageId,
                    final CanonicalScheduleIntent intent,
                    final com.nereusstream.delay.protocol.SourcePosition position) {
                throw new AssertionError("Prepare regression must not resolve Schedule");
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final DelayMessageId messageId,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                    final com.nereusstream.delay.protocol.SourcePosition position) {
                return new ResolvedPrepare(lane, Bytes.utf8("payload-semantic-drift-lane"));
            }
        };
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.forPrepare(
                new ProfileRef(
                        Bytes.utf8("payload-destination"),
                        1,
                        Bytes.sha256(Bytes.utf8("payload-destination-semantic")),
                        ProfileKind.DESTINATION),
                new RetryPolicyRef(Bytes.utf8("payload-retry"), 1, Bytes.sha256(Bytes.utf8("payload-retry-semantic"))),
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(
                shardId,
                intent,
                payload.length,
                Bytes.sha256(payload),
                4_000,
                pinnedTrustSet.ref(),
                profile.ref(),
                10_000);
        final KafkaSourcePosition preparePosition = new KafkaSourcePosition(
                shardId, "embedded", UUID.nameUUIDFromBytes(Bytes.utf8("embedded-command-topic")), 0, null, 1_000);
        final PayloadReservation reservation;

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(StableCode.OK, shard.apply(prepare, preparePosition).stableCode());
            reservation = shard.getReservation(Bytes.sha256(
                    Bytes.utf8("nereus-delay-reservation-id\0"),
                    prepare.commandId().bytes(),
                    prepare.delayMessageId().bytes(),
                    prepare.commandHash()));
        }

        final InMemoryPayloadObjectStore receiptProjector = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), pinnedTrustSet, 7, pinnedKey.getPrivate());
        receiptProjector.register(reservation, pinnedTrustSet.ref(), profile.ref());
        final PayloadReservationReceipt receipt = receiptProjector.reservationReceipt(reservation);
        final InMemoryPayloadObjectStore foreignAdapter = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), foreignTrustSet, 7, foreignKey.getPrivate());

        assertEquals(pinnedTrustSet.version(), foreignTrustSet.version());
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                config,
                shardId,
                Clock.fixed(Instant.ofEpochMilli(1_100), ZoneOffset.UTC),
                EmbeddedDelayServiceConfig.defaults(),
                foreignAdapter)) {
            final PayloadUploadHandleResponse result = service.issuePayloadUploadHandle(
                            receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                    .toCompletableFuture()
                    .join();
            assertEquals(PayloadUploadHandleOutcome.INTEGRITY_ERROR, result.outcome());
            assertEquals(StableCode.INTEGRITY_ERROR, result.error().code());
            assertEquals(
                    PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                    foreignAdapter
                            .issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                            .outcome());
        }
    }

    @Test
    void enqueueBatchReturnsIndependentOutcomesInInputOrder() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 2);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("batch-enqueue")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand first = schedule(shard, "batch-first", 2_000, 5_000, 10_000);
            final PreparedCommand foreign = schedule(foreignShard, "batch-foreign", 2_000, 5_000, 10_000);
            final PreparedCommand last = schedule(shard, "batch-last", 2_000, 5_000, 10_000);

            final List<EnqueueOutcome> outcomes = service.enqueueBatch(List.of(first, foreign, last))
                    .toCompletableFuture()
                    .join();

            assertEquals(3, outcomes.size());
            assertEquals(first, outcomes.get(0).preparedCommand());
            assertEquals(EnqueueStatus.QUEUED, outcomes.get(0).status());
            assertEquals(foreign, outcomes.get(1).preparedCommand());
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, outcomes.get(1).status());
            assertEquals(last, outcomes.get(2).preparedCommand());
            assertEquals(EnqueueStatus.QUEUED, outcomes.get(2).status());
            assertEquals(2, service.pendingCommandCount());
            assertEquals(0L, ((KafkaSourcePosition) outcomes.get(0).receipt().sourcePosition()).offset());
            assertEquals(1L, ((KafkaSourcePosition) outcomes.get(2).receipt().sourcePosition()).offset());
        }
    }

    @Test
    void awaitAppliedRejectsForeignSourceBeforeDraining() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 34);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("receipt-source-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"));
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("receipt-source")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = service.prepareSchedule(intent, 10_000);
            final EnqueueOutcome queued =
                    service.enqueue(command).toCompletableFuture().join();
            final CommandQueuedReceipt foreign = new CommandQueuedReceipt(
                    command.commandId(),
                    command.delayMessageId(),
                    shard,
                    new KafkaSourcePosition(shard, "foreign-cluster", UUID.randomUUID(), 0, null, 1_000));

            assertThrows(IllegalArgumentException.class, () -> service.awaitApplied(foreign));
            assertEquals(1, service.pendingCommandCount());
            assertEquals(EnqueueStatus.QUEUED, queued.status());
        }
    }

    @Test
    void awaitAppliedRejectsSameShardReceiptWithWrongPhysicalPositionBeforeDraining() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 37);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("await-position-fence")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final EnqueueOutcome first =
                    service.enqueue(cancel(shard, 10_000)).toCompletableFuture().join();
            final EnqueueOutcome target =
                    service.enqueue(cancel(shard, 10_000)).toCompletableFuture().join();
            final CommandQueuedReceipt forged = new CommandQueuedReceipt(
                    target.preparedCommand().commandId(),
                    target.preparedCommand().delayMessageId(),
                    shard,
                    first.receipt().sourcePosition());

            assertThrows(IllegalArgumentException.class, () -> service.awaitApplied(forged));
            assertEquals(2, service.pendingCommandCount());
            assertNull(service.shard().getCommandResult(target.preparedCommand().commandId()));

            final CommandResult applied =
                    service.awaitApplied(target.receipt()).toCompletableFuture().join();
            assertEquals(ApplyStatus.APPLIED, applied.applyStatus());
        }
    }

    @Test
    void awaitAppliedReturnsTheExactPendingPhysicalConflictResult() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 38);
        final ScheduleIntent firstIntent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("await-conflict-first")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("first"));
        final ScheduleIntent conflictingIntent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("await-conflict-second")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("second"));
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("await-physical-conflict")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand first = service.prepareSchedule(firstIntent, 10_000);
            final EnqueueOutcome firstOutcome =
                    service.enqueue(first).toCompletableFuture().join();
            final PreparedCommand conflicting = PreparedCommand.create(
                    shard,
                    first.commandId(),
                    first.delayMessageId(),
                    first.type(),
                    first.retryUntilEpochMs(),
                    CommandBodies.schedule(conflictingIntent));
            final EnqueueOutcome conflictingOutcome =
                    service.enqueue(conflicting).toCompletableFuture().join();

            final CommandResult physical = service.awaitApplied(conflictingOutcome.receipt())
                    .toCompletableFuture()
                    .join();

            assertEquals(ApplyStatus.REJECTED, physical.applyStatus());
            assertEquals(StableCode.COMMAND_ID_CONFLICT, physical.stableCode());
            assertEquals(
                    StableCode.SCHEDULED,
                    service.shard().getCommandResult(first.commandId()).stableCode());
            assertEquals(0, service.pendingCommandCount());
            assertEquals(EnqueueStatus.QUEUED, firstOutcome.status());
        }
    }

    @Test
    void awaitAppliedReturnsTheExactPhysicalConflictAfterAnExplicitDrain() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 39);
        final ScheduleIntent firstIntent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("await-drained-conflict-first")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("first"));
        final ScheduleIntent conflictingIntent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("await-drained-conflict-second")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("second"));
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("await-drained-physical-conflict")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand first = service.prepareSchedule(firstIntent, 10_000);
            service.enqueue(first).toCompletableFuture().join();
            final PreparedCommand conflicting = PreparedCommand.create(
                    shard,
                    first.commandId(),
                    first.delayMessageId(),
                    first.type(),
                    first.retryUntilEpochMs(),
                    CommandBodies.schedule(conflictingIntent));
            final EnqueueOutcome conflictingOutcome =
                    service.enqueue(conflicting).toCompletableFuture().join();

            service.drain();

            final CommandResult physical = service.awaitApplied(conflictingOutcome.receipt())
                    .toCompletableFuture()
                    .join();
            assertEquals(ApplyStatus.REJECTED, physical.applyStatus());
            assertEquals(StableCode.COMMAND_ID_CONFLICT, physical.stableCode());
            assertEquals(
                    StableCode.SCHEDULED,
                    service.shard().getCommandResult(first.commandId()).stableCode());
        }
    }

    @Test
    void queuedReceiptRejectsMessageIdFromAnotherShard() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 35);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 36);
        final CommandId commandId = CommandId.random(shard);
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shard, "embedded", UUID.randomUUID(), 0, null, 1_000);

        assertThrows(
                IllegalArgumentException.class,
                () -> new CommandQueuedReceipt(commandId, DelayMessageId.random(foreignShard), shard, source));
    }

    @Test
    void sdkBackpressureRejectsBeforeSourcePositionAndByteBudgetAreConsumed() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 30);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("backpressure-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"));
        final EmbeddedDelayServiceConfig bounded = new EmbeddedDelayServiceConfig(1, Long.MAX_VALUE);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("backpressure-count")), shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), bounded)) {
            final PreparedCommand first = service.prepareSchedule(intent, 10_000);
            final PreparedCommand second = service.prepareSchedule(intent, 10_000);
            assertEquals(
                    EnqueueStatus.QUEUED,
                    service.enqueue(first).toCompletableFuture().join().status());
            final EnqueueOutcome rejected =
                    service.enqueue(second).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, rejected.status());
            assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(), rejected.stableCode());
            assertEquals(1, service.pendingCommandCount());
            assertTrue(service.pendingCommandBytes() > 0);

            service.drain();
            assertEquals(0, service.pendingCommandCount());
            assertEquals(0, service.pendingCommandBytes());
            final PreparedCommand third = service.prepareSchedule(intent, 10_000);
            final EnqueueOutcome afterDrain =
                    service.enqueue(third).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, afterDrain.status());
            assertEquals(1, ((KafkaSourcePosition) afterDrain.receipt().sourcePosition()).offset());
        }

        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("backpressure-bytes")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
                new EmbeddedDelayServiceConfig(4, 1))) {
            final PreparedCommand command = service.prepareSchedule(intent, 10_000);
            final EnqueueOutcome rejected =
                    service.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, rejected.status());
            assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(), rejected.stableCode());
            assertEquals(0, service.pendingCommandCount());
            assertEquals(0, service.pendingCommandBytes());
        }
    }

    @Test
    void closeDrainsQueuedCommandsBeforeClosingTheShardDb() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 31);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("close-drain-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"));
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("close-drain"));
        final PreparedCommand command;
        final CommandQueuedReceipt receipt;
        try (EmbeddedDelayService service =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            command = service.prepareSchedule(intent, 10_000);
            receipt = service.enqueue(command).toCompletableFuture().join().receipt();
            assertEquals(1, service.pendingCommandCount());
        }

        try (EmbeddedDelayService reopened =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final CommandResult result =
                    reopened.awaitApplied(receipt).toCompletableFuture().join();
            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(0, reopened.pendingCommandCount());
        }
    }

    @Test
    void failedEmbeddedConstructionClosesStoreAfterSourceIdentityMismatch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 33);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("constructor-failure-cleanup"));
        final PreparedCommand command = PreparedCommand.cancel(shard, DelayMessageId.random(shard), -1, 10_000);
        final KafkaSourcePosition foreignPosition =
                new KafkaSourcePosition(shard, "foreign-cluster", UUID.randomUUID(), 0, null, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            new DelayShard(store, DelayShardConfig.defaults()).apply(command, foreignPosition);
        }

        assertThrows(
                IllegalStateException.class,
                () -> new EmbeddedDelayService(
                        config, shard, Clock.fixed(Instant.ofEpochMilli(1_001), ZoneOffset.UTC)));

        // The failed constructor must not leave its internally-created DB
        // handle or resource envelope holding the shard path. A raw reopen
        // proves that the next owner can retry the fail-closed inspection.
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore reopened = ShardStore.open(config, shard, resources)) {
            assertEquals(
                    foreignPosition, new DelayShard(reopened, DelayShardConfig.defaults()).lastAppliedSourcePosition());
        }
    }

    @Test
    void closedEmbeddedServiceDoesNotExposeShardOrBufferState() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 32);
        final EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("closed-access")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        service.close();
        try {
            assertThrows(IllegalStateException.class, service::shard);
            assertThrows(IllegalStateException.class, service::pendingCommandCount);
            assertThrows(IllegalStateException.class, service::pendingCommandBytes);
        } finally {
            service.close();
        }
    }

    @Test
    void reopenedEmbeddedServiceContinuesSourceOffsets() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reopen"));
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("reopen-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"));
        try (EmbeddedDelayService first =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = first.prepareSchedule(intent, 10_000);
            final EnqueueOutcome outcome =
                    first.enqueue(command).toCompletableFuture().join();
            first.awaitApplied(outcome.receipt()).toCompletableFuture().join();
        }
        try (EmbeddedDelayService second =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_001), ZoneOffset.UTC))) {
            final PreparedCommand command =
                    second.prepareCancel(com.nereusstream.delay.protocol.DelayMessageId.random(shard), -1, 10_000);
            final EnqueueOutcome outcome =
                    second.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, outcome.status());
            assertEquals(
                    1,
                    ((com.nereusstream.delay.protocol.KafkaSourcePosition)
                                    outcome.receipt().sourcePosition())
                            .offset());
        }
    }

    @Test
    void embeddedSourceOffsetExhaustionFailsBeforeMutatingOffset() throws ReflectiveOperationException {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("offset-exhaustion")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = service.prepareSchedule(
                    new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("offset-exhaustion-lane")),
                            2_000,
                            5_000,
                            OrderingMode.BEST_EFFORT,
                            Bytes.utf8("payload")),
                    10_000);
            final var nextOffset = EmbeddedDelayService.class.getDeclaredField("nextOffset");
            nextOffset.setAccessible(true);
            final var offsetExhausted = EmbeddedDelayService.class.getDeclaredField("offsetExhausted");
            offsetExhausted.setAccessible(true);
            offsetExhausted.setBoolean(service, true);

            assertThrows(IllegalStateException.class, () -> service.enqueue(command));
            assertEquals(0L, nextOffset.getLong(service));
            assertTrue(offsetExhausted.getBoolean(service));
        }
    }

    @Test
    void embeddedSourceAcceptsUnsignedMaximumOffsetThenExhausts() throws ReflectiveOperationException {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("offset-maximum")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = service.prepareSchedule(
                    new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("offset-maximum-lane")),
                            2_000,
                            5_000,
                            OrderingMode.BEST_EFFORT,
                            Bytes.utf8("payload")),
                    10_000);
            final var nextOffset = EmbeddedDelayService.class.getDeclaredField("nextOffset");
            nextOffset.setAccessible(true);
            nextOffset.setLong(service, Long.MAX_VALUE);

            final EnqueueOutcome outcome =
                    service.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, outcome.status());
            assertEquals(
                    Long.MAX_VALUE, ((KafkaSourcePosition) outcome.receipt().sourcePosition()).offset());

            // The all-ones offset is also a valid unsigned Kafka offset; it
            // must be accepted once before the sequence is exhausted.
            nextOffset.setLong(service, -1L);
            final PreparedCommand maximum = service.prepareCancel(DelayMessageId.random(shard), -1, 10_000);
            final EnqueueOutcome maximumOutcome =
                    service.enqueue(maximum).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, maximumOutcome.status());
            assertEquals(-1L, ((KafkaSourcePosition) maximumOutcome.receipt().sourcePosition()).offset());
            assertThrows(
                    IllegalStateException.class,
                    () -> service.enqueue(service.prepareCancel(DelayMessageId.random(shard), -1, 10_000)));
        }
    }

    @Test
    void reopenedEmbeddedServiceKeepsUnsignedMaximumSourceOffsetExhaustion() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reopen-offset-exhaustion"));
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("reopen-offset-exhaustion-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"));
        try (EmbeddedDelayService first =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = first.prepareSchedule(intent, 10_000);
            first.shard()
                    .apply(
                            command,
                            new KafkaSourcePosition(
                                    shard,
                                    "embedded",
                                    UUID.nameUUIDFromBytes(Bytes.utf8("embedded-command-topic")),
                                    -1L,
                                    null,
                                    1_000));
        }
        try (EmbeddedDelayService second =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_001), ZoneOffset.UTC))) {
            final PreparedCommand command = second.prepareCancel(DelayMessageId.random(shard), -1, 10_000);
            assertThrows(IllegalStateException.class, () -> second.enqueue(command));
        }
    }

    @Test
    void boundedLocalProjectorRequiresSafeBindingAndPreservesRuntimeStates() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "projector", UUID.randomUUID(), 4, 1, 1_000);
        final PublicDestinationBindingView binding = publicBinding();
        final CommandResult rejected = new CommandResult(
                ApplyStatus.REJECTED, StableCode.INVALID_COMMAND, -1, 0, null, source.canonicalBytes());
        assertEquals(
                com.nereusstream.delay.protocol.CommandQueryResult.REJECTED,
                BoundedLocalQueryProjector.command(rejected, 5_000, null).resultKind());

        final CommandResult applied = new CommandResult(
                ApplyStatus.APPLIED, StableCode.SCHEDULED, 0, 1, MessageStatus.SCHEDULED, source.canonicalBytes());
        assertEquals(
                com.nereusstream.delay.protocol.CommandQueryResult.APPLIED,
                BoundedLocalQueryProjector.command(applied, 5_000, binding).resultKind());

        final com.nereusstream.delay.protocol.DelayMessageId messageId =
                com.nereusstream.delay.protocol.DelayMessageId.random(shard);
        final MessageQuerySnapshot active = new MessageQuerySnapshot(
                messageId,
                0,
                1,
                GenerationAggregateState.SCHEDULED,
                2_000,
                5_000,
                PayloadAvailability.INLINE_RETAINED,
                false,
                null);
        final MessageQuerySnapshot terminal = new MessageQuerySnapshot(
                messageId,
                0,
                2,
                GenerationAggregateState.PUBLISHED,
                2_000,
                5_000,
                PayloadAvailability.INLINE_RETAINED,
                false,
                StableCode.OK);
        assertEquals(
                com.nereusstream.delay.protocol.MessageQueryResult.ACTIVE,
                BoundedLocalQueryProjector.message(active, binding, DlqExportState.NOT_CONFIGURED, null)
                        .resultKind());
        assertEquals(
                com.nereusstream.delay.protocol.MessageQueryResult.TERMINAL,
                BoundedLocalQueryProjector.message(terminal, binding, DlqExportState.NOT_CONFIGURED, null)
                        .resultKind());
        assertThrows(
                IllegalArgumentException.class, () -> BoundedLocalQueryProjector.command(rejected, 5_000, binding));
        assertThrows(
                IllegalArgumentException.class,
                () -> BoundedLocalQueryProjector.message(terminal, binding, DlqExportState.PUBLISHED, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new MessageQuerySnapshot(
                        messageId,
                        0,
                        2,
                        GenerationAggregateState.PUBLISHED,
                        2_000,
                        5_000,
                        PayloadAvailability.INLINE_RETAINED,
                        false,
                        StableCode.OK,
                        DlqExportState.PUBLISHED));
    }

    @Test
    void embeddedQueryUsesQueuedReceiptAsSourceBarrier() {
        final long now = 1_000;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC);
        try (EmbeddedDelayService service =
                new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir.resolve("query")), shard, clock)) {
            final PreparedCommand command = cancel(shard, 10_000);
            final EnqueueOutcome outcome =
                    service.enqueue(command).toCompletableFuture().join();
            final var queued = service.queuedReceipt(
                    outcome, 10_000, java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("attempt")), 16));

            assertEquals(
                    CommandQueryResult.PENDING,
                    service.queryCommand(queued, now, 10_000, publicBinding()).resultKind());
            service.drain();
            assertEquals(
                    CommandQueryResult.APPLIED,
                    service.queryCommand(queued, now, 10_000, null).resultKind());
            final CommandAppliedReceipt applied = service.appliedReceipt(queued, 10_000, null);
            assertEquals(
                    com.nereusstream.delay.protocol.ReceiptKind.COMMAND_APPLIED,
                    com.nereusstream.delay.protocol.ReceiptFrame.decode(applied.frame())
                            .kind());
            assertEquals(applied, CommandAppliedReceipt.decodeFrame(applied.frame()));
            assertEquals(
                    CommandQueryResult.RESULT_EXPIRED,
                    service.queryCommand(queued, 3_000, 2_000, publicBinding()).resultKind());
            assertEquals(
                    CommandQueryResult.INTEGRITY_ERROR,
                    service.queryCommand(queued, now, 999, publicBinding()).resultKind());
            assertEquals(
                    CommandQueryResult.RESULT_EVIDENCE_EXPIRED,
                    service.queryCommand(queued, 10_001, 10_000, publicBinding())
                            .resultKind());

            assertEquals(
                    MessageQueryResult.UNKNOWN,
                    service.queryMessage(
                                    DelayMessageId.random(shard),
                                    publicBinding(),
                                    DlqExportState.NOT_CONFIGURED,
                                    null,
                                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                            .resultKind());
            assertEquals(
                    MessageQueryResult.INVALID_RECEIPT,
                    service.queryMessage(
                                    null,
                                    publicBinding(),
                                    null,
                                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                            .resultKind());
            assertEquals(
                    MessageQueryResult.INVALID_RECEIPT,
                    service.getMessage(
                                    null,
                                    publicBinding(),
                                    DlqExportState.NOT_CONFIGURED,
                                    null,
                                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                            .toCompletableFuture()
                            .join()
                            .resultKind());
            assertEquals(
                    CommandQueryResult.INVALID_RECEIPT,
                    service.getCommandResult(null, now, 10_000, publicBinding())
                            .toCompletableFuture()
                            .join()
                            .resultKind());
        }
    }

    @Test
    void messageQueryMapsPublicProjectionDriftToClosedIntegrityError() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 29);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("message-query-projection")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = service.prepareSchedule(
                    new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("message-query-lane")),
                            2_000,
                            5_000,
                            OrderingMode.BEST_EFFORT,
                            Bytes.utf8("payload")),
                    10_000);
            service.enqueue(command).toCompletableFuture().join();
            service.drain();

            final MessageQueryResponse response = service.getMessage(
                            command.delayMessageId(),
                            publicBinding(),
                            DlqExportState.PUBLISHED,
                            null,
                            com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                    .toCompletableFuture()
                    .join();
            assertEquals(MessageQueryResult.INTEGRITY_ERROR, response.resultKind());
            assertEquals(StableCode.INTEGRITY_ERROR, response.error().code());
            final MessageQueryResponse missingBinding = service.queryMessage(
                    command.delayMessageId(),
                    null,
                    null,
                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN);
            assertEquals(MessageQueryResult.INTEGRITY_ERROR, missingBinding.resultKind());
            assertEquals(StableCode.INTEGRITY_ERROR, missingBinding.error().code());
        }
    }

    @Test
    void retiredMessageIdentitySurvivesQueryAndFreshProcessReopen() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 42);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("retired-message-query"));
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("retired-message-lane")),
                2_000,
                5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("retired-message"));
        final DelayMessageId messageId;
        try (EmbeddedDelayService service =
                new EmbeddedDelayService(config, shardId, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand schedule = service.prepareSchedule(intent, 10_000);
            messageId = schedule.delayMessageId();
            service.enqueue(schedule).toCompletableFuture().join();
            service.drain();
            final PreparedCommand cancel = PreparedCommand.cancel(shardId, messageId, 0, 10_000);
            service.enqueue(cancel).toCompletableFuture().join();
            service.drain();

            final long reuseUntil = messageId.routingId().logicalTimestampEpochMs()
                    + com.nereusstream.delay.runtime.DelayShardConfig.defaults().maxMessageLifetimeMs();
            final com.nereusstream.delay.runtime.RetiredMessageIdentityRecord retired =
                    com.nereusstream.delay.runtime.DelayShardTestSupport.retireMessageIdentity(
                            service.shard(), messageId, reuseUntil);
            assertEquals(reuseUntil, retired.messageIdentityReuseUntilEpochMs());
            assertNull(service.shard().getMessage(messageId));
            assertEquals(
                    MessageQueryResult.IDENTITY_RETIRED,
                    service.queryMessage(
                                    messageId,
                                    null,
                                    DlqExportState.NOT_CONFIGURED,
                                    null,
                                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                            .resultKind());

            final PreparedCommand reused = PreparedCommand.create(
                    shardId,
                    CommandId.random(shardId),
                    messageId,
                    CommandType.SCHEDULE,
                    10_000,
                    CommandBodies.schedule(intent));
            service.enqueue(reused).toCompletableFuture().join();
            service.drain();
            assertEquals(
                    com.nereusstream.delay.protocol.StableCode.DELAY_MESSAGE_ID_CONFLICT,
                    service.shard().getCommandResult(reused.commandId()).stableCode());
        }

        try (EmbeddedDelayService reopened =
                new EmbeddedDelayService(config, shardId, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            assertNull(reopened.shard().getMessage(messageId));
            assertTrue(reopened.shard().getRetiredMessageIdentity(messageId) != null);
            assertEquals(
                    MessageQueryResult.IDENTITY_RETIRED,
                    reopened.queryMessage(
                                    messageId,
                                    null,
                                    DlqExportState.NOT_CONFIGURED,
                                    null,
                                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                            .resultKind());
        }
    }

    @Test
    void embeddedQueryDerivesFullResultRetentionFromAppliedSourceTime() {
        final long now = 1_000;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 41);
        final CommandResultRetentionPolicy policy = new CommandResultRetentionPolicy(3, 4_000);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("query-retention-policy")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC))) {
            final PreparedCommand command = cancel(shard, 20_000);
            final EnqueueOutcome outcome =
                    service.enqueue(command).toCompletableFuture().join();
            final CanonicalCommandQueuedReceipt queued = service.queuedReceipt(
                    outcome, 20_000, java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("retention-policy-attempt")), 16));
            service.drain();

            assertEquals(
                    CommandQueryResult.APPLIED,
                    service.queryCommand(queued, 4_999, policy, null).resultKind());
            assertEquals(
                    CommandQueryResult.RESULT_EXPIRED,
                    service.queryCommand(queued, 5_001, policy, null).resultKind());
            assertEquals(5_000, service.appliedReceipt(queued, policy, null).fullResultRetainUntilEpochMs());
        }
    }

    @Test
    void awaitAppliedDrainsOnlyAfterReceiptValidation() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 27);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("await")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final DelayClient client = service;
            final PreparedCommand command =
                    client.prepareCancel(DelayMessageId.random(shard), new MessagePrecondition(0L, null), 10_000);
            final EnqueueOutcome queued =
                    client.enqueue(command).toCompletableFuture().join();
            final CanonicalCommandQueuedReceipt receipt = service.queuedReceipt(
                    queued, 10_000, java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("await-attempt")), 16));

            assertEquals(
                    CommandQueryResult.APPLIED,
                    client.awaitApplied(receipt, 1_000, 10_000, null)
                            .toCompletableFuture()
                            .join()
                            .resultKind());
            assertEquals(0, service.pendingCommandCount());

            final PreparedCommand foreign =
                    PreparedCommand.schedule(shard, scheduleIntent("foreign-await", 2_000, 5_000, "payload"), 10_000);
            final CanonicalCommandQueuedReceipt forged = CanonicalCommandQueuedReceipt.create(
                    foreign,
                    receipt.sourcePosition(),
                    receipt.brokerAck(),
                    receipt.receiptQueryUntilEpochMs(),
                    receipt.physicalEnqueueAttemptId());
            assertEquals(
                    CommandQueryResult.RECEIPT_MISMATCH,
                    client.awaitApplied(forged, 1_000, 10_000, null)
                            .toCompletableFuture()
                            .join()
                            .resultKind());
        }
    }

    @Test
    void delayClientExposesBoundedCommandAndMessageQueries() {
        final long now = 1_000;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("client-query")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC))) {
            final DelayClient client = service;
            final PreparedCommand command = cancel(shard, 10_000);
            final EnqueueOutcome outcome =
                    client.enqueue(command).toCompletableFuture().join();
            final CanonicalCommandQueuedReceipt receipt = service.queuedReceipt(
                    outcome, 10_000, java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("client-query-attempt")), 16));

            assertEquals(
                    CommandQueryResult.PENDING,
                    client.getCommandResult(receipt, now, 10_000, publicBinding())
                            .toCompletableFuture()
                            .join()
                            .resultKind());
            service.drain();
            assertEquals(
                    CommandQueryResult.APPLIED,
                    client.getCommandResult(receipt, now, 10_000, null)
                            .toCompletableFuture()
                            .join()
                            .resultKind());
            assertEquals(
                    MessageQueryResult.UNKNOWN,
                    client.getMessage(
                                    DelayMessageId.random(shard),
                                    publicBinding(),
                                    DlqExportState.NOT_CONFIGURED,
                                    null,
                                    com.nereusstream.delay.protocol.FirstScheduleEligibility.NOT_PROVEN)
                            .toCompletableFuture()
                            .join()
                            .resultKind());
        }
    }

    @Test
    void embeddedQueryRejectsSameOffsetReceiptWithConflictingCanonicalMetadata() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("query-position-fence")), shard, clock)) {
            final PreparedCommand command = cancel(shard, 10_000);
            final EnqueueOutcome outcome =
                    service.enqueue(command).toCompletableFuture().join();
            final byte[] attemptId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("query-position-attempt")), 16);
            final CanonicalCommandQueuedReceipt queued = service.queuedReceipt(outcome, 10_000, attemptId);
            final KafkaSourcePosition actual = (KafkaSourcePosition) queued.sourcePosition();
            final KafkaSourcePosition conflicting = new KafkaSourcePosition(
                    shard,
                    actual.authenticatedClusterId(),
                    actual.nativeTopicUuid(),
                    actual.offset(),
                    7,
                    Math.addExact(actual.brokerLogAppendTimeEpochMs(), 1));
            final CanonicalCommandQueuedReceipt forged = CanonicalCommandQueuedReceipt.create(
                    command,
                    conflicting,
                    new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                            actual.authenticatedClusterId(),
                            actual.nativeTopicUuid(),
                            shard.partition(),
                            actual.offset(),
                            7,
                            conflicting.brokerLogAppendTimeEpochMs(),
                            Bytes.sha256(Bytes.utf8("query-position-conflicting-ack"))),
                    10_000,
                    attemptId);

            service.drain();
            assertEquals(
                    com.nereusstream.delay.protocol.CommandQueryResult.INTEGRITY_ERROR,
                    service.queryCommand(forged, 1_000, 10_000, null).resultKind());
            assertThrows(IllegalStateException.class, () -> service.appliedReceipt(forged, 10_000, null));
        }
    }

    @Test
    void embeddedQueryBindsReceiptCommandHashToDurableDedupeIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("query-command-hash")), shard, clock)) {
            final PreparedCommand command = cancel(shard, 10_000);
            final EnqueueOutcome outcome =
                    service.enqueue(command).toCompletableFuture().join();
            final CanonicalCommandQueuedReceipt queued = service.queuedReceipt(
                    outcome,
                    10_000,
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("query-command-hash-attempt")), 16));
            final PreparedCommand forgedCommand = PreparedCommand.create(
                    shard,
                    command.commandId(),
                    command.delayMessageId(),
                    command.type(),
                    command.retryUntilEpochMs(),
                    CommandBodies.cancel(
                            command.delayMessageId(), command.retryUntilEpochMs(), new MessagePrecondition(1L, null)));
            final CanonicalCommandQueuedReceipt forged = CanonicalCommandQueuedReceipt.create(
                    forgedCommand,
                    queued.sourcePosition(),
                    queued.brokerAck(),
                    queued.receiptQueryUntilEpochMs(),
                    queued.physicalEnqueueAttemptId());

            service.drain();
            assertEquals(
                    CommandQueryResult.RECEIPT_MISMATCH,
                    service.queryCommand(forged, 1_000, 10_000, publicBinding()).resultKind());
            assertThrows(IllegalArgumentException.class, () -> service.appliedReceipt(forged, 10_000, null));
            assertEquals(
                    CommandQueryResult.APPLIED,
                    service.queryCommand(queued, 1_000, 10_000, null).resultKind());
        }
    }

    @Test
    void embeddedQueryBindsReceiptToExactPhysicalPositionAudit() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("query-position-audit")), shard, clock)) {
            final PreparedCommand filler = cancel(shard, 10_000);
            final PreparedCommand target = cancel(shard, 10_000);
            final EnqueueOutcome fillerOutcome =
                    service.enqueue(filler).toCompletableFuture().join();
            final EnqueueOutcome targetOutcome =
                    service.enqueue(target).toCompletableFuture().join();
            final byte[] attemptId =
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("query-position-audit-attempt")), 16);
            final CanonicalCommandQueuedReceipt fillerReceipt = service.queuedReceipt(fillerOutcome, 10_000, attemptId);
            final CanonicalCommandQueuedReceipt targetReceipt = service.queuedReceipt(targetOutcome, 10_000, attemptId);
            final CanonicalCommandQueuedReceipt forged = CanonicalCommandQueuedReceipt.create(
                    target,
                    fillerReceipt.sourcePosition(),
                    fillerReceipt.brokerAck(),
                    targetReceipt.receiptQueryUntilEpochMs(),
                    targetReceipt.physicalEnqueueAttemptId());

            service.drain();
            assertEquals(
                    CommandQueryResult.RECEIPT_MISMATCH,
                    service.queryCommand(forged, 1_000, 10_000, null).resultKind());
            assertThrows(IllegalArgumentException.class, () -> service.appliedReceipt(forged, 10_000, null));
            assertEquals(
                    CommandQueryResult.APPLIED,
                    service.queryCommand(targetReceipt, 1_000, 10_000, null).resultKind());
        }
    }

    @Test
    void embeddedQueuedReceiptRejectsNonQueuedOutcome() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("reject")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final ShardId otherShard = new ShardId(RouteIncarnation.random(), 0);
            final PreparedCommand otherShardCommand = schedule(otherShard, "other-lane", 2_000, 5_000, 10_000);
            final EnqueueOutcome outcome =
                    service.enqueue(otherShardCommand).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, outcome.status());
            assertThrows(IllegalArgumentException.class, () -> service.queuedReceipt(outcome, 10_000, new byte[16]));
        }
    }

    @Test
    void embeddedControlOperationEntryPointsPreserveReceiptBoundCas() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("control-operation"));
        final ControlOperationReceipt receipt = controlReceipt();
        final CurrentControlOperation initial = new CurrentControlOperation(
                receipt.operationId(),
                receipt.requestHash(),
                receipt.authenticatedScopeHash(),
                ControlOperationState.PENDING,
                1,
                List.of(),
                null);
        try (EmbeddedDelayService service =
                new EmbeddedDelayService(config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
                    service.registerControlOperation(receipt, initial).resultKind());
            assertEquals(initial, service.queryControlOperation(receipt, 2_000).current());
            assertEquals(
                    ControlOperationQueryResult.INVALID_RECEIPT,
                    service.queryControlOperation(null, 2_000).resultKind());
            assertEquals(
                    ControlOperationQueryResult.INVALID_RECEIPT,
                    service.queryControlOperation(receipt, -1).resultKind());
            final CurrentControlOperation dispatching = new CurrentControlOperation(
                    receipt.operationId(),
                    receipt.requestHash(),
                    receipt.authenticatedScopeHash(),
                    ControlOperationState.DISPATCHING,
                    2,
                    List.of(),
                    null);
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
                    service.advanceControlOperation(receipt, 1, dispatching).resultKind());
            final CurrentControlOperation next = new CurrentControlOperation(
                    receipt.operationId(),
                    receipt.requestHash(),
                    receipt.authenticatedScopeHash(),
                    ControlOperationState.IN_PROGRESS,
                    3,
                    List.of(),
                    null);
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
                    service.advanceControlOperation(receipt, 2, next).resultKind());
            assertEquals(next, service.queryControlOperation(receipt, 2_000).current());
        }
    }

    @Test
    void embeddedPreparedControlRegistrationUsesOneExactProjection() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ControlTargetRef target =
                new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(shard), null, null);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                Bytes.sha256(Bytes.utf8("embedded-prepared-control")),
                request.kind(),
                new ControlAuthor(
                        Bytes.sha256(Bytes.utf8("actor")),
                        Bytes.sha256(Bytes.utf8("roles")),
                        Bytes.sha256(Bytes.utf8("scope"))),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final TrustedUtcIntervalEvidence registeredAt = new TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("embedded-control-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("control-evidence")),
                0,
                null);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("prepared-control")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final var projection = service.registerPreparedControlOperation(
                    prepared, registeredAt, new ControlOperationQueryPolicy(1, 1_000));
            assertArrayEquals(prepared.operationId(), projection.receipt().operationId());
            assertEquals(ControlOperationState.PENDING, projection.current().state());
            assertEquals(2_100, projection.receipt().queryUntilEpochMs());
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
                    service.queryControlOperation(projection.receipt(), 1_500).resultKind());
        }
    }

    @Test
    void strictPreparedControlRegistrationRejectsPolicyDriftAndOverflowBeforeRegistration() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ControlTargetRef target =
                new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(shard), null, null);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                Bytes.sha256(Bytes.utf8("strict-control-policy")),
                request.kind(),
                new ControlAuthor(
                        Bytes.sha256(Bytes.utf8("actor")),
                        Bytes.sha256(Bytes.utf8("roles")),
                        Bytes.sha256(Bytes.utf8("scope"))),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final TrustedUtcIntervalEvidence registeredAt = new TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("strict-control-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("strict-control-evidence")),
                0,
                null);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("strict-control-policy")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> service.registerPreparedControlOperation(
                            prepared, registeredAt, new ControlOperationQueryPolicy(2, 1_000)));
            final TrustedUtcIntervalEvidence overflowAt = new TrustedUtcIntervalEvidence(
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                    Bytes.utf8("strict-control-overflow-clock"),
                    1,
                    1,
                    1,
                    Bytes.sha256(Bytes.utf8("strict-control-overflow-evidence")),
                    0,
                    null);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> service.registerPreparedControlOperation(
                            prepared, overflowAt, new ControlOperationQueryPolicy(1, 1)));
        }
    }

    @Test
    void compatibilityPreparedControlRegistrationValidatesBeforeTargetRegistration() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ControlTargetRef target =
                new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(shard), null, null);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                Bytes.sha256(Bytes.utf8("compatibility-control-window")),
                request.kind(),
                new ControlAuthor(
                        Bytes.sha256(Bytes.utf8("compatibility-actor")),
                        Bytes.sha256(Bytes.utf8("compatibility-roles")),
                        Bytes.sha256(Bytes.utf8("compatibility-scope"))),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final TrustedUtcIntervalEvidence registeredAt = new TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("compatibility-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("compatibility-evidence")),
                0,
                null);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("compatibility-control-window")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> service.registerPreparedControlOperation(prepared, registeredAt, -1));

            final Field authorityField =
                    EmbeddedDelayService.class.getDeclaredField("controlTargetRegistrationAuthority");
            authorityField.setAccessible(true);
            final ControlTargetRegistrationAuthority authority =
                    (ControlTargetRegistrationAuthority) authorityField.get(service);
            assertTrue(authority.find(prepared.operationId()).isEmpty());
        }
    }

    @Test
    void embeddedIngressProjectsAllManagedOutcomeBranches() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("outcome")),
                shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand queuedCommand = schedule(shard, "outcome-queued", 2_000, 5_000, 10_000);
            final EnqueueOutcome queued =
                    service.enqueue(queuedCommand).toCompletableFuture().join();
            final byte[] attemptId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("outcome-attempt")), 16);
            final EnqueueOutcomeMessage queuedWire = service.enqueueOutcome(queued, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKind.QUEUED, queuedWire.kind());
            assertEquals(queuedWire, EnqueueOutcomeMessage.decode(queuedWire.canonicalBytes()));
            final EnqueueOutcomeMessage malformedBoundary = service.enqueueOutcome(queued, 999, attemptId);
            assertEquals(EnqueueOutcomeKind.ENQUEUE_UNCERTAIN, malformedBoundary.kind());
            assertEquals(
                    StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    malformedBoundary.uncertain().error().code());
            assertEquals(
                    StableCode.INTEGRITY_ERROR.wireValue(),
                    malformedBoundary.uncertain().error().diagnosticCode());
            assertArrayEquals(attemptId, malformedBoundary.uncertain().physicalEnqueueAttemptId());
            final EnqueueOutcomeMessage invalidQueuedAttempt = service.enqueueOutcome(queued, 10_000, new byte[16]);
            assertEquals(EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED, invalidQueuedAttempt.kind());
            assertEquals(
                    StableCode.INVALID_PREPARED_COMMAND,
                    invalidQueuedAttempt.definitelyNotQueued().error().code());

            final ShardId rejectedShard = new ShardId(RouteIncarnation.random(), 0);
            final PreparedCommand rejectedCommand = schedule(rejectedShard, "outcome-rejected", 2_000, 5_000, 10_000);
            final EnqueueOutcome rejected =
                    service.enqueue(rejectedCommand).toCompletableFuture().join();
            final EnqueueOutcomeMessage definiteWire = service.enqueueOutcome(rejected, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED, definiteWire.kind());
            assertEquals(definiteWire, EnqueueOutcomeMessage.decode(definiteWire.canonicalBytes()));

            final PreparedCommand uncertainCommand = schedule(shard, "outcome-uncertain", 2_000, 5_000, 10_000);
            final EnqueueOutcome uncertain =
                    EnqueueOutcome.uncertain(uncertainCommand, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
            final EnqueueOutcomeMessage uncertainWire = service.enqueueOutcome(uncertain, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKind.ENQUEUE_UNCERTAIN, uncertainWire.kind());
            assertEquals(uncertainWire, EnqueueOutcomeMessage.decode(uncertainWire.canonicalBytes()));
            final EnqueueOutcomeMessage invalidUncertainAttempt =
                    service.enqueueOutcome(uncertain, 10_000, new byte[16]);
            assertEquals(EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED, invalidUncertainAttempt.kind());
            assertEquals(
                    StableCode.INVALID_PREPARED_COMMAND,
                    invalidUncertainAttempt.definitelyNotQueued().error().code());
        }
    }

    private record CommandResultView(StableCode code) {}

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
                1,
                OrderingMode.BEST_EFFORT);
    }

    private static PreparedCommand schedule(
            final ShardId shard, final String lane, final long deliverAt, final long expireAt, final long retryUntil) {
        return PreparedCommand.schedule(shard, scheduleIntent(lane, deliverAt, expireAt, "payload"), retryUntil);
    }

    private static PreparedCommand cancel(final ShardId shard, final long retryUntil) {
        return PreparedCommand.cancel(
                shard, DelayMessageId.random(shard), new MessagePrecondition(0L, null), retryUntil);
    }

    private static CanonicalScheduleIntent scheduleIntent(
            final String lane, final long deliverAt, final long expireAt, final String payload) {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination-" + lane),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + lane)),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy =
                new RetryPolicyRef(Bytes.utf8("retry-" + lane), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + lane)));
        return CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                deliverAt,
                expireAt,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8(payload),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static ProfileSemanticEnvelope payloadObjectStoreProfile() {
        final ObjectStoreProfileSemantic body = new ObjectStoreProfileSemantic(
                ObjectStoreProviderKind.S3,
                Bytes.sha256(Bytes.utf8("endpoint")),
                Bytes.sha256(Bytes.utf8("credential-scope")),
                1,
                true,
                true,
                true,
                true,
                Bytes.sha256(Bytes.utf8("encryption")),
                2 << 20,
                ObjectStoreProfileSemantic.SINGLE_PUT,
                1,
                Bytes.sha256(Bytes.utf8("lifecycle")));
        return new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("object-store"), 1, body);
    }

    private static PayloadProofTrustSetSemantic payloadTrustSet(final KeyPair keyPair) {
        return new PayloadProofTrustSetSemantic(
                9, List.of(PayloadProofVerifierKey.fromPublicKey(7, keyPair.getPublic(), 0, 9_000)));
    }

    private static ControlOperationReceipt controlReceipt() {
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("embedded-control-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("control-evidence")),
                0,
                null);
        return ControlOperationReceipt.create(
                Bytes.sha256(Bytes.utf8("operation")),
                Bytes.sha256(Bytes.utf8("request")),
                Bytes.sha256(Bytes.utf8("scope")),
                Bytes.sha256(Bytes.utf8("targets")),
                1,
                registered,
                4_000);
    }
}
