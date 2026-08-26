package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparation;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.submission.SubmissionCoordinator;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class GatewayScheduleServiceTest {
    @Test
    void sameKeyAndBodyReusesPreparedOutcomeWithoutAnotherCoordinatorCall() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        final PreparedSubmission prepared = PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        final FakeCore core = new FakeCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(
                core, new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleRequest request = request(600);

        final GatewaySubmissionOutcome first =
                service.schedule(tenant, request).toCompletableFuture().join();
        final GatewaySubmissionOutcome second =
                service.schedule(tenant, request).toCompletableFuture().join();

        assertTrue(first.hasSubmissionOutcome());
        assertTrue(second.hasSubmissionOutcome());
        assertArrayEquals(
                first.submissionOutcome().canonicalBytes(),
                second.submissionOutcome().canonicalBytes());
        assertEquals(1, core.prepareCalls);
        assertEquals(1, coordinator.calls);
    }

    @Test
    void completedAggregateReplaysAfterRetryDeadlineWithoutAnotherCoordinatorCall() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        final PreparedSubmission prepared = PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        final FakeCore core = new FakeCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final long[] now = {100};
        final TrustedClock clock = () -> now[0];
        final InMemoryGatewayIdempotencyStore store = new InMemoryGatewayIdempotencyStore(clock, 10, 20);
        final GatewayScheduleService service = new GatewayScheduleService(core, store, coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleRequest request = request(600);

        final GatewaySubmissionOutcome first =
                service.schedule(tenant, request).toCompletableFuture().join();
        now[0] = 601;
        final GatewaySubmissionOutcome replay =
                service.schedule(tenant, request).toCompletableFuture().join();

        assertArrayEquals(
                first.submissionOutcome().canonicalBytes(),
                replay.submissionOutcome().canonicalBytes());
        assertEquals(1, core.prepareCalls);
        assertEquals(1, coordinator.calls);
    }

    @Test
    void expiredPreparedRecordDoesNotCreateAnAttemptOrCallCoordinator() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 100);
        final PreparedSubmission prepared = PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        final FakeCore core = new FakeCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final InMemoryGatewayIdempotencyStore store = new InMemoryGatewayIdempotencyStore(clock, 10, 20);
        final GatewayScheduleService service = new GatewayScheduleService(core, store, coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleRequest request = request(100);

        final GatewaySubmissionOutcome outcome =
                service.schedule(tenant, request).toCompletableFuture().join();
        final com.nereusstream.delay.transport.Digest32 keyHash =
                GatewayIdempotencyHash.keyHash(tenant.authenticatedTenantScopeHash(), request.idempotencyKey());

        assertEquals(
                com.nereusstream.delay.protocol.EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED,
                outcome.submissionOutcome().managed().kind());
        assertEquals(
                StableCode.PREPARED_COMMAND_EXPIRED,
                outcome.submissionOutcome()
                        .managed()
                        .definitelyNotQueued()
                        .error()
                        .code());
        assertEquals(0, coordinator.calls);
        assertEquals(0, store.exact(keyHash).attempts().size());
        assertEquals(GatewayIdempotencyPhase.PREPARED, store.exact(keyHash).phase());
    }

    @Test
    void sameKeyWithDifferentCanonicalBodyIsPreparationConflictBeforeIo() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        final FakeCore core = new FakeCore(PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command)));
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(
                core, new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();

        service.schedule(tenant, request(600)).toCompletableFuture().join();
        final GatewaySubmissionOutcome conflict =
                service.schedule(tenant, request(601)).toCompletableFuture().join();

        assertFalse(conflict.hasSubmissionOutcome());
        assertEquals(
                StableCode.PREPARED_SUBMISSION_MISMATCH,
                conflict.preparationError().code());
        assertEquals(1, coordinator.calls);
    }

    @Test
    void retryUncertainReusesStoredBytesAndRetryRequestIdWithoutDuplicateAttempt() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        final FakeCore core = new FakeCore(PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command)));
        final CountingCoordinator coordinator = new CountingCoordinator(command, true);
        final TrustedClock clock = () -> 100;
        final InMemoryGatewayIdempotencyStore store = new InMemoryGatewayIdempotencyStore(clock, 10, 20);
        final GatewayScheduleService service = new GatewayScheduleService(core, store, coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleRequest request = request(600);

        final GatewaySubmissionOutcome uncertain =
                service.schedule(tenant, request).toCompletableFuture().join();
        final com.nereusstream.delay.transport.Digest32 keyHash =
                GatewayIdempotencyHash.keyHash(tenant.authenticatedTenantScopeHash(), request.idempotencyKey());
        final PhysicalEnqueueAttemptId expected =
                store.exact(keyHash).attempts().get(0).physicalAttemptId();
        final GatewayRetryUncertainRequest retry = new GatewayRetryUncertainRequest(
                request.idempotencyKey(), expected, PhysicalEnqueueAttemptId.require(bytes(16, 91)));

        final GatewaySubmissionOutcome retried =
                service.retryUncertain(tenant, retry).toCompletableFuture().join();
        final GatewaySubmissionOutcome repeated =
                service.retryUncertain(tenant, retry).toCompletableFuture().join();

        assertTrue(uncertain.hasSubmissionOutcome());
        assertEquals(
                com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED,
                uncertain.submissionOutcome().kind());
        assertEquals(
                com.nereusstream.delay.protocol.EnqueueOutcomeKind.ENQUEUE_UNCERTAIN,
                uncertain.submissionOutcome().managed().kind());
        assertTrue(retried.hasSubmissionOutcome());
        assertEquals(
                com.nereusstream.delay.protocol.EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED,
                retried.submissionOutcome().managed().kind());
        assertArrayEquals(
                uncertain.submissionOutcome().canonicalBytes(),
                repeated.submissionOutcome().canonicalBytes());
        assertEquals(
                com.nereusstream.delay.protocol.EnqueueOutcomeKind.ENQUEUE_UNCERTAIN,
                repeated.submissionOutcome().managed().kind());
        assertEquals(2, coordinator.calls);
        assertEquals(2, store.exact(keyHash).attempts().size());
    }

    @Test
    void cancelAndRescheduleUseTheSamePreparedBytesAndAttemptProtocol() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        final ControlCore core = new ControlCore(command);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(
                core, new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final MessagePrecondition precondition = new MessagePrecondition(1L, 2L);

        final GatewayCancelRequest cancel = new GatewayCancelRequest(bytes(16, 71), messageId, precondition, 600);
        final GatewayRescheduleRequest reschedule =
                new GatewayRescheduleRequest(bytes(16, 72), messageId, precondition, 350, 850, 600);

        assertTrue(service.cancel(tenant, cancel).toCompletableFuture().join().hasSubmissionOutcome());
        assertTrue(service.cancel(tenant, cancel).toCompletableFuture().join().hasSubmissionOutcome());
        assertTrue(service.reschedule(tenant, reschedule)
                .toCompletableFuture()
                .join()
                .hasSubmissionOutcome());
        assertEquals(1, core.cancelCalls);
        assertEquals(1, core.rescheduleCalls);
        assertEquals(2, coordinator.calls);
    }

    @Test
    void prepareAndCommitLargeReuseTheSamePreparedBytesAndAttemptProtocol() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        final LargeCore core = new LargeCore(command);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(
                core, new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final ProfileRef objectStore = objectStoreProfile();
        final PayloadProofTrustSetRef trustSet = trustSet();
        final GatewayPrepareLargeScheduleRequest prepare = new GatewayPrepareLargeScheduleRequest(
                bytes(16, 81),
                new RouteSelectionHint(com.nereusstream.delay.protocol.AdapterKind.KAFKA, Bytes.utf8("primary")),
                prepareIntent(),
                7,
                Bytes.sha256(Bytes.utf8("payload")),
                1_000,
                trustSet,
                objectStore,
                600);

        assertTrue(service.prepareLargeSchedule(tenant, prepare)
                .toCompletableFuture()
                .join()
                .hasSubmissionOutcome());
        assertTrue(service.prepareLargeSchedule(tenant, prepare)
                .toCompletableFuture()
                .join()
                .hasSubmissionOutcome());

        final DelayMessageId messageId = DelayMessageId.random(shard);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "gateway", UUID.randomUUID(), 3, null, 100);
        final byte[] payloadHash = Bytes.sha256(Bytes.utf8("payload"));
        final PayloadReservationReceipt receipt = PayloadReservationReceipt.create(
                Bytes.sha256(Bytes.utf8("reservation")),
                messageId,
                shard,
                source,
                1,
                objectStore,
                Bytes.utf8("container"),
                Bytes.utf8("object"),
                7,
                payloadHash,
                5_000,
                trustSet);
        final CanonicalPayloadCommitProof proof = CanonicalPayloadCommitProof.signed(
                receipt.reservationId(),
                tenant.tenantRoutingScope(),
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                objectStore,
                trustSet.version(),
                1,
                receipt.container(),
                receipt.objectKey(),
                Bytes.utf8("version"),
                null,
                7,
                payloadHash,
                4_500,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
        final GatewayCommitLargeScheduleRequest commit =
                new GatewayCommitLargeScheduleRequest(bytes(16, 82), receipt, proof, 600);

        assertTrue(service.commitLargeSchedule(tenant, commit)
                .toCompletableFuture()
                .join()
                .hasSubmissionOutcome());
        assertEquals(1, core.largeCalls);
        assertEquals(1, core.commitCalls);
        assertEquals(2, coordinator.calls);
    }

    private static GatewayScheduleRequest request(final long retryUntil) {
        return new GatewayScheduleRequest(
                bytes(16, 40),
                new RouteSelectionHint(com.nereusstream.delay.protocol.AdapterKind.KAFKA, Bytes.utf8("primary")),
                schedule(),
                retryUntil,
                SubmissionMode.MANAGED);
    }

    private static CanonicalScheduleIntent schedule() {
        return CanonicalScheduleIntent.create(
                new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKind.DESTINATION),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static CanonicalScheduleIntent prepareIntent() {
        return CanonicalScheduleIntent.forPrepare(
                new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKind.DESTINATION),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static ProfileRef objectStoreProfile() {
        return new ProfileRef(
                Bytes.utf8("object-store"),
                1,
                Bytes.sha256(Bytes.utf8("object-store-semantic")),
                ProfileKind.OBJECT_STORE);
    }

    private static PayloadProofTrustSetRef trustSet() {
        return new PayloadProofTrustSetRef(1, Bytes.sha256(Bytes.utf8("payload-trust-set")));
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class FakeCore implements DelaySemanticCore {
        private final PreparedSubmission prepared;
        private int prepareCalls;

        private FakeCore(final PreparedSubmission prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmission prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final CanonicalScheduleIntent intent,
                final long retryUntilEpochMs,
                final SubmissionMode submissionMode) {
            prepareCalls++;
            return prepared;
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparation request,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(
                final AuthenticatedTenantContext tenant,
                final PayloadReservationReceipt reservation,
                final CanonicalPayloadCommitProof proof,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CountingCoordinator implements SubmissionCoordinator {
        private final PreparedCommand command;
        private final boolean uncertainFirst;
        private int calls;

        private CountingCoordinator(final PreparedCommand command) {
            this(command, false);
        }

        private CountingCoordinator(final PreparedCommand command, final boolean uncertainFirst) {
            this.command = command;
            this.uncertainFirst = uncertainFirst;
        }

        @Override
        public CompletionStage<SubmissionOutcomeMessage> submit(
                final AuthenticatedTenantContext tenant,
                final PreparedSubmission submission,
                final TransportOwnershipPermit permit) {
            calls++;
            final PreparedCommand submitted = CommandCodec.decodeManagedFrame(submission.managedFrame());
            if (uncertainFirst && calls == 1) {
                return CompletableFuture.completedFuture(
                        SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.uncertain(
                                submitted,
                                permit.physicalAttemptId().bytes(),
                                StableCode.ENQUEUE_RESULT_UNCERTAIN,
                                null)));
            }
            return CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                    WireIngressOutcomeSupport.localDefinite(submitted, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
        }
    }

    private static final class ControlCore implements DelaySemanticCore {
        private final PreparedCommand command;
        private int cancelCalls;
        private int rescheduleCalls;

        private ControlCore(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public PreparedSubmission prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final CanonicalScheduleIntent intent,
                final long retryUntilEpochMs,
                final SubmissionMode submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparation request,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(
                final AuthenticatedTenantContext tenant,
                final PayloadReservationReceipt reservation,
                final CanonicalPayloadCommitProof proof,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long retryUntilEpochMs) {
            cancelCalls++;
            return PreparedCommand.cancel(command.shardId(), messageId, precondition, retryUntilEpochMs);
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            rescheduleCalls++;
            return PreparedCommand.reschedule(
                    command.shardId(), messageId, precondition, deliverAtEpochMs, expireAtEpochMs, retryUntilEpochMs);
        }

        @Override
        public PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            return PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        }
    }

    private static final class LargeCore implements DelaySemanticCore {
        private final PreparedCommand command;
        private int largeCalls;
        private int commitCalls;

        private LargeCore(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public PreparedSubmission prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final CanonicalScheduleIntent intent,
                final long retryUntilEpochMs,
                final SubmissionMode submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparation request,
                final long retryUntilEpochMs) {
            largeCalls++;
            return PreparedCommand.prepareLarge(
                    command.shardId(),
                    request.intentWithoutPayload(),
                    request.expectedPayloadLength(),
                    request.payloadSha256(),
                    request.reservationTtlMs(),
                    request.trustSet(),
                    request.objectStoreProfile(),
                    retryUntilEpochMs);
        }

        @Override
        public PreparedCommand preparePayloadCommit(
                final AuthenticatedTenantContext tenant,
                final PayloadReservationReceipt reservation,
                final CanonicalPayloadCommitProof proof,
                final long retryUntilEpochMs) {
            commitCalls++;
            return PreparedCommand.commitLarge(
                    command.shardId(), proof.delayMessageId(), proof.reservationId(), proof, retryUntilEpochMs);
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            return PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        }
    }
}
