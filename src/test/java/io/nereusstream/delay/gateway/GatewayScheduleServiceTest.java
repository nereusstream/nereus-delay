package io.nereusstream.delay.gateway;

import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DelaySemanticCore;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.submission.SubmissionCoordinator;
import io.nereusstream.delay.transport.TransportOwnershipPermit;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayScheduleServiceTest {
    @Test
    void sameKeyAndBodyReusesPreparedOutcomeWithoutAnotherCoordinatorCall() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, schedule(), 600);
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final FakeCore core = new FakeCore(prepared);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(core,
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleRequestV1 request = request(600);

        final GatewaySubmissionOutcomeV1 first = service.schedule(tenant, request).toCompletableFuture().join();
        final GatewaySubmissionOutcomeV1 second = service.schedule(tenant, request).toCompletableFuture().join();

        assertTrue(first.hasSubmissionOutcome());
        assertTrue(second.hasSubmissionOutcome());
        assertArrayEquals(first.submissionOutcome().canonicalBytes(), second.submissionOutcome().canonicalBytes());
        assertEquals(1, core.prepareCalls);
        assertEquals(1, coordinator.calls);
    }

    @Test
    void sameKeyWithDifferentCanonicalBodyIsPreparationConflictBeforeIo() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, schedule(), 600);
        final FakeCore core = new FakeCore(PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)));
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(core,
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();

        service.schedule(tenant, request(600)).toCompletableFuture().join();
        final GatewaySubmissionOutcomeV1 conflict = service.schedule(tenant, request(601))
                .toCompletableFuture().join();

        assertFalse(conflict.hasSubmissionOutcome());
        assertEquals(StableCode.PREPARED_SUBMISSION_MISMATCH, conflict.preparationError().code());
        assertEquals(1, coordinator.calls);
    }

    @Test
    void retryUncertainReusesStoredBytesAndRetryRequestIdWithoutDuplicateAttempt() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, schedule(), 600);
        final FakeCore core = new FakeCore(PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)));
        final CountingCoordinator coordinator = new CountingCoordinator(command, true);
        final TrustedClock clock = () -> 100;
        final InMemoryGatewayIdempotencyStore store = new InMemoryGatewayIdempotencyStore(clock, 10, 20);
        final GatewayScheduleService service = new GatewayScheduleService(core, store, coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleRequestV1 request = request(600);

        final GatewaySubmissionOutcomeV1 uncertain = service.schedule(tenant, request)
                .toCompletableFuture().join();
        final io.nereusstream.delay.transport.Digest32 keyHash = GatewayIdempotencyHashV1.keyHash(
                tenant.authenticatedTenantScopeHash(), request.idempotencyKey());
        final PhysicalEnqueueAttemptId expected = store.exact(keyHash).attempts().get(0).physicalAttemptId();
        final GatewayRetryUncertainRequestV1 retry = new GatewayRetryUncertainRequestV1(request.idempotencyKey(),
                expected, PhysicalEnqueueAttemptId.require(bytes(16, 91)));

        final GatewaySubmissionOutcomeV1 retried = service.retryUncertain(tenant, retry)
                .toCompletableFuture().join();
        final GatewaySubmissionOutcomeV1 repeated = service.retryUncertain(tenant, retry)
                .toCompletableFuture().join();

        assertTrue(uncertain.hasSubmissionOutcome());
        assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED,
                uncertain.submissionOutcome().kind());
        assertEquals(io.nereusstream.delay.protocol.EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN,
                uncertain.submissionOutcome().managed().kind());
        assertTrue(retried.hasSubmissionOutcome());
        assertArrayEquals(retried.submissionOutcome().canonicalBytes(), repeated.submissionOutcome().canonicalBytes());
        assertEquals(2, coordinator.calls);
        assertEquals(2, store.exact(keyHash).attempts().size());
    }

    @Test
    void cancelAndRescheduleUseTheSamePreparedBytesAndAttemptProtocol() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, schedule(), 600);
        final ControlCore core = new ControlCore(command);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(core,
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final MessagePreconditionV1 precondition = new MessagePreconditionV1(1L, 2L);

        final GatewayCancelRequestV1 cancel = new GatewayCancelRequestV1(bytes(16, 71), messageId, precondition, 600);
        final GatewayRescheduleRequestV1 reschedule = new GatewayRescheduleRequestV1(bytes(16, 72), messageId,
                precondition, 350, 850, 600);

        assertTrue(service.cancel(tenant, cancel).toCompletableFuture().join().hasSubmissionOutcome());
        assertTrue(service.cancel(tenant, cancel).toCompletableFuture().join().hasSubmissionOutcome());
        assertTrue(service.reschedule(tenant, reschedule).toCompletableFuture().join().hasSubmissionOutcome());
        assertEquals(1, core.cancelCalls);
        assertEquals(1, core.rescheduleCalls);
        assertEquals(2, coordinator.calls);
    }

    @Test
    void prepareAndCommitLargeReuseTheSamePreparedBytesAndAttemptProtocol() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, schedule(), 600);
        final LargeCore core = new LargeCore(command);
        final CountingCoordinator coordinator = new CountingCoordinator(command);
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(core,
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), coordinator, clock);
        final AuthenticatedTenantContext tenant = tenant();
        final ProfileRefV1 objectStore = objectStoreProfile();
        final PayloadProofTrustSetRefV1 trustSet = trustSet();
        final GatewayPrepareLargeScheduleRequestV1 prepare = new GatewayPrepareLargeScheduleRequestV1(
                bytes(16, 81), new RouteSelectionHint(io.nereusstream.delay.protocol.AdapterKindV1.KAFKA,
                Bytes.utf8("primary")), prepareIntent(), 7, Bytes.sha256(Bytes.utf8("payload")), 1_000,
                trustSet, objectStore, 600);

        assertTrue(service.prepareLargeSchedule(tenant, prepare).toCompletableFuture().join().hasSubmissionOutcome());
        assertTrue(service.prepareLargeSchedule(tenant, prepare).toCompletableFuture().join().hasSubmissionOutcome());

        final DelayMessageId messageId = DelayMessageId.random(shard);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "gateway", UUID.randomUUID(), 3,
                null, 100);
        final byte[] payloadHash = Bytes.sha256(Bytes.utf8("payload"));
        final PayloadReservationReceiptV1 receipt = PayloadReservationReceiptV1.create(
                Bytes.sha256(Bytes.utf8("reservation")), messageId, shard, source, 1, objectStore,
                Bytes.utf8("container"), Bytes.utf8("object"), 7, payloadHash, 5_000, trustSet);
        final PayloadCommitProofV1 proof = PayloadCommitProofV1.signed(receipt.reservationId(),
                tenant.tenantRoutingScope(), shard.routeIncarnation().bytes(), shard.partition(), messageId,
                objectStore, trustSet.version(), 1, receipt.container(), receipt.objectKey(),
                Bytes.utf8("version"), null, 7, payloadHash, 4_500,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
        final GatewayCommitLargeScheduleRequestV1 commit = new GatewayCommitLargeScheduleRequestV1(
                bytes(16, 82), receipt, proof, 600);

        assertTrue(service.commitLargeSchedule(tenant, commit).toCompletableFuture().join().hasSubmissionOutcome());
        assertEquals(1, core.largeCalls);
        assertEquals(1, core.commitCalls);
        assertEquals(2, coordinator.calls);
    }

    private static GatewayScheduleRequestV1 request(final long retryUntil) {
        return new GatewayScheduleRequestV1(bytes(16, 40),
                new RouteSelectionHint(io.nereusstream.delay.protocol.AdapterKindV1.KAFKA,
                        Bytes.utf8("primary")), schedule(), retryUntil, SubmissionModeV1.MANAGED);
    }

    private static ScheduleIntentV1 schedule() {
        return ScheduleIntentV1.create(new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60),
                        ProfileKindV1.DESTINATION), new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300, 800, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("key"),
                Bytes.utf8("payload"), null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                null, null);
    }

    private static ScheduleIntentV1 prepareIntent() {
        return ScheduleIntentV1.forPrepare(new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60),
                        ProfileKindV1.DESTINATION), new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300, 800, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("key"),
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
    }

    private static ProfileRefV1 objectStoreProfile() {
        return new ProfileRefV1(Bytes.utf8("object-store"), 1,
                Bytes.sha256(Bytes.utf8("object-store-semantic")), ProfileKindV1.OBJECT_STORE);
    }

    private static PayloadProofTrustSetRefV1 trustSet() {
        return new PayloadProofTrustSetRefV1(1, Bytes.sha256(Bytes.utf8("payload-trust-set")));
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
        private final PreparedSubmissionV1 prepared;
        private int prepareCalls;

        private FakeCore(final PreparedSubmissionV1 prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                     final long retryUntilEpochMs,
                                                     final SubmissionModeV1 submissionMode) {
            prepareCalls++;
            return prepared;
        }

        @Override
        public PreparedCommand prepareLargeSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route,
                                                     final LargeSchedulePreparationV1 request,
                                                     final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(final AuthenticatedTenantContext tenant,
                                                    final PayloadReservationReceiptV1 reservation,
                                                    final PayloadCommitProofV1 proof,
                                                    final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant,
                                             final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition,
                                             final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                                 final DelayMessageId messageId,
                                                 final MessagePreconditionV1 precondition,
                                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                                 final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand command) {
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
        public CompletionStage<SubmissionOutcomeMessageV1> submit(final AuthenticatedTenantContext tenant,
                                                                    final PreparedSubmissionV1 submission,
                                                                    final TransportOwnershipPermit permit) {
            calls++;
            if (uncertainFirst && calls == 1) {
                return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                        WireIngressOutcomeSupport.uncertain(command, permit.physicalAttemptId().bytes(),
                                StableCode.ENQUEUE_RESULT_UNCERTAIN, null)));
            }
            return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
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
        public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                     final long retryUntilEpochMs,
                                                     final SubmissionModeV1 submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route,
                                                     final LargeSchedulePreparationV1 request,
                                                     final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(final AuthenticatedTenantContext tenant,
                                                    final PayloadReservationReceiptV1 reservation,
                                                    final PayloadCommitProofV1 proof,
                                                    final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant, final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition,
                                             final long retryUntilEpochMs) {
            cancelCalls++;
            return command;
        }

        @Override
        public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                                 final DelayMessageId messageId,
                                                 final MessagePreconditionV1 precondition,
                                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                                 final long retryUntilEpochMs) {
            rescheduleCalls++;
            return command;
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand command) {
            return PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
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
        public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                     final long retryUntilEpochMs,
                                                     final SubmissionModeV1 submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route,
                                                     final LargeSchedulePreparationV1 request,
                                                     final long retryUntilEpochMs) {
            largeCalls++;
            return command;
        }

        @Override
        public PreparedCommand preparePayloadCommit(final AuthenticatedTenantContext tenant,
                                                    final PayloadReservationReceiptV1 reservation,
                                                    final PayloadCommitProofV1 proof,
                                                    final long retryUntilEpochMs) {
            commitCalls++;
            return command;
        }

        @Override
        public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant, final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition,
                                             final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                                 final DelayMessageId messageId,
                                                 final MessagePreconditionV1 precondition,
                                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                                 final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand command) {
            return PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        }
    }
}
