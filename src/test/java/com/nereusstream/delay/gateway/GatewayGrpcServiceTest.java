package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.protobuf.ByteString;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.gateway.wire.GatewayRouteSelector;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaMetadata;
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
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class GatewayGrpcServiceTest {
    @Test
    void scheduleDecodesGeneratedRequestAndReturnsCanonicalDomainBranch() {
        final TrustedClock clock = () -> 100;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, scheduleIntent(), 600);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleService domain = new GatewayScheduleService(
                new Core(PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command))),
                new InMemoryGatewayIdempotencyStore(clock, 10, 20),
                new Coordinator(command),
                clock);
        final GatewayIngressService ingress = new GatewayIngressService(
                domain,
                peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(4),
                clock);
        final GatewayGrpcService service =
                new GatewayGrpcService(ingress, () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY));
        final CapturingObserver observer = new CapturingObserver();

        service.schedule(
                com.nereusstream.delay.gateway.wire.GatewayScheduleRequest.newBuilder()
                        .setIdempotencyKey(ByteString.copyFrom(bytes(16, 40)))
                        .setRoute(GatewayRouteSelector.newBuilder()
                                .setIngressAdapterKind(AdapterKind.KAFKA.wireValue())
                                .setRouteAliasUtf8Nfc(ByteString.copyFromUtf8("primary")))
                        .setCanonicalScheduleIntent(
                                ByteString.copyFrom(scheduleIntent().canonicalBytes()))
                        .setRetryUntilEpochMs(600)
                        .setSubmissionMode(SubmissionMode.MANAGED.wireValue())
                        .build(),
                observer);

        assertNull(observer.failure);
        assertTrue(observer.completed);
        assertEquals(1, observer.outcomes.size());
        assertTrue(observer.outcomes.get(0).hasSubmissionOutcomeNdr1());
        assertEquals(
                StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(),
                SubmissionOutcomeMessage.decode(observer.outcomes
                                .get(0)
                                .getSubmissionOutcomeNdr1()
                                .toByteArray())
                        .managed()
                        .definitelyNotQueued()
                        .error()
                        .code()
                        .wireValue());
    }

    @Test
    void cancelDecodesPreconditionAndUsesTheControlIngressPath() {
        final TrustedClock clock = () -> 100;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, scheduleIntent(), 600);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleService domain = new GatewayScheduleService(
                new Core(PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command))),
                new InMemoryGatewayIdempotencyStore(clock, 10, 20),
                new Coordinator(command),
                clock);
        final GatewayIngressService ingress = new GatewayIngressService(
                domain,
                peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(4),
                clock);
        final GatewayGrpcService service =
                new GatewayGrpcService(ingress, () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY));
        final CapturingObserver observer = new CapturingObserver();
        final DelayMessageId messageId = DelayMessageId.random(shard);

        service.cancel(
                com.nereusstream.delay.gateway.wire.GatewayCancelRequest.newBuilder()
                        .setIdempotencyKey(ByteString.copyFrom(bytes(16, 41)))
                        .setDelayMessageId(ByteString.copyFrom(messageId.bytes()))
                        .setMessagePrecondition(ByteString.copyFrom(new MessagePrecondition(1L, 2L).canonicalBytes()))
                        .setRetryUntilEpochMs(600)
                        .build(),
                observer);

        assertNull(observer.failure);
        assertTrue(observer.completed);
        assertEquals(1, observer.outcomes.size());
        assertEquals(
                StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(),
                SubmissionOutcomeMessage.decode(observer.outcomes
                                .get(0)
                                .getSubmissionOutcomeNdr1()
                                .toByteArray())
                        .managed()
                        .definitelyNotQueued()
                        .error()
                        .code()
                        .wireValue());
    }

    @Test
    void prepareLargeScheduleDecodesRegistryReferencesAndUsesControlIngressPath() {
        final TrustedClock clock = () -> 100;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, scheduleIntent(), 600);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleService domain = new GatewayScheduleService(
                new Core(PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command))),
                new InMemoryGatewayIdempotencyStore(clock, 10, 20),
                new Coordinator(command),
                clock);
        final GatewayIngressService ingress = new GatewayIngressService(
                domain,
                peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(4),
                clock);
        final GatewayGrpcService service =
                new GatewayGrpcService(ingress, () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY));
        final CapturingObserver observer = new CapturingObserver();
        final ProfileRef objectStore =
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 70), ProfileKind.OBJECT_STORE);
        final PayloadProofTrustSetRef trustSet = new PayloadProofTrustSetRef(1, bytes(32, 71));

        service.prepareLargeSchedule(
                com.nereusstream.delay.gateway.wire.GatewayPrepareLargeScheduleRequest.newBuilder()
                        .setIdempotencyKey(ByteString.copyFrom(bytes(16, 42)))
                        .setRoute(GatewayRouteSelector.newBuilder()
                                .setIngressAdapterKind(AdapterKind.KAFKA.wireValue())
                                .setRouteAliasUtf8Nfc(ByteString.copyFromUtf8("primary")))
                        .setCanonicalScheduleIntent(
                                ByteString.copyFrom(prepareIntent().canonicalBytes()))
                        .setExpectedPayloadLength(7)
                        .setPayloadSha256(ByteString.copyFrom(bytes(32, 72)))
                        .setReservationTtlMs(1_000)
                        .setPayloadProofTrustSetRef(ByteString.copyFrom(trustSet.canonicalBytes()))
                        .setObjectStoreProfileRef(ByteString.copyFrom(objectStore.canonicalBytes()))
                        .setRetryUntilEpochMs(600)
                        .build(),
                observer);

        assertNull(observer.failure);
        assertTrue(observer.completed);
        assertEquals(1, observer.outcomes.size());
        assertEquals(
                StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(),
                SubmissionOutcomeMessage.decode(observer.outcomes
                                .get(0)
                                .getSubmissionOutcomeNdr1()
                                .toByteArray())
                        .managed()
                        .definitelyNotQueued()
                        .error()
                        .code()
                        .wireValue());
    }

    private static CanonicalScheduleIntent scheduleIntent() {
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
                AdapterMetadata.kafka(new KafkaMetadata(null, java.util.List.of())),
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
                AdapterMetadata.kafka(new KafkaMetadata(null, java.util.List.of())),
                null,
                null);
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

    private static final class CapturingObserver
            implements StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> {
        private final java.util.List<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> outcomes =
                new java.util.ArrayList<>();
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome value) {
            outcomes.add(value);
        }

        @Override
        public void onError(final Throwable throwable) {
            failure = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private static final class Core implements DelaySemanticCore {
        private final PreparedSubmission prepared;

        private Core(final PreparedSubmission prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmission prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final CanonicalScheduleIntent intent,
                final long retryUntilEpochMs,
                final SubmissionMode submissionMode) {
            return prepared;
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparation request,
                final long retryUntilEpochMs) {
            final PreparedCommand base = CommandCodec.decodeManagedFrame(prepared.managedFrame());
            return PreparedCommand.prepareLarge(
                    base.shardId(),
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
            final PreparedCommand base = CommandCodec.decodeManagedFrame(prepared.managedFrame());
            return PreparedCommand.commitLarge(
                    base.shardId(), proof.delayMessageId(), proof.reservationId(), proof, retryUntilEpochMs);
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long retryUntilEpochMs) {
            return PreparedCommand.cancel(messageId.routingId().shardId(), messageId, precondition, retryUntilEpochMs);
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            return PreparedCommand.reschedule(
                    messageId.routingId().shardId(),
                    messageId,
                    precondition,
                    deliverAtEpochMs,
                    expireAtEpochMs,
                    retryUntilEpochMs);
        }

        @Override
        public PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            return PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        }
    }

    private static final class Coordinator implements SubmissionCoordinator {
        private final PreparedCommand command;

        private Coordinator(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public CompletionStage<SubmissionOutcomeMessage> submit(
                final AuthenticatedTenantContext tenant,
                final PreparedSubmission submission,
                final TransportOwnershipPermit permit) {
            final PreparedCommand submitted = CommandCodec.decodeManagedFrame(submission.managedFrame());
            return CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                    WireIngressOutcomeSupport.localDefinite(submitted, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
        }
    }
}
