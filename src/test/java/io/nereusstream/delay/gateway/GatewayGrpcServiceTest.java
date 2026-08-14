package io.nereusstream.delay.gateway;

import com.google.protobuf.ByteString;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.gateway.v1.GatewayRouteSelectorV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
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
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DelaySemanticCore;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.submission.SubmissionCoordinator;
import io.nereusstream.delay.transport.TransportOwnershipPermit;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayGrpcServiceTest {
    @Test
    void scheduleDecodesGeneratedRequestAndReturnsCanonicalDomainBranch() {
        final TrustedClock clock = () -> 100;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, scheduleIntent(), 600);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleService domain = new GatewayScheduleService(
                new Core(PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command))),
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), new Coordinator(command), clock);
        final GatewayIngressService ingress = new GatewayIngressService(domain, peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(4), clock);
        final GatewayGrpcService service = new GatewayGrpcService(ingress,
                () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY));
        final CapturingObserver observer = new CapturingObserver();

        service.schedule(io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 40)))
                .setRoute(GatewayRouteSelectorV1.newBuilder()
                        .setIngressAdapterKind(AdapterKindV1.KAFKA.wireValue())
                        .setRouteAliasUtf8Nfc(ByteString.copyFromUtf8("primary")))
                .setScheduleIntentV1(ByteString.copyFrom(scheduleIntent().canonicalBytes()))
                .setRetryUntilEpochMs(600)
                .setSubmissionModeV1(SubmissionModeV1.MANAGED.wireValue())
                .build(), observer);

        assertNull(observer.failure);
        assertTrue(observer.completed);
        assertEquals(1, observer.outcomes.size());
        assertTrue(observer.outcomes.get(0).hasSubmissionOutcomeNdr1());
        assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(),
                SubmissionOutcomeMessageV1.decode(observer.outcomes.get(0).getSubmissionOutcomeNdr1().toByteArray())
                        .managed().definitelyNotQueued().error().code().wireValue());
    }

    @Test
    void cancelDecodesPreconditionAndUsesTheControlIngressPath() {
        final TrustedClock clock = () -> 100;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, scheduleIntent(), 600);
        final AuthenticatedTenantContext tenant = tenant();
        final GatewayScheduleService domain = new GatewayScheduleService(
                new Core(PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command))),
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), new Coordinator(command), clock);
        final GatewayIngressService ingress = new GatewayIngressService(domain, peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(4), clock);
        final GatewayGrpcService service = new GatewayGrpcService(ingress,
                () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY));
        final CapturingObserver observer = new CapturingObserver();
        final DelayMessageId messageId = DelayMessageId.random(shard);

        service.cancel(io.nereusstream.delay.gateway.v1.GatewayCancelRequestV1.newBuilder()
                .setIdempotencyKey(ByteString.copyFrom(bytes(16, 41)))
                .setDelayMessageId(ByteString.copyFrom(messageId.bytes()))
                .setMessagePreconditionV1(ByteString.copyFrom(new MessagePreconditionV1(1L, 2L).canonicalBytes()))
                .setRetryUntilEpochMs(600)
                .build(), observer);

        assertNull(observer.failure);
        assertTrue(observer.completed);
        assertEquals(1, observer.outcomes.size());
        assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(),
                SubmissionOutcomeMessageV1.decode(observer.outcomes.get(0).getSubmissionOutcomeNdr1().toByteArray())
                        .managed().definitelyNotQueued().error().code().wireValue());
    }

    private static ScheduleIntentV1 scheduleIntent() {
        return ScheduleIntentV1.create(new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60),
                        ProfileKindV1.DESTINATION), new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300, 800, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("key"),
                Bytes.utf8("payload"), null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, java.util.List.of())),
                null, null);
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
            implements StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1> {
        private final java.util.List<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1> outcomes =
                new java.util.ArrayList<>();
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1 value) {
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
        private final PreparedSubmissionV1 prepared;

        private Core(final PreparedSubmissionV1 prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                     final long retryUntilEpochMs,
                                                     final SubmissionModeV1 submissionMode) {
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
        public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant, final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition,
                                             final long retryUntilEpochMs) {
            return CommandCodec.decodeFrameV1(prepared.managedFrame());
        }

        @Override
        public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                                 final DelayMessageId messageId,
                                                 final MessagePreconditionV1 precondition,
                                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                                 final long retryUntilEpochMs) {
            return CommandCodec.decodeFrameV1(prepared.managedFrame());
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand command) {
            return prepared;
        }
    }

    private static final class Coordinator implements SubmissionCoordinator {
        private final PreparedCommand command;

        private Coordinator(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public CompletionStage<SubmissionOutcomeMessageV1> submit(final AuthenticatedTenantContext tenant,
                                                                    final PreparedSubmissionV1 submission,
                                                                    final TransportOwnershipPermit permit) {
            return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
        }
    }
}
