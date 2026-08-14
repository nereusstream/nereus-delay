package io.nereusstream.delay.gateway;

import com.google.protobuf.ByteString;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayQueryGrpcServiceTest {
    @Test
    void queryAndBoundedAwaitUseCanonicalLocatorsAndQueryIngress() {
        final TrustedClock clock = () -> 100;
        final AuthenticatedTenantContext tenant = tenant();
        final QueryFixture fixture = fixture();
        final GatewayQueryAuthority authority = new GatewayQueryAuthority() {
            @Override
            public CompletionStage<CommandQueryResponseV1> getCommandResult(
                    final AuthenticatedTenantContext context, final GatewayGetCommandResultRequestV1 request) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, request.receipt());
                return CompletableFuture.completedFuture(CommandQueryResponseV1.unknown());
            }

            @Override
            public CompletionStage<List<CommandQueryResponseV1>> awaitApplied(
                    final AuthenticatedTenantContext context, final GatewayAwaitAppliedRequestV1 request) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, request.receipt());
                return CompletableFuture.completedFuture(List.of(CommandQueryResponseV1.unknown(),
                        CommandQueryResponseV1.unknown()));
            }

            @Override
            public CompletionStage<MessageQueryResponseV1> getMessage(
                    final AuthenticatedTenantContext context, final GatewayGetMessageRequestV1 request) {
                assertEquals(tenant, context);
                assertEquals(fixture.messageId, request.delayMessageId());
                return CompletableFuture.completedFuture(MessageQueryResponseV1.identityRetired());
            }
        };
        final InMemoryGatewayAuditSink audit = new InMemoryGatewayAuditSink(8);
        final GatewayQueryIngressService queryIngress = new GatewayQueryIngressService(authority, peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1), audit, clock, 2);
        final GatewayGrpcService service = new GatewayGrpcService(noopIngress(clock, tenant),
                () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY), null, queryIngress);

        final QueryObserver command = new QueryObserver();
        service.getCommandResult(io.nereusstream.delay.gateway.v1.GatewayGetCommandResultRequestV1.newBuilder()
                .setCommandQueuedReceiptV1(ByteString.copyFrom(fixture.receipt.payload()))
                .build(), command);
        assertNull(command.failure);
        assertTrue(command.completed);
        assertEquals(CommandQueryResponseV1.unknown(), CommandQueryResponseV1.decode(
                command.responses.get(0).getCommandQueryResponseV1().toByteArray()));

        final QueryObserver await = new QueryObserver();
        service.awaitApplied(io.nereusstream.delay.gateway.v1.GatewayAwaitAppliedRequestV1.newBuilder()
                .setCommandQueuedReceiptV1(ByteString.copyFrom(fixture.receipt.payload()))
                .build(), await);
        assertNull(await.failure);
        assertTrue(await.completed);
        assertEquals(2, await.responses.size());

        final MessageObserver message = new MessageObserver();
        service.getMessage(io.nereusstream.delay.gateway.v1.GatewayGetMessageRequestV1.newBuilder()
                .setDelayMessageId(ByteString.copyFrom(fixture.messageId.bytes()))
                .build(), message);
        assertNull(message.failure);
        assertTrue(message.completed);
        assertEquals(MessageQueryResponseV1.identityRetired(), MessageQueryResponseV1.decode(
                message.response.getMessageQueryResponseV1().toByteArray()));
        assertEquals(6, audit.canonicalEvents().size());
    }

    private static GatewayIngressService noopIngress(final TrustedClock clock,
                                                      final AuthenticatedTenantContext tenant) {
        final GatewayScheduleService domain = new GatewayScheduleService(new NoopCore(),
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), new NoopCoordinator(), clock);
        return new GatewayIngressService(domain, peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1), new InMemoryGatewayAuditSink(8), clock);
    }

    private static QueryFixture fixture() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(
                new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 30), ProfileKindV1.DESTINATION),
                new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 31)), 300, 800, DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT, Bytes.utf8("key"), Bytes.utf8("payload"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 600);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "gateway", UUID.randomUUID(), 3,
                null, 100);
        final CommandQueuedReceiptV1.KafkaQueuedAck ack = new CommandQueuedReceiptV1.KafkaQueuedAck(
                "gateway", source.nativeTopicUuid(), 0, 3, null, 100, Bytes.sha256(Bytes.utf8("response")));
        final CommandQueuedReceiptV1 receipt = CommandQueuedReceiptV1.create(command, source, ack, 5_000,
                bytes(16, 40));
        return new QueryFixture(receipt, command.delayMessageId());
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

    private record QueryFixture(CommandQueuedReceiptV1 receipt, DelayMessageId messageId) {
    }

    private static final class QueryObserver
            implements StreamObserver<io.nereusstream.delay.gateway.v1.GatewayCommandQueryResponseV1> {
        private final java.util.ArrayList<io.nereusstream.delay.gateway.v1.GatewayCommandQueryResponseV1> responses =
                new java.util.ArrayList<>();
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final io.nereusstream.delay.gateway.v1.GatewayCommandQueryResponseV1 value) {
            responses.add(value);
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

    private static final class MessageObserver
            implements StreamObserver<io.nereusstream.delay.gateway.v1.GatewayMessageQueryResponseV1> {
        private io.nereusstream.delay.gateway.v1.GatewayMessageQueryResponseV1 response;
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final io.nereusstream.delay.gateway.v1.GatewayMessageQueryResponseV1 value) {
            response = value;
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

    private static final class NoopCore implements DelaySemanticCore {
        @Override
        public io.nereusstream.delay.protocol.PreparedSubmissionV1 prepareSchedule(
                final AuthenticatedTenantContext tenant, final RouteSelectionHint route,
                final ScheduleIntentV1 intent, final long retryUntilEpochMs,
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
        public io.nereusstream.delay.protocol.PreparedSubmissionV1 prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoopCoordinator implements SubmissionCoordinator {
        @Override
        public CompletionStage<SubmissionOutcomeMessageV1> submit(final AuthenticatedTenantContext tenant,
                                                                    final io.nereusstream.delay.protocol.PreparedSubmissionV1 submission,
                                                                    final TransportOwnershipPermit permit) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
