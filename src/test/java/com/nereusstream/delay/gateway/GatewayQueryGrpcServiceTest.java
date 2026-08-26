package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.protobuf.ByteString;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class GatewayQueryGrpcServiceTest {
    @Test
    void queryAndBoundedAwaitUseCanonicalLocatorsAndQueryIngress() {
        final TrustedClock clock = () -> 100;
        final AuthenticatedTenantContext tenant = tenant();
        final QueryFixture fixture = fixture();
        final GatewayQueryAuthority authority = new GatewayQueryAuthority() {
            @Override
            public CompletionStage<CommandQueryResponse> getCommandResult(
                    final AuthenticatedTenantContext context, final GatewayGetCommandResultRequest request) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, request.receipt());
                return CompletableFuture.completedFuture(CommandQueryResponse.unknown());
            }

            @Override
            public CompletionStage<List<CommandQueryResponse>> awaitApplied(
                    final AuthenticatedTenantContext context, final GatewayAwaitAppliedRequest request) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, request.receipt());
                return CompletableFuture.completedFuture(
                        List.of(CommandQueryResponse.unknown(), CommandQueryResponse.unknown()));
            }

            @Override
            public CompletionStage<MessageQueryResponse> getMessage(
                    final AuthenticatedTenantContext context, final GatewayGetMessageRequest request) {
                assertEquals(tenant, context);
                assertEquals(fixture.messageId, request.delayMessageId());
                return CompletableFuture.completedFuture(MessageQueryResponse.identityRetired());
            }
        };
        final InMemoryGatewayAuditSink audit = new InMemoryGatewayAuditSink(8);
        final GatewayQueryIngressService queryIngress = new GatewayQueryIngressService(
                authority, peer -> tenant, new InMemoryGatewayAdmissionController(1, 4096, 1, 1), audit, clock, 2);
        final GatewayGrpcService service = new GatewayGrpcService(
                noopIngress(clock, tenant),
                () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY),
                null,
                queryIngress);

        final QueryObserver command = new QueryObserver();
        service.getCommandResult(
                com.nereusstream.delay.gateway.wire.GatewayGetCommandResultRequest.newBuilder()
                        .setCanonicalCommandQueuedReceipt(ByteString.copyFrom(fixture.receipt.payload()))
                        .build(),
                command);
        assertNull(command.failure);
        assertTrue(command.completed);
        assertEquals(
                CommandQueryResponse.unknown(),
                CommandQueryResponse.decode(
                        command.responses.get(0).getCommandQueryResponse().toByteArray()));

        final QueryObserver await = new QueryObserver();
        service.awaitApplied(
                com.nereusstream.delay.gateway.wire.GatewayAwaitAppliedRequest.newBuilder()
                        .setCanonicalCommandQueuedReceipt(ByteString.copyFrom(fixture.receipt.payload()))
                        .build(),
                await);
        assertNull(await.failure);
        assertTrue(await.completed);
        assertEquals(2, await.responses.size());

        final MessageObserver message = new MessageObserver();
        service.getMessage(
                com.nereusstream.delay.gateway.wire.GatewayGetMessageRequest.newBuilder()
                        .setDelayMessageId(ByteString.copyFrom(fixture.messageId.bytes()))
                        .build(),
                message);
        assertNull(message.failure);
        assertTrue(message.completed);
        assertEquals(
                MessageQueryResponse.identityRetired(),
                MessageQueryResponse.decode(
                        message.response.getMessageQueryResponse().toByteArray()));
        assertEquals(6, audit.canonicalEvents().size());
    }

    private static GatewayIngressService noopIngress(
            final TrustedClock clock, final AuthenticatedTenantContext tenant) {
        final GatewayScheduleService domain = new GatewayScheduleService(
                new NoopCore(), new InMemoryGatewayIdempotencyStore(clock, 10, 20), new NoopCoordinator(), clock);
        return new GatewayIngressService(
                domain,
                peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(8),
                clock);
    }

    private static QueryFixture fixture() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 30), ProfileKind.DESTINATION),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 31)),
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
        final PreparedCommand command = PreparedCommand.schedule(shard, intent, 600);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "gateway", UUID.randomUUID(), 3, null, 100);
        final CanonicalCommandQueuedReceipt.KafkaQueuedAck ack = new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                "gateway", source.nativeTopicUuid(), 0, 3, null, 100, Bytes.sha256(Bytes.utf8("response")));
        final CanonicalCommandQueuedReceipt receipt =
                CanonicalCommandQueuedReceipt.create(command, source, ack, 5_000, bytes(16, 40));
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

    private record QueryFixture(CanonicalCommandQueuedReceipt receipt, DelayMessageId messageId) {}

    private static final class QueryObserver
            implements StreamObserver<com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse> {
        private final java.util.ArrayList<com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse> responses =
                new java.util.ArrayList<>();
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse value) {
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
            implements StreamObserver<com.nereusstream.delay.gateway.wire.GatewayMessageQueryResponse> {
        private com.nereusstream.delay.gateway.wire.GatewayMessageQueryResponse response;
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.wire.GatewayMessageQueryResponse value) {
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
        public com.nereusstream.delay.protocol.PreparedSubmission prepareSchedule(
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
        public com.nereusstream.delay.protocol.PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoopCoordinator implements SubmissionCoordinator {
        @Override
        public CompletionStage<SubmissionOutcomeMessage> submit(
                final AuthenticatedTenantContext tenant,
                final com.nereusstream.delay.protocol.PreparedSubmission submission,
                final TransportOwnershipPermit permit) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
