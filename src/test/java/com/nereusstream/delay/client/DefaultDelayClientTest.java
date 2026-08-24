package com.nereusstream.delay.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.AdapterMetadataV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandQueryResponseV1;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DlqExportStateV1;
import com.nereusstream.delay.protocol.FirstScheduleEligibilityV1;
import com.nereusstream.delay.protocol.KafkaMetadataV1;
import com.nereusstream.delay.protocol.MessagePreconditionV1;
import com.nereusstream.delay.protocol.MessageQueryResponseV1;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import com.nereusstream.delay.protocol.PayloadCommitProofV1;
import com.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import com.nereusstream.delay.protocol.PublicEvidenceRefV1;
import com.nereusstream.delay.protocol.RetryPolicyRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntentV1;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionModeV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.protocol.UploadHandleKindV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.submission.SubmissionCoordinator;
import com.nereusstream.delay.transport.CommandTransport;
import com.nereusstream.delay.transport.CommandTransportKey;
import com.nereusstream.delay.transport.CommandTransportRegistry;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DefaultDelayClientTest {
    @Test
    void outboxFinishFailureReturnsUncertainWithoutChangingPreparedAttempt() {
        final PreparedCommand command = command();
        final PreparedSubmissionV1 submission = PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
        final PhysicalEnqueueAttemptId attempt = PhysicalEnqueueAttemptId.random();
        final SubmissionCoordinator coordinator =
                (tenant, prepared, permit) -> CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                        WireIngressOutcomeSupport.localDefinite(command, StableCode.BROKER_DEFINITIVE_NOT_PERSISTED)));
        final ClientOutbox failingOutbox = new ClientOutbox() {
            @Override
            public void finish(
                    final PreparedSubmissionV1 prepared,
                    final PhysicalEnqueueAttemptId physicalAttempt,
                    final SubmissionOutcomeMessageV1 outcome) {
                throw new IllegalStateException("outbox completion evidence unavailable");
            }
        };

        try (DefaultDelayClient client = DefaultDelayClient.builder()
                .tenantContext(tenant())
                .defaultRoute(new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary")))
                .semanticCore(new UnsupportedSemanticCore())
                .submissionCoordinator(coordinator)
                .queryClient(new UnsupportedQueryClient())
                .outbox(failingOutbox)
                .build()) {
            final SubmissionOutcomeMessageV1 outcome = client.submit(submission, 10_000, attempt.bytes())
                    .toCompletableFuture()
                    .join();

            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED, outcome.kind());
            assertEquals(
                    com.nereusstream.delay.protocol.EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN,
                    outcome.managed().kind());
            assertArrayEquals(attempt.bytes(), outcome.managed().uncertain().physicalEnqueueAttemptId());
            assertEquals(
                    StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    outcome.managed().uncertain().error().code());
        }
    }

    @Test
    void closeRetriesEveryChildAfterTheFirstCloseFailure() {
        final AtomicInteger outboxCloseCalls = new AtomicInteger();
        final AtomicInteger queryCloseCalls = new AtomicInteger();
        final AtomicInteger transportCloseCalls = new AtomicInteger();
        final ClientOutbox outbox = new ClientOutbox() {
            @Override
            public void close() {
                if (outboxCloseCalls.incrementAndGet() == 1) {
                    throw new IllegalStateException("simulated outbox close failure");
                }
            }
        };
        final UnsupportedQueryClient query = new UnsupportedQueryClient(queryCloseCalls::incrementAndGet);
        final CommandTransportRegistry transports = new CommandTransportRegistry() {
            @Override
            public CommandTransport exact(final CommandTransportKey key) {
                return null;
            }

            @Override
            public void close() {
                transportCloseCalls.incrementAndGet();
            }
        };
        final SubmissionCoordinator coordinator =
                (tenant, prepared, permit) -> CompletableFuture.failedFuture(new UnsupportedOperationException());
        final DefaultDelayClient client = DefaultDelayClient.builder()
                .tenantContext(tenant())
                .defaultRoute(new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary")))
                .semanticCore(new UnsupportedSemanticCore())
                .submissionCoordinator(coordinator)
                .queryClient(query)
                .outbox(outbox)
                .transportRegistry(transports)
                .build();
        try {
            final IllegalStateException failure = assertThrows(IllegalStateException.class, client::close);

            assertEquals("simulated outbox close failure", failure.getMessage());
            assertEquals(1, outboxCloseCalls.get());
            assertEquals(1, queryCloseCalls.get());
            assertEquals(1, transportCloseCalls.get());

            client.close();

            assertEquals(2, outboxCloseCalls.get());
            assertEquals(2, queryCloseCalls.get());
            assertEquals(2, transportCloseCalls.get());
        } finally {
            if (outboxCloseCalls.get() < 2) {
                client.close();
            }
        }
    }

    private static PreparedCommand command() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        return PreparedCommand.scheduleV1(
                shard,
                ScheduleIntentV1.create(
                        new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKindV1.DESTINATION),
                        new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                        300,
                        800,
                        DeliveryMode.MANAGED,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("key"),
                        Bytes.utf8("payload"),
                        null,
                        AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                        null,
                        null),
                600);
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

    private static final class UnsupportedSemanticCore implements DelaySemanticCore {
        @Override
        public com.nereusstream.delay.protocol.PreparedSubmissionV1 prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final ScheduleIntentV1 intent,
                final long retryUntilEpochMs,
                final SubmissionModeV1 submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparationV1 request,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(
                final AuthenticatedTenantContext tenant,
                final PayloadReservationReceiptV1 reservation,
                final PayloadCommitProofV1 proof,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePreconditionV1 precondition,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePreconditionV1 precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class UnsupportedQueryClient implements QueryClient {
        private final Runnable closeAction;

        private UnsupportedQueryClient() {
            this(() -> {});
        }

        private UnsupportedQueryClient(final Runnable closeAction) {
            this.closeAction = closeAction;
        }

        @Override
        public void close() {
            closeAction.run();
        }

        @Override
        public CompletionStage<CommandQueryResponseV1> getCommandResult(
                final CommandQueuedReceiptV1 receipt,
                final long nowEpochMs,
                final long fullResultRetainUntilEpochMs,
                final PublicDestinationBindingViewV1 binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<CommandQueryResponseV1> getCommandResult(
                final CommandQueuedReceiptV1 receipt,
                final long nowEpochMs,
                final com.nereusstream.delay.adapter.CommandResultRetentionPolicy policy,
                final PublicDestinationBindingViewV1 binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<CommandQueryResponseV1> awaitAppliedV1(
                final CommandQueuedReceiptV1 receipt,
                final long nowEpochMs,
                final long fullResultRetainUntilEpochMs,
                final PublicDestinationBindingViewV1 binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<CommandQueryResponseV1> awaitAppliedV1(
                final CommandQueuedReceiptV1 receipt,
                final long nowEpochMs,
                final com.nereusstream.delay.adapter.CommandResultRetentionPolicy policy,
                final PublicDestinationBindingViewV1 binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<MessageQueryResponseV1> getMessage(
                final DelayMessageId messageId,
                final PublicDestinationBindingViewV1 binding,
                final DlqExportStateV1 dlqExportState,
                final PublicEvidenceRefV1 evidence,
                final FirstScheduleEligibilityV1 unknownEligibility) {
            return unsupported();
        }

        @Override
        public CompletionStage<PayloadUploadHandleResponseV1> issuePayloadUploadHandle(
                final PayloadReservationReceiptV1 reservation, final UploadHandleKindV1 kind, final long nowEpochMs) {
            return unsupported();
        }

        @Override
        public CompletionStage<PayloadAttestationResponseV1> attestPayloadUpload(
                final PayloadReservationReceiptV1 reservation,
                final OpaquePayloadUploadHandleV1 handle,
                final long nowEpochMs) {
            return unsupported();
        }

        @Override
        public CompletionStage<com.nereusstream.delay.runtime.CommandResult> awaitApplied(
                final CommandQueuedReceipt receipt) {
            return unsupported();
        }

        private static <T> CompletionStage<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
