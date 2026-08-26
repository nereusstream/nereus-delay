package com.nereusstream.delay.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.FirstScheduleEligibility;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PublicDestinationBindingView;
import com.nereusstream.delay.protocol.PublicEvidenceRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparation;
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
        final PreparedSubmission submission = PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
        final PhysicalEnqueueAttemptId attempt = PhysicalEnqueueAttemptId.random();
        final SubmissionCoordinator coordinator =
                (tenant, prepared, permit) -> CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                        WireIngressOutcomeSupport.localDefinite(command, StableCode.BROKER_DEFINITIVE_NOT_PERSISTED)));
        final ClientOutbox failingOutbox = new ClientOutbox() {
            @Override
            public void finish(
                    final PreparedSubmission prepared,
                    final PhysicalEnqueueAttemptId physicalAttempt,
                    final SubmissionOutcomeMessage outcome) {
                throw new IllegalStateException("outbox completion evidence unavailable");
            }
        };

        try (DefaultDelayClient client = DefaultDelayClient.builder()
                .tenantContext(tenant())
                .defaultRoute(new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary")))
                .semanticCore(new UnsupportedSemanticCore())
                .submissionCoordinator(coordinator)
                .queryClient(new UnsupportedQueryClient())
                .outbox(failingOutbox)
                .build()) {
            final SubmissionOutcomeMessage outcome = client.submit(submission, 10_000, attempt.bytes())
                    .toCompletableFuture()
                    .join();

            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    com.nereusstream.delay.protocol.EnqueueOutcomeKind.ENQUEUE_UNCERTAIN,
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
                .defaultRoute(new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary")))
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
        return PreparedCommand.schedule(
                shard,
                CanonicalScheduleIntent.create(
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
        public PreparedSubmission prepareManaged(
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
        public CompletionStage<CommandQueryResponse> getCommandResult(
                final CanonicalCommandQueuedReceipt receipt,
                final long nowEpochMs,
                final long fullResultRetainUntilEpochMs,
                final PublicDestinationBindingView binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<CommandQueryResponse> getCommandResult(
                final CanonicalCommandQueuedReceipt receipt,
                final long nowEpochMs,
                final com.nereusstream.delay.adapter.CommandResultRetentionPolicy policy,
                final PublicDestinationBindingView binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<CommandQueryResponse> awaitApplied(
                final CanonicalCommandQueuedReceipt receipt,
                final long nowEpochMs,
                final long fullResultRetainUntilEpochMs,
                final PublicDestinationBindingView binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<CommandQueryResponse> awaitApplied(
                final CanonicalCommandQueuedReceipt receipt,
                final long nowEpochMs,
                final com.nereusstream.delay.adapter.CommandResultRetentionPolicy policy,
                final PublicDestinationBindingView binding) {
            return unsupported();
        }

        @Override
        public CompletionStage<MessageQueryResponse> getMessage(
                final DelayMessageId messageId,
                final PublicDestinationBindingView binding,
                final DlqExportState dlqExportState,
                final PublicEvidenceRef evidence,
                final FirstScheduleEligibility unknownEligibility) {
            return unsupported();
        }

        @Override
        public CompletionStage<PayloadUploadHandleResponse> issuePayloadUploadHandle(
                final PayloadReservationReceipt reservation, final UploadHandleKind kind, final long nowEpochMs) {
            return unsupported();
        }

        @Override
        public CompletionStage<PayloadAttestationResponse> attestPayloadUpload(
                final PayloadReservationReceipt reservation,
                final OpaquePayloadUploadHandle handle,
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
