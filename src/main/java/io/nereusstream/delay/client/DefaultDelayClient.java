package io.nereusstream.delay.client;

import io.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import io.nereusstream.delay.adapter.QueuedReceiptQueryPolicy;
import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.FirstScheduleEligibilityV1;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.PublicEvidenceRefV1;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.route.RouteSnapshotProvider;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DefaultDelaySemanticCore;
import io.nereusstream.delay.semantic.DelaySemanticCore;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.LogicalUuidV7Generator;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.SemanticPreparationException;
import io.nereusstream.delay.semantic.SecureLogicalUuidV7Generator;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.semantic.NativePreparationSnapshotProvider;
import io.nereusstream.delay.submission.DefaultSubmissionCoordinator;
import io.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import io.nereusstream.delay.submission.SubmissionCoordinator;
import io.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
import io.nereusstream.delay.transport.CommandTransportRegistry;
import io.nereusstream.delay.transport.LocalTransportOwnershipPermit;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Explicitly configured Direct SDK facade over the shared Semantic Core/coordinator. */
public final class DefaultDelayClient implements DelayClient {
    private final AuthenticatedTenantContext tenant;
    private final RouteSelectionHint defaultRoute;
    private final DelaySemanticCore semanticCore;
    private final SubmissionCoordinator submissions;
    private final QueryClient queryClient;
    private final ClientAdmission admission;
    private final ClientOutbox outbox;
    private final CommandTransportRegistry transportRegistry;
    private final AtomicBoolean closed = new AtomicBoolean();

    private DefaultDelayClient(final Builder builder, final DelaySemanticCore semanticCore,
                               final SubmissionCoordinator submissions) {
        this.tenant = builder.tenant;
        this.defaultRoute = builder.defaultRoute;
        this.semanticCore = semanticCore;
        this.submissions = submissions;
        this.queryClient = builder.queryClient;
        this.admission = builder.admission;
        this.outbox = builder.outbox;
        this.transportRegistry = builder.transportRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PreparedCommand prepareSchedule(final ScheduleIntent intent, final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedCommand prepareScheduleV1(final ScheduleIntentV1 intent, final long retryUntilEpochMs) {
        ensureOpen();
        try {
            return CommandCodec.decodeFrameV1(prepareScheduleSubmissionV1(intent, retryUntilEpochMs,
                    SubmissionModeV1.MANAGED).managedFrame());
        } catch (SemanticPreparationException failure) {
            throw new PreparationFailure(failure.error(), failure);
        } catch (RuntimeException failure) {
            throw PreparationFailure.of(StableCode.INVALID_PREPARED_COMMAND, failure);
        }
    }

    @Override
    public PreparedCommand prepareLargeSchedule(final LargeScheduleIntent intent, final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedCommand prepareLargeScheduleV1(final ScheduleIntentV1 intentWithoutPayload,
                                                  final long expectedPayloadLength, final byte[] payloadSha256,
                                                  final long reservationTtlMs,
                                                  final PayloadProofTrustSetRefV1 trustSet,
                                                  final ProfileRefV1 objectStoreProfile,
                                                  final long retryUntilEpochMs) {
        ensureOpen();
        final PreparedCommand command = semanticCore.prepareLargeSchedule(tenant, defaultRoute,
                new LargeSchedulePreparationV1(intentWithoutPayload, expectedPayloadLength, payloadSha256,
                        reservationTtlMs, trustSet, objectStoreProfile), retryUntilEpochMs);
        return command;
    }

    @Override
    public PreparedCommand prepareLargePayloadCommit(final PayloadReservationReceiptV1 reservation,
                                                      final PayloadCommitProofV1 proof,
                                                      final long retryUntilEpochMs) {
        ensureOpen();
        return semanticCore.preparePayloadCommit(tenant, reservation, proof, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareCancel(final DelayMessageId messageId, final int expectedGeneration,
                                         final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedCommand prepareCancelV1(final DelayMessageId messageId, final MessagePreconditionV1 precondition,
                                           final long retryUntilEpochMs) {
        ensureOpen();
        return semanticCore.prepareCancel(tenant, messageId, precondition, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareReschedule(final DelayMessageId messageId, final int expectedGeneration,
                                             final long deliverAtEpochMs, final long expireAtEpochMs,
                                             final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedCommand prepareRescheduleV1(final DelayMessageId messageId, final MessagePreconditionV1 precondition,
                                               final long deliverAtEpochMs, final long expireAtEpochMs,
                                               final long retryUntilEpochMs) {
        ensureOpen();
        return semanticCore.prepareReschedule(tenant, messageId, precondition, deliverAtEpochMs,
                expireAtEpochMs, retryUntilEpochMs);
    }

    @Override
    public PreparedSubmissionV1 prepareManagedSubmissionV1(final PreparedCommand command) {
        ensureOpen();
        return semanticCore.prepareManaged(tenant, command);
    }

    @Override
    public PreparedSubmissionV1 prepareScheduleSubmissionV1(final ScheduleIntentV1 intent,
                                                            final long retryUntilEpochMs,
                                                            final SubmissionModeV1 submissionMode) {
        ensureOpen();
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(submissionMode, "submissionMode");
        try {
            return semanticCore.prepareSchedule(tenant, defaultRoute, intent, retryUntilEpochMs, submissionMode);
        } catch (SemanticPreparationException failure) {
            throw new PreparationFailure(failure.error(), failure);
        } catch (RuntimeException failure) {
            throw PreparationFailure.of(StableCode.INVALID_PREPARED_COMMAND, failure);
        }
    }

    @Override
    public PreparedSubmissionV1 prepareAutoFast(final AutoFastSchedule request) {
        ensureOpen();
        Objects.requireNonNull(request, "request");
        if (request.nativeCandidate() != null) {
            throw PreparationFailure.of(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE);
        }
        return semanticCore.prepareManaged(tenant, request.managedCommand());
    }

    @Override
    public List<PreparedSubmissionV1> prepareAutoFastBatch(final List<AutoFastSchedule> requests) {
        ensureOpen();
        Objects.requireNonNull(requests, "requests");
        final List<PreparedSubmissionV1> result = new ArrayList<>(requests.size());
        for (AutoFastSchedule request : requests) {
            result.add(prepareAutoFast(request));
        }
        return List.copyOf(result);
    }

    @Override
    public CompletionStage<SubmissionOutcomeMessageV1> submit(final PreparedSubmissionV1 submission,
                                                              final long receiptQueryUntilEpochMs,
                                                              final byte[] physicalEnqueueAttemptId) {
        return submitInternal(submission, PhysicalEnqueueAttemptId.require(physicalEnqueueAttemptId));
    }

    @Override
    public CompletionStage<SubmissionOutcomeMessageV1> submit(final PreparedSubmissionV1 submission,
                                                              final QueuedReceiptQueryPolicy routePolicy,
                                                              final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(routePolicy, "routePolicy");
        return submitInternal(submission, PhysicalEnqueueAttemptId.require(physicalEnqueueAttemptId));
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        return enqueueV1(command);
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueueV1(final PreparedCommand command) {
        ensureOpen();
        final PreparedSubmissionV1 submission = prepareManagedSubmissionV1(command);
        return submitInternal(submission, PhysicalEnqueueAttemptId.random()).thenApply(outcome -> toLegacy(command,
                outcome));
    }

    @Override
    public CompletionStage<List<EnqueueOutcome>> enqueueBatch(final List<PreparedCommand> commands) {
        return enqueueBatchV1(commands);
    }

    @Override
    public CompletionStage<List<EnqueueOutcome>> enqueueBatchV1(final List<PreparedCommand> commands) {
        ensureOpen();
        Objects.requireNonNull(commands, "commands");
        final List<CompletionStage<EnqueueOutcome>> stages = new ArrayList<>(commands.size());
        for (PreparedCommand command : commands) {
            stages.add(enqueueV1(command));
        }
        final CompletableFuture<?>[] futures = stages.stream().map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures).thenApply(ignored -> stages.stream()
                .map(stage -> stage.toCompletableFuture().join()).toList());
    }

    @Override
    public CompletionStage<CommandQueryResponseV1> getCommandResult(final CommandQueuedReceiptV1 receipt,
                                                                     final long nowEpochMs,
                                                                     final long fullResultRetainUntilEpochMs,
                                                                     final PublicDestinationBindingViewV1 binding) {
        ensureOpen();
        return queryClient.getCommandResult(receipt, nowEpochMs, fullResultRetainUntilEpochMs, binding);
    }

    @Override
    public CompletionStage<CommandQueryResponseV1> getCommandResult(final CommandQueuedReceiptV1 receipt,
                                                                     final long nowEpochMs,
                                                                     final CommandResultRetentionPolicy policy,
                                                                     final PublicDestinationBindingViewV1 binding) {
        ensureOpen();
        return queryClient.getCommandResult(receipt, nowEpochMs, policy, binding);
    }

    @Override
    public CompletionStage<CommandQueryResponseV1> awaitAppliedV1(final CommandQueuedReceiptV1 receipt,
                                                                   final long nowEpochMs,
                                                                   final long fullResultRetainUntilEpochMs,
                                                                   final PublicDestinationBindingViewV1 binding) {
        ensureOpen();
        return queryClient.awaitAppliedV1(receipt, nowEpochMs, fullResultRetainUntilEpochMs, binding);
    }

    @Override
    public CompletionStage<CommandQueryResponseV1> awaitAppliedV1(final CommandQueuedReceiptV1 receipt,
                                                                   final long nowEpochMs,
                                                                   final CommandResultRetentionPolicy policy,
                                                                   final PublicDestinationBindingViewV1 binding) {
        ensureOpen();
        return queryClient.awaitAppliedV1(receipt, nowEpochMs, policy, binding);
    }

    @Override
    public CompletionStage<MessageQueryResponseV1> getMessage(final DelayMessageId messageId,
                                                               final PublicDestinationBindingViewV1 binding,
                                                               final DlqExportStateV1 dlqExportState,
                                                               final PublicEvidenceRefV1 evidence,
                                                               final FirstScheduleEligibilityV1 unknownEligibility) {
        ensureOpen();
        return queryClient.getMessage(messageId, binding, dlqExportState, evidence, unknownEligibility);
    }

    @Override
    public CompletionStage<PayloadUploadHandleResponseV1> issuePayloadUploadHandle(
            final PayloadReservationReceiptV1 reservation, final UploadHandleKindV1 kind, final long nowEpochMs) {
        ensureOpen();
        return queryClient.issuePayloadUploadHandle(reservation, kind, nowEpochMs);
    }

    @Override
    public CompletionStage<PayloadAttestationResponseV1> attestPayloadUpload(
            final PayloadReservationReceiptV1 reservation, final OpaquePayloadUploadHandleV1 handle,
            final long nowEpochMs) {
        ensureOpen();
        return queryClient.attestPayloadUpload(reservation, handle, nowEpochMs);
    }

    @Override
    public CompletionStage<CommandResult> awaitApplied(final CommandQueuedReceipt receipt) {
        ensureOpen();
        return queryClient.awaitApplied(receipt);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RuntimeException first = null;
        try {
            outbox.close();
        } catch (RuntimeException failure) {
            first = failure;
        }
        try {
            queryClient.close();
        } catch (RuntimeException failure) {
            if (first == null) {
                first = failure;
            } else {
                first.addSuppressed(failure);
            }
        }
        if (transportRegistry != null) {
            try {
                transportRegistry.close();
            } catch (RuntimeException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private CompletionStage<SubmissionOutcomeMessageV1> submitInternal(final PreparedSubmissionV1 submission,
                                                                        final PhysicalEnqueueAttemptId attempt) {
        ensureOpen();
        Objects.requireNonNull(submission, "submission");
        try {
            admission.admit(submission);
            outbox.start(submission, attempt);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return submissions.submit(tenant, submission, new LocalTransportOwnershipPermit(attempt))
                .thenApply(outcome -> {
                    try {
                        outbox.finish(submission, attempt, outcome);
                        return outcome;
                    } catch (RuntimeException outboxFailure) {
                        // The transport outcome may already be real, but the
                        // local durable completion evidence is not. Preserve
                        // the exact branch/attempt and fail closed as a
                        // retryable public outcome rather than exceptional
                        // completion that hides the ambiguity.
                        return WireIngressOutcomeSupport.uncertain(submission, attempt);
                    }
                });
    }

    private static EnqueueOutcome toLegacy(final PreparedCommand command,
                                           final SubmissionOutcomeMessageV1 outcome) {
        if (outcome.kind() == io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED) {
            final var managed = outcome.managed();
            return switch (managed.kind()) {
                case QUEUED -> {
                    final CommandQueuedReceiptV1 receipt = managed.queued();
                    yield EnqueueOutcome.queued(command, new CommandQueuedReceipt(command.commandId(),
                            command.delayMessageId(), command.shardId(), receipt.sourcePosition()));
                }
                case DEFINITELY_NOT_QUEUED -> EnqueueOutcome.definitelyNotQueued(command,
                        managed.definitelyNotQueued().error().code().wireValue());
                case ENQUEUE_UNCERTAIN -> EnqueueOutcome.uncertain(command,
                        managed.uncertain().error().code().wireValue());
            };
        }
        return switch (outcome.kind()) {
            case NATIVE_RECEIPT -> {
                final var ack = outcome.nativeReceipt().brokerAck();
                final var entryKind = ack.batchSize() == 1
                        ? io.nereusstream.delay.protocol.PulsarSourcePosition.EntryKind.NON_BATCH
                        : io.nereusstream.delay.protocol.PulsarSourcePosition.EntryKind.BATCH;
                final var source = new io.nereusstream.delay.protocol.PulsarSourcePosition(command.shardId(),
                        ack.brokerResourceIncarnation(), ack.physicalTopic(), ack.ledgerId(), ack.entryId(),
                        ack.normalizedBatchIndex(), ack.batchSize(), entryKind, ack.brokerEntryTimestampEpochMs());
                yield EnqueueOutcome.queued(command, new CommandQueuedReceipt(command.commandId(),
                        command.delayMessageId(), command.shardId(), source));
            }
            case NATIVE_DEFINITELY_NOT_QUEUED -> EnqueueOutcome.definitelyNotQueued(command,
                    outcome.nativeDefinitelyNotQueued().error().code().wireValue());
            case NATIVE_ENQUEUE_UNCERTAIN -> EnqueueOutcome.uncertain(command,
                    outcome.nativeUncertain().error().code().wireValue());
            case MANAGED -> throw new IllegalStateException("managed branch handled above");
        };
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw PreparationFailure.of(StableCode.CLIENT_CLOSED);
        }
    }

    public static final class Builder {
        private AuthenticatedTenantContext tenant;
        private RouteSelectionHint defaultRoute;
        private DelaySemanticCore semanticCore;
        private RouteSnapshotProvider routeSnapshotProvider;
        private LogicalUuidV7Generator uuidGenerator;
        private TrustedClock trustedClock;
        private NativePreparationSnapshotProvider nativePreparationSnapshotProvider;
        private SubmissionCoordinator submissionCoordinator;
        private CommandTransportRegistry transportRegistry;
        private SubmissionOutcomeProjectorRegistry projectorRegistry;
        private QueryClient queryClient;
        private ClientAdmission admission = submission -> { };
        private ClientOutbox outbox = new ClientOutbox() { };

        public Builder tenantContext(final AuthenticatedTenantContext value) {
            tenant = value;
            return this;
        }

        public Builder defaultRoute(final RouteSelectionHint value) {
            defaultRoute = value;
            return this;
        }

        public Builder semanticCore(final DelaySemanticCore value) {
            semanticCore = value;
            return this;
        }

        public Builder routeSnapshotProvider(final RouteSnapshotProvider value) {
            routeSnapshotProvider = value;
            return this;
        }

        public Builder uuidGenerator(final LogicalUuidV7Generator value) {
            uuidGenerator = value;
            return this;
        }

        public Builder trustedClock(final TrustedClock value) {
            trustedClock = value;
            return this;
        }

        public Builder nativePreparationSnapshotProvider(final NativePreparationSnapshotProvider value) {
            nativePreparationSnapshotProvider = value;
            return this;
        }

        public Builder submissionCoordinator(final SubmissionCoordinator value) {
            submissionCoordinator = value;
            return this;
        }

        public Builder transportRegistry(final CommandTransportRegistry value) {
            transportRegistry = value;
            return this;
        }

        public Builder projectorRegistry(final SubmissionOutcomeProjectorRegistry value) {
            projectorRegistry = value;
            return this;
        }

        public Builder queryClient(final QueryClient value) {
            queryClient = value;
            return this;
        }

        public Builder admission(final ClientAdmission value) {
            admission = value;
            return this;
        }

        public Builder outbox(final ClientOutbox value) {
            outbox = value;
            return this;
        }

        public DefaultDelayClient build() {
            Objects.requireNonNull(tenant, "tenantContext");
            Objects.requireNonNull(defaultRoute, "defaultRoute");
            Objects.requireNonNull(queryClient, "queryClient");
            Objects.requireNonNull(admission, "admission");
            Objects.requireNonNull(outbox, "outbox");
            final TrustedClock clock = trustedClock == null ? System::currentTimeMillis : trustedClock;
            final DelaySemanticCore resolvedCore;
            if (semanticCore != null) {
                resolvedCore = semanticCore;
            } else {
                Objects.requireNonNull(routeSnapshotProvider, "routeSnapshotProvider");
                resolvedCore = new DefaultDelaySemanticCore(routeSnapshotProvider,
                        uuidGenerator == null ? new SecureLogicalUuidV7Generator() : uuidGenerator, clock,
                        nativePreparationSnapshotProvider, io.nereusstream.delay.semantic.NativeDeliveryIdGenerator.random());
            }
            final SubmissionCoordinator resolvedCoordinator;
            if (submissionCoordinator != null) {
                resolvedCoordinator = submissionCoordinator;
            } else {
                Objects.requireNonNull(routeSnapshotProvider, "routeSnapshotProvider");
                Objects.requireNonNull(transportRegistry, "transportRegistry");
                Objects.requireNonNull(projectorRegistry, "projectorRegistry");
                resolvedCoordinator = new DefaultSubmissionCoordinator(
                        new RouteBoundSubmissionTransportPlanResolver(routeSnapshotProvider, clock),
                        transportRegistry, projectorRegistry);
            }
            return new DefaultDelayClient(this, resolvedCore, resolvedCoordinator);
        }
    }
}
