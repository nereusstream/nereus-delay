package com.nereusstream.delay.client;

import com.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import com.nereusstream.delay.adapter.QueuedReceiptQueryPolicy;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.FirstScheduleEligibility;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PublicDestinationBindingView;
import com.nereusstream.delay.protocol.PublicEvidenceRef;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DefaultDelaySemanticCore;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparation;
import com.nereusstream.delay.semantic.LogicalUuidV7Generator;
import com.nereusstream.delay.semantic.NativePreparationSnapshotProvider;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.SecureLogicalUuidV7Generator;
import com.nereusstream.delay.semantic.SemanticPreparationException;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.submission.DefaultSubmissionCoordinator;
import com.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver;
import com.nereusstream.delay.submission.SubmissionCoordinator;
import com.nereusstream.delay.submission.SubmissionOutcomeProjectorRegistry;
import com.nereusstream.delay.transport.CommandTransportRegistry;
import com.nereusstream.delay.transport.LocalTransportOwnershipPermit;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
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
    private boolean closeCompleted;

    private DefaultDelayClient(
            final Builder builder, final DelaySemanticCore semanticCore, final SubmissionCoordinator submissions) {
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
    public PreparedCommand prepareSchedule(final CanonicalScheduleIntent intent, final long retryUntilEpochMs) {
        ensureOpen();
        try {
            return CommandCodec.decodeManagedFrame(
                    prepareScheduleSubmission(intent, retryUntilEpochMs, SubmissionMode.MANAGED)
                            .managedFrame());
        } catch (SemanticPreparationException failure) {
            throw new PreparationFailure(failure.error(), failure);
        } catch (RuntimeException failure) {
            throw PreparationFailure.of(StableCode.INVALID_PREPARED_COMMAND, failure);
        }
    }

    @Override
    public PreparedCommand prepareLargeSchedule(
            final CanonicalScheduleIntent intentWithoutPayload,
            final long expectedPayloadLength,
            final byte[] payloadSha256,
            final long reservationTtlMs,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile,
            final long retryUntilEpochMs) {
        ensureOpen();
        final PreparedCommand command = semanticCore.prepareLargeSchedule(
                tenant,
                defaultRoute,
                new LargeSchedulePreparation(
                        intentWithoutPayload,
                        expectedPayloadLength,
                        payloadSha256,
                        reservationTtlMs,
                        trustSet,
                        objectStoreProfile),
                retryUntilEpochMs);
        return command;
    }

    @Override
    public PreparedCommand prepareLargeSchedule(final LargeScheduleIntent intent, final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedCommand prepareLargePayloadCommit(
            final PayloadReservationReceipt reservation,
            final CanonicalPayloadCommitProof proof,
            final long retryUntilEpochMs) {
        ensureOpen();
        return semanticCore.preparePayloadCommit(tenant, reservation, proof, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareCancel(
            final DelayMessageId messageId, final MessagePrecondition precondition, final long retryUntilEpochMs) {
        ensureOpen();
        return semanticCore.prepareCancel(tenant, messageId, precondition, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareCancel(
            final DelayMessageId messageId, final int expectedGeneration, final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedCommand prepareReschedule(
            final DelayMessageId messageId,
            final MessagePrecondition precondition,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long retryUntilEpochMs) {
        ensureOpen();
        return semanticCore.prepareReschedule(
                tenant, messageId, precondition, deliverAtEpochMs, expireAtEpochMs, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareReschedule(
            final DelayMessageId messageId,
            final int expectedGeneration,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long retryUntilEpochMs) {
        throw PreparationFailure.of(StableCode.INVALID_COMMAND);
    }

    @Override
    public PreparedSubmission prepareManagedSubmission(final PreparedCommand command) {
        ensureOpen();
        return semanticCore.prepareManaged(tenant, command);
    }

    @Override
    public PreparedSubmission prepareScheduleSubmission(
            final CanonicalScheduleIntent intent, final long retryUntilEpochMs, final SubmissionMode submissionMode) {
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
    public PreparedSubmission prepareAutoFast(final AutoFastSchedule request) {
        ensureOpen();
        Objects.requireNonNull(request, "request");
        if (request.nativeCandidate() != null) {
            throw PreparationFailure.of(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE);
        }
        return semanticCore.prepareManaged(tenant, request.managedCommand());
    }

    @Override
    public List<PreparedSubmission> prepareAutoFastBatch(final List<AutoFastSchedule> requests) {
        ensureOpen();
        Objects.requireNonNull(requests, "requests");
        final List<PreparedSubmission> result = new ArrayList<>(requests.size());
        for (AutoFastSchedule request : requests) {
            result.add(prepareAutoFast(request));
        }
        return List.copyOf(result);
    }

    @Override
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final PreparedSubmission submission,
            final long receiptQueryUntilEpochMs,
            final byte[] physicalEnqueueAttemptId) {
        return submitInternal(submission, PhysicalEnqueueAttemptId.require(physicalEnqueueAttemptId));
    }

    @Override
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final PreparedSubmission submission,
            final QueuedReceiptQueryPolicy routePolicy,
            final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(routePolicy, "routePolicy");
        return submitInternal(submission, PhysicalEnqueueAttemptId.require(physicalEnqueueAttemptId));
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        ensureOpen();
        final PreparedSubmission submission = prepareManagedSubmission(command);
        return submitInternal(submission, PhysicalEnqueueAttemptId.random())
                .thenApply(outcome -> toLegacy(command, outcome));
    }

    @Override
    public CompletionStage<List<EnqueueOutcome>> enqueueBatch(final List<PreparedCommand> commands) {
        ensureOpen();
        Objects.requireNonNull(commands, "commands");
        final List<CompletionStage<EnqueueOutcome>> stages = new ArrayList<>(commands.size());
        for (PreparedCommand command : commands) {
            stages.add(enqueue(command));
        }
        final CompletableFuture<?>[] futures =
                stages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures).thenApply(ignored -> stages.stream()
                .map(stage -> stage.toCompletableFuture().join())
                .toList());
    }

    @Override
    public CompletionStage<CommandQueryResponse> getCommandResult(
            final CanonicalCommandQueuedReceipt receipt,
            final long nowEpochMs,
            final long fullResultRetainUntilEpochMs,
            final PublicDestinationBindingView binding) {
        ensureOpen();
        return queryClient.getCommandResult(receipt, nowEpochMs, fullResultRetainUntilEpochMs, binding);
    }

    @Override
    public CompletionStage<CommandQueryResponse> getCommandResult(
            final CanonicalCommandQueuedReceipt receipt,
            final long nowEpochMs,
            final CommandResultRetentionPolicy policy,
            final PublicDestinationBindingView binding) {
        ensureOpen();
        return queryClient.getCommandResult(receipt, nowEpochMs, policy, binding);
    }

    @Override
    public CompletionStage<CommandQueryResponse> awaitApplied(
            final CanonicalCommandQueuedReceipt receipt,
            final long nowEpochMs,
            final long fullResultRetainUntilEpochMs,
            final PublicDestinationBindingView binding) {
        ensureOpen();
        return queryClient.awaitApplied(receipt, nowEpochMs, fullResultRetainUntilEpochMs, binding);
    }

    @Override
    public CompletionStage<CommandQueryResponse> awaitApplied(
            final CanonicalCommandQueuedReceipt receipt,
            final long nowEpochMs,
            final CommandResultRetentionPolicy policy,
            final PublicDestinationBindingView binding) {
        ensureOpen();
        return queryClient.awaitApplied(receipt, nowEpochMs, policy, binding);
    }

    @Override
    public CompletionStage<MessageQueryResponse> getMessage(
            final DelayMessageId messageId,
            final PublicDestinationBindingView binding,
            final DlqExportState dlqExportState,
            final PublicEvidenceRef evidence,
            final FirstScheduleEligibility unknownEligibility) {
        ensureOpen();
        return queryClient.getMessage(messageId, binding, dlqExportState, evidence, unknownEligibility);
    }

    @Override
    public CompletionStage<PayloadUploadHandleResponse> issuePayloadUploadHandle(
            final PayloadReservationReceipt reservation, final UploadHandleKind kind, final long nowEpochMs) {
        ensureOpen();
        return queryClient.issuePayloadUploadHandle(reservation, kind, nowEpochMs);
    }

    @Override
    public CompletionStage<PayloadAttestationResponse> attestPayloadUpload(
            final PayloadReservationReceipt reservation,
            final OpaquePayloadUploadHandle handle,
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
    public synchronized void close() {
        if (closeCompleted) {
            return;
        }
        closed.set(true);
        Throwable first = null;
        try {
            outbox.close();
        } catch (RuntimeException | Error failure) {
            first = appendCloseFailure(first, failure);
        }
        try {
            queryClient.close();
        } catch (RuntimeException | Error failure) {
            first = appendCloseFailure(first, failure);
        }
        if (transportRegistry != null) {
            try {
                transportRegistry.close();
            } catch (RuntimeException | Error failure) {
                first = appendCloseFailure(first, failure);
            }
        }
        if (first != null) {
            throwUnchecked(first);
        }
        closeCompleted = true;
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    private CompletionStage<SubmissionOutcomeMessage> submitInternal(
            final PreparedSubmission submission, final PhysicalEnqueueAttemptId attempt) {
        ensureOpen();
        Objects.requireNonNull(submission, "submission");
        try {
            admission.admit(submission);
            outbox.start(submission, attempt);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return submissions
                .submit(tenant, submission, new LocalTransportOwnershipPermit(attempt))
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

    private static EnqueueOutcome toLegacy(final PreparedCommand command, final SubmissionOutcomeMessage outcome) {
        if (outcome.kind() == com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED) {
            final var managed = outcome.managed();
            return switch (managed.kind()) {
                case QUEUED -> {
                    final CanonicalCommandQueuedReceipt receipt = managed.queued();
                    yield EnqueueOutcome.queued(
                            command,
                            new CommandQueuedReceipt(
                                    command.commandId(),
                                    command.delayMessageId(),
                                    command.shardId(),
                                    receipt.sourcePosition()));
                }
                case DEFINITELY_NOT_QUEUED ->
                    EnqueueOutcome.definitelyNotQueued(
                            command,
                            managed.definitelyNotQueued().error().code().wireValue());
                case ENQUEUE_UNCERTAIN ->
                    EnqueueOutcome.uncertain(
                            command, managed.uncertain().error().code().wireValue());
            };
        }
        return switch (outcome.kind()) {
            case NATIVE_RECEIPT -> {
                final var ack = outcome.nativeReceipt().brokerAck();
                final var entryKind = ack.batchSize() == 1
                        ? com.nereusstream.delay.protocol.PulsarSourcePosition.EntryKind.NON_BATCH
                        : com.nereusstream.delay.protocol.PulsarSourcePosition.EntryKind.BATCH;
                final var source = new com.nereusstream.delay.protocol.PulsarSourcePosition(
                        command.shardId(),
                        ack.brokerResourceIncarnation(),
                        ack.physicalTopic(),
                        ack.ledgerId(),
                        ack.entryId(),
                        ack.normalizedBatchIndex(),
                        ack.batchSize(),
                        entryKind,
                        ack.brokerEntryTimestampEpochMs());
                yield EnqueueOutcome.queued(
                        command,
                        new CommandQueuedReceipt(
                                command.commandId(), command.delayMessageId(), command.shardId(), source));
            }
            case NATIVE_DEFINITELY_NOT_QUEUED ->
                EnqueueOutcome.definitelyNotQueued(
                        command,
                        outcome.nativeDefinitelyNotQueued().error().code().wireValue());
            case NATIVE_ENQUEUE_UNCERTAIN ->
                EnqueueOutcome.uncertain(
                        command, outcome.nativeUncertain().error().code().wireValue());
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
        private ClientAdmission admission = submission -> {};
        private ClientOutbox outbox = new ClientOutbox() {};

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
                resolvedCore = new DefaultDelaySemanticCore(
                        routeSnapshotProvider,
                        uuidGenerator == null ? new SecureLogicalUuidV7Generator() : uuidGenerator,
                        clock,
                        nativePreparationSnapshotProvider,
                        com.nereusstream.delay.semantic.NativeDeliveryIdGenerator.random());
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
                        transportRegistry,
                        projectorRegistry);
            }
            return new DefaultDelayClient(this, resolvedCore, resolvedCoordinator);
        }
    }
}
