package com.nereusstream.delay.gateway;

import com.google.protobuf.ByteString;
import com.nereusstream.delay.gateway.wire.DelayGatewayGrpc;
import com.nereusstream.delay.gateway.wire.GatewayAttestPayloadUploadRequest;
import com.nereusstream.delay.gateway.wire.GatewayAwaitAppliedRequest;
import com.nereusstream.delay.gateway.wire.GatewayCancelRequest;
import com.nereusstream.delay.gateway.wire.GatewayCommitLargeScheduleRequest;
import com.nereusstream.delay.gateway.wire.GatewayGetCommandResultRequest;
import com.nereusstream.delay.gateway.wire.GatewayGetMessageRequest;
import com.nereusstream.delay.gateway.wire.GatewayIssuePayloadUploadHandleRequest;
import com.nereusstream.delay.gateway.wire.GatewayPrepareLargeScheduleRequest;
import com.nereusstream.delay.gateway.wire.GatewayRescheduleRequest;
import com.nereusstream.delay.gateway.wire.GatewayRetryUncertainRequest;
import com.nereusstream.delay.gateway.wire.GatewayScheduleRequest;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * Generated gRPC adapter for the locally implemented submission paths.
 * Payload and query RPCs require their explicit authority compositions.
 */
public final class GatewayGrpcService extends DelayGatewayGrpc.DelayGatewayImplBase {
    private final GatewayIngressService ingress;
    private final GatewayPeerContextProvider peerContextProvider;
    private final GatewayPayloadIngressService payloadIngress;
    private final GatewayQueryIngressService queryIngress;

    public GatewayGrpcService(
            final GatewayIngressService ingress, final GatewayPeerContextProvider peerContextProvider) {
        this(ingress, peerContextProvider, null, null);
    }

    public GatewayGrpcService(
            final GatewayIngressService ingress,
            final GatewayPeerContextProvider peerContextProvider,
            final GatewayPayloadIngressService payloadIngress) {
        this(ingress, peerContextProvider, payloadIngress, null);
    }

    public GatewayGrpcService(
            final GatewayIngressService ingress,
            final GatewayPeerContextProvider peerContextProvider,
            final GatewayPayloadIngressService payloadIngress,
            final GatewayQueryIngressService queryIngress) {
        this.ingress = Objects.requireNonNull(ingress, "ingress");
        this.peerContextProvider = Objects.requireNonNull(peerContextProvider, "peerContextProvider");
        this.payloadIngress = payloadIngress;
        this.queryIngress = queryIngress;
    }

    @Override
    public void issuePayloadUploadHandle(
            final GatewayIssuePayloadUploadHandleRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse>
                    responseObserver) {
        if (payloadIngress == null) {
            fail(responseObserver, Status.Code.UNIMPLEMENTED);
            return;
        }
        final com.nereusstream.delay.gateway.GatewayIssuePayloadUploadHandleRequest domain;
        try {
            domain = decodeIssuePayloadUploadHandle(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        try {
            payloadIngress
                    .issueUploadHandle(peerContextProvider.current(), domain)
                    .whenComplete((response, failure) -> complete(responseObserver, response, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    @Override
    public void attestPayloadUpload(
            final GatewayAttestPayloadUploadRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse>
                    responseObserver) {
        if (payloadIngress == null) {
            fail(responseObserver, Status.Code.UNIMPLEMENTED);
            return;
        }
        final com.nereusstream.delay.gateway.GatewayAttestPayloadUploadRequest domain;
        try {
            domain = decodeAttestPayloadUpload(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        try {
            payloadIngress
                    .attestUpload(peerContextProvider.current(), domain)
                    .whenComplete((response, failure) -> complete(responseObserver, response, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    @Override
    public void getCommandResult(
            final GatewayGetCommandResultRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse> responseObserver) {
        if (queryIngress == null) {
            fail(responseObserver, Status.Code.UNIMPLEMENTED);
            return;
        }
        final com.nereusstream.delay.gateway.GatewayGetCommandResultRequest domain;
        try {
            domain = decodeGetCommandResult(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        try {
            queryIngress
                    .getCommandResult(peerContextProvider.current(), domain)
                    .whenComplete((response, failure) -> complete(responseObserver, response, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    @Override
    public void awaitApplied(
            final GatewayAwaitAppliedRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse> responseObserver) {
        if (queryIngress == null) {
            fail(responseObserver, Status.Code.UNIMPLEMENTED);
            return;
        }
        final com.nereusstream.delay.gateway.GatewayAwaitAppliedRequest domain;
        try {
            domain = decodeAwaitApplied(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        try {
            queryIngress
                    .awaitApplied(peerContextProvider.current(), domain)
                    .whenComplete((responses, failure) -> completeStream(responseObserver, responses, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    @Override
    public void getMessage(
            final GatewayGetMessageRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayMessageQueryResponse> responseObserver) {
        if (queryIngress == null) {
            fail(responseObserver, Status.Code.UNIMPLEMENTED);
            return;
        }
        final com.nereusstream.delay.gateway.GatewayGetMessageRequest domain;
        try {
            domain = decodeGetMessage(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        try {
            queryIngress
                    .getMessage(peerContextProvider.current(), domain)
                    .whenComplete((response, failure) -> complete(responseObserver, response, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    @Override
    public void schedule(
            final GatewayScheduleRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        final com.nereusstream.delay.gateway.GatewayScheduleRequest domain;
        try {
            domain = decodeSchedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.schedule(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void retryUncertain(
            final GatewayRetryUncertainRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        final com.nereusstream.delay.gateway.GatewayRetryUncertainRequest domain;
        try {
            domain = new com.nereusstream.delay.gateway.GatewayRetryUncertainRequest(
                    request.getOriginalIdempotencyKey().toByteArray(),
                    PhysicalEnqueueAttemptId.require(
                            request.getExpectedPriorPhysicalAttemptId().toByteArray()),
                    PhysicalEnqueueAttemptId.require(request.getRetryRequestId().toByteArray()));
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        try {
            ingress.retryUncertain(peerContextProvider.current(), domain)
                    .whenComplete((outcome, failure) -> complete(responseObserver, outcome, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    @Override
    public void prepareLargeSchedule(
            final GatewayPrepareLargeScheduleRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        final com.nereusstream.delay.gateway.GatewayPrepareLargeScheduleRequest domain;
        try {
            domain = decodePrepareLargeSchedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.prepareLargeSchedule(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void commitLargeSchedule(
            final GatewayCommitLargeScheduleRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        final com.nereusstream.delay.gateway.GatewayCommitLargeScheduleRequest domain;
        try {
            domain = decodeCommitLargeSchedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.commitLargeSchedule(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void cancel(
            final GatewayCancelRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        final com.nereusstream.delay.gateway.GatewayCancelRequest domain;
        try {
            domain = decodeCancel(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.cancel(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void reschedule(
            final GatewayRescheduleRequest request,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        final com.nereusstream.delay.gateway.GatewayRescheduleRequest domain;
        try {
            domain = decodeReschedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.reschedule(peerContextProvider.current(), domain), responseObserver);
    }

    private void invoke(
            final java.util.function.Supplier<
                            java.util.concurrent.CompletionStage<
                                    com.nereusstream.delay.gateway.GatewaySubmissionOutcome>>
                    call,
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver) {
        try {
            call.get().whenComplete((outcome, failure) -> complete(responseObserver, outcome, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    private static com.nereusstream.delay.gateway.GatewayScheduleRequest decodeSchedule(
            final GatewayScheduleRequest request) {
        if (!request.hasRoute() || request.getCanonicalScheduleIntent().isEmpty()) {
            throw new IllegalArgumentException("Gateway Schedule request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayScheduleRequest(
                request.getIdempotencyKey().toByteArray(),
                new RouteSelectionHint(
                        AdapterKind.fromWire(request.getRoute().getIngressAdapterKind()),
                        request.getRoute().getRouteAliasUtf8Nfc().toByteArray()),
                CanonicalScheduleIntent.decode(
                        request.getCanonicalScheduleIntent().toByteArray()),
                request.getRetryUntilEpochMs(),
                SubmissionMode.fromWire(request.getSubmissionMode()));
    }

    private static com.nereusstream.delay.gateway.GatewayCancelRequest decodeCancel(
            final GatewayCancelRequest request) {
        if (request.getDelayMessageId().isEmpty()) {
            throw new IllegalArgumentException("Gateway Cancel request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayCancelRequest(
                request.getIdempotencyKey().toByteArray(),
                new DelayMessageId(request.getDelayMessageId().toByteArray()),
                MessagePrecondition.decode(request.getMessagePrecondition().toByteArray()),
                request.getRetryUntilEpochMs());
    }

    private static com.nereusstream.delay.gateway.GatewayPrepareLargeScheduleRequest decodePrepareLargeSchedule(
            final GatewayPrepareLargeScheduleRequest request) {
        if (!request.hasRoute()
                || request.getCanonicalScheduleIntent().isEmpty()
                || request.getPayloadSha256().isEmpty()
                || request.getPayloadProofTrustSetRef().isEmpty()
                || request.getObjectStoreProfileRef().isEmpty()) {
            throw new IllegalArgumentException("Gateway PrepareLargeSchedule request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayPrepareLargeScheduleRequest(
                request.getIdempotencyKey().toByteArray(),
                new RouteSelectionHint(
                        AdapterKind.fromWire(request.getRoute().getIngressAdapterKind()),
                        request.getRoute().getRouteAliasUtf8Nfc().toByteArray()),
                CanonicalScheduleIntent.decode(
                        request.getCanonicalScheduleIntent().toByteArray()),
                request.getExpectedPayloadLength(),
                request.getPayloadSha256().toByteArray(),
                request.getReservationTtlMs(),
                PayloadProofTrustSetRef.decode(
                        request.getPayloadProofTrustSetRef().toByteArray()),
                ProfileRef.decode(request.getObjectStoreProfileRef().toByteArray()),
                request.getRetryUntilEpochMs());
    }

    private static com.nereusstream.delay.gateway.GatewayCommitLargeScheduleRequest decodeCommitLargeSchedule(
            final GatewayCommitLargeScheduleRequest request) {
        if (request.getPayloadReservationReceipt().isEmpty()
                || request.getCanonicalPayloadCommitProof().isEmpty()) {
            throw new IllegalArgumentException("Gateway CommitLargeSchedule request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayCommitLargeScheduleRequest(
                request.getIdempotencyKey().toByteArray(),
                PayloadReservationReceipt.decodePayload(
                        request.getPayloadReservationReceipt().toByteArray()),
                CanonicalPayloadCommitProof.decode(
                        request.getCanonicalPayloadCommitProof().toByteArray()),
                request.getRetryUntilEpochMs());
    }

    private static com.nereusstream.delay.gateway.GatewayRescheduleRequest decodeReschedule(
            final GatewayRescheduleRequest request) {
        if (request.getDelayMessageId().isEmpty()) {
            throw new IllegalArgumentException("Gateway Reschedule request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayRescheduleRequest(
                request.getIdempotencyKey().toByteArray(),
                new DelayMessageId(request.getDelayMessageId().toByteArray()),
                MessagePrecondition.decode(request.getMessagePrecondition().toByteArray()),
                request.getDeliverAtEpochMs(),
                request.getExpireAtEpochMs(),
                request.getRetryUntilEpochMs());
    }

    private static com.nereusstream.delay.gateway.GatewayIssuePayloadUploadHandleRequest decodeIssuePayloadUploadHandle(
            final GatewayIssuePayloadUploadHandleRequest request) {
        if (request.getPayloadReservationReceipt().isEmpty()) {
            throw new IllegalArgumentException("Gateway upload-handle request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayIssuePayloadUploadHandleRequest(
                PayloadReservationReceipt.decodePayload(
                        request.getPayloadReservationReceipt().toByteArray()),
                UploadHandleKind.fromWire(request.getUploadHandleKind()));
    }

    private static com.nereusstream.delay.gateway.GatewayAttestPayloadUploadRequest decodeAttestPayloadUpload(
            final GatewayAttestPayloadUploadRequest request) {
        if (request.getPayloadReservationReceipt().isEmpty()
                || request.getOpaquePayloadUploadHandle().isEmpty()) {
            throw new IllegalArgumentException("Gateway attestation request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayAttestPayloadUploadRequest(
                PayloadReservationReceipt.decodePayload(
                        request.getPayloadReservationReceipt().toByteArray()),
                OpaquePayloadUploadHandle.decode(
                        request.getOpaquePayloadUploadHandle().toByteArray()));
    }

    private static com.nereusstream.delay.gateway.GatewayGetCommandResultRequest decodeGetCommandResult(
            final GatewayGetCommandResultRequest request) {
        if (request.hasCanonicalCommandQueuedReceipt()) {
            return new com.nereusstream.delay.gateway.GatewayGetCommandResultRequest(
                    CanonicalCommandQueuedReceipt.decodePayload(
                            request.getCanonicalCommandQueuedReceipt().toByteArray()),
                    null);
        }
        if (request.hasCommandId()) {
            return new com.nereusstream.delay.gateway.GatewayGetCommandResultRequest(
                    null, new CommandId(request.getCommandId().toByteArray()));
        }
        throw new IllegalArgumentException("Gateway command query locator is missing");
    }

    private static com.nereusstream.delay.gateway.GatewayAwaitAppliedRequest decodeAwaitApplied(
            final GatewayAwaitAppliedRequest request) {
        if (request.getCanonicalCommandQueuedReceipt().isEmpty()) {
            throw new IllegalArgumentException("Gateway AwaitApplied request is incomplete");
        }
        return new com.nereusstream.delay.gateway.GatewayAwaitAppliedRequest(
                CanonicalCommandQueuedReceipt.decodePayload(
                        request.getCanonicalCommandQueuedReceipt().toByteArray()));
    }

    private static com.nereusstream.delay.gateway.GatewayGetMessageRequest decodeGetMessage(
            final GatewayGetMessageRequest request) {
        if (request.hasDelayMessageId()) {
            return new com.nereusstream.delay.gateway.GatewayGetMessageRequest(
                    new DelayMessageId(request.getDelayMessageId().toByteArray()), null);
        }
        if (request.hasCanonicalCommandQueuedReceipt()) {
            return new com.nereusstream.delay.gateway.GatewayGetMessageRequest(
                    null,
                    CanonicalCommandQueuedReceipt.decodePayload(
                            request.getCanonicalCommandQueuedReceipt().toByteArray()));
        }
        throw new IllegalArgumentException("Gateway message query locator is missing");
    }

    private static void complete(
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome> responseObserver,
            final GatewaySubmissionOutcome outcome,
            final Throwable failure) {
        if (failure != null || outcome == null) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        final com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome.Builder builder =
                com.nereusstream.delay.gateway.wire.GatewaySubmissionOutcome.newBuilder();
        if (outcome.hasSubmissionOutcome()) {
            builder.setSubmissionOutcomeNdr1(
                    ByteString.copyFrom(outcome.submissionOutcome().canonicalBytes()));
        } else {
            builder.setPreparationError(
                    ByteString.copyFrom(outcome.preparationError().canonicalBytes()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private static void complete(
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse>
                    responseObserver,
            final PayloadUploadHandleResponse response,
            final Throwable failure) {
        if (failure != null || response == null) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        responseObserver.onNext(com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse.newBuilder()
                .setPayloadUploadHandleResponse(ByteString.copyFrom(response.canonicalBytes()))
                .build());
        responseObserver.onCompleted();
    }

    private static void complete(
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse>
                    responseObserver,
            final PayloadAttestationResponse response,
            final Throwable failure) {
        if (failure != null || response == null) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        responseObserver.onNext(com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse.newBuilder()
                .setPayloadAttestationResponse(ByteString.copyFrom(response.canonicalBytes()))
                .build());
        responseObserver.onCompleted();
    }

    private static void complete(
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse> responseObserver,
            final CommandQueryResponse response,
            final Throwable failure) {
        if (failure != null || response == null) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        responseObserver.onNext(com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse.newBuilder()
                .setCommandQueryResponse(ByteString.copyFrom(response.canonicalBytes()))
                .build());
        responseObserver.onCompleted();
    }

    private static void completeStream(
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse> responseObserver,
            final List<CommandQueryResponse> responses,
            final Throwable failure) {
        if (failure != null || responses == null || responses.isEmpty()) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        for (CommandQueryResponse response : responses) {
            if (response == null) {
                fail(responseObserver, Status.Code.INTERNAL);
                return;
            }
            responseObserver.onNext(com.nereusstream.delay.gateway.wire.GatewayCommandQueryResponse.newBuilder()
                    .setCommandQueryResponse(ByteString.copyFrom(response.canonicalBytes()))
                    .build());
        }
        responseObserver.onCompleted();
    }

    private static void complete(
            final StreamObserver<com.nereusstream.delay.gateway.wire.GatewayMessageQueryResponse> responseObserver,
            final MessageQueryResponse response,
            final Throwable failure) {
        if (failure != null || response == null) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        responseObserver.onNext(com.nereusstream.delay.gateway.wire.GatewayMessageQueryResponse.newBuilder()
                .setMessageQueryResponse(ByteString.copyFrom(response.canonicalBytes()))
                .build());
        responseObserver.onCompleted();
    }

    private static Status.Code statusFor(final Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof OxiaGatewaySessionUnavailableException) {
            return Status.Code.UNAVAILABLE;
        }
        if (current instanceof GatewayIngressException ingressFailure) {
            return switch (ingressFailure.kind()) {
                case AUTHENTICATION -> Status.Code.UNAUTHENTICATED;
                case UNAVAILABLE -> Status.Code.UNAVAILABLE;
                case INTERNAL -> Status.Code.INTERNAL;
            };
        }
        return Status.Code.INTERNAL;
    }

    private static void fail(final StreamObserver<?> responseObserver, final Status.Code code) {
        responseObserver.onError(Status.fromCode(code).asRuntimeException());
    }
}
