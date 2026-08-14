package io.nereusstream.delay.gateway;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.nereusstream.delay.gateway.v1.DelayGatewayV1Grpc;
import io.nereusstream.delay.gateway.v1.GatewayCancelRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayCommitLargeScheduleRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayPrepareLargeScheduleRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayRescheduleRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayRetryUncertainRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * Generated gRPC adapter for the currently implemented submission paths.
 * Upload, query and await RPCs remain the generated
 * UNIMPLEMENTED boundary until its domain authority exists.
 */
public final class GatewayGrpcService extends DelayGatewayV1Grpc.DelayGatewayV1ImplBase {
    private final GatewayIngressService ingress;
    private final GatewayPeerContextProvider peerContextProvider;

    public GatewayGrpcService(final GatewayIngressService ingress,
                              final GatewayPeerContextProvider peerContextProvider) {
        this.ingress = Objects.requireNonNull(ingress, "ingress");
        this.peerContextProvider = Objects.requireNonNull(peerContextProvider, "peerContextProvider");
    }

    @Override
    public void schedule(final GatewayScheduleRequestV1 request,
                         final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                 responseObserver) {
        final io.nereusstream.delay.gateway.GatewayScheduleRequestV1 domain;
        try {
            domain = decodeSchedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.schedule(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void retryUncertain(final GatewayRetryUncertainRequestV1 request,
                               final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                       responseObserver) {
        final io.nereusstream.delay.gateway.GatewayRetryUncertainRequestV1 domain;
        try {
            domain = new io.nereusstream.delay.gateway.GatewayRetryUncertainRequestV1(
                    request.getOriginalIdempotencyKey().toByteArray(),
                    PhysicalEnqueueAttemptId.require(request.getExpectedPriorPhysicalAttemptId().toByteArray()),
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
    public void prepareLargeSchedule(final GatewayPrepareLargeScheduleRequestV1 request,
                                      final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                              responseObserver) {
        final io.nereusstream.delay.gateway.GatewayPrepareLargeScheduleRequestV1 domain;
        try {
            domain = decodePrepareLargeSchedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.prepareLargeSchedule(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void commitLargeSchedule(final GatewayCommitLargeScheduleRequestV1 request,
                                     final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                             responseObserver) {
        final io.nereusstream.delay.gateway.GatewayCommitLargeScheduleRequestV1 domain;
        try {
            domain = decodeCommitLargeSchedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.commitLargeSchedule(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void cancel(final GatewayCancelRequestV1 request,
                        final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                responseObserver) {
        final io.nereusstream.delay.gateway.GatewayCancelRequestV1 domain;
        try {
            domain = decodeCancel(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.cancel(peerContextProvider.current(), domain), responseObserver);
    }

    @Override
    public void reschedule(final GatewayRescheduleRequestV1 request,
                            final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                    responseObserver) {
        final io.nereusstream.delay.gateway.GatewayRescheduleRequestV1 domain;
        try {
            domain = decodeReschedule(request);
        } catch (RuntimeException invalidRequest) {
            fail(responseObserver, Status.Code.INVALID_ARGUMENT);
            return;
        }
        invoke(() -> ingress.reschedule(peerContextProvider.current(), domain), responseObserver);
    }

    private void invoke(final java.util.function.Supplier<java.util.concurrent.CompletionStage<
            io.nereusstream.delay.gateway.GatewaySubmissionOutcomeV1>> call,
                        final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                responseObserver) {
        try {
            call.get().whenComplete((outcome, failure) -> complete(responseObserver, outcome, failure));
        } catch (RuntimeException failure) {
            fail(responseObserver, statusFor(failure));
        }
    }

    private static io.nereusstream.delay.gateway.GatewayScheduleRequestV1 decodeSchedule(
            final GatewayScheduleRequestV1 request) {
        if (!request.hasRoute() || request.getScheduleIntentV1().isEmpty()) {
            throw new IllegalArgumentException("Gateway Schedule request is incomplete");
        }
        return new io.nereusstream.delay.gateway.GatewayScheduleRequestV1(
                request.getIdempotencyKey().toByteArray(),
                new RouteSelectionHint(
                        AdapterKindV1.fromWire(request.getRoute().getIngressAdapterKind()),
                        request.getRoute().getRouteAliasUtf8Nfc().toByteArray()),
                ScheduleIntentV1.decode(request.getScheduleIntentV1().toByteArray()),
                request.getRetryUntilEpochMs(),
                SubmissionModeV1.fromWire(request.getSubmissionModeV1()));
    }

    private static io.nereusstream.delay.gateway.GatewayCancelRequestV1 decodeCancel(
            final GatewayCancelRequestV1 request) {
        if (request.getDelayMessageId().isEmpty()) {
            throw new IllegalArgumentException("Gateway Cancel request is incomplete");
        }
        return new io.nereusstream.delay.gateway.GatewayCancelRequestV1(
                request.getIdempotencyKey().toByteArray(),
                new DelayMessageId(request.getDelayMessageId().toByteArray()),
                MessagePreconditionV1.decode(request.getMessagePreconditionV1().toByteArray()),
                request.getRetryUntilEpochMs());
    }

    private static io.nereusstream.delay.gateway.GatewayPrepareLargeScheduleRequestV1 decodePrepareLargeSchedule(
            final GatewayPrepareLargeScheduleRequestV1 request) {
        if (!request.hasRoute() || request.getScheduleIntentV1().isEmpty()
                || request.getPayloadSha256().isEmpty() || request.getPayloadProofTrustSetRefV1().isEmpty()
                || request.getObjectStoreProfileRefV1().isEmpty()) {
            throw new IllegalArgumentException("Gateway PrepareLargeSchedule request is incomplete");
        }
        return new io.nereusstream.delay.gateway.GatewayPrepareLargeScheduleRequestV1(
                request.getIdempotencyKey().toByteArray(),
                new RouteSelectionHint(
                        AdapterKindV1.fromWire(request.getRoute().getIngressAdapterKind()),
                        request.getRoute().getRouteAliasUtf8Nfc().toByteArray()),
                ScheduleIntentV1.decode(request.getScheduleIntentV1().toByteArray()),
                request.getExpectedPayloadLength(), request.getPayloadSha256().toByteArray(),
                request.getReservationTtlMs(),
                PayloadProofTrustSetRefV1.decode(request.getPayloadProofTrustSetRefV1().toByteArray()),
                ProfileRefV1.decode(request.getObjectStoreProfileRefV1().toByteArray()),
                request.getRetryUntilEpochMs());
    }

    private static io.nereusstream.delay.gateway.GatewayCommitLargeScheduleRequestV1 decodeCommitLargeSchedule(
            final GatewayCommitLargeScheduleRequestV1 request) {
        if (request.getPayloadReservationReceiptV1().isEmpty()
                || request.getPayloadCommitProofV1().isEmpty()) {
            throw new IllegalArgumentException("Gateway CommitLargeSchedule request is incomplete");
        }
        return new io.nereusstream.delay.gateway.GatewayCommitLargeScheduleRequestV1(
                request.getIdempotencyKey().toByteArray(),
                PayloadReservationReceiptV1.decodePayload(
                        request.getPayloadReservationReceiptV1().toByteArray()),
                PayloadCommitProofV1.decode(request.getPayloadCommitProofV1().toByteArray()),
                request.getRetryUntilEpochMs());
    }

    private static io.nereusstream.delay.gateway.GatewayRescheduleRequestV1 decodeReschedule(
            final GatewayRescheduleRequestV1 request) {
        if (request.getDelayMessageId().isEmpty()) {
            throw new IllegalArgumentException("Gateway Reschedule request is incomplete");
        }
        return new io.nereusstream.delay.gateway.GatewayRescheduleRequestV1(
                request.getIdempotencyKey().toByteArray(),
                new DelayMessageId(request.getDelayMessageId().toByteArray()),
                MessagePreconditionV1.decode(request.getMessagePreconditionV1().toByteArray()),
                request.getDeliverAtEpochMs(), request.getExpireAtEpochMs(), request.getRetryUntilEpochMs());
    }

    private static void complete(
            final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1> responseObserver,
            final GatewaySubmissionOutcomeV1 outcome, final Throwable failure) {
        if (failure != null || outcome == null) {
            fail(responseObserver, statusFor(failure));
            return;
        }
        final io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1.Builder builder =
                io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1.newBuilder();
        if (outcome.hasSubmissionOutcome()) {
            builder.setSubmissionOutcomeNdr1(ByteString.copyFrom(outcome.submissionOutcome().canonicalBytes()));
        } else {
            builder.setPreparationErrorV1(ByteString.copyFrom(outcome.preparationError().canonicalBytes()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private static Status.Code statusFor(final Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
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

    private static void fail(
            final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1> responseObserver,
            final Status.Code code) {
        responseObserver.onError(Status.fromCode(code).asRuntimeException());
    }

}
