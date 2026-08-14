package io.nereusstream.delay.gateway;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.nereusstream.delay.gateway.v1.DelayGatewayV1Grpc;
import io.nereusstream.delay.gateway.v1.GatewayRetryUncertainRequestV1;
import io.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * Generated gRPC adapter for the currently implemented Schedule and
 * RetryUncertain domain paths. Every other RPC remains the generated
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
        invoke(domainRequest -> ingress.schedule(peerContextProvider.current(), domainRequest), domain,
                responseObserver);
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

    private void invoke(final ScheduleCall call,
                        final io.nereusstream.delay.gateway.GatewayScheduleRequestV1 request,
                        final StreamObserver<io.nereusstream.delay.gateway.v1.GatewaySubmissionOutcomeV1>
                                responseObserver) {
        try {
            call.invoke(request).whenComplete((outcome, failure) -> complete(responseObserver, outcome, failure));
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

    @FunctionalInterface
    private interface ScheduleCall {
        java.util.concurrent.CompletionStage<GatewaySubmissionOutcomeV1> invoke(
                io.nereusstream.delay.gateway.GatewayScheduleRequestV1 request);
    }
}
