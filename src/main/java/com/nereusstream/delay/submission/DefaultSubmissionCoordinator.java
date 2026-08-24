package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.FailureStageV1;
import com.nereusstream.delay.protocol.NativeDefinitelyNotQueuedV1;
import com.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import com.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import com.nereusstream.delay.protocol.NonPersistenceProofV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableErrorV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.transport.CommandTransport;
import com.nereusstream.delay.transport.CommandTransportRegistry;
import com.nereusstream.delay.transport.LocalTransportOwnershipPermit;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import com.nereusstream.delay.transport.TransportOwnershipState;
import com.nereusstream.delay.transport.TransportResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Shared Direct SDK/Gateway state machine after exact semantic preparation. */
public final class DefaultSubmissionCoordinator implements SubmissionCoordinator {
    private final SubmissionTransportPlanResolver resolver;
    private final CommandTransportRegistry transports;
    private final SubmissionOutcomeProjectorRegistry projectors;

    public DefaultSubmissionCoordinator(
            final SubmissionTransportPlanResolver resolver,
            final CommandTransportRegistry transports,
            final SubmissionOutcomeProjectorRegistry projectors) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.transports = Objects.requireNonNull(transports, "transports");
        this.projectors = Objects.requireNonNull(projectors, "projectors");
    }

    /** Direct SDK convenience path allocating one local attempt capability. */
    public CompletionStage<SubmissionOutcomeMessageV1> submit(
            final AuthenticatedTenantContext tenant, final PreparedSubmissionV1 submission) {
        return submit(tenant, submission, new LocalTransportOwnershipPermit(PhysicalEnqueueAttemptId.random()));
    }

    @Override
    public CompletionStage<SubmissionOutcomeMessageV1> submit(
            final AuthenticatedTenantContext tenant,
            final PreparedSubmissionV1 submission,
            final TransportOwnershipPermit permit) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(submission, "submission");
        Objects.requireNonNull(permit, "permit");
        final PhysicalEnqueueAttemptId attempt = permit.physicalAttemptId();
        final SubmissionTransportPlan plan;
        try {
            plan = resolver.resolve(tenant, submission);
        } catch (SubmissionPlanException failure) {
            permit.close();
            return completed(localFailure(submission, attempt, failure.code()));
        } catch (RuntimeException failure) {
            permit.close();
            return completed(localFailure(submission, attempt, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE));
        }
        final SubmissionOutcomeProjector projector = projectors.exact(plan.projectionKey());
        if (projector == null) {
            permit.close();
            return completed(localFailure(plan, attempt, StableCode.CAPABILITY_UNAVAILABLE));
        }
        final CommandTransport transport = transports.exact(plan.transportKey());
        if (transport == null) {
            permit.close();
            return completed(projector.localFailure(plan, attempt, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE));
        }
        final CompletionStage<? extends TransportResult> stage;
        try {
            stage = transport.send(plan.request(), permit);
            permit.close();
        } catch (RuntimeException failure) {
            permit.close();
            return completed(
                    permit.state() == TransportOwnershipState.LIBRARY_OWNED
                            ? projector.uncertain(plan, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN)
                            : projector.localFailure(plan, attempt, StableCode.BROKER_RESOURCE_UNCERTIFIED));
        }
        if (stage == null) {
            return completed(
                    permit.state() == TransportOwnershipState.LIBRARY_OWNED
                            ? projector.uncertain(plan, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN)
                            : projector.localFailure(plan, attempt, StableCode.BROKER_RESOURCE_UNCERTIFIED));
        }
        if (permit.state() != TransportOwnershipState.LIBRARY_OWNED) {
            return completed(projector.localFailure(plan, attempt, StableCode.BROKER_RESOURCE_UNCERTIFIED));
        }
        try {
            final CompletionStage<SubmissionOutcomeMessageV1> mapped = stage.handle((result, error) -> {
                if (error != null || result == null) {
                    return projector.uncertain(plan, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN);
                }
                try {
                    if (!attempt.equals(result.physicalAttemptId())) {
                        return projector.uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
                    }
                    return projector.project(plan, attempt, result);
                } catch (RuntimeException malformed) {
                    return projector.uncertain(plan, attempt, StableCode.INTEGRITY_ERROR);
                }
            });
            return mapped == null
                    ? completed(projector.uncertain(plan, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN))
                    : mapped;
        } catch (RuntimeException registrationFailure) {
            return completed(projector.uncertain(plan, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN));
        }
    }

    private static CompletionStage<SubmissionOutcomeMessageV1> completed(final SubmissionOutcomeMessageV1 outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    private static SubmissionOutcomeMessageV1 localFailure(
            final SubmissionTransportPlan plan, final PhysicalEnqueueAttemptId attempt, final StableCode code) {
        if (plan.submission().isManaged()) {
            final PreparedCommand command =
                    CommandCodec.decodeFrameV1(plan.submission().managedFrame());
            return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.localDefinite(command, code));
        }
        final NativePreparedDeliveryV1 prepared = plan.submission().nativePrepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                null,
                ref.submissionHash(),
                null,
                null,
                null);
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueuedV1(ref, proof, error));
    }

    private static SubmissionOutcomeMessageV1 localFailure(
            final PreparedSubmissionV1 submission, final PhysicalEnqueueAttemptId attempt, final StableCode code) {
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeFrameV1(submission.managedFrame());
            return SubmissionOutcomeMessageV1.managed(WireIngressOutcomeSupport.localDefinite(command, code));
        }
        final NativePreparedDeliveryV1 prepared = submission.nativePrepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                null,
                ref.submissionHash(),
                null,
                null,
                null);
        final var error = StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessageV1.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueuedV1(ref, proof, error));
    }
}
