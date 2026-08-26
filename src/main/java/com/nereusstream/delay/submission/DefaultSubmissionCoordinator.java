package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.NativeDefinitelyNotQueued;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NonPersistenceProof;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
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
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final AuthenticatedTenantContext tenant, final PreparedSubmission submission) {
        return submit(tenant, submission, new LocalTransportOwnershipPermit(PhysicalEnqueueAttemptId.random()));
    }

    @Override
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final AuthenticatedTenantContext tenant,
            final PreparedSubmission submission,
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
            final CompletionStage<SubmissionOutcomeMessage> mapped = stage.handle((result, error) -> {
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

    private static CompletionStage<SubmissionOutcomeMessage> completed(final SubmissionOutcomeMessage outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    private static SubmissionOutcomeMessage localFailure(
            final SubmissionTransportPlan plan, final PhysicalEnqueueAttemptId attempt, final StableCode code) {
        if (plan.submission().isManaged()) {
            final PreparedCommand command =
                    CommandCodec.decodeManagedFrame(plan.submission().managedFrame());
            return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.localDefinite(command, code));
        }
        final NativePreparedDelivery prepared = plan.submission().nativePrepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.submissionHash(), null, null, null);
        final var error = StableError.of(FailureStage.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueued(ref, proof, error));
    }

    private static SubmissionOutcomeMessage localFailure(
            final PreparedSubmission submission, final PhysicalEnqueueAttemptId attempt, final StableCode code) {
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeManagedFrame(submission.managedFrame());
            return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.localDefinite(command, code));
        }
        final NativePreparedDelivery prepared = submission.nativePrepared();
        final var ref = prepared.preparedRef();
        final var proof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.submissionHash(), null, null, null);
        final var error = StableError.of(FailureStage.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueued(ref, proof, error));
    }
}
