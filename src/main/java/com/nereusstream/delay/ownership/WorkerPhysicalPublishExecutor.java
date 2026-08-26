package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

/**
 * Bridges one durable PUBLISHING attempt to a guarded destination adapter and
 * then to the source-log Outcome handoff.
 *
 * <p>The executor owns no RocksDB mutation. The supplied gate is the live
 * Owner/Store/Claim/channel/certificate/time authority and is checked once
 * before physical admission and again immediately before the adapter
 * delegate. A destination result is converted to an already signed
 * {@link SystemMutation} by the supplied factory and queued through
 * {@link OutcomeWorkClassExecutor}; only source-ordered replay may apply it
 * locally. A deferred gate leaves the PUBLISHING attempt open for a bounded
 * retry, while an observed destination result always follows the outcome
 * handoff path, including UNKNOWN.</p>
 */
public final class WorkerPhysicalPublishExecutor implements AutoCloseable {
    private final BoundedDestinationPublishAdapter physicalAdapter;
    private final OutcomeMutationSink outcomeSink;
    private final PublishOutcomeMutationFactory outcomeFactory;
    private final PhysicalPublishGate physicalGate;
    private final Runnable fenceOnFailure;

    /**
     * Creates the production composition and binds the physical admission
     * pool to the shared Worker registry.
     */
    public WorkerPhysicalPublishExecutor(
            final DestinationPublishAdapter delegate,
            final DestinationPhysicalAdmission physicalAdmission,
            final WorkClassExecutionRegistry workClasses,
            final Executor physicalExecutor,
            final OutcomeWorkClassExecutor outcomeExecutor,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure) {
        this(
                new BoundedDestinationPublishAdapter(
                        Objects.requireNonNull(delegate, "delegate"),
                        Objects.requireNonNull(physicalAdmission, "physicalAdmission"),
                        Objects.requireNonNull(workClasses, "workClasses"),
                        Objects.requireNonNull(physicalExecutor, "physicalExecutor")),
                (mutation, ownerClock) -> Objects.requireNonNull(outcomeExecutor, "outcomeExecutor")
                        .submit(mutation, ownerClock),
                physicalGate,
                outcomeFactory,
                fenceOnFailure);
    }

    /** Creates the bridge around an already composed bounded physical adapter. */
    public WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeWorkClassExecutor outcomeExecutor,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure) {
        this(
                physicalAdapter,
                (mutation, ownerClock) -> Objects.requireNonNull(outcomeExecutor, "outcomeExecutor")
                        .submit(mutation, ownerClock),
                physicalGate,
                outcomeFactory,
                fenceOnFailure);
    }

    WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeMutationSink outcomeSink,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure) {
        this.physicalAdapter = Objects.requireNonNull(physicalAdapter, "physicalAdapter");
        this.outcomeSink = Objects.requireNonNull(outcomeSink, "outcomeSink");
        this.physicalGate = Objects.requireNonNull(physicalGate, "physicalGate");
        this.outcomeFactory = Objects.requireNonNull(outcomeFactory, "outcomeFactory");
        this.fenceOnFailure = Objects.requireNonNull(fenceOnFailure, "fenceOnFailure");
    }

    /**
     * Builds the exact adapter request from the canonical Admission retained
     * in a PUBLISHING ledger. Object-backed payloads must be supplied by the
     * external Object Store authority and are checked against the immutable
     * length/hash projection before any target call is possible.
     */
    public static DestinationPublishRequest prepareRequest(final PublishAttemptLedger attempt, final byte[] payload) {
        requirePublishingAttempt(attempt);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(attempt.admissionBytes());
        final PreparedPublishDescriptor descriptor = admission.descriptor().value();
        requireAttemptIdentity(attempt, descriptor, admission);
        final byte[] exactPayload = Objects.requireNonNull(payload, "payload");
        final PayloadForPublish payloadProjection = descriptor.payload();
        if (payloadProjection.hasInlinePayload()) {
            if (!Arrays.equals(payloadProjection.inlinePayload(), exactPayload)) {
                throw new IllegalArgumentException("physical payload differs from inline Publish Admission");
            }
        } else if (payloadProjection.length() != exactPayload.length
                || !Arrays.equals(payloadProjection.payloadSha256(), Bytes.sha256(exactPayload))) {
            throw new IllegalArgumentException("physical payload differs from Object Store commitment");
        }
        return new DestinationPublishRequest(
                descriptor.destinationLaneId(),
                descriptor.laneIncarnation(),
                descriptor.messageId(),
                Math.toIntExact(descriptor.generation()),
                descriptor.publishAttemptId(),
                descriptor.actionAtEpochMs(),
                descriptor.deliverAtEpochMs(),
                exactPayload,
                descriptor.businessMetadata().canonicalBytes());
    }

    /** Starts one bounded physical attempt or returns a deferred gate result. */
    public Submission submit(
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final LongSupplier ownerClock) {
        final PublishAttemptLedger exactAttempt = requirePublishingAttempt(attempt);
        final DestinationPublishRequest exactRequest = Objects.requireNonNull(request, "request");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireAttemptIdentity(exactAttempt, exactRequest);

        final Decision initial = checkGate(exactAttempt, exactRequest, clock);
        if (initial.kind() == DecisionKind.DEFERRED) {
            return Submission.deferred(exactAttempt, exactRequest, initial);
        }
        if (initial.kind() == DecisionKind.DEFINITIVELY_NOT_PUBLISHED) {
            return handoff(exactAttempt, exactRequest, initial.result(), clock);
        }

        final Submission submission = Submission.pending(exactAttempt, exactRequest);
        final BoundedDestinationPublishAdapter.PublishCall call;
        try {
            call = physicalAdapter.submit(
                    exactRequest,
                    SourcePositionCodec.decode(exactAttempt.sourcePosition()),
                    exactAttempt.preparedPublishHash(),
                    ignored -> lateGateResult(exactAttempt, exactRequest, clock));
            submission.attachPhysicalCall(call);
            registerCompletion(call.outcome(), exactResult -> completeResult(submission, exactResult, clock));
            return submission;
        } catch (RuntimeException | Error failure) {
            fail(submission, failure);
            throw failure;
        }
    }

    private void completeResult(
            final Submission submission, final DestinationPublishResult result, final LongSupplier ownerClock) {
        try {
            final DestinationPublishResult exact = Objects.requireNonNull(result, "destination publish result");
            final SystemMutation mutation = outcomeFactory.create(submission.attempt(), submission.request(), exact);
            outcomeSink.submit(Objects.requireNonNull(mutation, "outcome mutation"), ownerClock);
            submission.complete(exact, mutation);
        } catch (RuntimeException | Error failure) {
            fail(submission, failure);
        }
    }

    private Submission handoff(
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final DestinationPublishResult result,
            final LongSupplier ownerClock) {
        final Submission submission = Submission.pending(attempt, request);
        completeResult(submission, result, ownerClock);
        return submission;
    }

    private Decision checkGate(
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final LongSupplier ownerClock) {
        try {
            return Objects.requireNonNull(
                    physicalGate.check(attempt, request, ownerClock), "physical publish gate decision");
        } catch (RuntimeException | Error failure) {
            failClosed(failure);
            throw failure;
        }
    }

    private DestinationPublishResult lateGateResult(
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final LongSupplier ownerClock) {
        final Decision decision;
        try {
            decision = Objects.requireNonNull(
                    physicalGate.check(attempt, request, ownerClock), "physical publish gate decision");
        } catch (RuntimeException | Error failure) {
            failClosed(failure);
            return DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
        }
        return switch (decision.kind()) {
            case ALLOWED -> null;
            case DEFINITIVELY_NOT_PUBLISHED -> decision.result();
            case DEFERRED -> DestinationPublishResult.unknown(decision.code(), decision.evidence());
        };
    }

    private void fail(final Submission submission, final Throwable failure) {
        failClosed(failure);
        submission.fail(failure);
    }

    private void failClosed(final Throwable failure) {
        try {
            fenceOnFailure.run();
        } catch (RuntimeException | Error fenceFailure) {
            failure.addSuppressed(fenceFailure);
        }
    }

    private static void registerCompletion(
            final CompletionStage<DestinationPublishResult> stage,
            final java.util.function.Consumer<DestinationPublishResult> callback) {
        Objects.requireNonNull(stage, "physical outcome stage").whenComplete((value, error) -> {
            if (error != null || value == null) {
                callback.accept(DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
            } else {
                callback.accept(value);
            }
        });
    }

    private static PublishAttemptLedger requirePublishingAttempt(final PublishAttemptLedger attempt) {
        final PublishAttemptLedger exact = Objects.requireNonNull(attempt, "attempt");
        if (exact.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalArgumentException("physical publish requires a PUBLISHING ledger");
        }
        return exact;
    }

    private static void requireAttemptIdentity(
            final PublishAttemptLedger attempt,
            final PreparedPublishDescriptor descriptor,
            final PublishAdmissionBody admission) {
        if (!Arrays.equals(attempt.publishAttemptId(), descriptor.publishAttemptId())
                || !Arrays.equals(attempt.publishAttemptId(), admission.publishAttemptId())
                || !Arrays.equals(attempt.claimId(), admission.claimId())
                || attempt.generation() != descriptor.generation()
                || attempt.attemptNo() != descriptor.attemptNo()
                || !attempt.laneId().equals(descriptor.destinationLaneId())
                || !Arrays.equals(attempt.laneIncarnation(), descriptor.laneIncarnation())
                || !attempt.delayMessageId().equals(descriptor.messageId())
                || !Arrays.equals(attempt.preparedPublishHash(), descriptor.preparedPublishHash())) {
            throw new IllegalArgumentException("Publish Admission and attempt ledger identities differ");
        }
    }

    private static void requireAttemptIdentity(
            final PublishAttemptLedger attempt, final DestinationPublishRequest request) {
        if (!attempt.delayMessageId().equals(request.delayMessageId())
                || attempt.generation() != request.generation()
                || !attempt.laneId().equals(request.laneId())
                || !Arrays.equals(attempt.laneIncarnation(), request.laneIncarnation())
                || !Arrays.equals(attempt.publishAttemptId(), request.publishAttemptId())) {
            throw new IllegalArgumentException("physical request does not match the PUBLISHING ledger");
        }
    }

    @Override
    public void close() {
        physicalAdapter.close();
    }

    @FunctionalInterface
    public interface OutcomeMutationSink {
        void submit(SystemMutation mutation, LongSupplier ownerClock);
    }

    @FunctionalInterface
    public interface PublishOutcomeMutationFactory {
        SystemMutation create(
                PublishAttemptLedger attempt, DestinationPublishRequest request, DestinationPublishResult result);
    }

    /** Live Owner/Store/Claim/channel/certificate/time fence for one physical call. */
    @FunctionalInterface
    public interface PhysicalPublishGate {
        Decision check(PublishAttemptLedger attempt, DestinationPublishRequest request, LongSupplier ownerClock);
    }

    public enum DecisionKind {
        ALLOWED,
        DEFERRED,
        DEFINITIVELY_NOT_PUBLISHED
    }

    public record Decision(DecisionKind kind, StableCode code, byte[] evidence) {
        public Decision {
            Objects.requireNonNull(kind, "kind");
            if (kind == DecisionKind.ALLOWED) {
                if (code != null || evidence != null) {
                    throw new IllegalArgumentException("allowed gate decision cannot carry rejection evidence");
                }
            } else {
                if (code == null || code == StableCode.OK) {
                    throw new IllegalArgumentException("blocked gate decision requires a non-OK stable code");
                }
                evidence = evidence == null ? null : Bytes.copy(evidence);
            }
        }

        public static Decision allowed() {
            return new Decision(DecisionKind.ALLOWED, null, null);
        }

        public static Decision deferred(final StableCode code, final byte[] evidence) {
            return new Decision(DecisionKind.DEFERRED, code, evidence);
        }

        public static Decision definitivelyNotPublished(final StableCode code, final byte[] evidence) {
            return new Decision(DecisionKind.DEFINITIVELY_NOT_PUBLISHED, code, evidence);
        }

        private DestinationPublishResult result() {
            if (kind != DecisionKind.DEFINITIVELY_NOT_PUBLISHED) {
                throw new IllegalStateException("gate decision has no definitive result");
            }
            return DestinationPublishResult.definitelyNotPublished(code, evidence);
        }

        @Override
        public byte[] evidence() {
            return evidence == null ? null : Bytes.copy(evidence);
        }
    }

    public enum SubmissionState {
        PENDING,
        DEFERRED,
        OUTCOME_HANDOFF_QUEUED,
        FAILED
    }

    public static final class Submission {
        private final PublishAttemptLedger attempt;
        private final DestinationPublishRequest request;
        private volatile SubmissionState state;
        private volatile DestinationPublishResult physicalResult;
        private volatile SystemMutation outcomeMutation;
        private volatile BoundedDestinationPublishAdapter.PublishCall physicalCall;
        private volatile Throwable failure;

        private Submission(
                final PublishAttemptLedger attempt,
                final DestinationPublishRequest request,
                final SubmissionState state) {
            this.attempt = Objects.requireNonNull(attempt, "attempt");
            this.request = Objects.requireNonNull(request, "request");
            this.state = Objects.requireNonNull(state, "state");
        }

        private static Submission pending(final PublishAttemptLedger attempt, final DestinationPublishRequest request) {
            return new Submission(attempt, request, SubmissionState.PENDING);
        }

        private static Submission deferred(
                final PublishAttemptLedger attempt, final DestinationPublishRequest request, final Decision decision) {
            final Submission submission = new Submission(attempt, request, SubmissionState.DEFERRED);
            submission.physicalResult = DestinationPublishResult.unknown(decision.code(), decision.evidence());
            return submission;
        }

        private synchronized void attachPhysicalCall(final BoundedDestinationPublishAdapter.PublishCall call) {
            if (physicalCall != null) {
                throw new IllegalStateException("physical publish call is already attached");
            }
            physicalCall = Objects.requireNonNull(call, "physical call");
        }

        private synchronized void complete(final DestinationPublishResult result, final SystemMutation mutation) {
            if (state != SubmissionState.PENDING) {
                throw new IllegalStateException("physical publish submission is already complete");
            }
            physicalResult = Objects.requireNonNull(result, "physical result");
            outcomeMutation = Objects.requireNonNull(mutation, "outcome mutation");
            state = SubmissionState.OUTCOME_HANDOFF_QUEUED;
        }

        private synchronized void fail(final Throwable failure) {
            if (state == SubmissionState.OUTCOME_HANDOFF_QUEUED || state == SubmissionState.DEFERRED) {
                return;
            }
            this.failure = Objects.requireNonNull(failure, "failure");
            state = SubmissionState.FAILED;
        }

        public PublishAttemptLedger attempt() {
            return attempt;
        }

        public DestinationPublishRequest request() {
            return request;
        }

        public SubmissionState state() {
            return state;
        }

        public Optional<DestinationPublishResult> physicalResult() {
            return Optional.ofNullable(physicalResult);
        }

        public Optional<SystemMutation> outcomeMutation() {
            return Optional.ofNullable(outcomeMutation);
        }

        public Optional<BoundedDestinationPublishAdapter.PublishCall> physicalCall() {
            return Optional.ofNullable(physicalCall);
        }

        public Optional<Throwable> failure() {
            return Optional.ofNullable(failure);
        }
    }
}
