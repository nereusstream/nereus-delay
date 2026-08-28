package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarPreparedRecordFactory;
import com.nereusstream.delay.assessment.DataResetActivationGate;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.ResolvedPayload;
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
import java.util.concurrent.atomic.AtomicReference;
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
    private final DataResetActivationGate dataResetActivationGate;
    private final Executor completionExecutor;
    private ManagedPulsarContext managedPulsarContext;

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
                delegate,
                physicalAdmission,
                workClasses,
                physicalExecutor,
                outcomeExecutor,
                physicalGate,
                outcomeFactory,
                fenceOnFailure,
                null);
    }

    /** Production composition with an exact H6 generation/send barrier. */
    public WorkerPhysicalPublishExecutor(
            final DestinationPublishAdapter delegate,
            final DestinationPhysicalAdmission physicalAdmission,
            final WorkClassExecutionRegistry workClasses,
            final Executor physicalExecutor,
            final OutcomeWorkClassExecutor outcomeExecutor,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure,
            final DataResetActivationGate dataResetActivationGate) {
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
                fenceOnFailure,
                dataResetActivationGate,
                physicalExecutor);
    }

    /** Creates the bridge around an already composed bounded physical adapter. */
    public WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeWorkClassExecutor outcomeExecutor,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure) {
        this(physicalAdapter, outcomeExecutor, physicalGate, outcomeFactory, fenceOnFailure, null);
    }

    /** Bounded-adapter composition with an exact H6 generation/send barrier. */
    public WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeWorkClassExecutor outcomeExecutor,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure,
            final DataResetActivationGate dataResetActivationGate) {
        this(
                physicalAdapter,
                (mutation, ownerClock) -> Objects.requireNonNull(outcomeExecutor, "outcomeExecutor")
                        .submit(mutation, ownerClock),
                physicalGate,
                outcomeFactory,
                fenceOnFailure,
                dataResetActivationGate,
                Runnable::run);
    }

    WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeMutationSink outcomeSink,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure) {
        this(physicalAdapter, outcomeSink, physicalGate, outcomeFactory, fenceOnFailure, null, Runnable::run);
    }

    WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeMutationSink outcomeSink,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure,
            final DataResetActivationGate dataResetActivationGate) {
        this(
                physicalAdapter,
                outcomeSink,
                physicalGate,
                outcomeFactory,
                fenceOnFailure,
                dataResetActivationGate,
                Runnable::run);
    }

    WorkerPhysicalPublishExecutor(
            final BoundedDestinationPublishAdapter physicalAdapter,
            final OutcomeMutationSink outcomeSink,
            final PhysicalPublishGate physicalGate,
            final PublishOutcomeMutationFactory outcomeFactory,
            final Runnable fenceOnFailure,
            final DataResetActivationGate dataResetActivationGate,
            final Executor completionExecutor) {
        this.physicalAdapter = Objects.requireNonNull(physicalAdapter, "physicalAdapter");
        this.outcomeSink = Objects.requireNonNull(outcomeSink, "outcomeSink");
        this.physicalGate = Objects.requireNonNull(physicalGate, "physicalGate");
        this.outcomeFactory = Objects.requireNonNull(outcomeFactory, "outcomeFactory");
        this.fenceOnFailure = Objects.requireNonNull(fenceOnFailure, "fenceOnFailure");
        this.dataResetActivationGate = dataResetActivationGate;
        this.completionExecutor = Objects.requireNonNull(completionExecutor, "completionExecutor");
    }

    /**
     * Binds the single current managed-Pulsar Journal authority before this
     * executor is used. The binding is one-way so a live Producer cannot be
     * silently switched to another sequence or artifact generation.
     */
    public synchronized void bindManagedPulsarContext(final ManagedPulsarContext context) {
        if (managedPulsarContext != null) {
            throw new IllegalStateException("managed Pulsar context is already bound");
        }
        managedPulsarContext = Objects.requireNonNull(context, "context");
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
        requireExactPayload(descriptor.payload(), exactPayload);
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

    /**
     * Joins a current Pulsar Admission to its exact durable Journal mapping.
     * The mapping must already be durable; this method has no transport side
     * effect and deliberately cannot allocate a new sequence.
     */
    public static PulsarPreparedRecord preparePulsarRecord(
            final PublishAttemptLedger attempt,
            final PulsarAttemptJournal.Mapping mapping,
            final ArtifactGenerationSet artifacts,
            final byte[] payload) {
        final PublishAttemptLedger exactAttempt = requirePublishingAttempt(attempt);
        final PulsarAttemptJournal.Mapping exactMapping = Objects.requireNonNull(mapping, "mapping");
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(exactAttempt.admissionBytes());
        final PreparedPublishDescriptor descriptor = admission.descriptor().value();
        requireAttemptIdentity(exactAttempt, descriptor, admission);
        if (!Arrays.equals(exactAttempt.sourcePosition(), exactMapping.sourcePosition())) {
            throw new IllegalArgumentException("Journal mapping source position differs from the attempt ledger");
        }
        final byte[] exactPayload = Objects.requireNonNull(payload, "payload");
        requireExactPayload(descriptor.payload(), exactPayload);
        return PulsarPreparedRecordFactory.managed(
                descriptor,
                exactMapping,
                ResolvedPayload.of(exactPayload),
                Objects.requireNonNull(artifacts, "artifacts"));
    }

    /** Builds the current H3 Journal identity from the admitted descriptor. */
    public static PulsarAttemptJournal.CurrentAttemptIdentity prepareCurrentPulsarJournalIdentity(
            final PublishAttemptLedger attempt, final ArtifactGenerationSet artifacts) {
        final PublishAttemptLedger exactAttempt = requirePublishingAttempt(attempt);
        final ArtifactGenerationSet exactArtifacts = Objects.requireNonNull(artifacts, "artifacts");
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(exactAttempt.admissionBytes());
        final PreparedPublishDescriptor descriptor = admission.descriptor().value();
        requireAttemptIdentity(exactAttempt, descriptor, admission);
        if (descriptor.pulsarRecordTemplate() == null
                || descriptor.recordTemplateHash() == null
                || !Arrays.equals(descriptor.artifactGenerationSetDigest(), exactArtifacts.setDigest())) {
            throw new IllegalArgumentException("current Pulsar Journal identity requires the exact record template");
        }
        return new PulsarAttemptJournal.CurrentAttemptIdentity(
                descriptor.messageId(),
                Math.toIntExact(descriptor.generation()),
                descriptor.publishAttemptId(),
                descriptor.preparedPublishHash(),
                descriptor.recordTemplateHash(),
                descriptor.deliveryContract(),
                exactAttempt.sourcePosition(),
                exactArtifacts.setDigest());
    }

    private Submission submitPulsarRecord(
            final ManagedPulsarContext context,
            final PublishAttemptLedger attempt,
            final byte[] payload,
            final LongSupplier ownerClock) {
        final ManagedPulsarContext exactContext = Objects.requireNonNull(context, "context");
        final PulsarAttemptJournal exactJournal = exactContext.journal();
        final PulsarAttemptJournal.ProducerKey exactProducer = exactContext.producer();
        final ArtifactGenerationSet artifacts = exactContext.artifacts();
        final PublishAttemptLedger exactAttempt = requirePublishingAttempt(attempt);
        final DestinationPublishRequest request = prepareRequest(exactAttempt, payload);
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (request.actionAtEpochMs() < request.deliverAtEpochMs()) {
            return handoff(
                    exactAttempt,
                    request,
                    DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null),
                    clock);
        }
        try {
            requirePhysicalArtifact(exactContext);
        } catch (RuntimeException failure) {
            return handoff(
                    exactAttempt,
                    request,
                    DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null),
                    clock);
        }
        final boolean recoveryOwner =
                exactContext.ownerEpoch() != 0 && exactContext.ownerEpoch() != exactAttempt.ownerEpoch();
        if (!recoveryOwner) {
            final Decision initial = checkGate(exactAttempt, request, clock);
            if (initial.kind() == DecisionKind.DEFERRED) {
                return Submission.deferred(exactAttempt, request, initial);
            }
            if (initial.kind() == DecisionKind.DEFINITIVELY_NOT_PUBLISHED) {
                return handoff(exactAttempt, request, initial.result(), clock);
            }
        }

        final PulsarAttemptJournal.CurrentAttemptIdentity journalIdentity =
                prepareCurrentPulsarJournalIdentity(exactAttempt, artifacts);
        if (journalIdentity.deliveryContract()
                != com.nereusstream.delay.protocol.DeliveryContract.NEREUS_MANAGED_NOT_BEFORE) {
            throw new IllegalArgumentException("managed Journal submission cannot use the native contract");
        }
        final Optional<PulsarAttemptJournal.Mapping> recoveredMapping =
                recoveryOwner ? exactJournal.findCurrent(exactProducer, journalIdentity) : Optional.empty();
        final PulsarAttemptJournal.Mapping mapping = recoveredMapping.orElseGet(() -> exactJournal
                .appendOrReuseCurrent(exactProducer, journalIdentity)
                .record()
                .mapping());
        if (!exactAttempt.mappingDurable()) {
            exactContext
                    .projectionSink()
                    .recordMapped(
                            exactAttempt,
                            mapping.sequenceId(),
                            journalPosition(exactJournal, mapping, PulsarAttemptJournal.RecordKind.MAPPED));
        } else if (exactAttempt.journalSequenceId() != mapping.sequenceId()) {
            throw new IllegalStateException("durable attempt and Journal sequence differ");
        }

        if (recoveryOwner) {
            final PulsarAttemptJournal.AttemptState journalState = exactJournal.state(mapping.mappingId());
            if (journalState == PulsarAttemptJournal.AttemptState.MAPPED) {
                exactContext.projectionSink().markRetirementPending(exactAttempt);
                final PulsarAttemptJournal.JournalRecord retired =
                        exactJournal.retireNotPublished(mapping.mappingId()).record();
                exactContext
                        .projectionSink()
                        .recordRetired(exactAttempt, retired.position().canonicalBytes());
            } else if (journalState == PulsarAttemptJournal.AttemptState.RETIRED_NOT_PUBLISHED
                    && exactAttempt.retirementPending()) {
                exactContext
                        .projectionSink()
                        .recordRetired(
                                exactAttempt,
                                journalPosition(
                                        exactJournal, mapping, PulsarAttemptJournal.RecordKind.RETIRED_NOT_PUBLISHED));
            }
            return handoff(
                    exactAttempt,
                    request,
                    DestinationPublishResult.unknown(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null),
                    clock);
        }

        final PulsarPreparedRecord record = preparePulsarRecord(exactAttempt, mapping, artifacts, payload);
        final Decision ownershipGate = checkGate(exactAttempt, request, clock);
        if (ownershipGate.kind() == DecisionKind.DEFERRED) {
            return Submission.deferred(
                    exactAttempt, request, Decision.deferred(ownershipGate.code(), ownershipGate.evidence()));
        }
        if (ownershipGate.kind() == DecisionKind.DEFINITIVELY_NOT_PUBLISHED) {
            return retireBeforeOwnershipAndHandoff(
                    exactContext, exactAttempt, request, exactJournal, mapping, ownershipGate.result(), clock);
        }
        try {
            requirePhysicalArtifact(exactContext);
        } catch (RuntimeException failure) {
            return Submission.deferred(
                    exactAttempt, request, Decision.deferred(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final Submission submission = Submission.pending(exactAttempt, request);
        try {
            final AtomicReference<BoundedDestinationPublishAdapter.PublishCall> physicalCall = new AtomicReference<>();
            final CompletionStage<DestinationPublishResult> journalStage =
                    exactJournal.sendAfterMapped(mapping, ignored -> {
                        final BoundedDestinationPublishAdapter.PublishCall call = physicalAdapter.submitPreparedRecord(
                                record,
                                artifacts,
                                request.laneId(),
                                request.laneIncarnation(),
                                (ignoredRecord, ignoredArtifacts) -> {
                                    final Decision late = checkGate(exactAttempt, request, clock);
                                    if (late.kind() == DecisionKind.DEFINITIVELY_NOT_PUBLISHED) {
                                        return late.result();
                                    }
                                    if (late.kind() == DecisionKind.DEFERRED) {
                                        return DestinationPublishResult.unknown(late.code(), late.evidence());
                                    }
                                    try {
                                        requirePhysicalArtifact(exactContext);
                                    } catch (RuntimeException failure) {
                                        return DestinationPublishResult.unknown(
                                                StableCode.CAPABILITY_UNAVAILABLE, null);
                                    }
                                    exactJournal.markOwnershipStarted(mapping);
                                    return null;
                                });
                        physicalCall.set(call);
                        return call.outcome();
                    });
            final BoundedDestinationPublishAdapter.PublishCall call = physicalCall.get();
            if (call == null) {
                throw new IllegalStateException("Journal sender did not expose the bounded physical call");
            }
            submission.attachPhysicalCall(call);
            registerCompletion(
                    journalStage,
                    completionExecutor,
                    exactResult -> completePulsarJournalResult(
                            submission, exactResult, exactContext, exactJournal, mapping, clock));
            return submission;
        } catch (RuntimeException | Error failure) {
            fail(submission, failure);
            throw failure;
        }
    }

    private Submission retireBeforeOwnershipAndHandoff(
            final ManagedPulsarContext context,
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final PulsarAttemptJournal journal,
            final PulsarAttemptJournal.Mapping mapping,
            final DestinationPublishResult result,
            final LongSupplier ownerClock) {
        context.projectionSink().markRetirementPending(attempt);
        final PulsarAttemptJournal.JournalRecord retired =
                journal.retireNotPublished(mapping.mappingId()).record();
        context.projectionSink().recordRetired(attempt, retired.position().canonicalBytes());
        return handoff(attempt, request, result, ownerClock);
    }

    private static byte[] journalPosition(
            final PulsarAttemptJournal journal,
            final PulsarAttemptJournal.Mapping mapping,
            final PulsarAttemptJournal.RecordKind kind) {
        for (PulsarAttemptJournal.JournalRecord record : journal.records()) {
            if (record.kind() == kind && Arrays.equals(record.mapping().mappingId(), mapping.mappingId())) {
                return record.position().canonicalBytes();
            }
        }
        throw new IllegalStateException("durable Attempt Journal state has no local position: " + kind);
    }

    private void completePulsarJournalResult(
            final Submission submission,
            final DestinationPublishResult result,
            final ManagedPulsarContext context,
            final PulsarAttemptJournal journal,
            final PulsarAttemptJournal.Mapping mapping,
            final LongSupplier ownerClock) {
        DestinationPublishResult resolved = result;
        try {
            final PulsarAttemptJournal.AttemptState journalState = journal.state(mapping.mappingId());
            if (result.disposition() == DestinationPublishResult.Disposition.PUBLISHED) {
                if (journalState != PulsarAttemptJournal.AttemptState.OWNERSHIP_STARTED) {
                    throw new IllegalStateException("published target result has no durable ownership marker");
                }
                journal.markPublished(mapping);
            } else if (result.disposition() == DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED
                    && journalState == PulsarAttemptJournal.AttemptState.MAPPED) {
                context.projectionSink().markRetirementPending(submission.attempt());
                final PulsarAttemptJournal.JournalRecord retired =
                        journal.retireNotPublished(mapping.mappingId()).record();
                context.projectionSink()
                        .recordRetired(submission.attempt(), retired.position().canonicalBytes());
            } else if (result.disposition() == DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED) {
                resolved = DestinationPublishResult.unknown(StableCode.PULSAR_EVIDENCE_DIVERGENCE, null);
            }
        } catch (RuntimeException | Error failure) {
            failClosed(failure);
            resolved = DestinationPublishResult.unknown(StableCode.PULSAR_EVIDENCE_DIVERGENCE, null);
        }
        completeResult(submission, resolved, ownerClock);
    }

    private void requirePhysicalArtifact(final ManagedPulsarContext context) {
        context.artifactGate().require(context.artifacts());
        if (dataResetActivationGate != null) {
            dataResetActivationGate.requirePhysicalSend(
                    context.artifacts(), dataResetActivationGate.manifest().manifestDigest());
        }
    }

    private static void requireExactPayload(final PayloadForPublish payloadProjection, final byte[] exactPayload) {
        Objects.requireNonNull(payloadProjection, "payloadProjection");
        if (payloadProjection.hasInlinePayload()) {
            if (!Arrays.equals(payloadProjection.inlinePayload(), exactPayload)) {
                throw new IllegalArgumentException("physical payload differs from inline Publish Admission");
            }
        } else if (payloadProjection.length() != exactPayload.length
                || !Arrays.equals(payloadProjection.payloadSha256(), Bytes.sha256(exactPayload))) {
            throw new IllegalArgumentException("physical payload differs from Object Store commitment");
        }
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

        // H0 keeps the not-yet-closed native handoff path before every
        // physical gate, reservation, and adapter/delegate invocation.
        if (exactRequest.actionAtEpochMs() < exactRequest.deliverAtEpochMs()) {
            return handoff(
                    exactAttempt,
                    exactRequest,
                    DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null),
                    clock);
        }

        final ManagedPulsarContext pulsarContext;
        synchronized (this) {
            pulsarContext = managedPulsarContext;
        }
        if (pulsarContext != null
                && exactRequest.laneId().equals(pulsarContext.producer().laneId())
                && Arrays.equals(
                        exactRequest.laneIncarnation(), pulsarContext.producer().laneIncarnation())) {
            return submitPulsarRecord(pulsarContext, exactAttempt, exactRequest.payload(), clock);
        }

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
            registerCompletion(
                    call.outcome(), completionExecutor, exactResult -> completeResult(submission, exactResult, clock));
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
            final Executor completionExecutor,
            final java.util.function.Consumer<DestinationPublishResult> callback) {
        Objects.requireNonNull(stage, "physical outcome stage").whenComplete((value, error) -> {
            final DestinationPublishResult result = error != null || value == null
                    ? DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null)
                    : value;
            try {
                Objects.requireNonNull(completionExecutor, "completionExecutor").execute(() -> callback.accept(result));
            } catch (RuntimeException rejected) {
                // Do not run blocking Journal or source handoff work on a Broker I/O callback.
                callback.accept(DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
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

    /** Exact H6 or disposable-local authority for one immutable artifact set. */
    @FunctionalInterface
    public interface PhysicalArtifactGate {
        void require(ArtifactGenerationSet artifacts);
    }

    /** Durable local projection updated only after the corresponding Journal ACK. */
    public interface JournalProjectionSink {
        void recordMapped(PublishAttemptLedger attempt, long sequenceId, byte[] journalPosition);

        void markRetirementPending(PublishAttemptLedger attempt);

        void recordRetired(PublishAttemptLedger attempt, byte[] journalPosition);
    }

    /** One immutable managed Pulsar producer/Journal/artifact authority. */
    public record ManagedPulsarContext(
            PulsarAttemptJournal journal,
            PulsarAttemptJournal.ProducerKey producer,
            ArtifactGenerationSet artifacts,
            PhysicalArtifactGate artifactGate,
            JournalProjectionSink projectionSink,
            long ownerEpoch) {
        public ManagedPulsarContext {
            Objects.requireNonNull(journal, "journal");
            Objects.requireNonNull(producer, "producer");
            Objects.requireNonNull(artifacts, "artifacts");
            Objects.requireNonNull(artifactGate, "artifactGate");
            Objects.requireNonNull(projectionSink, "projectionSink");
        }
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
