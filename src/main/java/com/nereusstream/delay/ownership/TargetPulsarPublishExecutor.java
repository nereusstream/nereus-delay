package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarPreparedRecordFactory;
import com.nereusstream.delay.adapter.PulsarSendAckEvidence;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Source-owned publisher with durable Journal, final Store/live gate and exact Outcome handoff. */
public final class TargetPulsarPublishExecutor {
    private final PulsarAttemptJournal journal;
    private final BoundedDestinationPublishAdapter adapter;
    private final ArtifactGenerationSet artifacts;
    private final Supplier<TargetPublishAdmissionStore.Applied> current;
    private final BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight liveGate;
    private final ResultAuthority results;
    private final TargetPublishOutcomeMutationFactory outcomes;
    private final Consumer<SystemMutation> handoff;
    private final Executor completionExecutor;
    private Submission pending;

    public TargetPulsarPublishExecutor(
            PulsarAttemptJournal journal, BoundedDestinationPublishAdapter adapter, ArtifactGenerationSet artifacts,
            Supplier<TargetPublishAdmissionStore.Applied> current,
            BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight liveGate, ResultAuthority results,
            TargetPublishOutcomeMutationFactory outcomes, Consumer<SystemMutation> handoff,
            Executor completionExecutor) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.current = Objects.requireNonNull(current, "current");
        this.liveGate = Objects.requireNonNull(liveGate, "liveGate");
        this.results = Objects.requireNonNull(results, "results");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        this.handoff = Objects.requireNonNull(handoff, "handoff");
        this.completionExecutor = Objects.requireNonNull(completionExecutor, "completionExecutor");
    }

    public synchronized Submission submit(final PayloadForPublish payload, final ResolvedPayload resolved) {
        final var applied = Objects.requireNonNull(current.get(), "applied Admission");
        if (pending != null) {
            pending.applied.requireSame(applied);
            return pending;
        }
        final var publication = applied.body().publication();
        final var producer = PulsarAttemptJournal.ProducerKey.target(publication.channel(), publication.physical());
        final var identity = PulsarPreparedRecordFactory.targetJournalIdentity(
                publication, applied.message(), payload, applied.source());
        final var mapping = journal.appendOrReuseCurrent(producer, identity).record().mapping();
        final var record = PulsarPreparedRecordFactory.targetManaged(
                publication, applied.message(), payload, resolved, mapping, artifacts);
        final var submission = new Submission(applied, mapping, record);
        pending = submission;
        if (journal.state(mapping.mappingId()) != PulsarAttemptJournal.AttemptState.MAPPED) {
            submission.result = DestinationPublishResult.unknown(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null);
            completionExecutor.execute(() -> complete(submission, submission.result));
            return submission;
        }
        journal.markOwnershipStarted(mapping);
        final var physical = journal.sendAfterOwnershipStarted(mapping, ignored -> {
            submission.call = adapter.submitTargetPreparedRecord(publication, record, artifacts, (p, r, a) -> {
                applied.requireSame(Objects.requireNonNull(current.get(), "current applied Admission"));
                return liveGate.check(p, r, a);
            });
            return submission.call.outcome();
        });
        physical.whenComplete((value, failure) -> {
            submission.result = failure == null && value != null ? value
                    : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
            try {
                completionExecutor.execute(() -> complete(submission, submission.result));
            } catch (RuntimeException rejectedCompletion) {
                submission.failure = rejectedCompletion;
            }
        });
        return submission;
    }

    /** Retries only completion/handoff, retaining the observed result and any already signed exact bytes. */
    public synchronized void retryCompletion() {
        final var submission = Objects.requireNonNull(pending, "pending publication");
        if (submission.result == null) {
            throw new IllegalStateException("physical publication has not completed");
        }
        completionExecutor.execute(() -> complete(submission, submission.result));
    }

    /** Releases only this in-process pending slot after a verified actual Source first result, never by timeout. */
    public synchronized boolean settleApplied(
            final TargetOutcomeWorkClassExecutor source, final LongSupplier ownerClock) {
        final var submission = Objects.requireNonNull(pending, "pending publication");
        if (submission.mutation == null || !submission.handedOff) {
            return false;
        }
        final var result = source.readApplied(submission.mutation, ownerClock).orElse(null);
        if (result == null) {
            return false;
        }
        if (result.applyStatus() != ApplyStatus.APPLIED
                || !Bytes.constantTimeEquals(result.mutationHash(), submission.mutation.mutationHash())) {
            throw new IllegalStateException("Target publication Outcome was not applied as its exact first result");
        }
        pending = null;
        return true;
    }

    private void complete(final Submission submission, final DestinationPublishResult observed) {
        synchronized (submission) {
            try {
                if (submission.mutation == null) {
                    if (observed.disposition() == DestinationPublishResult.Disposition.PUBLISHED) {
                        PulsarSendAckEvidence.requireRecordBinding(
                                PublishEvidence.decode(observed.evidence()), submission.record, artifacts);
                    }
                    final var context = Objects.requireNonNull(
                            results.requireAuthenticated(submission.applied, submission.record, observed),
                            "Outcome context");
                    final var image = outcomes.create(submission.applied, observed, context);
                    submission.mutation = image;
                }
                if (observed.disposition() == DestinationPublishResult.Disposition.PUBLISHED
                        && !submission.journalResolved) {
                    journal.markPublished(submission.mapping);
                    submission.journalResolved = true;
                }
                if (!submission.handedOff) {
                    handoff.accept(submission.mutation);
                    submission.handedOff = true;
                }
                submission.failure = null;
            } catch (RuntimeException completionFailure) {
                submission.failure = completionFailure;
            }
        }
    }

    /** Accepted adapter evidence/retention and pinned retry/time policy; status alone is insufficient. */
    @FunctionalInterface
    public interface ResultAuthority {
        WorkerPublishOutcomeMutationFactory.OutcomeContext requireAuthenticated(
                TargetPublishAdmissionStore.Applied applied, PulsarPreparedRecord record,
                DestinationPublishResult result);
    }

    public static final class Submission {
        private final TargetPublishAdmissionStore.Applied applied;
        private final PulsarAttemptJournal.Mapping mapping;
        private final PulsarPreparedRecord record;
        private volatile BoundedDestinationPublishAdapter.PublishCall call;
        private volatile DestinationPublishResult result;
        private volatile SystemMutation mutation;
        private volatile Throwable failure;
        private volatile boolean handedOff;
        private volatile boolean journalResolved;

        private Submission(TargetPublishAdmissionStore.Applied applied,
                PulsarAttemptJournal.Mapping mapping, PulsarPreparedRecord record) {
            this.applied = applied;
            this.mapping = mapping;
            this.record = record;
        }

        public Optional<SystemMutation> mutation() { return Optional.ofNullable(mutation); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
        public boolean handedOff() { return handedOff; }
        public boolean markCallbackTimeout() { return call != null && call.markCallbackTimeout(); }
    }
}
