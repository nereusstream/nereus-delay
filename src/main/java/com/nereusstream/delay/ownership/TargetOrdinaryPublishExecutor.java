package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.TargetOrdinaryPublishRequest;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PublishEvidence;
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

/** One actual Source-owned ordinary SEND followed by the original signed Outcome handoff. */
public final class TargetOrdinaryPublishExecutor {
    private final BoundedDestinationPublishAdapter adapter;
    private final ArtifactGenerationSet artifacts;
    private final Supplier<TargetPublishAdmissionStore.Applied> current;
    private final BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate;
    private final ResultAuthority results;
    private final TargetPublishOutcomeMutationFactory signer;
    private final Consumer<SystemMutation> handoff;
    private final Executor completion;
    private Submission pending;

    public TargetOrdinaryPublishExecutor(final BoundedDestinationPublishAdapter adapter,
            final ArtifactGenerationSet artifacts, final Supplier<TargetPublishAdmissionStore.Applied> current,
            final BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            final ResultAuthority results, final TargetPublishOutcomeMutationFactory signer,
            final Consumer<SystemMutation> handoff, final Executor completion) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.current = Objects.requireNonNull(current, "current Admission");
        this.gate = Objects.requireNonNull(gate, "final ownership gate");
        this.results = Objects.requireNonNull(results, "authenticated Outcome authority");
        this.signer = Objects.requireNonNull(signer, "Outcome signer");
        this.handoff = Objects.requireNonNull(handoff, "Source handoff");
        this.completion = Objects.requireNonNull(completion, "bounded completion executor");
    }

    public synchronized Submission submit(final PayloadForPublish payload, final ResolvedPayload resolved) {
        final var applied = Objects.requireNonNull(current.get(), "actual applied Admission");
        if (pending != null) { pending.request.applied().requireSame(applied); return pending; }
        if (applied.body().publication() == null
                || (applied.body().channel().context().kind() != ChannelKind.BASELINE_PRODUCER
                        && applied.body().channel().context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT)) {
            throw new IllegalArgumentException("ordinary publication requires its actual baseline or K2 Admission");
        }
        final var request = TargetOrdinaryPublishRequest.fromApplied(applied, payload, resolved, artifacts);
        final var submission = new Submission(request);
        pending = submission;
        try {
            submission.call = adapter.submitTargetOrdinary(request, actual -> {
                if (actual != request) { return DestinationPublishResult.unknown(StableCode.PREPARED_SUBMISSION_MISMATCH, null); }
                applied.requireSame(Objects.requireNonNull(current.get(), "current Admission"));
                return gate.check(actual);
            });
            submission.call.outcome().whenComplete((result, failure) -> observe(submission, result, failure));
        } catch (RuntimeException uncertain) { observe(submission, null, uncertain); }
        return submission;
    }

    private void observe(final Submission submission, final DestinationPublishResult result, final Throwable failure) {
        DestinationPublishResult accepted = failure == null && result != null ? result
                : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
        submission.failure = failure;
        try {
            if (accepted.disposition() == DestinationPublishResult.Disposition.PUBLISHED) {
                final var evidence = PublishEvidence.decode(accepted.evidence());
                if (submission.request.applied().kafkaMapping() == null) {
                    evidence.requireOrdinaryTargetPublishedBinding(submission.request.publication());
                } else {
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.requirePublished(
                            submission.request.applied().kafkaMapping(), evidence);
                }
            } else if (accepted.disposition() == DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED) {
                final var evidence = PublishEvidence.decode(accepted.evidence());
                if (submission.request.applied().kafkaMapping() == null) {
                    evidence.requireOrdinaryTargetNotPublishedBinding(submission.request.publication());
                } else {
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.requireNotPublished(
                            submission.request.applied().kafkaMapping(), evidence);
                }
            } else { accepted = DestinationPublishResult.unknown(accepted.stableCode(), null); }
        } catch (RuntimeException unboundEvidence) {
            submission.failure = unboundEvidence;
            accepted = DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        submission.result = accepted;
        scheduleCompletion(submission);
    }

    private void scheduleCompletion(final Submission submission) {
        try { completion.execute(() -> complete(submission)); }
        catch (RuntimeException rejected) { submission.failure = rejected; }
    }

    public synchronized void retryCompletion() {
        final var submission = Objects.requireNonNull(pending, "pending publication");
        if (submission.result == null) { throw new IllegalStateException("physical publication has not completed"); }
        scheduleCompletion(submission);
    }

    private void complete(final Submission submission) {
        synchronized (submission) {
            try {
                if (submission.mutation == null) {
                    final var context = Objects.requireNonNull(results.requireAuthenticated(
                            submission.request, submission.result), "authenticated Outcome context");
                    submission.mutation = signer.create(submission.request.applied(), submission.result, context);
                }
                if (!submission.handedOff) { handoff.accept(submission.mutation); submission.handedOff = true; }
                submission.failure = null;
            } catch (RuntimeException failed) { submission.failure = failed; }
        }
    }

    /** UNKNOWN enters the durable Source hold; this releases only the completed in-process flight. */
    public synchronized boolean settleApplied(final TargetOutcomeWorkClassExecutor source, final LongSupplier clock) {
        final var submission = Objects.requireNonNull(pending, "pending publication");
        if (submission.mutation == null) { return false; }
        final var first = source.readApplied(submission.mutation, clock).orElse(null);
        if (first == null) { return false; }
        if (first.applyStatus() != ApplyStatus.APPLIED
                || !Bytes.constantTimeEquals(first.mutationHash(), submission.mutation.mutationHash())) {
            throw new IllegalStateException("baseline Outcome did not settle as its exact Source first result");
        }
        pending = null;
        return true;
    }

    @FunctionalInterface
    public interface ResultAuthority {
        WorkerPublishOutcomeMutationFactory.OutcomeContext requireAuthenticated(
                TargetOrdinaryPublishRequest request, DestinationPublishResult result);
    }

    public static final class Submission {
        void releasePhysicalAfterTeardown() { if (call != null) { call.releasePhysicalCharge(); } }
        private final TargetOrdinaryPublishRequest request;
        private volatile BoundedDestinationPublishAdapter.PublishCall call;
        private volatile DestinationPublishResult result;
        private volatile SystemMutation mutation;
        private volatile Throwable failure;
        private volatile boolean handedOff;
        private Submission(final TargetOrdinaryPublishRequest request) { this.request = request; }
        public Optional<SystemMutation> mutation() { return Optional.ofNullable(mutation); }
        public Optional<DestinationPublishResult> result() { return Optional.ofNullable(result); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
        public boolean handedOff() { return handedOff; }
        public boolean markCallbackTimeout() { return call != null && call.markCallbackTimeout(); }
    }
}
