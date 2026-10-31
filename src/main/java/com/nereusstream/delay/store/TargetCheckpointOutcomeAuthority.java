package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetPublishOutcomeVerifier;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Original Outcome authentication on one complete image cut; history/key/evidence providers retain their rights. */
public final class TargetCheckpointOutcomeAuthority implements TargetCheckpointDependencies.OutcomeImageAuthority {
    @FunctionalInterface
    public interface HistoryAuthority {
        void requireOriginal(TargetCheckpointDependencies.OutcomeImage input, TargetResultRecord first);
    }

    private final TargetCheckpointDependencies dependencies;
    private final List<TargetCheckpointDependencies.AdmissionImage> admissions;
    private final List<TargetCheckpointDependencies.OutcomeImage> outcomes;
    private final HistoryAuthority history;
    private final TargetPublishOutcomeVerifier.Authority source;
    private final BoundedReadBudget budget;
    private final Runnable protection;

    public TargetCheckpointOutcomeAuthority(
            TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,
            List<TargetCheckpointDependencies.OutcomeImage> outcomes,
            long maximumFrameBytes,
            HistoryAuthority history,
            TargetPublishOutcomeVerifier.Authority source,
            BoundedReadBudget budget,
            Runnable protection) {
        this.dependencies = Objects.requireNonNull(dependencies, "dependencies");
        dependencies.withAdmissionImages(admissions, maximumFrameBytes);
        this.admissions = List.copyOf(admissions);
        this.outcomes = dependencies.correlateOutcomeImages(outcomes, maximumFrameBytes);
        this.history = Objects.requireNonNull(history, "history");
        this.source = Objects.requireNonNull(source, "source");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.protection = Objects.requireNonNull(protection, "protection");
    }

    @Override
    public PublicKey authenticate(TargetCheckpointDependencies.OutcomeImage input, TargetResultRecord first) {
        final var correlated = dependencies.outcomeReferences().stream()
                .filter(ref -> Arrays.equals(ref.first().canonicalBytes(), first.canonicalBytes()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Outcome first result belongs to another cut"));
        correlated.requireImage(input.image(), input.source());
        before();
        history.requireOriginal(input, first);
        after();
        if (SystemMutationResult.decode(first.typedPayload()).applyStatus() != ApplyStatus.APPLIED) {
            return null;
        }
        final var decision = TargetPublishOutcomeVerifier.decideFirstApplication(
                dependencies.root().root().scope(), input.image(), input.source(), (scope, writer, mutation, at) -> {
                    before();
                    final var authority = source.resolve(scope, writer, mutation, at);
                    after();
                    return authority;
                });
        after();
        if (decision.rejection() != null) {
            throw new IllegalArgumentException("original APPLIED Outcome is unauthorized: " + decision.rejection());
        }
        final var body = decision.body();
        final var authority = decision.authorization();
        final var retry = authority.retryContext();
        if (retry == null) {
            throw new IllegalStateException("Outcome original retry/Admission context unavailable");
        }
        final var current = requireAdmission(retry.admission());
        final var originalFirst = requireAdmission(retry.firstAdmission());
        final var admitted = TargetPublishAdmissionBody.decode(current.image().canonicalBody());
        final var firstBody =
                TargetPublishAdmissionBody.decode(originalFirst.image().canonicalBody());
        if (!Arrays.equals(body.publishAttemptId(), admitted.publishAttemptId())
                || !admitted.locator().equals(firstBody.locator())
                || firstBody.attemptNo() != 1
                || originalFirst.source().compareTo(current.source()) > 0
                || current.source().compareTo(input.source()) >= 0) {
            throw new IllegalArgumentException("Outcome changes original Admission/generation/Source history");
        }
        final var binding = dependencies.requireBinding(admitted.locator());
        if (!binding.intent().retryPolicy().equals(retry.policy().ref())) {
            throw new IllegalArgumentException("Outcome retry policy changes original immutable Schedule binding");
        }
        before();
        TargetPublishOutcomeVerifier.requireHistoricalRetryWindow(admitted, firstBody, retry.policy(), body);
        after();
        final boolean resolution = input.image().type() == SystemMutationType.EVIDENCE_RESOLUTION;
        final var writer = AuthorIdentity.decode(input.image().authorIdentity());
        if (body.sideEffect() == 3) {
            final boolean recovery = body.disposition() == 4
                    && body.stableCode() == StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                    && body.retryDecision().hasFullShape()
                    && body.retryDecision().kind() == 5
                    && !body.retryDecision().hasNextRetryAt();
            if (!admitted.owner().equals(writer.asOwnerIdentity())
                    && !(recovery && authority.activeOwner().equals(writer.asOwnerIdentity()))) {
                throw new IllegalArgumentException("UNKNOWN writer is neither admitted nor original recovery Owner");
            }
        } else {
            final var evidenceContext = authority.evidenceContext();
            if (evidenceContext == null || !admitted.materialized()) {
                throw new IllegalStateException("Outcome original physical evidence context unavailable");
            }
            final var evidenceAdmission = requireAdmission(evidenceContext.admission());
            if (!Arrays.equals(
                            current.image().canonicalEnvelope(),
                            evidenceAdmission.image().canonicalEnvelope())
                    || !Arrays.equals(body.transfer(), admitted.outcomeTransfer())
                    || !resolution && !admitted.owner().equals(writer.asOwnerIdentity())
                    || body.retryDecision().cause() != body.stableCode()) {
                throw new IllegalArgumentException("Outcome evidence changes original Admission/transfer/writer/cause");
            }
            final var evidence = PublishEvidence.decode(body.evidence());
            if (body.recoveryProof() != null) {
                before();
                final var snapshot = Objects.requireNonNull(
                        evidenceContext.recoveryProof(), "original full recovery object/seal snapshot");
                snapshot.requireExact(input.image());
                snapshot.verified().requireExact(body.recoveryProof());
                final var proof = snapshot.verified().proof();
                if (admitted.kafkaReceipt() == null
                        || !Arrays.equals(
                                proof.admission().canonicalEnvelope(),
                                current.image().canonicalEnvelope())
                        || !Arrays.equals(proof.evidence().canonicalBytes(), body.evidence())) {
                    throw new IllegalStateException("checkpoint recovery snapshot changed original Admission/evidence");
                }
                final var owner = dependencies.recoveryEvidence().stream()
                        .filter(value ->
                                Arrays.equals(value.outcomeId(), input.image().systemMutationId()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("APPLIED recovery Outcome lacks Native owner"));
                owner.requireProof(proof);
                owner.requireSource(input.image(), input.source());
                if (resolution) {
                    requireUnknown(admitted.publishAttemptId(), input);
                    if (!Arrays.equals(
                            body.evidenceCursor().canonicalBytes(),
                            com.nereusstream.delay.protocol.TargetKafkaReceiptWire.cursor(proof.mapping(), evidence)
                                    .canonicalBytes())) {
                        throw new IllegalStateException("checkpoint recovery resolution cursor changed original proof");
                    }
                }
            } else if (evidenceContext.recoveryProof() != null) {
                throw new IllegalStateException("archived recovery cannot authenticate a legacy Outcome");
            } else if (admitted.nativePublication() != null) {
                final var publication = admitted.nativePublication();
                if (body.sideEffect() == 1) {
                    evidence.requireNativeTargetPublishedBinding(publication);
                } else {
                    evidence.requireNativeTargetNotPublishedBinding(publication);
                }
                before();
                if (resolution) {
                    if (evidenceContext.nativeResolutionAuthority() == null) {
                        throw new IllegalStateException(
                                "Native resolution original cursor/history authority unavailable");
                    }
                    final var unknown = requireUnknown(admitted.publishAttemptId(), input);
                    evidenceContext
                            .nativeResolutionAuthority()
                            .requireAuthenticated(
                                    publication,
                                    evidence,
                                    body.evidenceCursor(),
                                    current.source(),
                                    unknown.source(),
                                    input.source());
                } else {
                    evidenceContext
                            .nativeAuthority()
                            .requireAuthenticated(publication, evidence, current.source(), input.source());
                }
            } else {
                if (body.sideEffect() == 1) {
                    evidence.requireOrdinaryTargetPublishedBinding(admitted.publication());
                } else {
                    evidence.requireOrdinaryTargetNotPublishedBinding(admitted.publication());
                }
                before();
                if (resolution) {
                    if (evidenceContext.resolutionAuthority() == null) {
                        throw new IllegalStateException(
                                "Outcome resolution original cursor/history authority unavailable");
                    }
                    final var unknown = requireUnknown(admitted.publishAttemptId(), input);
                    evidenceContext
                            .resolutionAuthority()
                            .requireAuthenticated(
                                    admitted.publication(),
                                    evidence,
                                    body.evidenceCursor(),
                                    current.source(),
                                    unknown.source(),
                                    input.source());
                } else {
                    evidenceContext
                            .authority()
                            .requireAuthenticated(admitted.publication(), evidence, current.source(), input.source());
                }
            }
            after();
        }
        return authority.writerKey();
    }

    private TargetCheckpointDependencies.AdmissionImage requireAdmission(SystemMutation image) {
        return admissions.stream()
                .filter(input -> Arrays.equals(input.image().canonicalEnvelope(), image.canonicalEnvelope()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Outcome lacks exact retained Admission image"));
    }

    private TargetCheckpointDependencies.OutcomeImage requireUnknown(
            byte[] attempt, TargetCheckpointDependencies.OutcomeImage resolution) {
        final var applied = new TreeMap<String, TargetResultRecord>();
        dependencies
                .outcomeResults()
                .forEach(first -> applied.put(
                        Bytes.hex(SystemMutationResult.decode(first.typedPayload())
                                .mutationId()),
                        first));
        return outcomes.stream()
                .filter(input -> input.image().type() == SystemMutationType.PUBLISH_OUTCOME
                        && input.source().compareTo(resolution.source()) < 0)
                .filter(input -> {
                    final var body = PublishOutcomeBody.decode(input.image().canonicalBody());
                    final var first = applied.get(Bytes.hex(input.image().systemMutationId()));
                    return body.sideEffect() == 3
                            && Arrays.equals(body.publishAttemptId(), attempt)
                            && SystemMutationResult.decode(first.typedPayload()).applyStatus() == ApplyStatus.APPLIED;
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("resolution lacks original APPLIED UNKNOWN"));
    }

    private void before() {
        protection.run();
        if (!budget.beforeRead()) {
            throw budget.incomplete();
        }
    }

    private void after() {
        protection.run();
        if (!budget.beforeTimedWork()) {
            throw budget.incomplete();
        }
    }
}
