package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/**
 * Builds the typed, signed initial Publish Outcome consumed by the physical
 * Worker bridge.
 *
 * <p>This factory owns canonical body construction and envelope signing only.
 * The {@link OutcomeContextProvider} remains an external authority for the
 * retry-policy decision, charge transfer and trusted observation interval; it
 * must not be treated as a local proof of Broker persistence. The destination
 * adapter still supplies the physical evidence and the source log still owns
 * ordering and application.</p>
 */
public final class WorkerPublishOutcomeMutationFactory
        implements WorkerPhysicalPublishExecutor.PublishOutcomeMutationFactory {
    private final OutcomeContextProvider contextProvider;
    private final byte[] authorIdentity;
    private final int signingKeyVersion;
    private final PrivateKey signingKey;

    /** Creates a factory for one owner-authorized signing key and context source. */
    public WorkerPublishOutcomeMutationFactory(
            final OutcomeContextProvider contextProvider,
            final byte[] authorIdentity,
            final int signingKeyVersion,
            final PrivateKey signingKey) {
        this.contextProvider = Objects.requireNonNull(contextProvider, "contextProvider");
        this.authorIdentity = Bytes.copy(Objects.requireNonNull(authorIdentity, "authorIdentity"));
        this.signingKeyVersion = signingKeyVersion;
        this.signingKey = Objects.requireNonNull(signingKey, "signingKey");
        if (signingKeyVersion == 0) {
            throw new IllegalArgumentException("signingKeyVersion must be non-zero");
        }
    }

    /** Constructs and signs one exact initial PUBLISH_OUTCOME mutation. */
    @Override
    public SystemMutation create(
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final DestinationPublishResult result) {
        final PublishAttemptLedger exactAttempt = requireAttempt(attempt);
        final DestinationPublishRequest exactRequest = requireRequest(exactAttempt, request);
        final DestinationPublishResult exactResult = Objects.requireNonNull(result, "result");
        final OutcomeContext context = Objects.requireNonNull(
                contextProvider.context(exactAttempt, exactRequest, exactResult), "outcome context");
        final int sideEffect = sideEffect(exactResult);
        validateResultEvidence(exactAttempt, exactResult, sideEffect);
        validateDisposition(exactResult, context.disposition(), sideEffect);
        final byte[] evidence = sideEffect == 3 ? null : exactResult.evidence();
        final byte[] body = PublishOutcomeBody.encodeInitial(
                exactRequest.delayMessageId().routingId().shardId(),
                context.retryUntilEpochMs(),
                exactAttempt.publishAttemptId(),
                sideEffect,
                context.disposition(),
                exactResult.stableCode(),
                evidence,
                context.transfer(),
                context.observedAt(),
                context.retryDecision());
        return SystemMutation.signed(
                exactRequest.delayMessageId().routingId().shardId(),
                SystemMutationType.PUBLISH_OUTCOME,
                context.retryUntilEpochMs(),
                exactAttempt.publishAttemptId(),
                body,
                authorIdentity,
                signingKeyVersion,
                signingKey);
    }

    private static PublishAttemptLedger requireAttempt(final PublishAttemptLedger attempt) {
        final PublishAttemptLedger exact = Objects.requireNonNull(attempt, "attempt");
        if (exact.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalArgumentException("Publish Outcome requires a PUBLISHING ledger");
        }
        return exact;
    }

    private static DestinationPublishRequest requireRequest(
            final PublishAttemptLedger attempt, final DestinationPublishRequest request) {
        final DestinationPublishRequest exact = Objects.requireNonNull(request, "request");
        if (!attempt.delayMessageId().equals(exact.delayMessageId())
                || attempt.generation() != exact.generation()
                || !attempt.laneId().equals(exact.laneId())
                || !Arrays.equals(attempt.laneIncarnation(), exact.laneIncarnation())
                || !Arrays.equals(attempt.publishAttemptId(), exact.publishAttemptId())) {
            throw new IllegalArgumentException("Publish Outcome request does not match the PUBLISHING ledger");
        }
        return exact;
    }

    private static int sideEffect(final DestinationPublishResult result) {
        return switch (result.disposition()) {
            case PUBLISHED -> 1;
            case DEFINITIVELY_NOT_PUBLISHED -> 2;
            case UNKNOWN -> 3;
        };
    }

    private static void validateResultEvidence(
            final PublishAttemptLedger attempt, final DestinationPublishResult result, final int sideEffect) {
        if (sideEffect == 3) {
            if (result.evidence() != null && result.evidence().length != 0) {
                throw new IllegalArgumentException("UNKNOWN Publish Outcome cannot carry typed evidence");
            }
            return;
        }
        final byte[] evidence = result.evidence();
        final PublishEvidence parsed = PublishEvidence.decode(evidence);
        parsed.requireBusinessMutation(attempt.publishAttemptId(), sideEffect == 1);
    }

    private static void validateDisposition(
            final DestinationPublishResult result, final int disposition, final int sideEffect) {
        if (disposition < 0 || disposition > 4) {
            throw new IllegalArgumentException("Publish Outcome disposition is outside the range");
        }
        if ((sideEffect == 1 && disposition != 0)
                || (sideEffect == 2 && (disposition < 1 || disposition > 3))
                || (sideEffect == 3 && disposition != 4)) {
            throw new IllegalArgumentException(
                    "Publish Outcome disposition does not match physical result" + ": " + result.disposition());
        }
    }

    @FunctionalInterface
    public interface OutcomeContextProvider {
        OutcomeContext context(
                PublishAttemptLedger attempt, DestinationPublishRequest request, DestinationPublishResult result);
    }

    /** External typed context required to materialize one source mutation. */
    public record OutcomeContext(
            long retryUntilEpochMs,
            int disposition,
            byte[] transfer,
            TrustedUtcIntervalEvidence observedAt,
            byte[] retryDecision) {
        public OutcomeContext {
            if (retryUntilEpochMs < 0) {
                throw new IllegalArgumentException("retryUntilEpochMs must be non-negative");
            }
            if (disposition < 0 || disposition > 4) {
                throw new IllegalArgumentException("disposition is outside the range");
            }
            transfer = Bytes.copy(Objects.requireNonNull(transfer, "transfer"));
            observedAt = Objects.requireNonNull(observedAt, "observedAt");
            retryDecision = Bytes.copy(Objects.requireNonNull(retryDecision, "retryDecision"));
        }

        @Override
        public byte[] transfer() {
            return Bytes.copy(transfer);
        }

        @Override
        public byte[] retryDecision() {
            return Bytes.copy(retryDecision);
        }
    }
}
