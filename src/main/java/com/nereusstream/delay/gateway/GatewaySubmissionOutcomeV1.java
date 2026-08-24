package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.StableErrorV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import java.util.Objects;

/** Gateway response union: preparation errors are separate from prepared enqueue outcomes. */
public final class GatewaySubmissionOutcomeV1 {
    private final SubmissionOutcomeMessageV1 submissionOutcome;
    private final StableErrorV1 preparationError;

    private GatewaySubmissionOutcomeV1(
            final SubmissionOutcomeMessageV1 submissionOutcome, final StableErrorV1 preparationError) {
        if ((submissionOutcome == null) == (preparationError == null)) {
            throw new IllegalArgumentException("Gateway outcome must select one response branch");
        }
        this.submissionOutcome = submissionOutcome;
        this.preparationError = preparationError;
    }

    public static GatewaySubmissionOutcomeV1 submission(final SubmissionOutcomeMessageV1 outcome) {
        return new GatewaySubmissionOutcomeV1(Objects.requireNonNull(outcome, "outcome"), null);
    }

    public static GatewaySubmissionOutcomeV1 preparationError(final StableErrorV1 error) {
        return new GatewaySubmissionOutcomeV1(null, Objects.requireNonNull(error, "error"));
    }

    public boolean hasSubmissionOutcome() {
        return submissionOutcome != null;
    }

    public SubmissionOutcomeMessageV1 submissionOutcome() {
        return submissionOutcome;
    }

    public StableErrorV1 preparationError() {
        return preparationError;
    }
}
