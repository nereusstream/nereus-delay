package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import java.util.Objects;

/** Gateway response union: preparation errors are separate from prepared enqueue outcomes. */
public final class GatewaySubmissionOutcome {
    private final SubmissionOutcomeMessage submissionOutcome;
    private final StableError preparationError;

    private GatewaySubmissionOutcome(
            final SubmissionOutcomeMessage submissionOutcome, final StableError preparationError) {
        if ((submissionOutcome == null) == (preparationError == null)) {
            throw new IllegalArgumentException("Gateway outcome must select one response branch");
        }
        this.submissionOutcome = submissionOutcome;
        this.preparationError = preparationError;
    }

    public static GatewaySubmissionOutcome submission(final SubmissionOutcomeMessage outcome) {
        return new GatewaySubmissionOutcome(Objects.requireNonNull(outcome, "outcome"), null);
    }

    public static GatewaySubmissionOutcome preparationError(final StableError error) {
        return new GatewaySubmissionOutcome(null, Objects.requireNonNull(error, "error"));
    }

    public boolean hasSubmissionOutcome() {
        return submissionOutcome != null;
    }

    public SubmissionOutcomeMessage submissionOutcome() {
        return submissionOutcome;
    }

    public StableError preparationError() {
        return preparationError;
    }
}
