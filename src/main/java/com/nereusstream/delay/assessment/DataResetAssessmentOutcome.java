package com.nereusstream.delay.assessment;

/** Closed G0 outcomes. A PASS informs Gate C but never grants deployment authority itself. */
public enum DataResetAssessmentOutcome {
    PASS_DIRECT_REPLACE,
    PASS_RETAIN,
    MIGRATION_REQUIRED,
    INCOMPLETE;

    public boolean decisionReady() {
        return this == PASS_DIRECT_REPLACE || this == PASS_RETAIN;
    }
}
