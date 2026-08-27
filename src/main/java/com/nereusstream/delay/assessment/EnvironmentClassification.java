package com.nereusstream.delay.assessment;

/** Closed classification used to separate disposable development from persistent environments. */
public enum EnvironmentClassification {
    DISPOSABLE_LOCAL,
    EXISTING,
    STAGING,
    PRODUCTION,
    UNKNOWN;

    /** Whether this class can be the subject of a real DataResetAssessment and Gate C decision. */
    public boolean requiresDeploymentSafetyAssessment() {
        return this == EXISTING || this == STAGING || this == PRODUCTION;
    }
}
