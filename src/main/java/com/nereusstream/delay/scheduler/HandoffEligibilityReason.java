package com.nereusstream.delay.scheduler;

/** Closed reason set for native eligibility and its ordinary fallback. */
public enum HandoffEligibilityReason {
    FORBIDDEN,
    WRONG_ADAPTER,
    ORDERING_UNAVAILABLE,
    NOT_INITIAL_ATTEMPT,
    CAPABILITY_UNAVAILABLE,
    POLICY_DISABLED,
    POLICY_SHADOW,
    POLICY_OUT_OF_BOUNDS,
    POLICY_NOT_YET_VALID,
    POLICY_EXPIRED,
    ELIGIBLE
}
