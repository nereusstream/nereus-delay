package com.nereusstream.delay.scheduler;

/** Process-local result action for one bounded handoff eligibility visit. */
public enum HandoffEligibilityAction {
    WAIT_UNTIL,
    TIME_SAMPLE_REQUIRED,
    ORDINARY_DUE,
    MANAGED_NATIVE_CANDIDATE
}
