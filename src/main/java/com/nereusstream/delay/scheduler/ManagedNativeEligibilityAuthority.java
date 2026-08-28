package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.MessageRecord;

/** Live authority that turns one durable static native index into a current process-local decision. */
@FunctionalInterface
public interface ManagedNativeEligibilityAuthority {
    HandoffEligibilityResolver.Decision resolve(
            MessageRecord message, ScheduleBinding binding, TrustedUtcIntervalEvidence trustedTime);
}
