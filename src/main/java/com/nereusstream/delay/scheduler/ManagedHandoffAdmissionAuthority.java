package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

/** Admission-time authority that freezes one CAS-stable Managed Handoff lease. */
@FunctionalInterface
public interface ManagedHandoffAdmissionAuthority {
    HandoffPolicySnapshot freezeCurrent(
            ClaimMaterialization materialization, ScheduleBinding binding, TrustedUtcIntervalEvidence decisionTime);
}
