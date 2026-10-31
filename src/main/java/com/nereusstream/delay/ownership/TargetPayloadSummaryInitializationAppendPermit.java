package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SystemMutation;

/** Independently paid PLATFORM_OPERATOR initialization capability, checked at the real original Source SDK gate. */
public sealed interface TargetPayloadSummaryInitializationAppendPermit
        permits TargetPayloadSummaryInitializationMaintenance.AppendPermit {
    void beforeSdk(OwnedSourceSystemWriter actual, SystemMutation exact, TargetScopeExposureSource.CopyContract actualCopies);
}
