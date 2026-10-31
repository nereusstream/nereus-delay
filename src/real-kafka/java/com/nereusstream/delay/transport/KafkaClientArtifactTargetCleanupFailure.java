package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetSdkCleanupFailure;

/** Retains the exact SDK handle and its resource charge after a Target client close failed. */
public final class KafkaClientArtifactTargetCleanupFailure extends TargetSdkCleanupFailure {
    private static final long serialVersionUID = 1L;

    public KafkaClientArtifactTargetCleanupFailure(Throwable cause, Runnable close) {
        super(cause, close);
    }
}
