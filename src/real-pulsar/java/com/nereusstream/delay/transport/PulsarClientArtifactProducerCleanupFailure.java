package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetSdkCleanupFailure;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/** Original P1 handle retained for its explicit shared-client owner; channel cleanup never shuts that client down. */
public final class PulsarClientArtifactProducerCleanupFailure extends TargetSdkCleanupFailure {
    private static final long serialVersionUID = 1L;
    private final transient PulsarClientArtifactProducerClose owned;

    PulsarClientArtifactProducerCleanupFailure(Throwable cause, PulsarClientArtifactProducerClose owned) {
        super(cause, owned::close);
        this.owned = owned;
    }

    /** Caller must first quiesce every user of the original client, including Source and Journal operations. */
    public CompletableFuture<Void> closeClientBackendAfterFailedClose() {
        return owned.closeClientBackendAfterFailedClose().thenRun(this::retryClose);
    }

    /** Same backend owner may retry actual resource termination after its first client close failed. */
    public CompletableFuture<Void> shutdownClientBackendAfterFailedClose(ThreadPoolExecutor ownerIo) {
        return owned.shutdownClientBackendAfterFailedClose(ownerIo).thenRun(this::retryClose);
    }
}
