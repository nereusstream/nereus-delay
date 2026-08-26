package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.StableCode;
import java.util.concurrent.CompletableFuture;

/**
 * A logical destination result completed without observing the underlying
 * transport stage's physical completion. The bounded admission wrapper uses
 * this marker to retain the request/byte charge as a zombie rather than
 * treating {@code UNKNOWN} as a release proof.
 */
final class UnobservedDestinationPublishStage extends CompletableFuture<DestinationPublishResult> {
    private UnobservedDestinationPublishStage(final DestinationPublishResult result) {
        complete(result);
    }

    static UnobservedDestinationPublishStage unknown() {
        return new UnobservedDestinationPublishStage(
                DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
    }
}
