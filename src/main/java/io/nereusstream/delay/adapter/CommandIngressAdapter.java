package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.EnqueueOutcome;
import io.nereusstream.delay.protocol.PreparedCommand;

import java.util.concurrent.CompletionStage;

/** Command Topic ingress boundary; adapters must preserve the three-state enqueue contract. */
public interface CommandIngressAdapter extends AutoCloseable {
    CompletionStage<EnqueueOutcome> enqueue(PreparedCommand command);

    @Override
    default void close() {
        // Implementations with broker resources override this method.
    }
}
