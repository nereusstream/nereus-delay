package io.nereusstream.delay.adapter;

import java.util.concurrent.CompletionStage;

/** Target-side publish boundary. It reports side-effect evidence only; it never mutates shard state. */
public interface DestinationPublishAdapter extends AutoCloseable {
    CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request);

    @Override
    default void close() {
        // Implementations with broker resources override this method.
    }
}
