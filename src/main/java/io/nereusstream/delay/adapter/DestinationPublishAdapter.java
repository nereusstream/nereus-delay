package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Target-side publish boundary. It reports side-effect evidence only; it never mutates shard state. */
public interface DestinationPublishAdapter extends AutoCloseable {
    CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request);

    /**
     * Publishes with the source position and prepared hash retained by the
     * durable attempt. Ordinary adapters use the compatibility path; K2
     * target-plus-receipt adapters override this boundary to require both
     * identities before physical submission.
     */
    default CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request,
                                                               final SourcePosition sourcePosition,
                                                               final byte[] preparedPublishHash) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        return publish(request);
    }

    @Override
    default void close() {
        // Implementations with broker resources override this method.
    }
}
