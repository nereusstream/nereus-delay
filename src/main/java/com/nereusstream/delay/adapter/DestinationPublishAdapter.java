package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
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
    default CompletionStage<DestinationPublishResult> publish(
            final DestinationPublishRequest request,
            final SourcePosition sourcePosition,
            final byte[] preparedPublishHash) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        return publish(request);
    }

    /**
     * Publishes a current final Pulsar record projection. Adapters that do
     * not own a source-locked Pulsar record encoder fail closed by default.
     * The bounded wrapper must use its lane-aware prepared submission method
     * so this hook cannot bypass physical admission.
     */
    default CompletionStage<DestinationPublishResult> publishPreparedRecord(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        return java.util.concurrent.CompletableFuture.completedFuture(
                DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override
    default void close() {
        // Implementations with broker resources override this method.
    }
}
