package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.KafkaProduceRequest;
import io.nereusstream.delay.adapter.KafkaProduceResult;
import io.nereusstream.delay.adapter.PinnedKafkaCommandIngress;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Kafka transport bridge that transfers ownership immediately before Produce. */
public final class GuardedKafkaCommandTransport implements CommandTransport {
    private final KafkaCommandTransportKey key;
    private final PinnedKafkaCommandIngress.KafkaProduceTransport producer;

    public GuardedKafkaCommandTransport(final KafkaCommandTransportKey key,
                                        final PinnedKafkaCommandIngress.KafkaProduceTransport producer) {
        this.key = Objects.requireNonNull(key, "key");
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    @Override
    public CommandTransportKey key() {
        return key;
    }

    @Override
    public CompletionStage<? extends TransportResult> send(final TransportRequest request,
                                                            final TransportOwnershipPermit ownershipPermit) {
        if (!(request instanceof KafkaProduceRequest kafka)
                || !key.authenticatedClusterId().equals(kafka.authenticatedClusterId())
                || !key.nativeTopicUuid().equals(kafka.nativeTopicUuid())
                || key.partition() != kafka.partition()) {
            return CompletableFuture.completedFuture(KafkaProduceResult.definitelyNotPersisted(
                    StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), null));
        }
        Objects.requireNonNull(ownershipPermit, "ownershipPermit");
        if (!ownershipPermit.tryTransferToLibraryOwnership()) {
            return CompletableFuture.completedFuture(KafkaProduceResult.definitelyNotPersisted(
                    StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), null));
        }
        return producer.produce(kafka);
    }

    @Override
    public void close() {
        producer.close();
    }
}
