package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PinnedKafkaCommandIngress;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Explicit production composition seam for a Kafka Producer v13 client.
 * Construction refuses stock name-only or auto-create configurations; the
 * supplied client must already be bound to the exact key identity.
 */
public final class ProductionKafkaProduceTransport implements CommandTransport {
    private final GuardedKafkaCommandTransport delegate;

    public ProductionKafkaProduceTransport(final KafkaCommandTransportKey key,
                                           final Configuration configuration,
                                           final PinnedKafkaCommandIngress.KafkaProduceTransport producer) {
        Objects.requireNonNull(configuration, "configuration").validate();
        this.delegate = new GuardedKafkaCommandTransport(key, producer);
    }

    @Override
    public CommandTransportKey key() {
        return delegate.key();
    }

    @Override
    public CompletionStage<? extends TransportResult> send(final TransportRequest request,
                                                            final TransportOwnershipPermit ownershipPermit) {
        return delegate.send(request, ownershipPermit);
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** Configuration that must be proven by the upstream Kafka client factory. */
    public record Configuration(int acks, boolean idempotenceEnabled, boolean autoTopicCreationDisabled,
                                String tlsIdentity) {
        public Configuration {
            Objects.requireNonNull(tlsIdentity, "tlsIdentity");
            if (tlsIdentity.isBlank()) {
                throw new IllegalArgumentException("tlsIdentity must be nonblank");
            }
        }

        private void validate() {
            if (acks != -1 || !idempotenceEnabled || !autoTopicCreationDisabled) {
                throw new IllegalArgumentException(
                        "guarded Kafka transport requires acks=all, idempotence, and auto-create disabled");
            }
        }
    }
}
