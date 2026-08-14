package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PinnedPulsarCommandIngress;
import io.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Explicit production composition seam for guarded Pulsar SEND/native SEND.
 * It requires exact resource identity and disables client behaviours that
 * would obscure one-record evidence at the NDR1 boundary.
 */
public final class ProductionPulsarSendTransport implements CommandTransport {
    private final GuardedPulsarCommandTransport delegate;

    public ProductionPulsarSendTransport(final PulsarCommandTransportKey key,
                                          final Configuration configuration,
                                          final PinnedPulsarCommandIngress.PulsarSendTransport managedSender,
                                          final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport
                                                  nativeSender) {
        Objects.requireNonNull(configuration, "configuration").validate();
        this.delegate = new GuardedPulsarCommandTransport(key, managedSender, nativeSender);
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

    /** Configuration that must be proven by the upstream Pulsar client factory. */
    public record Configuration(boolean batchingDisabled, boolean chunkingDisabled,
                                boolean autoTopicCreationDisabled, String tlsIdentity) {
        public Configuration {
            Objects.requireNonNull(tlsIdentity, "tlsIdentity");
            if (tlsIdentity.isBlank()) {
                throw new IllegalArgumentException("tlsIdentity must be nonblank");
            }
        }

        private void validate() {
            if (!batchingDisabled || !chunkingDisabled || !autoTopicCreationDisabled) {
                throw new IllegalArgumentException(
                        "guarded Pulsar transport requires batching, chunking, and auto-create disabled");
            }
        }
    }
}
