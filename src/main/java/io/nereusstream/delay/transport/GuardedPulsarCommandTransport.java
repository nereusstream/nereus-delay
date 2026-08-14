package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PinnedPulsarCommandIngress;
import io.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter;
import io.nereusstream.delay.adapter.PulsarNativeSendRequest;
import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Pulsar transport bridge for managed SEND and native delayed SEND. */
public final class GuardedPulsarCommandTransport implements CommandTransport {
    private final PulsarCommandTransportKey key;
    private final PinnedPulsarCommandIngress.PulsarSendTransport managedSender;
    private final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport nativeSender;

    public GuardedPulsarCommandTransport(final PulsarCommandTransportKey key,
                                         final PinnedPulsarCommandIngress.PulsarSendTransport managedSender,
                                         final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport nativeSender) {
        this.key = Objects.requireNonNull(key, "key");
        this.managedSender = Objects.requireNonNull(managedSender, "managedSender");
        this.nativeSender = Objects.requireNonNull(nativeSender, "nativeSender");
    }

    @Override
    public CommandTransportKey key() {
        return key;
    }

    @Override
    public CompletionStage<? extends TransportResult> send(final TransportRequest request,
                                                            final TransportOwnershipPermit ownershipPermit) {
        Objects.requireNonNull(ownershipPermit, "ownershipPermit");
        if (!matches(request)) {
            return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                    StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), null));
        }
        if (!ownershipPermit.tryTransferToLibraryOwnership()) {
            return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                    StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), null));
        }
        if (request instanceof PulsarNativeSendRequest nativeRequest) {
            return nativeSender.send(nativeRequest);
        }
        return managedSender.send((PulsarSendRequest) request);
    }

    @Override
    public void close() {
        managedSender.close();
        nativeSender.close();
    }

    private boolean matches(final TransportRequest request) {
        if (request instanceof PulsarSendRequest managed) {
            return matches(managed.authenticatedClusterId(), managed.resourceIncarnation(), managed.physicalTopic(),
                    managed.physicalTopicCreationTimestamp(), managed.partition());
        }
        if (request instanceof PulsarNativeSendRequest nativeRequest) {
            return matches(nativeRequest.authenticatedClusterId(), nativeRequest.resourceIncarnation(),
                    nativeRequest.physicalTopic(), nativeRequest.physicalTopicCreationTimestamp(),
                    nativeRequest.partition());
        }
        return false;
    }

    private boolean matches(final String cluster, final byte[] resource, final String topic,
                            final long creationTimestamp, final int partition) {
        return key.authenticatedClusterId().equals(cluster)
                && key.resourceIncarnation().equals(new Bytes32(resource))
                && key.canonicalPhysicalTopic().equals(topic)
                && key.topicCreationTimestamp() == creationTimestamp && key.partition() == partition;
    }
}
