package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PinnedPulsarCommandIngress;
import com.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter;
import com.nereusstream.delay.adapter.PulsarNativePreparedRecordValidator;
import com.nereusstream.delay.adapter.PulsarNativeSendRequest;
import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Pulsar transport bridge for managed SEND and native delayed SEND. */
public final class GuardedPulsarCommandTransport implements CommandTransport {
    private final PulsarCommandTransportKey key;
    private final PinnedPulsarCommandIngress.PulsarSendTransport managedSender;
    private final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport nativeSender;
    private final PulsarNativePreparedRecordValidator nativePreparedRecordValidator;

    public GuardedPulsarCommandTransport(
            final PulsarCommandTransportKey key,
            final PinnedPulsarCommandIngress.PulsarSendTransport managedSender,
            final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport nativeSender) {
        this(key, managedSender, nativeSender, null);
    }

    /** Activated H5 composition. The same validator must materialize the request in the resolver. */
    public GuardedPulsarCommandTransport(
            final PulsarCommandTransportKey key,
            final PinnedPulsarCommandIngress.PulsarSendTransport managedSender,
            final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport nativeSender,
            final PulsarNativePreparedRecordValidator nativePreparedRecordValidator) {
        this.key = Objects.requireNonNull(key, "key");
        this.managedSender = Objects.requireNonNull(managedSender, "managedSender");
        this.nativeSender = Objects.requireNonNull(nativeSender, "nativeSender");
        this.nativePreparedRecordValidator = nativePreparedRecordValidator;
    }

    @Override
    public CommandTransportKey key() {
        return key;
    }

    @Override
    public CompletionStage<? extends TransportResult> send(
            final TransportRequest request, final TransportOwnershipPermit ownershipPermit) {
        Objects.requireNonNull(ownershipPermit, "ownershipPermit");
        if (!matches(request)) {
            return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                    ownershipPermit.physicalAttemptId(), StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), null));
        }
        if (request instanceof PulsarNativeSendRequest nativeRequest) {
            if (nativePreparedRecordValidator == null || !nativeRequest.hasPreparedRecord()) {
                return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                        ownershipPermit.physicalAttemptId(), StableCode.CAPABILITY_UNAVAILABLE.wireValue(), null));
            }
            final StableCode rejection = nativePreparedRecordValidator.validate(
                    com.nereusstream.delay.protocol.NativePreparedDelivery.decode(nativeRequest.preparedBytes()),
                    nativeRequest.preparedRecord(),
                    nativeRequest.artifacts());
            if (rejection != null) {
                return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                        ownershipPermit.physicalAttemptId(), rejection.wireValue(), null));
            }
        }
        if (!ownershipPermit.tryTransferToLibraryOwnership()) {
            return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                    ownershipPermit.physicalAttemptId(), StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), null));
        }
        final CompletionStage<PulsarSendResult> result;
        if (request instanceof PulsarNativeSendRequest nativeRequest) {
            result = nativeSender.sendPreparedRecord(nativeRequest.preparedRecord(), nativeRequest.artifacts());
        } else {
            result = managedSender.send((PulsarSendRequest) request);
        }
        if (result == null) {
            return null;
        }
        try {
            return result.thenApply(
                    value -> value == null ? null : value.bindPhysicalAttemptId(ownershipPermit.physicalAttemptId()));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void close() {
        Throwable first = null;
        try {
            managedSender.close();
        } catch (RuntimeException | Error failure) {
            first = appendCloseFailure(first, failure);
        }
        try {
            nativeSender.close();
        } catch (RuntimeException | Error failure) {
            first = appendCloseFailure(first, failure);
        }
        if (first != null) {
            throwUnchecked(first);
        }
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    private boolean matches(final TransportRequest request) {
        if (request instanceof PulsarSendRequest managed) {
            return matches(
                    managed.authenticatedClusterId(),
                    managed.resourceIncarnation(),
                    managed.physicalTopic(),
                    managed.physicalTopicCreationTimestamp(),
                    managed.partition());
        }
        if (request instanceof PulsarNativeSendRequest nativeRequest) {
            return matches(
                    nativeRequest.authenticatedClusterId(),
                    nativeRequest.resourceIncarnation(),
                    nativeRequest.physicalTopic(),
                    nativeRequest.physicalTopicCreationTimestamp(),
                    nativeRequest.partition());
        }
        return false;
    }

    private boolean matches(
            final String cluster,
            final byte[] resource,
            final String topic,
            final long creationTimestamp,
            final int partition) {
        return key.authenticatedClusterId().equals(cluster)
                && key.resourceIncarnation().equals(new Bytes32(resource))
                && key.canonicalPhysicalTopic().equals(topic)
                && key.topicCreationTimestamp() == creationTimestamp
                && key.partition() == partition;
    }
}
