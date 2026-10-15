package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import com.nereusstream.delay.adapter.PulsarDestinationTimingPolicy;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.OwnedSourceCursor;
import com.nereusstream.delay.ownership.TargetDestinationActivation;
import com.nereusstream.delay.ownership.TargetDestinationChannelRuntime;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

/** Actual P1 BASELINE/managed producer opening within an already owned shared backend generation. */
public final class PulsarClientArtifactTargetProducerFactory {
    private PulsarClientArtifactTargetProducerFactory() {}

    public record Configuration(Duration timeout, long maximumPublicationBytes, CapacityVector cost, boolean nativeEnabled) {
        public Configuration {
            Objects.requireNonNull(timeout); Objects.requireNonNull(cost);
            if (timeout.toMillis() < 1 || timeout.toMillis() > 60_000 || maximumPublicationBytes < 1
                    || maximumPublicationBytes == Long.MAX_VALUE
                    || cost.amount(CapacityDimension.ADAPTER_PRODUCERS) < 1
                    || cost.amount(CapacityDimension.BUFFERED_MESSAGES) < 1
                    || cost.amount(CapacityDimension.BUFFERED_BYTES) < maximumPublicationBytes) {
                throw new IllegalArgumentException("configured P1 producer cost/settings cannot serve its publication bound");
            }
        }
    }

    public static Configuration load(Path path) throws java.io.IOException {
        final var f = com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader.loadFields(path);
        final boolean nativeEnabled = switch (f.text("producer.nativeEnabled")) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("P1 nativeEnabled requires an explicit boolean");
        };
        final var result = new Configuration(Duration.ofMillis(f.number("producer.timeoutMs")),
                f.number("producer.maximumPublicationBytes"), f.capacity("producer.cost."), nativeEnabled);
        f.requireComplete(); return result;
    }

    public static TargetDestinationChannelRuntime openConfigured(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            PulsarClientArtifactOwnedBackendGroup group,
            com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch, byte[] channelDigest, Path configuration,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver resolver,
            com.nereusstream.delay.runtime.ProfileCatalog profiles,
            com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads,
            TargetDestinationSdkBudget budget) throws java.io.IOException {
        final var config = load(configuration);
        try {
            group.requireWorkerGraph(startup.graph().classes(), startup.graph().resources());
            final var activation = startup.preparePublicationChannel(channelDigest, resolver, profiles, trust, reads, time);
            final var factory = opening(group, activation.channel(), activation.physical(), config.timeout(),
                    config.maximumPublicationBytes(), activation, config.nativeEnabled());
            return startup.openPublicationChannel(dispatch, activation.channel(), activation.physical(), budget, config.cost(),
                    config.maximumPublicationBytes(), factory, reads);
        } catch (RuntimeException | Error failure) { startup.failResourceStartup(failure); throw failure; }
    }

    public static TargetDestinationChannelRuntime.Open opening(PulsarClientArtifactOwnedBackendGroup group,
            TargetChannelIdentity channel, CanonicalTargetPartition physical, Duration timeout,
            long maximumPublicationBytes, TargetDestinationActivation activation, boolean nativeEnabled) {
        Objects.requireNonNull(group, "actual shared P1 owner");
        Objects.requireNonNull(activation, "actual Source/Profile activation");
        final var receipt = activation.verifiedMaterial();
        group.requireLoadedMaterial(receipt);
        if (physical.resource().kind() != BrokerResourceIdentity.Kind.PULSAR
                || !physical.id().equals(channel.context().target()) || maximumPublicationBytes <= 0
                || maximumPublicationBytes == Long.MAX_VALUE || timeout.toMillis() <= 0 || timeout.toMillis() > 60_000
                || channel.context().kind() != ChannelKind.BASELINE_PRODUCER
                    && channel.context().kind() != ChannelKind.PULSAR_DEDUP_PRODUCER
                || nativeEnabled && channel.context().kind() != ChannelKind.PULSAR_DEDUP_PRODUCER) {
            throw new IllegalArgumentException("P1 opening requires the exact finite BASELINE/managed Target");
        }
        final var resource = physical.resource().pulsar();
        if (!resource.authenticatedClusterId().equals(group.authenticatedClusterId())) {
            throw new IllegalArgumentException("P1 client endpoint cluster differs from the actual Source Target");
        }
        final int partition = Math.toIntExact(physical.physicalPartition());
        final var expectedGuard = new TopicResourceGuard(resource.authenticatedClusterId(),
                resource.resourceIncarnation(), resource.physicalTopicCreationTimestamp());
        final var expectedAttestation = new TopicResourceGuardAttestation(expectedGuard, resource.physicalTopic(), partition);
        final byte[] producerIdentity = channel.context().producerIdentity();
        final String name = new String(producerIdentity, StandardCharsets.UTF_8);
        return new TargetDestinationChannelRuntime.Open() {
            @Override public PulsarClientArtifactOwnedBackendGroup ownedBackendGroup() { return group; }
            @Override public com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt() { return receipt; }
            @Override public void requireLoadedMaterial(TargetChannelIdentity current) { activation.requireLoadedMaterial(current); }

            @Override public com.nereusstream.delay.adapter.DestinationPublishAdapter create(
                    Runnable guard, TargetDestinationSdkBudget.Client reserved) {
                guard.run(); activation.run();
                group.requireTargetCredential(channel, receipt);
                final long[] minimum = new long[CapacityDimension.COUNT];
                minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
                minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = maximumPublicationBytes;
                reserved.requireCovers(new CapacityVector(minimum));
                final var client = group.nativeClientForFactory();
                final var builder = PulsarClientArtifactProducerFactory.builder(client,
                        resource.authenticatedClusterId(), resource.resourceIncarnation(), resource.physicalTopic(),
                        resource.physicalTopicCreationTimestamp(), name)
                        .sendTimeout(Math.toIntExact(timeout.toMillis()), java.util.concurrent.TimeUnit.MILLISECONDS);
                final var creating = new CompletableFuture<Producer<byte[]>>();
                final PulsarClientArtifactProducerClose[] closing = new PulsarClientArtifactProducerClose[1];
                final var owned = new OwnedSourceCursor<>(creating, producer -> {
                    synchronized (closing) {
                        if (closing[0] == null) { closing[0] = new PulsarClientArtifactProducerClose(client, producer, timeout, reserved); }
                        return closing[0].closeFuture();
                    }
                }, group);
                try {
                    group.registerCleanup(owned, owned::operationExited, owned::cleanupComplete, proof -> {
                        proof.requireClient(client); owned.requestClose(); owned.backendShutdownCompleted(group);
                    });
                    PulsarClientArtifactOwnedBackendGroup.createProducer(client, builder, reserved)
                            .whenComplete((producer, failure) -> {
                                if (failure == null) { creating.complete(producer); }
                                else { creating.completeExceptionally(failure); }
                            });
                } catch (RuntimeException | Error failure) {
                    reserved.releaseUnstarted();
                    // No SDK call was made: a frozen/full registration window rejected this opening.
                    owned.abandonBeforeOpen();
                    throw failure;
                }
                try {
                    return owned.read(timeout.toMillis(), producer -> {
                        guard.run(); activation.run();
                        group.requireProducerArtifact(producer);
                        // This locked SDK completes createAsync only after validating the configured guard response.
                        if (!resource.physicalTopic().equals(producer.getTopic())
                                || !Arrays.equals(Bytes.utf8(producer.getProducerName()), producerIdentity)
                                || !Bytes.constantTimeEquals(channel.context().resourceGuardAttestationDigest(),
                                        PulsarClientArtifactSourceRecordConsumer.attestationDigest(expectedAttestation))) {
                            throw new IllegalStateException("P1 live producer does not attest the original channel identity");
                        }
                        final var transport = new PulsarClientArtifactDestinationTransport(client, producer,
                                resource.authenticatedClusterId(), resource.resourceIncarnation(), resource.physicalTopic(),
                                resource.physicalTopicCreationTimestamp(), partition, Bytes.sha256(producerIdentity), null,
                                nativeEnabled, timeout, reserved);
                        return new PinnedPulsarDestinationAdapter(new PulsarTargetResource(resource.authenticatedClusterId(),
                                resource.resourceIncarnation(), resource.physicalTopic(), resource.physicalTopicCreationTimestamp(), partition),
                                transport, PulsarDestinationTimingPolicy.ordinaryManaged(), nativeEnabled);
                    });
                } catch (Exception | Error failure) {
                    if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                    owned.requestClose();
                    throw new CreationFailure(failure, group, owned);
                } finally {
                    group.cleanupProgress();
                }
            }
        };
    }

    /** Owns the original creation, late producer and close; only its actual group may terminate missing handles. */
    public static final class CreationFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient PulsarClientArtifactOwnedBackendGroup group;
        private final transient OwnedSourceCursor<?> owned;
        private CreationFailure(Throwable cause, PulsarClientArtifactOwnedBackendGroup group, OwnedSourceCursor<?> owned) {
            super("P1 Target producer opening/cleanup is not confirmed", cause); this.group = group; this.owned = owned;
        }
        @Override public boolean cleanupComplete() { return owned == null ? group.resourcesTerminated() : owned.cleanupComplete(); }
        @Override public CompletableFuture<Void> retryCleanup() {
            return owned == null ? group.requestHostQuarantine() : owned.retryClose();
        }
        public CompletableFuture<Void> requestBackendOwnerCleanup() { return group.requestHostQuarantine(); }
    }
}
