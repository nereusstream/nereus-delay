package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;

import java.util.Objects;

/**
 * Local, already verified AUTO_FAST authority.  It is a value supplied by a
 * Route/catalog refresher, never constructed from caller target/token input.
 */
public final class NativePreparationSnapshotV1 {
    private final ProfileSemanticEnvelopeV1 destination;
    private final ProfileSemanticEnvelopeV1 capability;
    private final PulsarBrokerResourceIdentityV1 target;
    private final int physicalPartition;
    private final NativeCapabilitySnapshotV1 capabilitySnapshot;
    private final long brokerDeliverAtEpochMs;

    public NativePreparationSnapshotV1(final ProfileSemanticEnvelopeV1 destination,
                                       final ProfileSemanticEnvelopeV1 capability,
                                       final PulsarBrokerResourceIdentityV1 target, final int physicalPartition,
                                       final NativeCapabilitySnapshotV1 capabilitySnapshot,
                                       final long brokerDeliverAtEpochMs) {
        this.destination = requireKind(destination, ProfileKindV1.DESTINATION, "destination");
        this.capability = requireKind(capability, ProfileKindV1.DELIVERY_CAPABILITY, "capability");
        this.target = Objects.requireNonNull(target, "target");
        if (physicalPartition < 0) {
            throw new IllegalArgumentException("physicalPartition must be non-negative");
        }
        this.physicalPartition = physicalPartition;
        this.capabilitySnapshot = Objects.requireNonNull(capabilitySnapshot, "capabilitySnapshot");
        if (!destination.ref().equals(capabilitySnapshot.destination())
                || !capability.ref().equals(capabilitySnapshot.capability())
                || !target.equals(capabilitySnapshot.target())
                || physicalPartition != capabilitySnapshot.physicalPartition()) {
            throw new IllegalArgumentException("native snapshot projection disagrees with capability snapshot");
        }
        if (brokerDeliverAtEpochMs < 0) {
            throw new IllegalArgumentException("brokerDeliverAtEpochMs must be non-negative");
        }
        this.brokerDeliverAtEpochMs = brokerDeliverAtEpochMs;
        final DestinationProfileSemanticV1 destinationBody = destinationBody();
        final DeliveryCapabilitySemanticV1 capabilityBody = capabilityBody();
        if (destinationBody.adapterKind() != io.nereusstream.delay.protocol.AdapterKindV1.PULSAR
                || capabilityBody.adapterKind() != io.nereusstream.delay.protocol.AdapterKindV1.PULSAR
                || (capabilityBody.timingCapabilityBits()
                & io.nereusstream.delay.protocol.TimingCapabilityV1.PULSAR_AUTO_FAST) == 0
                || !destinationBody.targetResource().pulsar().equals(
                new PulsarBrokerResourceIdentityV1(target.authenticatedClusterId(),
                        target.resourceIncarnation(), target.physicalTopic(),
                        target.physicalTopicCreationTimestamp()))) {
            throw new IllegalArgumentException("native snapshot does not carry a certified Pulsar AUTO_FAST path");
        }
    }

    public ProfileSemanticEnvelopeV1 destination() {
        return destination;
    }

    public ProfileSemanticEnvelopeV1 capability() {
        return capability;
    }

    public PulsarBrokerResourceIdentityV1 target() {
        return target;
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public NativeCapabilitySnapshotV1 capabilitySnapshot() {
        return capabilitySnapshot;
    }

    public long brokerDeliverAtEpochMs() {
        return brokerDeliverAtEpochMs;
    }

    private DestinationProfileSemanticV1 destinationBody() {
        if (!(destination.body() instanceof DestinationProfileSemanticV1 body)) {
            throw new IllegalArgumentException("destination profile body is not a Destination profile");
        }
        return body;
    }

    private DeliveryCapabilitySemanticV1 capabilityBody() {
        if (!(capability.body() instanceof DeliveryCapabilitySemanticV1 body)) {
            throw new IllegalArgumentException("capability profile body is not a capability profile");
        }
        return body;
    }

    private static ProfileSemanticEnvelopeV1 requireKind(final ProfileSemanticEnvelopeV1 value,
                                                         final ProfileKindV1 kind, final String name) {
        Objects.requireNonNull(value, name);
        if (value.profileKind() != kind) {
            throw new IllegalArgumentException(name + " has the wrong Profile kind");
        }
        return value;
    }
}
