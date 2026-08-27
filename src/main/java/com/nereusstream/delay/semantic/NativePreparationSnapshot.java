package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import java.util.Objects;

/**
 * Local, already verified AUTO_FAST authority. It is a value supplied by a
 * Route/catalog refresher, never constructed from caller target/token input.
 */
public final class NativePreparationSnapshot {
    private final ProfileSemanticEnvelope destination;
    private final ProfileSemanticEnvelope capability;
    private final PulsarBrokerResourceIdentity target;
    private final int physicalPartition;
    private final NativeCapabilitySnapshot capabilitySnapshot;
    private final HandoffPolicySnapshot handoffPolicySnapshot;
    private final Long legacyBrokerDeliverAtEpochMs;

    public NativePreparationSnapshot(
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final NativeCapabilitySnapshot capabilitySnapshot,
            final long brokerDeliverAtEpochMs) {
        this(destination, capability, target, physicalPartition, capabilitySnapshot, brokerDeliverAtEpochMs, null);
    }

    /** Current H2/H5 form: the candidate time is the Schedule's business deliverAt. */
    public NativePreparationSnapshot(
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final NativeCapabilitySnapshot capabilitySnapshot,
            final HandoffPolicySnapshot handoffPolicySnapshot) {
        this(destination, capability, target, physicalPartition, capabilitySnapshot, null, handoffPolicySnapshot);
    }

    private NativePreparationSnapshot(
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final NativeCapabilitySnapshot capabilitySnapshot,
            final Long legacyBrokerDeliverAtEpochMs,
            final HandoffPolicySnapshot handoffPolicySnapshot) {
        this.destination = requireKind(destination, ProfileKind.DESTINATION, "destination");
        this.capability = requireKind(capability, ProfileKind.DELIVERY_CAPABILITY, "capability");
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
        if (legacyBrokerDeliverAtEpochMs != null && legacyBrokerDeliverAtEpochMs < 0) {
            throw new IllegalArgumentException("legacy Broker delivery time must be non-negative");
        }
        this.legacyBrokerDeliverAtEpochMs = legacyBrokerDeliverAtEpochMs;
        this.handoffPolicySnapshot = handoffPolicySnapshot;
        final DestinationProfileSemantic destinationBody = destinationBody();
        final DeliveryCapabilitySemantic capabilityBody = capabilityBody();
        if (destinationBody.adapterKind() != com.nereusstream.delay.protocol.AdapterKind.PULSAR
                || capabilityBody.adapterKind() != com.nereusstream.delay.protocol.AdapterKind.PULSAR
                || (capabilityBody.timingCapabilityBits()
                                & com.nereusstream.delay.protocol.TimingCapability.PULSAR_AUTO_FAST)
                        == 0
                || !destinationBody
                        .targetResource()
                        .pulsar()
                        .equals(new PulsarBrokerResourceIdentity(
                                target.authenticatedClusterId(),
                                target.resourceIncarnation(),
                                target.physicalTopic(),
                                target.physicalTopicCreationTimestamp()))) {
            throw new IllegalArgumentException("native snapshot does not carry a certified Pulsar AUTO_FAST path");
        }
    }

    public ProfileSemanticEnvelope destination() {
        return destination;
    }

    public ProfileSemanticEnvelope capability() {
        return capability;
    }

    public PulsarBrokerResourceIdentity target() {
        return target;
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public NativeCapabilitySnapshot capabilitySnapshot() {
        return capabilitySnapshot;
    }

    public HandoffPolicySnapshot handoffPolicySnapshot() {
        return handoffPolicySnapshot;
    }

    /**
     * Compatibility accessor for the removed shifted Broker timestamp. New
     * snapshots do not carry one and therefore fail closed if this method is
     * called.
     */
    public long brokerDeliverAtEpochMs() {
        if (legacyBrokerDeliverAtEpochMs == null) {
            throw new IllegalStateException("current native snapshot has no shifted Broker timestamp");
        }
        return legacyBrokerDeliverAtEpochMs;
    }

    private DestinationProfileSemantic destinationBody() {
        if (!(destination.body() instanceof DestinationProfileSemantic body)) {
            throw new IllegalArgumentException("destination profile body is not a Destination profile");
        }
        return body;
    }

    private DeliveryCapabilitySemantic capabilityBody() {
        if (!(capability.body() instanceof DeliveryCapabilitySemantic body)) {
            throw new IllegalArgumentException("capability profile body is not a capability profile");
        }
        return body;
    }

    private static ProfileSemanticEnvelope requireKind(
            final ProfileSemanticEnvelope value, final ProfileKind kind, final String name) {
        Objects.requireNonNull(value, name);
        if (value.profileKind() != kind) {
            throw new IllegalArgumentException(name + " has the wrong Profile kind");
        }
        return value;
    }
}
