package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.TargetPartitionHash;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TimingCapability;
import java.util.Objects;

/**
 * Shared zero-I/O checks for a verified Pulsar AUTO_FAST candidate.
 *
 * <p>The provider decides which immutable capability snapshot is available;
 * this class verifies that the snapshot can actually describe the current
 * intent and the already-frozen managed identity. It deliberately does not
 * resolve credentials, contact Oxia, or probe a Broker.</p>
 */
public final class NativePreparationEligibility {
    private NativePreparationEligibility() {}

    public static void require(
            final AuthenticatedTenantContext context,
            final RouteSnapshot managedRoute,
            final CanonicalScheduleIntent intent,
            final PreparedCommand managedCommand,
            final NativePreparationSnapshot candidate,
            final TrustedTimeSnapshot trustedTime) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(managedRoute, "managedRoute");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(trustedTime, "trustedTime");

        managedRoute.requireUsableForNewSchedule(
                context.authenticatedTenantScopeHash(), context.tenantRoutingScope(), trustedTime.epochMs());
        if (managedRoute.ingress().adapterKind() != AdapterKind.PULSAR
                || intent.adapterMetadata().kind() != AdapterMetadata.Kind.PULSAR
                || !intent.hasInlinePayload()
                || managedCommand != null
                        && !managedCommand.shardId().routeIncarnation().equals(managedRoute.routeIncarnation())) {
            throw new IllegalArgumentException("native candidate is not eligible for the managed Route");
        }

        final ProfileSemanticEnvelope destination = candidate.destination();
        final ProfileSemanticEnvelope capability = candidate.capability();
        if (destination.profileKind() != ProfileKind.DESTINATION
                || capability.profileKind() != ProfileKind.DELIVERY_CAPABILITY
                || !destination.ref().equals(intent.profile())) {
            throw new IllegalArgumentException("native candidate Profile does not match the Schedule");
        }
        if (!(destination.body() instanceof DestinationProfileSemantic destinationBody)
                || !(capability.body() instanceof DeliveryCapabilitySemantic capabilityBody)) {
            throw new IllegalArgumentException("native candidate Profile body is invalid");
        }
        if (destinationBody.adapterKind() != AdapterKind.PULSAR
                || capabilityBody.adapterKind() != AdapterKind.PULSAR
                || !TimingCapability.includes(capabilityBody.timingCapabilityBits(), TimingCapability.PULSAR_AUTO_FAST)
                || !destinationBody.deliveryCapability().equals(capability.ref())
                || !destinationBody
                        .targetResource()
                        .equals(com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(candidate.target()))) {
            throw new IllegalArgumentException("native candidate is not a certified Pulsar AUTO_FAST path");
        }

        final long physicalPartition = Integer.toUnsignedLong(candidate.physicalPartition());
        final long partitionCount = Integer.toUnsignedLong(destinationBody.targetPartitionCount());
        if (physicalPartition >= partitionCount) {
            throw new IllegalArgumentException("native candidate target partition is outside the Profile");
        }
        final boolean explicitPartition =
                destinationBody.allowedExplicitPartitions().contains(candidate.physicalPartition());
        final boolean partitionEligible =
                switch (destinationBody.targetPartitionPolicy()) {
                    case EXPLICIT_ONLY -> explicitPartition;
                    case HASH_ONLY, EXPLICIT_OR_HASH ->
                        explicitPartition
                                || TargetPartitionHash.partition(
                                                destination.ref(),
                                                destinationBody.targetPartitionCount(),
                                                routingBytes(
                                                        intent,
                                                        managedCommand,
                                                        destinationBody.targetPartitionHashInput()))
                                        == physicalPartition;
                };
        if (!partitionEligible) {
            throw new IllegalArgumentException("native candidate target partition violates the Profile policy");
        }

        final var snapshot = candidate.capabilitySnapshot();
        if (!java.util.Arrays.equals(snapshot.sdkPrincipalScopeDigest(), context.principalScopeHash())
                || !snapshot.destination().equals(destination.ref())
                || !snapshot.capability().equals(capability.ref())
                || !snapshot.target().equals(candidate.target())
                || snapshot.physicalPartition() != candidate.physicalPartition()
                || trustedTime.epochMs() < snapshot.issuedAt().earliestEpochMs()
                || trustedTime.epochMs() >= snapshot.notAfterEpochMs()
                || intent.deliverAtEpochMs() >= snapshot.notAfterEpochMs()
                || intent.deliverAtEpochMs() > intent.expireAtEpochMs()
                || intent.deliverAtEpochMs() > managedRoute.validUntilEpochMs()) {
            throw new IllegalArgumentException("native capability snapshot is stale or mismatched");
        }
        final var handoff = candidate.handoffPolicySnapshot();
        if (handoff != null) {
            if (intent.nativeDeliveryPolicy()
                            != com.nereusstream.delay.protocol.NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF
                    || handoff.mode() != com.nereusstream.delay.protocol.HandoffPolicyMode.ENABLED
                    || !handoff.allows(com.nereusstream.delay.protocol.HandoffPath.AUTO_FAST)) {
                throw new IllegalArgumentException(
                        "native candidate is not explicitly authorized by the Schedule policy");
            }
            if (trustedTime.epochMs() < handoff.validFromEpochMs()
                    || trustedTime.epochMs() >= handoff.validUntilEpochMs()) {
                throw new IllegalArgumentException("native handoff snapshot is not active at trusted time");
            }
            handoff.requireLeadAtMost(destinationBody.handoffLeadMs());
            if (intent.deliverAtEpochMs() >= handoff.validUntilEpochMs()) {
                throw new IllegalArgumentException("native candidate is outside the handoff lease");
            }
        }

        final long payloadBytes = intent.inlinePayload().length;
        final long metadataBytes = intent.adapterMetadata().pulsar().canonicalBytes().length;
        if (payloadBytes > destinationBody.maxPayloadBytes()
                || metadataBytes > destinationBody.maxAdapterMetadataBytes()
                || payloadBytes > destinationBody.maxTargetRecordBytes() - metadataBytes) {
            throw new IllegalArgumentException("native target record exceeds the Profile limits");
        }
    }

    private static byte[] routingBytes(
            final CanonicalScheduleIntent intent,
            final PreparedCommand managedCommand,
            final TargetPartitionHashInput hashInput) {
        return switch (hashInput) {
            case ORDERING_KEY -> {
                final byte[] orderingKey = intent.adapterMetadata().pulsar().orderingKey();
                yield orderingKey == null ? new byte[0] : orderingKey;
            }
            case ADAPTER_MESSAGE_KEY -> {
                final byte[] partitionKey = intent.adapterMetadata().pulsar().partitionKey();
                yield partitionKey == null ? new byte[0] : partitionKey;
            }
            case DELAY_MESSAGE_ID -> managedCommand.delayMessageId().bytes();
        };
    }
}
