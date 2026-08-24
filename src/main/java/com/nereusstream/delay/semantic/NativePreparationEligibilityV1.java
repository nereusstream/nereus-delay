package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.AdapterMetadataV1;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import com.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.ScheduleIntentV1;
import com.nereusstream.delay.protocol.TargetPartitionHashInputV1;
import com.nereusstream.delay.protocol.TargetPartitionHashV1;
import com.nereusstream.delay.protocol.TimingCapabilityV1;
import java.util.Objects;

/**
 * Shared zero-I/O checks for a verified Pulsar AUTO_FAST candidate.
 *
 * <p>The provider decides which immutable capability snapshot is available;
 * this class verifies that the snapshot can actually describe the current
 * intent and the already-frozen managed identity.  It deliberately does not
 * resolve credentials, contact Oxia, or probe a Broker.</p>
 */
public final class NativePreparationEligibilityV1 {
    private NativePreparationEligibilityV1() {}

    public static void require(
            final AuthenticatedTenantContext context,
            final RouteSnapshotV1 managedRoute,
            final ScheduleIntentV1 intent,
            final PreparedCommand managedCommand,
            final NativePreparationSnapshotV1 candidate,
            final TrustedTimeSnapshot trustedTime) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(managedRoute, "managedRoute");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(trustedTime, "trustedTime");

        managedRoute.requireUsableForNewSchedule(
                context.authenticatedTenantScopeHash(), context.tenantRoutingScope(), trustedTime.epochMs());
        if (managedRoute.ingress().adapterKind() != AdapterKindV1.PULSAR
                || intent.adapterMetadata().kind() != AdapterMetadataV1.Kind.PULSAR
                || !intent.hasInlinePayload()
                || managedCommand != null
                        && !managedCommand.shardId().routeIncarnation().equals(managedRoute.routeIncarnation())) {
            throw new IllegalArgumentException("native candidate is not eligible for the managed Route");
        }

        final ProfileSemanticEnvelopeV1 destination = candidate.destination();
        final ProfileSemanticEnvelopeV1 capability = candidate.capability();
        if (destination.profileKind() != ProfileKindV1.DESTINATION
                || capability.profileKind() != ProfileKindV1.DELIVERY_CAPABILITY
                || !destination.ref().equals(intent.profile())) {
            throw new IllegalArgumentException("native candidate Profile does not match the Schedule");
        }
        if (!(destination.body() instanceof DestinationProfileSemanticV1 destinationBody)
                || !(capability.body() instanceof DeliveryCapabilitySemanticV1 capabilityBody)) {
            throw new IllegalArgumentException("native candidate Profile body is invalid");
        }
        if (destinationBody.adapterKind() != AdapterKindV1.PULSAR
                || capabilityBody.adapterKind() != AdapterKindV1.PULSAR
                || !TimingCapabilityV1.includes(
                        capabilityBody.timingCapabilityBits(), TimingCapabilityV1.PULSAR_AUTO_FAST)
                || !destinationBody.deliveryCapability().equals(capability.ref())
                || !destinationBody
                        .targetResource()
                        .equals(com.nereusstream.delay.protocol.BrokerResourceIdentityV1.pulsar(candidate.target()))) {
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
                                || TargetPartitionHashV1.partition(
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
                || candidate.brokerDeliverAtEpochMs() < intent.deliverAtEpochMs()
                || candidate.brokerDeliverAtEpochMs() > intent.expireAtEpochMs()
                || candidate.brokerDeliverAtEpochMs() > managedRoute.validUntilEpochMs()) {
            throw new IllegalArgumentException("native capability snapshot is stale or mismatched");
        }
        final long targetClockDelta;
        try {
            targetClockDelta = Math.subtractExact(candidate.brokerDeliverAtEpochMs(), intent.deliverAtEpochMs());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("native broker delivery time overflow", overflow);
        }
        if (targetClockDelta > destinationBody.targetClockAheadBoundMs()) {
            throw new IllegalArgumentException("native broker delivery time exceeds the Profile clock bound");
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
            final ScheduleIntentV1 intent,
            final PreparedCommand managedCommand,
            final TargetPartitionHashInputV1 hashInput) {
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
