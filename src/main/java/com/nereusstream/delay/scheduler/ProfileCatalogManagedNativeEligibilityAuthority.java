package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.CanonicalLaneTuple;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicyScope;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.semantic.HandoffPolicyAuthority;
import com.nereusstream.delay.semantic.HandoffPolicyTrustStore;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Production-shaped current-policy resolver for Managed Pulsar handoff.
 *
 * <p>The durable native index contains only the immutable maximum-lead boundary. This authority resolves the
 * source-pinned Profiles, derives the exact policy scope, verifies the current Oxia publication through historical
 * trust, and returns a process-local decision. Missing or untrusted live state disables only the native optimization;
 * malformed durable schedule identity remains an integrity failure.</p>
 */
public final class ProfileCatalogManagedNativeEligibilityAuthority
        implements ManagedNativeEligibilityAuthority, ManagedHandoffAdmissionAuthority {
    private final ProfileCatalog profiles;
    private final HandoffPolicyAuthority policies;
    private final HandoffPolicyTrustStore trustStore;
    private final ArtifactGenerationSet artifacts;
    private final Supplier<SourcePosition> trustPosition;

    public ProfileCatalogManagedNativeEligibilityAuthority(
            final ProfileCatalog profiles,
            final HandoffPolicyAuthority policies,
            final HandoffPolicyTrustStore trustStore,
            final ArtifactGenerationSet artifacts,
            final Supplier<SourcePosition> trustPosition) {
        this.profiles = Objects.requireNonNull(profiles, "profiles");
        this.policies = Objects.requireNonNull(policies, "policies");
        this.trustStore = Objects.requireNonNull(trustStore, "trustStore");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.trustPosition = Objects.requireNonNull(trustPosition, "trustPosition");
    }

    @Override
    public HandoffEligibilityResolver.Decision resolve(
            final MessageRecord message, final ScheduleBinding binding, final TrustedUtcIntervalEvidence trustedTime) {
        final MessageRecord exactMessage = Objects.requireNonNull(message, "message");
        final ScheduleBinding exactBinding = Objects.requireNonNull(binding, "binding");
        final CanonicalLaneTuple.Projection lane = CanonicalLaneTuple.project(exactBinding.canonicalLaneTuple());
        if (!exactBinding
                        .delayMessageId()
                        .routingId()
                        .shardId()
                        .equals(SourcePositionCodec.decode(exactMessage.scheduleSourcePosition())
                                .shardId())
                || !exactBinding.laneId().equals(exactMessage.laneId())
                || lane.targetResource().kind() != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.PULSAR
                || exactMessage.earliestNativeCandidateAtEpochMs() >= exactMessage.deliverAtEpochMs()) {
            throw new IllegalStateException("durable native candidate identity is inconsistent");
        }
        final ResolvedProfiles resolved;
        try {
            resolved = resolveProfiles(lane);
        } catch (RuntimeException unavailable) {
            return ordinary(exactMessage, trustedTime, null, null, false);
        }
        if (resolved == null) {
            return ordinary(exactMessage, trustedTime, null, null, false);
        }
        requireStaticBoundary(exactMessage, resolved.destination());
        final int scopePathBits = exactMessage.nativeDeliveryPolicy().allowsAutoFast()
                ? HandoffPath.VALID_MASK
                : HandoffPath.MANAGED_HANDOFF;
        final byte[] scope = HandoffPolicyScope.digest(
                lane.tenantRouteScopeDigest(),
                lane.destinationProfile(),
                lane.capabilityProfile(),
                lane.targetResource(),
                lane.physicalPartition(),
                exactMessage.orderingMode(),
                scopePathBits,
                artifacts);
        final Optional<HandoffPolicyAuthority.Publication> current;
        try {
            current = policies.current(scope);
        } catch (RuntimeException unavailable) {
            return ordinary(exactMessage, trustedTime, resolved.destination(), resolved.capability(), false);
        }
        if (current.isEmpty()) {
            return ordinary(exactMessage, trustedTime, resolved.destination(), resolved.capability(), false);
        }
        final HandoffPolicyAuthority.Publication publication = current.orElseThrow();
        final HandoffPolicySnapshot snapshot = publication.head().snapshot();
        try {
            final SourcePosition position = Objects.requireNonNull(trustPosition.get(), "policy trust position");
            trustStore.requireTrusted(snapshot, scope, artifacts.setDigest(), position);
            snapshot.requireLeadAtMost(resolved.destination().handoffLeadMs());
        } catch (RuntimeException unavailableOrUntrusted) {
            return ordinary(exactMessage, trustedTime, resolved.destination(), resolved.capability(), false);
        }
        return HandoffEligibilityResolver.resolve(
                input(exactMessage, trustedTime, resolved.destination(), resolved.capability(), snapshot, true),
                publication);
    }

    /**
     * Rereads the current Oxia head on both sides of validation and returns
     * only the exact signed snapshot referenced by the Claim. Any drift,
     * unavailable authority, or mismatched source-bound input fails closed
     * before a Publish Admission can be constructed.
     */
    @Override
    public HandoffPolicySnapshot freezeCurrent(
            final ClaimMaterialization materialization,
            final ScheduleBinding binding,
            final TrustedUtcIntervalEvidence decisionTime) {
        final ClaimMaterialization exact = Objects.requireNonNull(materialization, "materialization");
        final ScheduleBinding exactBinding = Objects.requireNonNull(binding, "binding");
        final TrustedUtcIntervalEvidence exactTime = Objects.requireNonNull(decisionTime, "decisionTime");
        if (exact.legacyEncoding()
                || exact.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID
                || !exact.nativeDeliveryPolicy().allowsManagedHandoff()
                || exact.handoffPolicyHeadRef() == null
                || exact.actionAtEpochMs() >= exact.deliverAtEpochMs()
                || !exactBinding.delayMessageId().equals(exact.messageId())) {
            throw new IllegalArgumentException("Claim does not select Managed Pulsar native delivery");
        }
        exactBinding.requireClaimLaneProjection(exact);
        final CanonicalLaneTuple.Projection lane = CanonicalLaneTuple.project(exactBinding.canonicalLaneTuple());
        final CanonicalScheduleIntent intent = exactBinding.intent();
        if (intent.legacyPolicyDefault()
                || intent.deliveryMode() != com.nereusstream.delay.protocol.DeliveryMode.MANAGED
                || intent.nativeDeliveryPolicy() != exact.nativeDeliveryPolicy()
                || intent.orderingMode() != OrderingMode.BEST_EFFORT
                || !intent.profile().equals(exact.destinationProfile())
                || !intent.adapterMetadata().equals(exact.businessMetadata())
                || !Objects.equals(intent.eventTimeEpochMs(), exact.eventTimeEpochMs())
                || intent.deliverAtEpochMs() != exact.deliverAtEpochMs()
                || intent.expireAtEpochMs() != exact.expireAtEpochMs()) {
            throw new IllegalArgumentException("Claim differs from its source-bound native Schedule intent");
        }
        final ResolvedProfiles resolved = Objects.requireNonNull(
                resolveProfiles(lane), "Managed Handoff Profiles are unavailable or inconsistent");
        if (!TimingCapability.includes(
                        resolved.capability().timingCapabilityBits(), TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF)
                || resolved.destination().handoffLeadMs() <= 0) {
            throw new IllegalArgumentException("Managed Handoff capability is unavailable");
        }
        final int scopePathBits =
                exact.nativeDeliveryPolicy().allowsAutoFast() ? HandoffPath.VALID_MASK : HandoffPath.MANAGED_HANDOFF;
        final byte[] scope = HandoffPolicyScope.digest(
                lane.tenantRouteScopeDigest(),
                lane.destinationProfile(),
                lane.capabilityProfile(),
                lane.targetResource(),
                lane.physicalPartition(),
                intent.orderingMode(),
                scopePathBits,
                artifacts);
        final HandoffPolicyAuthority.Publication first = policies.requireCurrent(scope);
        if (!first.head().ref(first.oxiaVersion()).equals(exact.handoffPolicyHeadRef())) {
            throw new IllegalArgumentException("Claim policy head is no longer current");
        }
        final HandoffPolicySnapshot snapshot = first.head().snapshot();
        final SourcePosition position = Objects.requireNonNull(trustPosition.get(), "policy trust position");
        trustStore.requireTrusted(snapshot, scope, artifacts.setDigest(), position);
        snapshot.requireActiveAt(exactTime);
        snapshot.requireLeadAtMost(resolved.destination().handoffLeadMs());
        if (snapshot.mode() != HandoffPolicyMode.ENABLED
                || !snapshot.allows(HandoffPath.MANAGED_HANDOFF)
                || snapshot.effectiveLeadMs() <= 0
                || exactTime.earliestEpochMs() < exact.actionAtEpochMs()
                || exactTime.latestEpochMs() >= exact.deliverAtEpochMs()) {
            throw new IllegalArgumentException("Managed Handoff lease is not valid for this Admission interval");
        }
        final long expectedActionAt;
        try {
            expectedActionAt = Math.subtractExact(exact.deliverAtEpochMs(), snapshot.effectiveLeadMs());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Managed Handoff action time overflows", overflow);
        }
        if (expectedActionAt < 0 || exact.actionAtEpochMs() != expectedActionAt) {
            throw new IllegalArgumentException("Claim action time does not match the current signed lead");
        }
        final HandoffPolicyAuthority.Publication second = policies.requireCurrent(scope);
        if (!first.sameHead(second)) {
            throw new IllegalStateException("handoff policy head changed during Admission validation");
        }
        return snapshot;
    }

    private ResolvedProfiles resolveProfiles(final CanonicalLaneTuple.Projection lane) {
        final ProfileSemanticEnvelope destinationEnvelope = profiles.resolve(lane.destinationProfile());
        final ProfileSemanticEnvelope capabilityEnvelope = profiles.resolve(lane.capabilityProfile());
        if (destinationEnvelope == null
                || capabilityEnvelope == null
                || destinationEnvelope.profileKind() != ProfileKind.DESTINATION
                || capabilityEnvelope.profileKind() != ProfileKind.DELIVERY_CAPABILITY
                || !destinationEnvelope.ref().equals(lane.destinationProfile())
                || !capabilityEnvelope.ref().equals(lane.capabilityProfile())
                || !(destinationEnvelope.body() instanceof DestinationProfileSemantic destination)
                || !(capabilityEnvelope.body() instanceof DeliveryCapabilitySemantic capability)
                || destination.adapterKind() != AdapterKind.PULSAR
                || capability.adapterKind() != AdapterKind.PULSAR
                || !destination.deliveryCapability().equals(lane.capabilityProfile())
                || !destination.targetResource().equals(lane.targetResource())) {
            return null;
        }
        return new ResolvedProfiles(destination, capability);
    }

    private static void requireStaticBoundary(
            final MessageRecord message, final DestinationProfileSemantic destination) {
        final long expected;
        try {
            expected = Math.subtractExact(message.deliverAtEpochMs(), destination.handoffLeadMs());
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("native candidate boundary overflows", overflow);
        }
        if (destination.handoffLeadMs() <= 0
                || expected < 0
                || message.earliestNativeCandidateAtEpochMs() != expected) {
            throw new IllegalStateException("native candidate does not match the pinned Profile maximum lead");
        }
    }

    private static HandoffEligibilityResolver.Decision ordinary(
            final MessageRecord message,
            final TrustedUtcIntervalEvidence trustedTime,
            final DestinationProfileSemantic destination,
            final DeliveryCapabilitySemantic capability,
            final boolean capabilityAvailable) {
        return HandoffEligibilityResolver.resolve(
                input(message, trustedTime, destination, capability, null, capabilityAvailable));
    }

    private static HandoffEligibilityResolver.Input input(
            final MessageRecord message,
            final TrustedUtcIntervalEvidence trustedTime,
            final DestinationProfileSemantic destination,
            final DeliveryCapabilitySemantic capability,
            final HandoffPolicySnapshot snapshot,
            final boolean trustAvailable) {
        final boolean initialAttempt = message.runtimeIndex().timeline() != null
                && message.runtimeIndex().timeline().workKind() == TimelineWorkKind.INITIAL_SCHEDULE
                && message.runtimeIndex().timeline().candidateAttemptNo() == 1
                && message.runtimeIndex().admissionsUsed() == 0;
        final boolean capabilityAvailable = trustAvailable
                && capability != null
                && TimingCapability.includes(
                        capability.timingCapabilityBits(), TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF);
        return new HandoffEligibilityResolver.Input(
                AdapterKind.PULSAR,
                message.nativeDeliveryPolicy(),
                message.orderingMode(),
                initialAttempt,
                capabilityAvailable,
                destination,
                capability,
                snapshot,
                message.deliverAtEpochMs(),
                message.retryEligibilityAtEpochMs(),
                trustedTime);
    }

    private record ResolvedProfiles(DestinationProfileSemantic destination, DeliveryCapabilitySemantic capability) {}
}
