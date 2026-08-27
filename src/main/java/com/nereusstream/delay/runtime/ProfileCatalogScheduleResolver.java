package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TimingCapability;
import java.util.Objects;

/**
 * Local resolver decorator that requires an exact published Destination
 * Profile and credential Head before delegating Lane/payload resolution.
 *
 * <p>Profile first-binding activation/deprecation is intentionally left to the
 * shard's source-ordered {@code ProfileBindingControlState}; this decorator
 * only prevents a resolver from deriving a route from an unknown or
 * credential-less Profile snapshot.</p>
 */
public final class ProfileCatalogScheduleResolver implements ScheduleResolver {
    private final ScheduleResolver delegate;
    private final ProfileCatalog profileCatalog;

    public ProfileCatalogScheduleResolver(final ScheduleResolver delegate, final ProfileCatalog profileCatalog) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (delegate instanceof ProfileCatalogScheduleResolver) {
            throw new IllegalArgumentException("Profile catalog Schedule resolver must not be nested");
        }
        this.profileCatalog = Objects.requireNonNull(profileCatalog, "profileCatalog");
    }

    /** Requires the shard's later Admission/recovery lookups to use the exact same catalog authority. */
    void requireProfileCatalog(final ProfileCatalog expected) {
        if (profileCatalog != Objects.requireNonNull(expected, "expected")) {
            throw new IllegalArgumentException("Profile catalog Schedule resolver is bound to another Profile catalog");
        }
    }

    @Override
    public ResolvedSchedule resolveSchedule(
            final ShardId shardId,
            final DelayMessageId messageId,
            final CanonicalScheduleIntent intent,
            final SourcePosition sourcePosition) {
        final ResolvedProfileSemantics semantics = requireDestinationProfile(intent.profile());
        requireIntentProfileLimits(intent, payloadLength(intent), semantics.destination());
        final ResolvedSchedule resolved = Objects.requireNonNull(
                delegate.resolveSchedule(shardId, messageId, intent, sourcePosition), "resolved Schedule projection");
        final long expectedActionAt = expectedActionAt(
                intent.deliverAtEpochMs(),
                intent.nativeDeliveryPolicy(),
                intent.orderingMode(),
                semantics.destination(),
                semantics.capability(),
                intent.legacyPolicyDefault());
        if (resolved.actionAtEpochMs() != null && resolved.actionAtEpochMs() != expectedActionAt) {
            throw new CommandResolutionException(
                    StableCode.INVALID_COMMAND,
                    "resolved Schedule actionAt does not match the immutable Destination Profile");
        }
        return new ResolvedSchedule(
                resolved.laneId(),
                resolved.canonicalLaneTuple(),
                resolved.inlinePayload(),
                resolved.payloadReference(),
                expectedActionAt);
    }

    @Override
    public ResolvedPrepare resolvePrepare(
            final ShardId shardId,
            final DelayMessageId messageId,
            final PrepareLargeScheduleBody body,
            final SourcePosition sourcePosition) {
        final ResolvedProfileSemantics semantics =
                requireDestinationProfile(body.intentWithoutPayload().profile());
        requireIntentProfileLimits(body.intentWithoutPayload(), body.expectedPayloadLength(), semantics.destination());
        // Prepare reserves quota and authorizes a potentially expensive object
        // upload. Reject an impossible certified handoff before either action;
        // Commit will re-derive the same boundary from the durable binding.
        expectedActionAt(
                body.intentWithoutPayload().deliverAtEpochMs(),
                body.intentWithoutPayload().nativeDeliveryPolicy(),
                body.intentWithoutPayload().orderingMode(),
                semantics.destination(),
                semantics.capability(),
                body.intentWithoutPayload().legacyPolicyDefault());
        return delegate.resolvePrepare(shardId, messageId, body, sourcePosition);
    }

    private ResolvedProfileSemantics requireDestinationProfile(final ProfileRef reference) {
        if (reference.profileKind() != ProfileKind.DESTINATION) {
            throw unavailable(" Schedule requires a Destination Profile");
        }
        final ProfileSemanticEnvelope semantic = profileCatalog.resolve(reference);
        final CredentialBindingHead head = profileCatalog.resolveHead(reference);
        if (semantic == null
                || semantic.profileKind() != ProfileKind.DESTINATION
                || !semantic.ref().equals(reference)
                || head == null
                || !head.profile().equals(reference)) {
            throw unavailable("Destination Profile semantic or credential Head is unavailable");
        }
        if (!(semantic.body() instanceof DestinationProfileSemantic destination)) {
            throw unavailable("Destination Profile body is unavailable");
        }
        final ProfileRef capabilityReference = destination.deliveryCapability();
        final ProfileSemanticEnvelope capability = profileCatalog.resolve(capabilityReference);
        if (capability == null
                || capability.profileKind() != ProfileKind.DELIVERY_CAPABILITY
                || !capability.ref().equals(capabilityReference)
                || !(capability.body() instanceof DeliveryCapabilitySemantic deliveryCapability)
                || deliveryCapability.adapterKind() != destination.adapterKind()) {
            throw unavailable("Delivery Capability semantic or adapter binding is unavailable");
        }
        return new ResolvedProfileSemantics(destination, deliveryCapability);
    }

    private static void requireIntentProfileLimits(
            final CanonicalScheduleIntent intent,
            final long payloadLength,
            final DestinationProfileSemantic destination) {
        final boolean adapterMatches =
                switch (destination.adapterKind()) {
                    case KAFKA ->
                        intent.adapterMetadata().kind() == com.nereusstream.delay.protocol.AdapterMetadata.Kind.KAFKA;
                    case PULSAR ->
                        intent.adapterMetadata().kind() == com.nereusstream.delay.protocol.AdapterMetadata.Kind.PULSAR;
                };
        if (!adapterMatches) {
            throw new CommandResolutionException(
                    StableCode.INVALID_METADATA,
                    "Schedule metadata branch does not match the immutable Destination Profile");
        }
        if (intent.nativeDeliveryPolicy() != com.nereusstream.delay.protocol.NativeDeliveryPolicy.FORBID
                && (destination.adapterKind() != com.nereusstream.delay.protocol.AdapterKind.PULSAR
                        || intent.orderingMode() != com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT)) {
            throw new CommandResolutionException(
                    StableCode.ORDERING_CAPABILITY_UNAVAILABLE,
                    "native delivery requires a Pulsar BEST_EFFORT Schedule");
        }
        final int orderingBit =
                intent.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT ? 0x01 : 0x02;
        if ((destination.allowedOrderingModeBits() & orderingBit) == 0) {
            throw new CommandResolutionException(
                    StableCode.ORDERING_CAPABILITY_UNAVAILABLE,
                    "Schedule ordering mode is not allowed by the immutable Destination Profile");
        }
        if (payloadLength > destination.maxPayloadBytes()) {
            throw new CommandResolutionException(
                    StableCode.PAYLOAD_TOO_LARGE, "Schedule payload exceeds the immutable Destination Profile maximum");
        }
        if (intent.adapterMetadata().canonicalBytes().length > destination.maxAdapterMetadataBytes()) {
            throw new CommandResolutionException(
                    StableCode.INVALID_METADATA, "Schedule metadata exceeds the immutable Destination Profile maximum");
        }
    }

    private static long payloadLength(final CanonicalScheduleIntent intent) {
        return intent.hasInlinePayload()
                ? intent.inlinePayload().length
                : intent.committedPayload().length();
    }

    private record ResolvedProfileSemantics(
            DestinationProfileSemantic destination, DeliveryCapabilitySemantic capability) {}

    private static long expectedActionAt(
            final long deliverAt,
            final com.nereusstream.delay.protocol.NativeDeliveryPolicy policy,
            final com.nereusstream.delay.protocol.OrderingMode orderingMode,
            final DestinationProfileSemantic destination,
            final DeliveryCapabilitySemantic capability,
            final boolean legacyPolicyDefault) {
        final boolean legacyCertifiedHandoff = legacyPolicyDefault
                && policy == com.nereusstream.delay.protocol.NativeDeliveryPolicy.FORBID
                && orderingMode == com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                && destination.adapterKind() == com.nereusstream.delay.protocol.AdapterKind.PULSAR
                && destination.handoffLeadMs() > 0
                && capability != null
                && TimingCapability.includes(
                        capability.timingCapabilityBits(), TimingCapability.PULSAR_GUARDED_HANDOFF);
        final boolean nativeCertifiedHandoff = policy != com.nereusstream.delay.protocol.NativeDeliveryPolicy.FORBID
                && orderingMode == com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                && destination.adapterKind() == com.nereusstream.delay.protocol.AdapterKind.PULSAR
                && destination.handoffLeadMs() > 0
                && capability != null
                && TimingCapability.includes(
                        capability.timingCapabilityBits(), TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF);
        if (legacyCertifiedHandoff || nativeCertifiedHandoff) {
            try {
                final long actionAt = Math.subtractExact(deliverAt, destination.handoffLeadMs());
                if (actionAt < 0) {
                    throw new CommandResolutionException(
                            StableCode.INVALID_DELIVERY_WINDOW, "certified handoff actionAt underflows deliverAt");
                }
                return actionAt;
            } catch (ArithmeticException overflow) {
                throw new CommandResolutionException(
                        StableCode.INVALID_DELIVERY_WINDOW, "certified handoff actionAt arithmetic overflow");
            }
        }
        return deliverAt;
    }

    private static CommandResolutionException unavailable(final String message) {
        return new CommandResolutionException(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, message);
    }
}
