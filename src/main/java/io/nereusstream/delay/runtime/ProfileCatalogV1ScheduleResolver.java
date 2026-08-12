package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.TimingCapabilityV1;

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
public final class ProfileCatalogV1ScheduleResolver implements V1ScheduleResolver {
    private final V1ScheduleResolver delegate;
    private final ProfileCatalog profileCatalog;

    public ProfileCatalogV1ScheduleResolver(final V1ScheduleResolver delegate,
                                            final ProfileCatalog profileCatalog) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (delegate instanceof ProfileCatalogV1ScheduleResolver) {
            throw new IllegalArgumentException("Profile catalog Schedule resolver must not be nested");
        }
        this.profileCatalog = Objects.requireNonNull(profileCatalog, "profileCatalog");
    }

    /** Requires the shard's later Admission/recovery lookups to use the exact same catalog authority. */
    void requireProfileCatalog(final ProfileCatalog expected) {
        if (profileCatalog != Objects.requireNonNull(expected, "expected")) {
            throw new IllegalArgumentException(
                    "Profile catalog Schedule resolver is bound to another Profile catalog");
        }
    }

    @Override
    public ResolvedSchedule resolveSchedule(final ShardId shardId, final DelayMessageId messageId,
                                            final ScheduleIntentV1 intent,
                                            final SourcePosition sourcePosition) {
        final ResolvedProfileSemantics semantics = requireDestinationProfile(intent.profile());
        requireIntentProfileLimits(intent, payloadLength(intent), semantics.destination());
        final ResolvedSchedule resolved = Objects.requireNonNull(
                delegate.resolveSchedule(shardId, messageId, intent, sourcePosition),
                "resolved Schedule projection");
        final long expectedActionAt = expectedActionAt(intent.deliverAtEpochMs(), semantics.destination(),
                semantics.capability());
        if (resolved.actionAtEpochMs() != null && resolved.actionAtEpochMs() != expectedActionAt) {
            throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                    "resolved Schedule actionAt does not match the immutable Destination Profile");
        }
        return new ResolvedSchedule(resolved.laneId(), resolved.canonicalLaneTuple(), resolved.inlinePayload(),
                resolved.payloadReference(), expectedActionAt);
    }

    @Override
    public ResolvedPrepare resolvePrepare(final ShardId shardId, final DelayMessageId messageId,
                                          final PrepareLargeScheduleBodyV1 body,
                                          final SourcePosition sourcePosition) {
        final ResolvedProfileSemantics semantics = requireDestinationProfile(
                body.intentWithoutPayload().profile());
        requireIntentProfileLimits(body.intentWithoutPayload(), body.expectedPayloadLength(),
                semantics.destination());
        // Prepare reserves quota and authorizes a potentially expensive object
        // upload. Reject an impossible certified handoff before either action;
        // Commit will re-derive the same boundary from the durable binding.
        expectedActionAt(body.intentWithoutPayload().deliverAtEpochMs(), semantics.destination(),
                semantics.capability());
        return delegate.resolvePrepare(shardId, messageId, body, sourcePosition);
    }

    private ResolvedProfileSemantics requireDestinationProfile(final ProfileRefV1 reference) {
        if (reference.profileKind() != ProfileKindV1.DESTINATION) {
            throw unavailable("V1 Schedule requires a Destination Profile");
        }
        final ProfileSemanticEnvelopeV1 semantic = profileCatalog.resolve(reference);
        final CredentialBindingHeadV1 head = profileCatalog.resolveHead(reference);
        if (semantic == null || semantic.profileKind() != ProfileKindV1.DESTINATION
                || !semantic.ref().equals(reference) || head == null || !head.profile().equals(reference)) {
            throw unavailable("Destination Profile semantic or credential Head is unavailable");
        }
        if (!(semantic.body() instanceof DestinationProfileSemanticV1 destination)) {
            throw unavailable("Destination Profile body is unavailable");
        }
        final ProfileRefV1 capabilityReference = destination.deliveryCapability();
        final ProfileSemanticEnvelopeV1 capability = profileCatalog.resolve(capabilityReference);
        if (capability == null || capability.profileKind() != ProfileKindV1.DELIVERY_CAPABILITY
                || !capability.ref().equals(capabilityReference)
                || !(capability.body() instanceof DeliveryCapabilitySemanticV1 deliveryCapability)
                || deliveryCapability.adapterKind() != destination.adapterKind()) {
            throw unavailable("Delivery Capability semantic or adapter binding is unavailable");
        }
        return new ResolvedProfileSemantics(destination, deliveryCapability);
    }

    private static void requireIntentProfileLimits(final ScheduleIntentV1 intent, final long payloadLength,
                                                   final DestinationProfileSemanticV1 destination) {
        final boolean adapterMatches = switch (destination.adapterKind()) {
            case KAFKA -> intent.adapterMetadata().kind()
                    == io.nereusstream.delay.protocol.AdapterMetadataV1.Kind.KAFKA;
            case PULSAR -> intent.adapterMetadata().kind()
                    == io.nereusstream.delay.protocol.AdapterMetadataV1.Kind.PULSAR;
        };
        if (!adapterMatches) {
            throw new V1CommandResolutionException(StableCode.INVALID_METADATA,
                    "Schedule metadata branch does not match the immutable Destination Profile");
        }
        final int orderingBit = intent.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                ? 0x01 : 0x02;
        if ((destination.allowedOrderingModeBits() & orderingBit) == 0) {
            throw new V1CommandResolutionException(StableCode.ORDERING_CAPABILITY_UNAVAILABLE,
                    "Schedule ordering mode is not allowed by the immutable Destination Profile");
        }
        if (payloadLength > destination.maxPayloadBytes()) {
            throw new V1CommandResolutionException(StableCode.PAYLOAD_TOO_LARGE,
                    "Schedule payload exceeds the immutable Destination Profile maximum");
        }
        if (intent.adapterMetadata().canonicalBytes().length > destination.maxAdapterMetadataBytes()) {
            throw new V1CommandResolutionException(StableCode.INVALID_METADATA,
                    "Schedule metadata exceeds the immutable Destination Profile maximum");
        }
    }

    private static long payloadLength(final ScheduleIntentV1 intent) {
        return intent.hasInlinePayload() ? intent.inlinePayload().length : intent.committedPayload().length();
    }

    private record ResolvedProfileSemantics(DestinationProfileSemanticV1 destination,
                                            DeliveryCapabilitySemanticV1 capability) {
    }

    private static long expectedActionAt(final long deliverAt,
                                         final DestinationProfileSemanticV1 destination,
                                         final DeliveryCapabilitySemanticV1 capability) {
        if (destination.adapterKind() == io.nereusstream.delay.protocol.AdapterKindV1.PULSAR
                && destination.handoffLeadMs() > 0
                && capability != null
                && TimingCapabilityV1.includes(capability.timingCapabilityBits(),
                TimingCapabilityV1.PULSAR_GUARDED_HANDOFF)) {
            try {
                final long actionAt = Math.subtractExact(deliverAt, destination.handoffLeadMs());
                if (actionAt < 0) {
                    throw new V1CommandResolutionException(StableCode.INVALID_DELIVERY_WINDOW,
                            "certified handoff actionAt underflows deliverAt");
                }
                return actionAt;
            } catch (ArithmeticException overflow) {
                throw new V1CommandResolutionException(StableCode.INVALID_DELIVERY_WINDOW,
                        "certified handoff actionAt arithmetic overflow");
            }
        }
        return deliverAt;
    }

    private static V1CommandResolutionException unavailable(final String message) {
        return new V1CommandResolutionException(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, message);
    }
}
