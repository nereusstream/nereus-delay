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
        this.profileCatalog = Objects.requireNonNull(profileCatalog, "profileCatalog");
    }

    @Override
    public ResolvedSchedule resolveSchedule(final ShardId shardId, final DelayMessageId messageId,
                                            final ScheduleIntentV1 intent,
                                            final SourcePosition sourcePosition) {
        final DestinationProfileSemanticV1 destination = requireDestinationProfile(intent.profile());
        final ProfileSemanticEnvelopeV1 capabilityEnvelope = profileCatalog.resolve(destination.deliveryCapability());
        final DeliveryCapabilitySemanticV1 capability = capabilityEnvelope == null
                || !(capabilityEnvelope.body() instanceof DeliveryCapabilitySemanticV1 value)
                ? null : value;
        final ResolvedSchedule resolved = Objects.requireNonNull(
                delegate.resolveSchedule(shardId, messageId, intent, sourcePosition),
                "resolved Schedule projection");
        final long expectedActionAt = expectedActionAt(intent.deliverAtEpochMs(), destination, capability);
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
        requireDestinationProfile(body.intentWithoutPayload().profile());
        return delegate.resolvePrepare(shardId, messageId, body, sourcePosition);
    }

    private DestinationProfileSemanticV1 requireDestinationProfile(final ProfileRefV1 reference) {
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
        return destination;
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
