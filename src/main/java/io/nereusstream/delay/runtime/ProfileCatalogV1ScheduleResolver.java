package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;

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
        requireDestinationProfile(intent.profile());
        return delegate.resolveSchedule(shardId, messageId, intent, sourcePosition);
    }

    @Override
    public ResolvedPrepare resolvePrepare(final ShardId shardId, final DelayMessageId messageId,
                                          final PrepareLargeScheduleBodyV1 body,
                                          final SourcePosition sourcePosition) {
        requireDestinationProfile(body.intentWithoutPayload().profile());
        return delegate.resolvePrepare(shardId, messageId, body, sourcePosition);
    }

    private void requireDestinationProfile(final ProfileRefV1 reference) {
        if (reference.profileKind() != ProfileKindV1.DESTINATION) {
            throw unavailable("V1 Schedule requires a Destination Profile");
        }
        final ProfileSemanticEnvelopeV1 semantic = profileCatalog.resolve(reference);
        final CredentialBindingHeadV1 head = profileCatalog.resolveHead(reference);
        if (semantic == null || semantic.profileKind() != ProfileKindV1.DESTINATION
                || !semantic.ref().equals(reference) || head == null || !head.profile().equals(reference)) {
            throw unavailable("Destination Profile semantic or credential Head is unavailable");
        }
    }

    private static V1CommandResolutionException unavailable(final String message) {
        return new V1CommandResolutionException(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, message);
    }
}
