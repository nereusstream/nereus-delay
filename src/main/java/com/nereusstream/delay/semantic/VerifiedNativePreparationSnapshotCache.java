package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.ScheduleIntentV1;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable-view cache for issuer-verified native capability snapshots.
 *
 * <p>Installation is the authority boundary: the cache verifies the exact
 * canonical profile and snapshot bytes and the issuer signature.  Eligibility
 * is then a synchronized read over the local map and the caller's already
 * authenticated context.  No method performs Oxia, credential, or Broker
 * I/O.</p>
 */
public final class VerifiedNativePreparationSnapshotCache implements NativePreparationSnapshotProvider {
    private final PublicKey issuerKey;
    private final Map<ProfileRefV1, List<NativePreparationSnapshotV1>> byDestination = new HashMap<>();

    public VerifiedNativePreparationSnapshotCache(final PublicKey issuerKey) {
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
    }

    /** Installs one exact issuer-verified candidate, replacing the same target partition snapshot. */
    public synchronized void install(final NativePreparationSnapshotV1 candidate) {
        Objects.requireNonNull(candidate, "candidate");
        verifyCandidate(candidate);
        final ProfileRefV1 destination = candidate.destination().ref();
        final List<NativePreparationSnapshotV1> next =
                new ArrayList<>(byDestination.getOrDefault(destination, List.of()));
        next.removeIf(existing -> existing.physicalPartition() == candidate.physicalPartition()
                && existing.target().equals(candidate.target()));
        next.add(candidate);
        next.sort(VerifiedNativePreparationSnapshotCache::compareCandidates);
        byDestination.put(destination, List.copyOf(next));
    }

    /** Replaces the cache atomically after verifying every candidate. */
    public synchronized void replaceAll(final Collection<NativePreparationSnapshotV1> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        final Map<ProfileRefV1, List<NativePreparationSnapshotV1>> replacement = new HashMap<>();
        for (NativePreparationSnapshotV1 candidate : candidates) {
            verifyCandidate(candidate);
            replacement
                    .computeIfAbsent(candidate.destination().ref(), ignored -> new ArrayList<>())
                    .add(candidate);
        }
        final Map<ProfileRefV1, List<NativePreparationSnapshotV1>> immutable = new HashMap<>();
        for (Map.Entry<ProfileRefV1, List<NativePreparationSnapshotV1>> entry : replacement.entrySet()) {
            final List<NativePreparationSnapshotV1> values = new ArrayList<>(entry.getValue());
            values.sort(VerifiedNativePreparationSnapshotCache::compareCandidates);
            immutable.put(entry.getKey(), List.copyOf(values));
        }
        byDestination.clear();
        byDestination.putAll(immutable);
    }

    public synchronized void remove(final ProfileRefV1 destination) {
        byDestination.remove(Objects.requireNonNull(destination, "destination"));
    }

    public synchronized void clear() {
        byDestination.clear();
    }

    @Override
    public Optional<NativePreparationSnapshotV1> eligibleFor(
            final AuthenticatedTenantContext context,
            final RouteSnapshotV1 managedRoute,
            final ScheduleIntentV1 intent,
            final TrustedTimeSnapshot trustedTime) {
        return select(context, managedRoute, intent, null, trustedTime);
    }

    @Override
    public synchronized Optional<NativePreparationSnapshotV1> eligibleFor(
            final AuthenticatedTenantContext context,
            final RouteSnapshotV1 managedRoute,
            final ScheduleIntentV1 intent,
            final PreparedCommand managedCommand,
            final TrustedTimeSnapshot trustedTime) {
        return select(context, managedRoute, intent, managedCommand, trustedTime);
    }

    private Optional<NativePreparationSnapshotV1> select(
            final AuthenticatedTenantContext context,
            final RouteSnapshotV1 managedRoute,
            final ScheduleIntentV1 intent,
            final PreparedCommand managedCommand,
            final TrustedTimeSnapshot trustedTime) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(managedRoute, "managedRoute");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(trustedTime, "trustedTime");
        final List<NativePreparationSnapshotV1> candidates;
        synchronized (this) {
            candidates = byDestination.getOrDefault(intent.profile(), List.of());
        }
        for (NativePreparationSnapshotV1 candidate : candidates) {
            try {
                NativePreparationEligibilityV1.require(
                        context, managedRoute, intent, managedCommand, candidate, trustedTime);
                return Optional.of(candidate);
            } catch (RuntimeException ignored) {
                // Native ineligibility is the managed fallback; it is not a
                // reason to make the already-valid managed command fail.
            }
        }
        return Optional.empty();
    }

    private void verifyCandidate(final NativePreparationSnapshotV1 candidate) {
        Objects.requireNonNull(candidate, "candidate");
        final ProfileSemanticEnvelopeV1 destination =
                ProfileSemanticEnvelopeV1.decode(candidate.destination().canonicalBytes());
        final ProfileSemanticEnvelopeV1 capability =
                ProfileSemanticEnvelopeV1.decode(candidate.capability().canonicalBytes());
        final NativeCapabilitySnapshotV1 snapshot =
                NativeCapabilitySnapshotV1.decode(candidate.capabilitySnapshot().canonicalBytes());
        if (!destination.equals(candidate.destination())
                || !capability.equals(candidate.capability())
                || !snapshot.equals(candidate.capabilitySnapshot())
                || !snapshot.verifySignature(issuerKey)) {
            throw new IllegalArgumentException("native candidate canonical or signature verification failed");
        }
    }

    private static int compareCandidates(
            final NativePreparationSnapshotV1 left, final NativePreparationSnapshotV1 right) {
        final int partition = Integer.compareUnsigned(left.physicalPartition(), right.physicalPartition());
        if (partition != 0) {
            return partition;
        }
        return Arrays.compareUnsigned(
                left.capabilitySnapshot().snapshotDigest(),
                right.capabilitySnapshot().snapshotDigest());
    }
}
