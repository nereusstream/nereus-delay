package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RouteSnapshot;
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
 * canonical profile and snapshot bytes and the issuer signature. Eligibility
 * is then a synchronized read over the local map and the caller's already
 * authenticated context. No method performs Oxia, credential, or Broker
 * I/O.</p>
 */
public final class VerifiedNativePreparationSnapshotCache implements NativePreparationSnapshotProvider {
    private final PublicKey issuerKey;
    private final Map<ProfileRef, List<NativePreparationSnapshot>> byDestination = new HashMap<>();

    public VerifiedNativePreparationSnapshotCache(final PublicKey issuerKey) {
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
    }

    /** Installs one exact issuer-verified candidate, replacing the same target partition snapshot. */
    public synchronized void install(final NativePreparationSnapshot candidate) {
        Objects.requireNonNull(candidate, "candidate");
        verifyCandidate(candidate);
        final ProfileRef destination = candidate.destination().ref();
        final List<NativePreparationSnapshot> next =
                new ArrayList<>(byDestination.getOrDefault(destination, List.of()));
        next.removeIf(existing -> existing.physicalPartition() == candidate.physicalPartition()
                && existing.target().equals(candidate.target()));
        next.add(candidate);
        next.sort(VerifiedNativePreparationSnapshotCache::compareCandidates);
        byDestination.put(destination, List.copyOf(next));
    }

    /** Replaces the cache atomically after verifying every candidate. */
    public synchronized void replaceAll(final Collection<NativePreparationSnapshot> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        final Map<ProfileRef, List<NativePreparationSnapshot>> replacement = new HashMap<>();
        for (NativePreparationSnapshot candidate : candidates) {
            verifyCandidate(candidate);
            replacement
                    .computeIfAbsent(candidate.destination().ref(), ignored -> new ArrayList<>())
                    .add(candidate);
        }
        final Map<ProfileRef, List<NativePreparationSnapshot>> immutable = new HashMap<>();
        for (Map.Entry<ProfileRef, List<NativePreparationSnapshot>> entry : replacement.entrySet()) {
            final List<NativePreparationSnapshot> values = new ArrayList<>(entry.getValue());
            values.sort(VerifiedNativePreparationSnapshotCache::compareCandidates);
            immutable.put(entry.getKey(), List.copyOf(values));
        }
        byDestination.clear();
        byDestination.putAll(immutable);
    }

    public synchronized void remove(final ProfileRef destination) {
        byDestination.remove(Objects.requireNonNull(destination, "destination"));
    }

    public synchronized void clear() {
        byDestination.clear();
    }

    @Override
    public Optional<NativePreparationSnapshot> eligibleFor(
            final AuthenticatedTenantContext context,
            final RouteSnapshot managedRoute,
            final CanonicalScheduleIntent intent,
            final TrustedTimeSnapshot trustedTime) {
        return select(context, managedRoute, intent, null, trustedTime);
    }

    @Override
    public synchronized Optional<NativePreparationSnapshot> eligibleFor(
            final AuthenticatedTenantContext context,
            final RouteSnapshot managedRoute,
            final CanonicalScheduleIntent intent,
            final PreparedCommand managedCommand,
            final TrustedTimeSnapshot trustedTime) {
        return select(context, managedRoute, intent, managedCommand, trustedTime);
    }

    private Optional<NativePreparationSnapshot> select(
            final AuthenticatedTenantContext context,
            final RouteSnapshot managedRoute,
            final CanonicalScheduleIntent intent,
            final PreparedCommand managedCommand,
            final TrustedTimeSnapshot trustedTime) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(managedRoute, "managedRoute");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(trustedTime, "trustedTime");
        final List<NativePreparationSnapshot> candidates;
        synchronized (this) {
            candidates = byDestination.getOrDefault(intent.profile(), List.of());
        }
        for (NativePreparationSnapshot candidate : candidates) {
            try {
                NativePreparationEligibility.require(
                        context, managedRoute, intent, managedCommand, candidate, trustedTime);
                return Optional.of(candidate);
            } catch (RuntimeException ignored) {
                // Native ineligibility is the managed fallback; it is not a
                // reason to make the already-valid managed command fail.
            }
        }
        return Optional.empty();
    }

    private void verifyCandidate(final NativePreparationSnapshot candidate) {
        Objects.requireNonNull(candidate, "candidate");
        final ProfileSemanticEnvelope destination =
                ProfileSemanticEnvelope.decode(candidate.destination().canonicalBytes());
        final ProfileSemanticEnvelope capability =
                ProfileSemanticEnvelope.decode(candidate.capability().canonicalBytes());
        final NativeCapabilitySnapshot snapshot =
                NativeCapabilitySnapshot.decode(candidate.capabilitySnapshot().canonicalBytes());
        if (!destination.equals(candidate.destination())
                || !capability.equals(candidate.capability())
                || !snapshot.equals(candidate.capabilitySnapshot())
                || !snapshot.verifySignature(issuerKey)) {
            throw new IllegalArgumentException("native candidate canonical or signature verification failed");
        }
    }

    private static int compareCandidates(final NativePreparationSnapshot left, final NativePreparationSnapshot right) {
        final int partition = Integer.compareUnsigned(left.physicalPartition(), right.physicalPartition());
        if (partition != 0) {
            return partition;
        }
        return Arrays.compareUnsigned(
                left.capabilitySnapshot().snapshotDigest(),
                right.capabilitySnapshot().snapshotDigest());
    }
}
