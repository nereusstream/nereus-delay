package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.ProfileCatalog;
import java.util.Objects;

/** Immutable Profile/binding checks shared by actual Target SDK opening and retained receipt readers. */
public final class TargetDestinationCredentialChecks {
    private TargetDestinationCredentialChecks() {}

    /** Returns the exact attested binding; this does not prove which private material the SDK loaded. */
    public static CredentialBinding requireBinding(TargetChannelIdentity channel, CanonicalTargetPartition physical,
            ProfileCatalog profiles, CredentialAttestationTrustSet trust) {
        final var lease = Objects.requireNonNull(channel, "channel").credentialLease();
        final var profile = Objects.requireNonNull(profiles.resolve(lease.profile()), "original destination Profile");
        if (!profile.ref().equals(lease.profile()) || profile.profileKind() != ProfileKind.DESTINATION
                || !(profile.body() instanceof DestinationProfileSemantic destination)) {
            throw new IllegalStateException("Target channel lacks its exact Destination Profile");
        }
        if (!destination.targetResource().equals(physical.resource())
                || physical.physicalPartition() >= Integer.toUnsignedLong(destination.targetPartitionCount())
                || destination.targetPartitionPolicy() == com.nereusstream.delay.protocol.TargetPartitionPolicy.EXPLICIT_ONLY
                    && !destination.allowedExplicitPartitions().contains((int) physical.physicalPartition())) {
            throw new IllegalStateException("Target SDK resource differs from its Destination Profile");
        }
        final var binding = Objects.requireNonNull(profiles.resolveBinding(profile.ref(), lease.secretGeneration()),
                "original exact credential binding");
        lease.requireBinding(binding);
        final var attestation = binding.equivalenceAttestation();
        attestation.requireAuthorizationScopeDigest(destination.credentialAuthorizationScopeDigest());
        if (lease.validUntilEpochMs() > attestation.notAfterEpochMs()
                || lease.issuedAt().earliestEpochMs() < attestation.verifiedAt().latestEpochMs()) {
            throw new IllegalStateException("Target credential lease lies outside its original attestation");
        }
        Objects.requireNonNull(trust, "credential attestation trust").verify(profile, binding);
        return binding;
    }
}
