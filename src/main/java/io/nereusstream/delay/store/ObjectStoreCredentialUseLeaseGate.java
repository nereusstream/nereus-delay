package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;

import java.time.Clock;
import java.util.Objects;

/**
 * Local pre-provider-call gate for one exact Object Store credential lease.
 *
 * <p>The caller supplies the already-authorized immutable binding, protection
 * projection and resolved credential fingerprint. The gate rechecks those
 * values before each provider ownership attempt and bounds both the lease and
 * attestation age. Oxia Head/protection CAS, trust-set verification and secret
 * resolution remain external authorities.</p>
 */
public final class ObjectStoreCredentialUseLeaseGate {
    private static final int HASH_LENGTH = 32;

    private final ProfileSemanticEnvelopeV1 profile;
    private final CredentialBindingV1 binding;
    private final CredentialBindingProtectionV1 protection;
    private final CredentialUseLeaseV1 lease;
    private final byte[] loadedCredentialFingerprintDigest;
    private final Clock clock;
    private final long maximumLeaseTtlMs;
    private final long maximumAttestationAgeMs;

    public ObjectStoreCredentialUseLeaseGate(final ProfileSemanticEnvelopeV1 profile,
                                             final CredentialBindingV1 binding,
                                             final CredentialBindingProtectionV1 protection,
                                             final CredentialUseLeaseV1 lease,
                                             final byte[] loadedCredentialFingerprintDigest,
                                             final Clock clock,
                                             final long maximumLeaseTtlMs,
                                             final long maximumAttestationAgeMs) {
        this.profile = requireObjectStoreProfile(profile);
        this.binding = Objects.requireNonNull(binding, "binding");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.loadedCredentialFingerprintDigest = fixed(loadedCredentialFingerprintDigest,
                "loadedCredentialFingerprintDigest");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maximumLeaseTtlMs <= 0 || maximumAttestationAgeMs <= 0) {
            throw new IllegalArgumentException("credential age bounds must be positive");
        }
        this.maximumLeaseTtlMs = maximumLeaseTtlMs;
        this.maximumAttestationAgeMs = maximumAttestationAgeMs;
        validateStaticIdentity();
    }

    /** Checks the complete local gate immediately before provider ownership. */
    public void requireBeforeProviderCall() {
        lease.requireBinding(binding);
        lease.requireProtectedBy(protection);
        lease.requireTtlAtMost(maximumLeaseTtlMs);
        final CredentialEquivalenceAttestationV1 attestation = binding.equivalenceAttestation();
        attestation.requireNotAfterAtMost(maximumAttestationAgeMs);
        if (!Bytes.constantTimeEquals(loadedCredentialFingerprintDigest,
                lease.resolvedCredentialFingerprintDigest())) {
            throw new IllegalStateException("loaded Object Store credential fingerprint drifted");
        }
        final long now = clock.millis();
        if (now < 0 || now < lease.issuedAt().earliestEpochMs()) {
            throw new IllegalStateException("trusted time is before Object Store credential lease evidence");
        }
        if (now >= lease.validUntilEpochMs()) {
            throw new IllegalStateException("Object Store credential lease expired");
        }
        if (now < attestation.verifiedAt().earliestEpochMs() || now >= attestation.notAfterEpochMs()) {
            throw new IllegalStateException("Object Store credential attestation is not current");
        }
    }

    public ProfileRefV1 profile() {
        return profile.ref();
    }

    public CredentialUseLeaseV1 lease() {
        return lease;
    }

    private void validateStaticIdentity() {
        if (!profile.ref().equals(binding.profile()) || !profile.ref().equals(protection.profile())
                || !profile.ref().equals(lease.profile())) {
            throw new IllegalArgumentException("Object Store credential gate Profile identity differs");
        }
        if (lease.kind() != CredentialUseKindV1.OBJECT_STORE_ADAPTER) {
            throw new IllegalArgumentException("Object Store credential gate requires OBJECT_STORE_ADAPTER lease");
        }
        lease.requireBinding(binding);
        lease.requireProtectedBy(protection);
        lease.requireTtlAtMost(maximumLeaseTtlMs);
        final CredentialEquivalenceAttestationV1 attestation = binding.equivalenceAttestation();
        attestation.requireCandidate(profile.ref(), binding.secretGeneration(), binding.secretReferenceSha256());
        attestation.requireAuthorizationScopeDigest(
                ((ObjectStoreProfileSemanticV1) profile.body()).credentialAuthorizationScopeDigest());
        attestation.requireNotAfterAtMost(maximumAttestationAgeMs);
        if (lease.validUntilEpochMs() > attestation.notAfterEpochMs()) {
            throw new IllegalArgumentException("Object Store credential lease outlives its attestation");
        }
        if (lease.issuedAt().earliestEpochMs() < attestation.verifiedAt().latestEpochMs()) {
            throw new IllegalArgumentException("Object Store credential lease predates attestation evidence");
        }
        if (!Bytes.constantTimeEquals(loadedCredentialFingerprintDigest,
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalArgumentException("loaded Object Store credential fingerprint differs from lease");
        }
    }

    private static ProfileSemanticEnvelopeV1 requireObjectStoreProfile(
            final ProfileSemanticEnvelopeV1 value) {
        final ProfileSemanticEnvelopeV1 profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKindV1.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemanticV1)) {
            throw new IllegalArgumentException("Object Store credential gate requires an OBJECT_STORE Profile");
        }
        return profile;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
