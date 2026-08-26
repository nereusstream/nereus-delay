package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import java.time.Clock;
import java.util.Objects;

/**
 * Local pre-provider-call gate for one exact Object Store credential lease.
 *
 * <p>The caller supplies the already-authorized immutable binding, protection
 * projection and resolved credential fingerprint. The gate rechecks those
 * values before each provider ownership attempt and bounds both the lease and
 * attestation age. A control-plane renewal may atomically replace the local
 * lease/protection projection; provider calls still never read Oxia. Oxia
 * Head/protection CAS, trust-set verification and secret resolution remain
 * external authorities.</p>
 */
public final class ObjectStoreCredentialUseLeaseGate {
    private static final int HASH_LENGTH = 32;

    private final ProfileSemanticEnvelope profile;
    private final CredentialBinding binding;
    private volatile Projection projection;
    private final Clock clock;
    private final long maximumLeaseTtlMs;
    private final long maximumAttestationAgeMs;

    public ObjectStoreCredentialUseLeaseGate(
            final ProfileSemanticEnvelope profile,
            final CredentialBinding binding,
            final CredentialBindingProtection protection,
            final CredentialUseLease lease,
            final byte[] loadedCredentialFingerprintDigest,
            final Clock clock,
            final long maximumLeaseTtlMs,
            final long maximumAttestationAgeMs) {
        this.profile = requireObjectStoreProfile(profile);
        this.binding = Objects.requireNonNull(binding, "binding");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maximumLeaseTtlMs <= 0 || maximumAttestationAgeMs <= 0) {
            throw new IllegalArgumentException("credential age bounds must be positive");
        }
        this.maximumLeaseTtlMs = maximumLeaseTtlMs;
        this.maximumAttestationAgeMs = maximumAttestationAgeMs;
        final Projection initial = new Projection(protection, lease, loadedCredentialFingerprintDigest);
        validateStaticIdentity(initial);
        this.projection = initial;
    }

    /** Checks the complete local gate immediately before provider ownership. */
    public void requireBeforeProviderCall() {
        final Projection current = projection;
        final CredentialUseLease lease = current.lease();
        lease.requireBinding(binding);
        lease.requireProtectedBy(current.protection());
        lease.requireTtlAtMost(maximumLeaseTtlMs);
        final CredentialEquivalenceAttestation attestation = binding.equivalenceAttestation();
        attestation.requireNotAfterAtMost(maximumAttestationAgeMs);
        if (!Bytes.constantTimeEquals(
                current.loadedCredentialFingerprintDigest(), lease.resolvedCredentialFingerprintDigest())) {
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

    public ProfileRef profile() {
        return profile.ref();
    }

    public CredentialUseLease lease() {
        return projection.lease();
    }

    /** Returns the current lease expiry used by the renewal coordinator. */
    public long validUntilEpochMs() {
        return projection.lease().validUntilEpochMs();
    }

    /** Atomically replaces the control-plane projection after a successful renewal. */
    public synchronized void replace(
            final CredentialBindingProtection protection,
            final CredentialUseLease lease,
            final byte[] loadedCredentialFingerprintDigest) {
        final Projection next = new Projection(protection, lease, loadedCredentialFingerprintDigest);
        validateStaticIdentity(next);
        if (next.lease().validUntilEpochMs() < projection.lease().validUntilEpochMs()) {
            throw new IllegalArgumentException("renewed Object Store credential lease moves expiry backwards");
        }
        projection = next;
    }

    private void validateStaticIdentity(final Projection candidate) {
        final CredentialBindingProtection protection = candidate.protection();
        final CredentialUseLease lease = candidate.lease();
        final byte[] loadedCredentialFingerprintDigest = candidate.loadedCredentialFingerprintDigest();
        if (!profile.ref().equals(binding.profile())
                || !profile.ref().equals(protection.profile())
                || !profile.ref().equals(lease.profile())) {
            throw new IllegalArgumentException("Object Store credential gate Profile identity differs");
        }
        if (lease.kind() != CredentialUseKind.OBJECT_STORE_ADAPTER) {
            throw new IllegalArgumentException("Object Store credential gate requires OBJECT_STORE_ADAPTER lease");
        }
        lease.requireBinding(binding);
        lease.requireProtectedBy(protection);
        lease.requireTtlAtMost(maximumLeaseTtlMs);
        final CredentialEquivalenceAttestation attestation = binding.equivalenceAttestation();
        attestation.requireCandidate(profile.ref(), binding.secretGeneration(), binding.secretReferenceSha256());
        attestation.requireAuthorizationScopeDigest(
                ((ObjectStoreProfileSemantic) profile.body()).credentialAuthorizationScopeDigest());
        attestation.requireNotAfterAtMost(maximumAttestationAgeMs);
        if (lease.validUntilEpochMs() > attestation.notAfterEpochMs()) {
            throw new IllegalArgumentException("Object Store credential lease outlives its attestation");
        }
        if (lease.issuedAt().earliestEpochMs() < attestation.verifiedAt().latestEpochMs()) {
            throw new IllegalArgumentException("Object Store credential lease predates attestation evidence");
        }
        if (!Bytes.constantTimeEquals(
                loadedCredentialFingerprintDigest,
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalArgumentException("loaded Object Store credential fingerprint differs from lease");
        }
    }

    private record Projection(
            CredentialBindingProtection protection,
            CredentialUseLease lease,
            byte[] loadedCredentialFingerprintDigest) {
        private Projection {
            Objects.requireNonNull(protection, "protection");
            Objects.requireNonNull(lease, "lease");
            loadedCredentialFingerprintDigest =
                    fixed(loadedCredentialFingerprintDigest, "loadedCredentialFingerprintDigest");
        }

        @Override
        public byte[] loadedCredentialFingerprintDigest() {
            return Bytes.copy(loadedCredentialFingerprintDigest);
        }
    }

    private static ProfileSemanticEnvelope requireObjectStoreProfile(final ProfileSemanticEnvelope value) {
        final ProfileSemanticEnvelope profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKind.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemantic)) {
            throw new IllegalArgumentException("Object Store credential gate requires an OBJECT_STORE Profile");
        }
        return profile;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
