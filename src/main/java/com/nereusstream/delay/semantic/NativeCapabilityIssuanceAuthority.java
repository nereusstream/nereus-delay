package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

/**
 * External authority surface used by the native capability issuer.
 * Implementations own Broker guard reads and the transactional monotonic
 * credential-protection update; the issuer never invents either proof.
 */
public interface NativeCapabilityIssuanceAuthority {
    /**
     * Resolves the complete guarded Broker rollout for the exact Profile
     * pair, partition and authenticated principal scope.
     */
    GuardEvidence resolveGuard(
            ProfileRef destination,
            ProfileRef capability,
            int physicalPartition,
            byte[] principalScopeDigest,
            TrustedUtcIntervalEvidence issuedAt);

    /**
     * Durably extends native-capability protection before the snapshot is
     * exposed. The returned value must be the exact binding with a protection
     * horizon covering {@code notAfterEpochMs}.
     */
    CredentialBindingProtection protectNativeCapability(CredentialBinding binding, long notAfterEpochMs);

    /** Exact immutable Broker guard projection returned by the external authority. */
    record GuardEvidence(
            com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity target,
            int physicalPartition,
            byte[] resourceGuardAttestationSha256,
            long resourceGuardConfigGeneration,
            byte[] principalScopeDigest,
            long validUntilEpochMs) {
        public GuardEvidence {
            java.util.Objects.requireNonNull(target, "target");
            if (physicalPartition < 0 || resourceGuardConfigGeneration == 0 || validUntilEpochMs < 0) {
                throw new IllegalArgumentException("invalid native guard evidence numbers");
            }
            resourceGuardAttestationSha256 = fixed(resourceGuardAttestationSha256, "resourceGuardAttestationSha256");
            principalScopeDigest = fixed(principalScopeDigest, "principalScopeDigest");
        }

        @Override
        public byte[] resourceGuardAttestationSha256() {
            return com.nereusstream.delay.protocol.Bytes.copy(resourceGuardAttestationSha256);
        }

        @Override
        public byte[] principalScopeDigest() {
            return com.nereusstream.delay.protocol.Bytes.copy(principalScopeDigest);
        }

        private static byte[] fixed(final byte[] value, final String name) {
            com.nereusstream.delay.protocol.Bytes.requireLength(value, 32, name);
            boolean nonZero = false;
            for (byte element : value) {
                nonZero |= element != 0;
            }
            if (!nonZero) {
                throw new IllegalArgumentException(name + " must be non-zero");
            }
            return com.nereusstream.delay.protocol.Bytes.copy(value);
        }
    }
}
