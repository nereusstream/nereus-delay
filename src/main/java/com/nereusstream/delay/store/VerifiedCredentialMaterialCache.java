package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable-view cache of already resolved Object Store credential material.
 *
 * <p>Installation is the control-plane boundary: the exact Object Store
 * Profile, binding scope, attestation trust set and resolved fingerprint are
 * checked before private material enters the cache. Reads are local exact-key
 * lookups; a miss never resolves a different generation or performs external
 * I/O.</p>
 */
public final class VerifiedCredentialMaterialCache
        implements OxiaObjectStoreCredentialLeaseActivator.CredentialMaterialResolver {
    private final CredentialAttestationTrustSet attestationTrustSet;
    private volatile Map<CacheKey, OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial> entries =
            Map.of();

    public VerifiedCredentialMaterialCache(final CredentialAttestationTrustSet attestationTrustSet) {
        this.attestationTrustSet = Objects.requireNonNull(attestationTrustSet, "attestationTrustSet");
    }

    /** Installs one exact, already resolved material value. */
    public synchronized void install(
            final ProfileSemanticEnvelope profile,
            final CredentialBinding binding,
            final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial material) {
        final Entry validated = validate(profile, binding, material);
        final CacheKey key = CacheKey.from(validated.profile(), validated.binding());
        final Map<CacheKey, OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial> next =
                new HashMap<>(entries);
        final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial previous =
                next.put(key, validated.material());
        if (previous != null
                && !Bytes.constantTimeEquals(
                        previous.resolvedCredentialFingerprintDigest(),
                        validated.material().resolvedCredentialFingerprintDigest())) {
            throw new IllegalArgumentException("credential material fingerprint changed for an exact cache key");
        }
        entries = Map.copyOf(next);
    }

    /** Atomically replaces the cache after validating every entry. */
    public synchronized void replaceAll(final Collection<Entry> values) {
        Objects.requireNonNull(values, "values");
        final Map<CacheKey, OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial> replacement =
                new HashMap<>();
        for (Entry value : values) {
            final Entry validated = validate(value.profile(), value.binding(), value.material());
            final CacheKey key = CacheKey.from(validated.profile(), validated.binding());
            final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial previous =
                    replacement.put(key, validated.material());
            if (previous != null
                    && !Bytes.constantTimeEquals(
                            previous.resolvedCredentialFingerprintDigest(),
                            validated.material().resolvedCredentialFingerprintDigest())) {
                throw new IllegalArgumentException("credential material cache contains a conflicting exact key");
            }
        }
        entries = Map.copyOf(replacement);
    }

    /** Removes one exact generation without affecting another binding. */
    public synchronized void remove(final ProfileSemanticEnvelope profile, final CredentialBinding binding) {
        final CacheKey key =
                CacheKey.from(Objects.requireNonNull(profile, "profile"), Objects.requireNonNull(binding, "binding"));
        final Map<CacheKey, OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial> next =
                new HashMap<>(entries);
        next.remove(key);
        entries = Map.copyOf(next);
    }

    public synchronized void clear() {
        entries = Map.of();
    }

    public int size() {
        return entries.size();
    }

    @Override
    public OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial resolve(
            final ProfileSemanticEnvelope profile, final CredentialBinding binding) {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(binding, "binding");
        return entries.get(CacheKey.from(profile, binding));
    }

    private Entry validate(
            final ProfileSemanticEnvelope profile,
            final CredentialBinding binding,
            final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial material) {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(material, "material");
        if (profile.profileKind() != ProfileKind.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemantic semantic)) {
            throw new IllegalArgumentException("verified credential cache requires an OBJECT_STORE Profile");
        }
        if (!profile.ref().equals(binding.profile())) {
            throw new IllegalArgumentException("verified credential cache Profile differs from binding");
        }
        binding.equivalenceAttestation()
                .requireCandidate(profile.ref(), binding.secretGeneration(), binding.secretReferenceSha256());
        binding.equivalenceAttestation().requireAuthorizationScopeDigest(semantic.credentialAuthorizationScopeDigest());
        attestationTrustSet.verify(profile, binding);
        if (!Bytes.constantTimeEquals(
                material.resolvedCredentialFingerprintDigest(),
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalArgumentException("verified credential cache material fingerprint differs from binding");
        }
        return new Entry(profile, binding, material);
    }

    /** Exact private cache input; it is never a public command or receipt value. */
    public record Entry(
            ProfileSemanticEnvelope profile,
            CredentialBinding binding,
            OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial material) {
        public Entry {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(material, "material");
        }
    }

    private static final class CacheKey {
        private final ProfileRef profile;
        private final long generation;
        private final byte[] bindingDigest;
        private final byte[] referenceSha256;

        private CacheKey(
                final ProfileRef profile,
                final long generation,
                final byte[] bindingDigest,
                final byte[] referenceSha256) {
            this.profile = Objects.requireNonNull(profile, "profile");
            this.generation = generation;
            this.bindingDigest = Bytes.copy(bindingDigest);
            this.referenceSha256 = Bytes.copy(referenceSha256);
        }

        private static CacheKey from(final ProfileSemanticEnvelope profile, final CredentialBinding binding) {
            return new CacheKey(
                    profile.ref(),
                    binding.secretGeneration(),
                    binding.bindingDigest(),
                    binding.secretReferenceSha256());
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof CacheKey that
                    && generation == that.generation
                    && profile.equals(that.profile)
                    && Bytes.constantTimeEquals(bindingDigest, that.bindingDigest)
                    && Bytes.constantTimeEquals(referenceSha256, that.referenceSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(profile, generation, Bytes.hex(bindingDigest), Bytes.hex(referenceSha256));
        }
    }
}
