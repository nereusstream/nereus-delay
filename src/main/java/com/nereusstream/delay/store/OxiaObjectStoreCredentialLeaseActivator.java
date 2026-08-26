package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialProfileAuthority;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Activates one Object Store adapter from exact Profile authority state.
 *
 * <p>Activation resolves the immutable binding and current Head once, asks the
 * authority to protect and issue one bounded lease, and installs the local
 * fingerprint gate into the adapter. Provider calls do not reread Oxia; the
 * optional renewable activation path performs bounded control-plane renewal
 * only inside its explicit renewal window.</p>
 */
public final class OxiaObjectStoreCredentialLeaseActivator {
    private final CredentialProfileAuthority authority;
    private final CredentialMaterialResolver materialResolver;
    private final long maximumLeaseTtlMs;
    private final long maximumAttestationAgeMs;

    public OxiaObjectStoreCredentialLeaseActivator(
            final CredentialProfileAuthority authority,
            final CredentialMaterialResolver materialResolver,
            final long maximumLeaseTtlMs,
            final long maximumAttestationAgeMs) {
        this.authority = Objects.requireNonNull(authority, "authority");
        this.materialResolver = Objects.requireNonNull(materialResolver, "materialResolver");
        if (maximumLeaseTtlMs <= 0 || maximumAttestationAgeMs <= 0) {
            throw new IllegalArgumentException("credential activation bounds must be positive");
        }
        this.maximumLeaseTtlMs = maximumLeaseTtlMs;
        this.maximumAttestationAgeMs = maximumAttestationAgeMs;
    }

    /** Activates a lease-gated S3/S3-compatible checkpoint adapter. */
    public S3CompatibleCheckpointObjectStoreAdapter activateS3Compatible(final ActivationRequest request) {
        return activate(request).adapter();
    }

    /**
     * Activates an adapter that renews its control-plane lease before the
     * current lease enters the configured renewal window.
     *
     * <p>The evidence supplier is called only during a renewal attempt and
     * must provide fresh trusted time evidence. Provider calls remain behind
     * the same local gate and never perform an Oxia read.</p>
     */
    public RenewableS3CompatibleCheckpointObjectStoreAdapter activateRenewableS3Compatible(
            final ActivationRequest request,
            final long renewBeforeMs,
            final Supplier<TrustedUtcIntervalEvidence> renewalEvidenceSupplier) {
        final Activated activated = activate(request);
        return new RenewableS3CompatibleCheckpointObjectStoreAdapter(
                activated.adapter(),
                activated.gate(),
                authority,
                materialResolver,
                activated.profile(),
                activated.binding(),
                request.holderScopeDigest(),
                maximumLeaseTtlMs,
                maximumAttestationAgeMs,
                renewBeforeMs,
                request.clock(),
                renewalEvidenceSupplier);
    }

    private Activated activate(final ActivationRequest request) {
        Objects.requireNonNull(request, "request");
        final ProfileSemanticEnvelope profile = authority.resolve(request.profile());
        if (profile == null || !profile.ref().equals(request.profile())) {
            throw new IllegalStateException("Object Store Profile authority did not return the exact Profile");
        }
        if (profile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("Object Store activation requires an OBJECT_STORE Profile");
        }
        final CredentialBindingHead head = Objects.requireNonNull(
                authority.resolveHead(profile.ref()), "Object Store Profile has no credential Head");
        final CredentialBinding binding = Objects.requireNonNull(
                authority.resolveBinding(profile.ref(), head.secretGeneration()),
                "Object Store Profile Head has no immutable binding");
        if (!profile.ref().equals(binding.profile())
                || head.secretGeneration() != binding.secretGeneration()
                || !Bytes.constantTimeEquals(head.bindingDigest(), binding.bindingDigest())) {
            throw new IllegalStateException("Object Store Profile Head and binding are not exact");
        }
        final ObjectStoreCredentialMaterial material =
                Objects.requireNonNull(materialResolver.resolve(profile, binding), "Object Store credential material");
        if (!Bytes.constantTimeEquals(
                material.resolvedCredentialFingerprintDigest(),
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalStateException("resolved Object Store credential fingerprint differs from binding");
        }
        final CredentialUseLease lease = Objects.requireNonNull(
                authority.issueCredentialUseLease(
                        profile.ref(),
                        CredentialUseKind.OBJECT_STORE_ADAPTER,
                        request.holderScopeDigest(),
                        head.secretGeneration(),
                        binding.bindingDigest(),
                        material.resolvedCredentialFingerprintDigest(),
                        request.issuedAt(),
                        request.validUntilEpochMs(),
                        head.headRevision()),
                "Object Store credential lease");
        final CredentialBindingProtection protection = Objects.requireNonNull(
                authority.resolveProtection(profile.ref(), lease.secretGeneration()),
                "Object Store credential protection");
        lease.requireBinding(binding);
        lease.requireProtectedBy(protection);
        final ObjectStoreCredentialUseLeaseGate gate = new ObjectStoreCredentialUseLeaseGate(
                profile,
                binding,
                protection,
                lease,
                material.resolvedCredentialFingerprintDigest(),
                request.clock(),
                maximumLeaseTtlMs,
                maximumAttestationAgeMs);
        final S3CompatibleCheckpointObjectStoreAdapter adapter = new S3CompatibleCheckpointObjectStoreAdapter(
                profile,
                request.endpoint(),
                request.region(),
                request.bucket(),
                material.accessKeyId(),
                material.secretAccessKey(),
                material.sessionToken(),
                request.limits(),
                gate,
                request.client(),
                request.clock(),
                request.requestTimeout());
        return new Activated(adapter, gate, profile, binding);
    }

    private record Activated(
            S3CompatibleCheckpointObjectStoreAdapter adapter,
            ObjectStoreCredentialUseLeaseGate gate,
            ProfileSemanticEnvelope profile,
            CredentialBinding binding) {
        private Activated {
            Objects.requireNonNull(adapter, "adapter");
            Objects.requireNonNull(gate, "gate");
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(binding, "binding");
        }
    }

    @FunctionalInterface
    public interface CredentialMaterialResolver {
        ObjectStoreCredentialMaterial resolve(ProfileSemanticEnvelope profile, CredentialBinding binding);
    }

    /** Private activation material; it is never part of a command/receipt/checkpoint projection. */
    public record ObjectStoreCredentialMaterial(
            String accessKeyId,
            String secretAccessKey,
            String sessionToken,
            byte[] resolvedCredentialFingerprintDigest) {
        public ObjectStoreCredentialMaterial {
            Objects.requireNonNull(accessKeyId, "accessKeyId");
            Objects.requireNonNull(secretAccessKey, "secretAccessKey");
            Bytes.requireLength(
                    resolvedCredentialFingerprintDigest,
                    CredentialBinding.HASH_LENGTH,
                    "resolvedCredentialFingerprintDigest");
            resolvedCredentialFingerprintDigest = Bytes.copy(resolvedCredentialFingerprintDigest);
        }

        @Override
        public byte[] resolvedCredentialFingerprintDigest() {
            return Bytes.copy(resolvedCredentialFingerprintDigest);
        }
    }

    /** Exact activation inputs; all provider/lease bounds remain explicit at the call site. */
    public record ActivationRequest(
            ProfileRef profile,
            URI endpoint,
            String region,
            String bucket,
            byte[] holderScopeDigest,
            TrustedUtcIntervalEvidence issuedAt,
            long validUntilEpochMs,
            CheckpointManifestLimits limits,
            HttpClient client,
            Clock clock,
            Duration requestTimeout) {
        public ActivationRequest {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(endpoint, "endpoint");
            Objects.requireNonNull(region, "region");
            Objects.requireNonNull(bucket, "bucket");
            Bytes.requireLength(holderScopeDigest, CredentialUseLease.HASH_LENGTH, "holderScopeDigest");
            Objects.requireNonNull(issuedAt, "issuedAt");
            Objects.requireNonNull(limits, "limits");
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(clock, "clock");
            Objects.requireNonNull(requestTimeout, "requestTimeout");
            holderScopeDigest = Bytes.copy(holderScopeDigest);
        }

        @Override
        public byte[] holderScopeDigest() {
            return Bytes.copy(holderScopeDigest);
        }
    }
}
