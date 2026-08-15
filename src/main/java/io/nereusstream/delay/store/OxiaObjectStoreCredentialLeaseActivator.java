package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.CredentialProfileAuthority;

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

    public OxiaObjectStoreCredentialLeaseActivator(final CredentialProfileAuthority authority,
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
    public S3CompatibleCheckpointObjectStoreAdapter activateS3Compatible(
            final ActivationRequest request) {
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
            final ActivationRequest request, final long renewBeforeMs,
            final Supplier<TrustedUtcIntervalEvidence> renewalEvidenceSupplier) {
        final Activated activated = activate(request);
        return new RenewableS3CompatibleCheckpointObjectStoreAdapter(activated.adapter(), activated.gate(),
                authority, materialResolver, activated.profile(), activated.binding(), request.holderScopeDigest(),
                maximumLeaseTtlMs, maximumAttestationAgeMs, renewBeforeMs, request.clock(),
                renewalEvidenceSupplier);
    }

    private Activated activate(final ActivationRequest request) {
        Objects.requireNonNull(request, "request");
        final ProfileSemanticEnvelopeV1 profile = authority.resolve(request.profile());
        if (profile == null || !profile.ref().equals(request.profile())) {
            throw new IllegalStateException("Object Store Profile authority did not return the exact Profile");
        }
        if (profile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("Object Store activation requires an OBJECT_STORE Profile");
        }
        final CredentialBindingHeadV1 head = Objects.requireNonNull(authority.resolveHead(profile.ref()),
                "Object Store Profile has no credential Head");
        final CredentialBindingV1 binding = Objects.requireNonNull(authority.resolveBinding(profile.ref(),
                head.secretGeneration()), "Object Store Profile Head has no immutable binding");
        if (!profile.ref().equals(binding.profile())
                || head.secretGeneration() != binding.secretGeneration()
                || !Bytes.constantTimeEquals(head.bindingDigest(), binding.bindingDigest())) {
            throw new IllegalStateException("Object Store Profile Head and binding are not exact");
        }
        final ObjectStoreCredentialMaterial material = Objects.requireNonNull(
                materialResolver.resolve(profile, binding), "Object Store credential material");
        if (!Bytes.constantTimeEquals(material.resolvedCredentialFingerprintDigest(),
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalStateException("resolved Object Store credential fingerprint differs from binding");
        }
        final CredentialUseLeaseV1 lease = Objects.requireNonNull(authority.issueCredentialUseLease(
                profile.ref(), CredentialUseKindV1.OBJECT_STORE_ADAPTER, request.holderScopeDigest(),
                head.secretGeneration(), binding.bindingDigest(), material.resolvedCredentialFingerprintDigest(),
                request.issuedAt(), request.validUntilEpochMs(), head.headRevision()),
                "Object Store credential lease");
        final CredentialBindingProtectionV1 protection = Objects.requireNonNull(authority.resolveProtection(
                profile.ref(), lease.secretGeneration()), "Object Store credential protection");
        lease.requireBinding(binding);
        lease.requireProtectedBy(protection);
        final ObjectStoreCredentialUseLeaseGate gate = new ObjectStoreCredentialUseLeaseGate(
                profile, binding, protection, lease, material.resolvedCredentialFingerprintDigest(),
                request.clock(), maximumLeaseTtlMs, maximumAttestationAgeMs);
        final S3CompatibleCheckpointObjectStoreAdapter adapter = new S3CompatibleCheckpointObjectStoreAdapter(
                profile, request.endpoint(), request.region(),
                request.bucket(), material.accessKeyId(), material.secretAccessKey(), material.sessionToken(),
                request.limits(), gate, request.client(), request.clock(), request.requestTimeout());
        return new Activated(adapter, gate, profile, binding);
    }

    private record Activated(S3CompatibleCheckpointObjectStoreAdapter adapter,
                             ObjectStoreCredentialUseLeaseGate gate,
                             ProfileSemanticEnvelopeV1 profile, CredentialBindingV1 binding) {
        private Activated {
            Objects.requireNonNull(adapter, "adapter");
            Objects.requireNonNull(gate, "gate");
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(binding, "binding");
        }
    }

    @FunctionalInterface
    public interface CredentialMaterialResolver {
        ObjectStoreCredentialMaterial resolve(ProfileSemanticEnvelopeV1 profile, CredentialBindingV1 binding);
    }

    /** Private activation material; it is never part of a command/receipt/checkpoint projection. */
    public record ObjectStoreCredentialMaterial(String accessKeyId, String secretAccessKey, String sessionToken,
                                                byte[] resolvedCredentialFingerprintDigest) {
        public ObjectStoreCredentialMaterial {
            Objects.requireNonNull(accessKeyId, "accessKeyId");
            Objects.requireNonNull(secretAccessKey, "secretAccessKey");
            Bytes.requireLength(resolvedCredentialFingerprintDigest, CredentialBindingV1.HASH_LENGTH,
                    "resolvedCredentialFingerprintDigest");
            resolvedCredentialFingerprintDigest = Bytes.copy(resolvedCredentialFingerprintDigest);
        }

        @Override
        public byte[] resolvedCredentialFingerprintDigest() {
            return Bytes.copy(resolvedCredentialFingerprintDigest);
        }
    }

    /** Exact activation inputs; all provider/lease bounds remain explicit at the call site. */
    public record ActivationRequest(ProfileRefV1 profile, URI endpoint, String region, String bucket,
                                    byte[] holderScopeDigest, TrustedUtcIntervalEvidence issuedAt,
                                    long validUntilEpochMs, CheckpointManifestLimits limits,
                                    HttpClient client, Clock clock, Duration requestTimeout) {
        public ActivationRequest {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(endpoint, "endpoint");
            Objects.requireNonNull(region, "region");
            Objects.requireNonNull(bucket, "bucket");
            Bytes.requireLength(holderScopeDigest, CredentialUseLeaseV1.HASH_LENGTH, "holderScopeDigest");
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
