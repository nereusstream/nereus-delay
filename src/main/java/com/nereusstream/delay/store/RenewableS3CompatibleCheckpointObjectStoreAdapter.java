package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialProfileAuthority;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Control-plane renewing wrapper around one lease-gated S3-compatible adapter.
 *
 * <p>Renewal is opportunistic and automatic at the upload/download boundary:
 * only when the current lease is inside the configured window does this class
 * read the authority, resolve the exact same generation/material fingerprint,
 * issue a new protected lease and atomically replace the local gate. A Head
 * rotation is a quiescence boundary and is rejected; this wrapper never
 * silently changes the provider credentials while an adapter is live.</p>
 */
public final class RenewableS3CompatibleCheckpointObjectStoreAdapter
        implements CheckpointUploadAdapter, CheckpointDownloadAdapter {
    private final S3CompatibleCheckpointObjectStoreAdapter delegate;
    private final ObjectStoreCredentialUseLeaseGate gate;
    private final CredentialProfileAuthority authority;
    private final OxiaObjectStoreCredentialLeaseActivator.CredentialMaterialResolver materialResolver;
    private final ProfileSemanticEnvelope profile;
    private final CredentialBinding binding;
    private final byte[] holderScopeDigest;
    private final long maximumLeaseTtlMs;
    private final long maximumAttestationAgeMs;
    private final long renewBeforeMs;
    private final Clock clock;
    private final Supplier<TrustedUtcIntervalEvidence> renewalEvidenceSupplier;

    public RenewableS3CompatibleCheckpointObjectStoreAdapter(
            final S3CompatibleCheckpointObjectStoreAdapter delegate,
            final ObjectStoreCredentialUseLeaseGate gate,
            final CredentialProfileAuthority authority,
            final OxiaObjectStoreCredentialLeaseActivator.CredentialMaterialResolver materialResolver,
            final ProfileSemanticEnvelope profile,
            final CredentialBinding binding,
            final byte[] holderScopeDigest,
            final long maximumLeaseTtlMs,
            final long maximumAttestationAgeMs,
            final long renewBeforeMs,
            final Clock clock,
            final Supplier<TrustedUtcIntervalEvidence> renewalEvidenceSupplier) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.gate = Objects.requireNonNull(gate, "gate");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.materialResolver = Objects.requireNonNull(materialResolver, "materialResolver");
        this.profile = requireObjectStoreProfile(profile);
        this.binding = Objects.requireNonNull(binding, "binding");
        Bytes.requireLength(holderScopeDigest, CredentialUseLease.HASH_LENGTH, "holderScopeDigest");
        this.holderScopeDigest = Bytes.copy(holderScopeDigest);
        if (maximumLeaseTtlMs <= 0
                || maximumAttestationAgeMs <= 0
                || renewBeforeMs <= 0
                || renewBeforeMs >= maximumLeaseTtlMs) {
            throw new IllegalArgumentException("invalid Object Store credential renewal bounds");
        }
        this.maximumLeaseTtlMs = maximumLeaseTtlMs;
        this.maximumAttestationAgeMs = maximumAttestationAgeMs;
        this.renewBeforeMs = renewBeforeMs;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.renewalEvidenceSupplier = Objects.requireNonNull(renewalEvidenceSupplier, "renewalEvidenceSupplier");
        if (!profile.ref().equals(binding.profile()) || !profile.ref().equals(gate.profile())) {
            throw new IllegalArgumentException("renewable Object Store adapter identity differs");
        }
    }

    @Override
    public synchronized com.nereusstream.delay.protocol.CheckpointResource upload(
            final CheckpointUploadRequest request) {
        requireProviderAdmission();
        renewIfNeeded();
        return delegate.upload(request);
    }

    @Override
    public synchronized Path download(final CheckpointDownloadRequest request, final Path targetDirectory) {
        requireProviderAdmission();
        renewIfNeeded();
        return delegate.download(request, targetDirectory);
    }

    /** Returns the current local lease projection without reading the authority. */
    public CredentialUseLease lease() {
        return gate.lease();
    }

    /** Permanently fences this adapter generation before any renewal or provider call. */
    public synchronized void beginProviderQuiescence() {
        delegate.beginProviderQuiescence();
    }

    /** Returns the delegate's local provider ownership observation. */
    public synchronized ObjectStoreProviderOwnershipTracker.Observation providerOwnershipObservation() {
        return delegate.providerOwnershipObservation();
    }

    /** Requires the delegate's local fence, drain and uncertainty horizon to be closed. */
    public synchronized ObjectStoreProviderOwnershipTracker.Observation requireProviderQuiescence() {
        return delegate.requireProviderQuiescence();
    }

    /** Performs one automatic renewal check; provider I/O is not performed here. */
    void renewIfNeeded() {
        final long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("trusted time is negative");
        }
        if (gate.validUntilEpochMs() - now > renewBeforeMs) {
            return;
        }
        final ProfileSemanticEnvelope currentProfile = authority.resolve(profile.ref());
        if (currentProfile == null || !currentProfile.equals(profile)) {
            throw new IllegalStateException("Object Store Profile authority changed during renewal");
        }
        final CredentialBindingHead head = Objects.requireNonNull(
                authority.resolveHead(profile.ref()), "Object Store Profile has no renewal Head");
        if (head.secretGeneration() != binding.secretGeneration()
                || !Bytes.constantTimeEquals(head.bindingDigest(), binding.bindingDigest())) {
            throw new IllegalStateException("Object Store credential rotation requires adapter quiescence");
        }
        final CredentialBinding currentBinding = Objects.requireNonNull(
                authority.resolveBinding(profile.ref(), head.secretGeneration()),
                "Object Store Profile has no renewal binding");
        if (!currentBinding.equals(binding)) {
            throw new IllegalStateException("Object Store credential binding changed during renewal");
        }
        final OxiaObjectStoreCredentialLeaseActivator.ObjectStoreCredentialMaterial material = Objects.requireNonNull(
                materialResolver.resolve(currentProfile, currentBinding), "Object Store renewal material");
        if (!Bytes.constantTimeEquals(
                material.resolvedCredentialFingerprintDigest(),
                binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
            throw new IllegalStateException("resolved Object Store renewal fingerprint differs from binding");
        }
        binding.equivalenceAttestation().requireNotAfterAtMost(maximumAttestationAgeMs);
        final TrustedUtcIntervalEvidence evidence =
                Objects.requireNonNull(renewalEvidenceSupplier.get(), "Object Store renewal evidence");
        if (evidence.earliestEpochMs() > now) {
            throw new IllegalStateException("Object Store renewal evidence is from the future");
        }
        final long validUntil;
        try {
            validUntil = Math.addExact(evidence.earliestEpochMs(), maximumLeaseTtlMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("Object Store renewal expiry overflows epoch range", overflow);
        }
        if (validUntil <= now) {
            throw new IllegalStateException("Object Store renewal evidence cannot produce a live lease");
        }
        final CredentialUseLease renewedLease = Objects.requireNonNull(
                authority.issueCredentialUseLease(
                        profile.ref(),
                        CredentialUseKind.OBJECT_STORE_ADAPTER,
                        holderScopeDigest,
                        binding.secretGeneration(),
                        binding.bindingDigest(),
                        material.resolvedCredentialFingerprintDigest(),
                        evidence,
                        validUntil,
                        head.headRevision()),
                "Object Store renewed credential lease");
        final CredentialBindingProtection protection = Objects.requireNonNull(
                authority.resolveProtection(profile.ref(), renewedLease.secretGeneration()),
                "Object Store renewed protection");
        renewedLease.requireBinding(binding);
        renewedLease.requireProtectedBy(protection);
        renewedLease.requireTtlAtMost(maximumLeaseTtlMs);
        gate.replace(protection, renewedLease, material.resolvedCredentialFingerprintDigest());
    }

    private void requireProviderAdmission() {
        if (!delegate.providerOwnershipObservation().acceptingNewOperations()) {
            throw new IllegalStateException("Object Store provider ownership is fenced");
        }
    }

    private static ProfileSemanticEnvelope requireObjectStoreProfile(final ProfileSemanticEnvelope value) {
        final ProfileSemanticEnvelope profile = Objects.requireNonNull(value, "profile");
        if (profile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("renewable adapter requires an OBJECT_STORE Profile");
        }
        return profile;
    }
}
