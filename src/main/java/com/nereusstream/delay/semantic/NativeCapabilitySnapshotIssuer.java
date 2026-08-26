package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ProfileCatalog;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Objects;

/**
 * Issuance-side composition for one Pulsar AUTO_FAST capability snapshot.
 *
 * <p>The issuer owns the signing key and chooses the bounded expiry. It
 * resolves immutable Profile/Binding bytes from the catalog, obtains the
 * exact guarded Broker evidence from an external authority, and persists the
 * native protection horizon before returning a snapshot. No plaintext secret
 * or caller-selected target is accepted by this boundary.</p>
 */
public final class NativeCapabilitySnapshotIssuer {
    private final ProfileCatalog profileCatalog;
    private final NativeCapabilityIssuanceAuthority authority;
    private final PublicKey routeVerificationKey;
    private final PublicKey credentialAttestationKey;
    private final PrivateKey issuerKey;
    private final int issuerSigningKeyVersion;
    private final long maximumSnapshotLifetimeMs;

    public NativeCapabilitySnapshotIssuer(
            final ProfileCatalog profileCatalog,
            final NativeCapabilityIssuanceAuthority authority,
            final PublicKey routeVerificationKey,
            final PublicKey credentialAttestationKey,
            final PrivateKey issuerKey,
            final int issuerSigningKeyVersion,
            final long maximumSnapshotLifetimeMs) {
        this.profileCatalog = Objects.requireNonNull(profileCatalog, "profileCatalog");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.routeVerificationKey = Objects.requireNonNull(routeVerificationKey, "routeVerificationKey");
        this.credentialAttestationKey = Objects.requireNonNull(credentialAttestationKey, "credentialAttestationKey");
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
        if (issuerSigningKeyVersion <= 0 || maximumSnapshotLifetimeMs <= 0) {
            throw new IllegalArgumentException("invalid native issuer configuration");
        }
        this.issuerSigningKeyVersion = issuerSigningKeyVersion;
        this.maximumSnapshotLifetimeMs = maximumSnapshotLifetimeMs;
    }

    /**
     * Issues one target/partition-specific snapshot after all external
     * evidence and protection checks succeed.
     */
    public NativePreparationSnapshot issue(
            final AuthenticatedTenantContext context,
            final RouteSnapshot route,
            final ProfileRef destinationReference,
            final int physicalPartition,
            final long brokerDeliverAtEpochMs,
            final TrustedUtcIntervalEvidence issuedAt) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(route, "route");
        final ProfileRef destinationRef = Objects.requireNonNull(destinationReference, "destinationReference");
        Objects.requireNonNull(issuedAt, "issuedAt");
        if (physicalPartition < 0 || brokerDeliverAtEpochMs < 0) {
            throw new IllegalArgumentException("native issuance partition/time is invalid");
        }
        final RouteSnapshot verifiedRoute = RouteSnapshot.decode(route.canonicalBytes(), routeVerificationKey);
        if (brokerDeliverAtEpochMs > verifiedRoute.validUntilEpochMs()) {
            throw new IllegalArgumentException("native broker delivery time exceeds the Route validity");
        }
        verifiedRoute.requireUsableForNewSchedule(
                context.authenticatedTenantScopeHash(), context.tenantRoutingScope(), issuedAt.latestEpochMs());

        final ProfileSemanticEnvelope destination = exactProfile(destinationRef, ProfileKind.DESTINATION);
        if (!(destination.body() instanceof DestinationProfileSemantic destinationBody)
                || destinationBody.adapterKind() != com.nereusstream.delay.protocol.AdapterKind.PULSAR) {
            throw new IllegalArgumentException("native issuance requires a Pulsar Destination Profile");
        }
        if (Integer.toUnsignedLong(physicalPartition) >= Integer.toUnsignedLong(destinationBody.targetPartitionCount())
                || destinationBody.targetPartitionPolicy()
                                == com.nereusstream.delay.protocol.TargetPartitionPolicy.EXPLICIT_ONLY
                        && !destinationBody.allowedExplicitPartitions().contains(physicalPartition)) {
            throw new IllegalArgumentException("native issuance partition violates the Destination Profile");
        }
        final ProfileRef capabilityRef = destinationBody.deliveryCapability();
        final ProfileSemanticEnvelope capability = exactProfile(capabilityRef, ProfileKind.DELIVERY_CAPABILITY);
        if (!(capability.body() instanceof DeliveryCapabilitySemantic capabilityBody)
                || capabilityBody.adapterKind() != com.nereusstream.delay.protocol.AdapterKind.PULSAR
                || !TimingCapability.includes(
                        capabilityBody.timingCapabilityBits(), TimingCapability.PULSAR_AUTO_FAST)) {
            throw new IllegalArgumentException("native issuance requires a Pulsar AUTO_FAST capability");
        }

        final CredentialBindingHead head =
                Objects.requireNonNull(profileCatalog.resolveHead(destinationRef), "destination credential Head");
        if (!destinationRef.equals(head.profile())) {
            throw new IllegalArgumentException("credential Head does not belong to the Destination Profile");
        }
        final CredentialBinding binding = Objects.requireNonNull(
                profileCatalog.resolveBinding(destinationRef, head.secretGeneration()),
                "current destination credential binding");
        if (!destinationRef.equals(binding.profile())
                || !Bytes.constantTimeEquals(binding.bindingDigest(), head.bindingDigest())) {
            throw new IllegalArgumentException("credential binding Head does not identify exact immutable bytes");
        }
        final CredentialEquivalenceAttestation attestation = binding.equivalenceAttestation();
        if (!attestation.verifySignature(credentialAttestationKey)) {
            throw new IllegalArgumentException("credential equivalence attestation signature is invalid");
        }
        attestation.requireAuthorizationScopeDigest(destinationBody.credentialAuthorizationScopeDigest());

        final NativeCapabilityIssuanceAuthority.GuardEvidence guard = Objects.requireNonNull(
                authority.resolveGuard(
                        destinationRef, capabilityRef, physicalPartition, context.principalScopeHash(), issuedAt),
                "native guard evidence");
        final PulsarBrokerResourceIdentity expectedTarget =
                destinationBody.targetResource().pulsar();
        if (!expectedTarget.equals(guard.target())
                || guard.physicalPartition() != physicalPartition
                || !Bytes.constantTimeEquals(guard.principalScopeDigest(), context.principalScopeHash())
                || guard.validUntilEpochMs() <= issuedAt.latestEpochMs()) {
            throw new IllegalArgumentException("native guard evidence does not match the requested scope");
        }

        final long notAfter = boundedNotAfter(issuedAt, verifiedRoute, attestation, guard);
        final CredentialBindingProtection protectedBinding = Objects.requireNonNull(
                authority.protectNativeCapability(binding, notAfter), "native credential protection");
        requireProtection(protectedBinding, binding, notAfter);

        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destinationRef,
                capabilityRef,
                guard.target(),
                physicalPartition,
                guard.resourceGuardAttestationSha256(),
                guard.resourceGuardConfigGeneration(),
                binding.secretGeneration(),
                binding.bindingDigest(),
                attestation.resolvedCredentialFingerprintDigest(),
                context.principalScopeHash(),
                issuedAt,
                notAfter,
                issuerSigningKeyVersion,
                issuerKey);
        return new NativePreparationSnapshot(
                destination, capability, guard.target(), physicalPartition, snapshot, brokerDeliverAtEpochMs);
    }

    private ProfileSemanticEnvelope exactProfile(final ProfileRef reference, final ProfileKind expectedKind) {
        final ProfileSemanticEnvelope profile =
                Objects.requireNonNull(profileCatalog.resolve(reference), "Profile semantic");
        final ProfileSemanticEnvelope canonical = ProfileSemanticEnvelope.decode(profile.canonicalBytes());
        if (!canonical.equals(profile)
                || canonical.profileKind() != expectedKind
                || !canonical.ref().equals(reference)) {
            throw new IllegalArgumentException("Profile catalog returned non-exact semantic bytes");
        }
        return canonical;
    }

    private long boundedNotAfter(
            final TrustedUtcIntervalEvidence issuedAt,
            final RouteSnapshot route,
            final CredentialEquivalenceAttestation attestation,
            final NativeCapabilityIssuanceAuthority.GuardEvidence guard) {
        final long issuerBound;
        try {
            issuerBound = Math.addExact(issuedAt.earliestEpochMs(), maximumSnapshotLifetimeMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("native issuer lifetime overflows epoch range", overflow);
        }
        final long notAfter = Math.min(
                Math.min(issuerBound, route.validUntilEpochMs()),
                Math.min(attestation.notAfterEpochMs(), guard.validUntilEpochMs()));
        if (notAfter <= issuedAt.latestEpochMs()) {
            throw new IllegalArgumentException("native capability prerequisites expire before issuance");
        }
        return notAfter;
    }

    private static void requireProtection(
            final CredentialBindingProtection protection, final CredentialBinding binding, final long notAfter) {
        if (!binding.profile().equals(protection.profile())
                || binding.secretGeneration() != protection.secretGeneration()
                || !Bytes.constantTimeEquals(binding.bindingDigest(), protection.bindingDigest())
                || protection.nativeCapabilityProtectionUntilEpochMs() < notAfter) {
            throw new IllegalArgumentException("native credential protection is not exact or durable through expiry");
        }
    }
}
