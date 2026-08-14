package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.TimingCapabilityV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ProfileCatalog;

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

    public NativeCapabilitySnapshotIssuer(final ProfileCatalog profileCatalog,
                                          final NativeCapabilityIssuanceAuthority authority,
                                          final PublicKey routeVerificationKey,
                                          final PublicKey credentialAttestationKey,
                                          final PrivateKey issuerKey,
                                          final int issuerSigningKeyVersion,
                                          final long maximumSnapshotLifetimeMs) {
        this.profileCatalog = Objects.requireNonNull(profileCatalog, "profileCatalog");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.routeVerificationKey = Objects.requireNonNull(routeVerificationKey, "routeVerificationKey");
        this.credentialAttestationKey = Objects.requireNonNull(credentialAttestationKey,
                "credentialAttestationKey");
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
    public NativePreparationSnapshotV1 issue(final AuthenticatedTenantContext context,
                                             final RouteSnapshotV1 route,
                                             final ProfileRefV1 destinationReference,
                                             final int physicalPartition,
                                             final long brokerDeliverAtEpochMs,
                                             final TrustedUtcIntervalEvidence issuedAt) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(route, "route");
        final ProfileRefV1 destinationRef = Objects.requireNonNull(destinationReference,
                "destinationReference");
        Objects.requireNonNull(issuedAt, "issuedAt");
        if (physicalPartition < 0 || brokerDeliverAtEpochMs < 0) {
            throw new IllegalArgumentException("native issuance partition/time is invalid");
        }
        final RouteSnapshotV1 verifiedRoute = RouteSnapshotV1.decode(route.canonicalBytes(), routeVerificationKey);
        if (brokerDeliverAtEpochMs > verifiedRoute.validUntilEpochMs()) {
            throw new IllegalArgumentException("native broker delivery time exceeds the Route validity");
        }
        verifiedRoute.requireUsableForNewSchedule(context.authenticatedTenantScopeHash(), context.tenantRoutingScope(),
                issuedAt.latestEpochMs());

        final ProfileSemanticEnvelopeV1 destination = exactProfile(destinationRef, ProfileKindV1.DESTINATION);
        if (!(destination.body() instanceof DestinationProfileSemanticV1 destinationBody)
                || destinationBody.adapterKind() != io.nereusstream.delay.protocol.AdapterKindV1.PULSAR) {
            throw new IllegalArgumentException("native issuance requires a Pulsar Destination Profile");
        }
        if (Integer.toUnsignedLong(physicalPartition)
                >= Integer.toUnsignedLong(destinationBody.targetPartitionCount())
                || destinationBody.targetPartitionPolicy()
                == io.nereusstream.delay.protocol.TargetPartitionPolicyV1.EXPLICIT_ONLY
                && !destinationBody.allowedExplicitPartitions().contains(physicalPartition)) {
            throw new IllegalArgumentException("native issuance partition violates the Destination Profile");
        }
        final ProfileRefV1 capabilityRef = destinationBody.deliveryCapability();
        final ProfileSemanticEnvelopeV1 capability = exactProfile(capabilityRef,
                ProfileKindV1.DELIVERY_CAPABILITY);
        if (!(capability.body() instanceof DeliveryCapabilitySemanticV1 capabilityBody)
                || capabilityBody.adapterKind() != io.nereusstream.delay.protocol.AdapterKindV1.PULSAR
                || !TimingCapabilityV1.includes(capabilityBody.timingCapabilityBits(),
                TimingCapabilityV1.PULSAR_AUTO_FAST)) {
            throw new IllegalArgumentException("native issuance requires a Pulsar AUTO_FAST capability");
        }

        final CredentialBindingHeadV1 head = Objects.requireNonNull(
                profileCatalog.resolveHead(destinationRef), "destination credential Head");
        if (!destinationRef.equals(head.profile())) {
            throw new IllegalArgumentException("credential Head does not belong to the Destination Profile");
        }
        final CredentialBindingV1 binding = Objects.requireNonNull(
                profileCatalog.resolveBinding(destinationRef, head.secretGeneration()),
                "current destination credential binding");
        if (!destinationRef.equals(binding.profile())
                || !Bytes.constantTimeEquals(binding.bindingDigest(), head.bindingDigest())) {
            throw new IllegalArgumentException("credential binding Head does not identify exact immutable bytes");
        }
        final CredentialEquivalenceAttestationV1 attestation = binding.equivalenceAttestation();
        if (!attestation.verifySignature(credentialAttestationKey)) {
            throw new IllegalArgumentException("credential equivalence attestation signature is invalid");
        }
        attestation.requireAuthorizationScopeDigest(destinationBody.credentialAuthorizationScopeDigest());

        final NativeCapabilityIssuanceAuthority.GuardEvidence guard = Objects.requireNonNull(
                authority.resolveGuard(destinationRef, capabilityRef, physicalPartition,
                        context.principalScopeHash(), issuedAt), "native guard evidence");
        final PulsarBrokerResourceIdentityV1 expectedTarget = destinationBody.targetResource().pulsar();
        if (!expectedTarget.equals(guard.target())
                || guard.physicalPartition() != physicalPartition
                || !Bytes.constantTimeEquals(guard.principalScopeDigest(), context.principalScopeHash())
                || guard.validUntilEpochMs() <= issuedAt.latestEpochMs()) {
            throw new IllegalArgumentException("native guard evidence does not match the requested scope");
        }

        final long notAfter = boundedNotAfter(issuedAt, verifiedRoute, attestation, guard);
        final CredentialBindingProtectionV1 protectedBinding = Objects.requireNonNull(
                authority.protectNativeCapability(binding, notAfter), "native credential protection");
        requireProtection(protectedBinding, binding, notAfter);

        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destinationRef, capabilityRef,
                guard.target(), physicalPartition, guard.resourceGuardAttestationSha256(),
                guard.resourceGuardConfigGeneration(), binding.secretGeneration(), binding.bindingDigest(),
                attestation.resolvedCredentialFingerprintDigest(), context.principalScopeHash(), issuedAt,
                notAfter, issuerSigningKeyVersion, issuerKey);
        return new NativePreparationSnapshotV1(destination, capability, guard.target(), physicalPartition, snapshot,
                brokerDeliverAtEpochMs);
    }

    private ProfileSemanticEnvelopeV1 exactProfile(final ProfileRefV1 reference, final ProfileKindV1 expectedKind) {
        final ProfileSemanticEnvelopeV1 profile = Objects.requireNonNull(profileCatalog.resolve(reference),
                "Profile semantic");
        final ProfileSemanticEnvelopeV1 canonical = ProfileSemanticEnvelopeV1.decode(profile.canonicalBytes());
        if (!canonical.equals(profile) || canonical.profileKind() != expectedKind
                || !canonical.ref().equals(reference)) {
            throw new IllegalArgumentException("Profile catalog returned non-exact semantic bytes");
        }
        return canonical;
    }

    private long boundedNotAfter(final TrustedUtcIntervalEvidence issuedAt, final RouteSnapshotV1 route,
                                 final CredentialEquivalenceAttestationV1 attestation,
                                 final NativeCapabilityIssuanceAuthority.GuardEvidence guard) {
        final long issuerBound;
        try {
            issuerBound = Math.addExact(issuedAt.earliestEpochMs(), maximumSnapshotLifetimeMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("native issuer lifetime overflows epoch range", overflow);
        }
        final long notAfter = Math.min(Math.min(issuerBound, route.validUntilEpochMs()),
                Math.min(attestation.notAfterEpochMs(), guard.validUntilEpochMs()));
        if (notAfter <= issuedAt.latestEpochMs()) {
            throw new IllegalArgumentException("native capability prerequisites expire before issuance");
        }
        return notAfter;
    }

    private static void requireProtection(final CredentialBindingProtectionV1 protection,
                                          final CredentialBindingV1 binding, final long notAfter) {
        if (!binding.profile().equals(protection.profile())
                || binding.secretGeneration() != protection.secretGeneration()
                || !Bytes.constantTimeEquals(binding.bindingDigest(), protection.bindingDigest())
                || protection.nativeCapabilityProtectionUntilEpochMs() < notAfter) {
            throw new IllegalArgumentException("native credential protection is not exact or durable through expiry");
        }
    }
}
