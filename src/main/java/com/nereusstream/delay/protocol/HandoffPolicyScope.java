package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Canonical preimage helper for a target/partition-scoped handoff lease. */
public final class HandoffPolicyScope {
    private static final String HASH_DOMAIN = "nereus-delay-handoff-policy-scope\0";

    private HandoffPolicyScope() {}

    public static byte[] digest(
            final byte[] tenantRouteScopeDigest,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final BrokerResourceIdentity targetResource,
            final long physicalPartition,
            final OrderingMode orderingMode,
            final int allowedPathBits,
            final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(tenantRouteScopeDigest, "tenantRouteScopeDigest");
        Objects.requireNonNull(destinationProfile, "destinationProfile");
        Objects.requireNonNull(capabilityProfile, "capabilityProfile");
        Objects.requireNonNull(targetResource, "targetResource");
        Objects.requireNonNull(orderingMode, "orderingMode");
        Objects.requireNonNull(artifacts, "artifacts");
        if (tenantRouteScopeDigest.length == 0 || physicalPartition < 0 || physicalPartition > 0xffff_ffffL) {
            throw new IllegalArgumentException("invalid handoff policy scope");
        }
        HandoffPath.requireValid(allowedPathBits);
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, tenantRouteScopeDigest);
            CanonicalProtobuf.bytes(output, 2, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, destinationProfile.semanticHash());
            CanonicalProtobuf.bytes(output, 4, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, capabilityProfile.semanticHash());
            CanonicalProtobuf.bytes(output, 6, targetResource.canonicalBytes());
            CanonicalProtobuf.uint32(output, 7, physicalPartition);
            CanonicalProtobuf.uint32(output, 8, orderingMode.wireValue());
            CanonicalProtobuf.uint32Bits(output, 9, allowedPathBits);
            CanonicalProtobuf.bytes(output, 10, artifacts.setDigest());
        });
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), fields);
    }
}
