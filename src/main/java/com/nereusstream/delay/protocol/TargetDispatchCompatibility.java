package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Normalized, immutable channel requirements; Profile identity and mutable credential generations are excluded. */
public final class TargetDispatchCompatibility {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 18;
    public static final int MAX_CAPABILITY_BYTES =
            2 + 2 + 2 + 4 + (CanonicalTargetPartition.MAX_CANONICAL_BYTES - 9) + 6 + 11 + 11 + 11 + 34 + 34 + 6 + 6;
    public static final int MAX_CANONICAL_BYTES = 2 + 34 + 34 + 6 + 4 + MAX_CAPABILITY_BYTES + 34 + 11 + 11 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-dispatch-compatibility\0");
    private final TargetPartitionId target;
    private final byte[] authorizationScope;
    private final int adapterEncodingVersion;
    private final DeliveryCapabilitySemantic capability;
    private final byte[] capabilityBytes;
    private final byte[] targetPrerequisitePolicyDigest;
    private final long minimumTargetTopicTtlMs;
    private final long minimumTargetTopicRetentionMs;
    private final byte[] digest;

    public TargetDispatchCompatibility(
            final TargetPartitionId target,
            final byte[] authorizationScope,
            final int adapterEncodingVersion,
            final DeliveryCapabilitySemantic capability,
            final byte[] targetPrerequisitePolicyDigest,
            final long minimumTargetTopicTtlMs,
            final long minimumTargetTopicRetentionMs) {
        this.target = Objects.requireNonNull(target, "target");
        this.authorizationScope = TargetCompatibilityCodec.assigned(authorizationScope, 32, "authorizationScope");
        if (adapterEncodingVersion <= 0) {
            throw new IllegalArgumentException("Target adapter encoding version must be positive");
        }
        this.adapterEncodingVersion = adapterEncodingVersion;
        this.capability = Objects.requireNonNull(capability, "capability");
        if (capability.requiresEvidenceResource()) {
            // Evidence identity uses the same closed physical-resource bounds as Target identity.
            new CanonicalTargetPartition(capability.evidenceResource(), 0);
            TargetCompatibilityCodec.assigned(capability.brokerPrerequisiteDigest(), 32, "brokerPrerequisiteDigest");
            TargetCompatibilityCodec.assigned(capability.sourceLockDigest(), 32, "sourceLockDigest");
        }
        capabilityBytes = capability.canonicalBytes();
        if (capabilityBytes.length > MAX_CAPABILITY_BYTES) {
            throw new IllegalArgumentException("Target capability exceeds its byte bound");
        }
        this.targetPrerequisitePolicyDigest =
                TargetCompatibilityCodec.assigned(targetPrerequisitePolicyDigest, 32, "targetPrerequisitePolicyDigest");
        if (minimumTargetTopicTtlMs < 0 || minimumTargetTopicRetentionMs < 0) {
            throw new IllegalArgumentException("Target topic lifetime requirements must be nonnegative");
        }
        this.minimumTargetTopicTtlMs = minimumTargetTopicTtlMs;
        this.minimumTargetTopicRetentionMs = minimumTargetTopicRetentionMs;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    /**
     * Validates immutable Profile projections. Live activation, credential attestation
     * and resource guards remain required.
     */
    public static TargetDispatchCompatibility fromProfiles(
            final CanonicalTargetPartition identity,
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capabilityProfile) {
        if (!(destination.body() instanceof DestinationProfileSemantic dest)
                || !(capabilityProfile.body() instanceof DeliveryCapabilitySemantic cap)
                || !dest.deliveryCapability().equals(capabilityProfile.ref())
                || !dest.targetResource().equals(identity.resource())
                || dest.adapterKind() != cap.adapterKind()
                || identity.physicalPartition() >= Integer.toUnsignedLong(dest.targetPartitionCount())
                || (dest.targetPartitionPolicy() == TargetPartitionPolicy.EXPLICIT_ONLY
                        && !dest.allowedExplicitPartitions().contains((int) identity.physicalPartition()))) {
            throw new IllegalArgumentException("Target dispatch Profile/resource/partition projection mismatch");
        }
        final var result = new TargetDispatchCompatibility(
                identity.id(),
                dest.credentialAuthorizationScopeDigest(),
                dest.adapterEncodingVersion(),
                cap,
                dest.prerequisitePolicyDigest(),
                dest.minimumTopicTtlMs(),
                dest.minimumTopicRetentionMs());
        result.requireTargetProjection(identity);
        return result;
    }

    public TargetPartitionId target() {
        return target;
    }

    public byte[] authorizationScope() {
        return Bytes.copy(authorizationScope);
    }

    public int adapterEncodingVersion() {
        return adapterEncodingVersion;
    }

    public DeliveryCapabilitySemantic capability() {
        return capability;
    }

    public byte[] targetPrerequisitePolicyDigest() {
        return Bytes.copy(targetPrerequisitePolicyDigest);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public long minimumTargetTopicTtlMs() {
        return minimumTargetTopicTtlMs;
    }

    public long minimumTargetTopicRetentionMs() {
        return minimumTargetTopicRetentionMs;
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.dispatchCompatibility(digest);
    }

    /**
     * An already bound execution class may satisfy a weaker Profile requirement without changing its identity.
     * Offered guarantees and producer ceilings must themselves be certified; this comparison grants no authority.
     */
    public boolean canServe(final TargetDispatchCompatibility required) {
        final DeliveryCapabilitySemantic need = required.capability;
        return target.equals(required.target)
                && Arrays.equals(authorizationScope, required.authorizationScope)
                && adapterEncodingVersion == required.adapterEncodingVersion
                && Arrays.equals(targetPrerequisitePolicyDigest, required.targetPrerequisitePolicyDigest)
                && capability.adapterKind() == need.adapterKind()
                && capability.outcomeCapability() == need.outcomeCapability()
                && TimingCapability.includes(capability.timingCapabilityBits(), need.timingCapabilityBits())
                && Objects.equals(capability.evidenceResource(), need.evidenceResource())
                && capability.evidencePartitionCount() == need.evidencePartitionCount()
                && capability.minimumEvidenceRetentionMs() >= need.minimumEvidenceRetentionMs()
                && capability.minimumDedupHorizonMs() >= need.minimumDedupHorizonMs()
                && capability.maximumCertifiedProducerKeys() <= need.maximumCertifiedProducerKeys()
                && Arrays.equals(capability.brokerPrerequisiteDigest(), need.brokerPrerequisiteDigest())
                && Arrays.equals(capability.sourceLockDigest(), need.sourceLockDigest())
                && capability.adapterConformanceVersion() == need.adapterConformanceVersion()
                && capability.rejectionClassifierVersion() == need.rejectionClassifierVersion()
                && minimumTargetTopicTtlMs >= required.minimumTargetTopicTtlMs
                && minimumTargetTopicRetentionMs >= required.minimumTargetTopicRetentionMs;
    }

    public void requireTargetProjection(final CanonicalTargetPartition identity) {
        if (!target.equals(identity.id())
                || ((capability.adapterKind() == AdapterKind.KAFKA)
                        != (identity.resource().kind() == BrokerResourceIdentity.Kind.KAFKA))) {
            throw new IllegalArgumentException("Target dispatch physical adapter/identity mismatch");
        }
        if (capability.outcomeCapability() == OutcomeCapability.KAFKA_TRANSACTIONAL_RECEIPT
                && !capability
                        .evidenceResource()
                        .kafka()
                        .authenticatedClusterId()
                        .equals(identity.resource().kafka().authenticatedClusterId())) {
            throw new IllegalArgumentException("Kafka target and receipt must share the authenticated cluster");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, target.bytes());
            CanonicalProtobuf.bytes(out, 3, authorizationScope);
            CanonicalProtobuf.uint32(out, 4, adapterEncodingVersion);
            CanonicalProtobuf.bytes(out, 5, capabilityBytes);
            CanonicalProtobuf.bytes(out, 6, targetPrerequisitePolicyDigest);
            CanonicalProtobuf.uint64(out, 7, minimumTargetTopicTtlMs);
            CanonicalProtobuf.uint64(out, 8, minimumTargetTopicRetentionMs);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetDispatchCompatibility decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "TargetDispatchCompatibility");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "TargetDispatchCompatibility");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target dispatch compatibility schema");
        }
        final byte[] cap = QueryCodecSupport.bytes(fields.get(4), 5);
        TargetCompatibilityCodec.read(cap, MAX_CAPABILITY_BYTES, 12, false, "Target delivery capability");
        final var result = new TargetDispatchCompatibility(
                new TargetPartitionId(QueryCodecSupport.fixed(fields.get(1), 2, 32)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                QueryCodecSupport.uint32(fields.get(3), 4),
                DeliveryCapabilitySemantic.decode(cap),
                QueryCodecSupport.fixed(fields.get(5), 6, 32),
                QueryCodecSupport.uint(fields.get(6), 7),
                QueryCodecSupport.uint(fields.get(7), 8));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(8), 9, 32))) {
            throw new IllegalArgumentException("Target dispatch compatibility digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetDispatchCompatibility");
        return result;
    }

    public static TargetDispatchCompatibility decodeReferenced(
            final byte[] reference, final byte[] encoded, final CanonicalTargetPartition identity) {
        final var result = decode(encoded);
        if (!Bytes.constantTimeEquals(reference, result.digest)) {
            throw new IllegalArgumentException("Target dispatch reference mismatch");
        }
        result.requireTargetProjection(identity);
        return result;
    }

    public static TargetDispatchCompatibility decodeForStore(
            final byte[] key, final byte[] encoded, final CanonicalTargetPartition identity) {
        final var result = decode(encoded);
        result.requireTargetProjection(identity);
        if (!Arrays.equals(key, result.encodedKey())) {
            throw new IllegalArgumentException("Target dispatch Store key mismatch");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetDispatchCompatibility that
                && target.equals(that.target)
                && Arrays.equals(authorizationScope, that.authorizationScope)
                && adapterEncodingVersion == that.adapterEncodingVersion
                && Arrays.equals(capabilityBytes, that.capabilityBytes)
                && Arrays.equals(targetPrerequisitePolicyDigest, that.targetPrerequisitePolicyDigest)
                && minimumTargetTopicTtlMs == that.minimumTargetTopicTtlMs
                && minimumTargetTopicRetentionMs == that.minimumTargetTopicRetentionMs;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
