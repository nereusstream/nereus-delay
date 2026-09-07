package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable source-applied membership claims. Decoding does not authenticate their issuer. */
public final class TargetMembershipGrant {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 22;
    public static final int MAX_REGISTRATION_BYTES = 2
            + 34
            + 3
            + TargetChannelIdentity.MAX_PROFILE_REF_BYTES
            + 2 * (4 + TargetDispatchCompatibility.MAX_CANONICAL_BYTES)
            + 3
            + TargetControlScope.MAX_CANONICAL_BYTES
            + 34
            + 34;
    public static final int MAX_CANONICAL_BYTES =
            MAX_REGISTRATION_BYTES + 34 + 4 + TargetSourcePosition.MAX_CANONICAL_BYTES + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-membership-grant\0");

    private final byte[] tenantScope;
    private final ProfileRef memberProfile;
    private final TargetDispatchCompatibility required;
    private final TargetDispatchCompatibility offered;
    private final TargetControlScope controls;
    private final byte[] authorityPolicyRef;
    private final byte[] authorityOperationId;
    private final byte[] sourceMutationDigest;
    private final SourcePosition activationSource;
    private final byte[] registration;
    private final byte[] digest;

    public TargetMembershipGrant(
            final byte[] tenantScope,
            final ProfileRef memberProfile,
            final TargetDispatchCompatibility required,
            final TargetDispatchCompatibility offered,
            final TargetControlScope controls,
            final byte[] authorityPolicyRef,
            final byte[] authorityOperationId,
            final byte[] sourceMutationDigest,
            final SourcePosition activationSource) {
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantScope");
        this.memberProfile = Objects.requireNonNull(memberProfile, "memberProfile");
        if (memberProfile.profileKind() != ProfileKind.DESTINATION
                || memberProfile.profileId().length > TargetChannelIdentity.MAX_PROFILE_ID_BYTES) {
            throw new IllegalArgumentException("Target membership requires a bounded Destination Profile");
        }
        TargetCompatibilityCodec.assigned(memberProfile.semanticHash(), 32, "memberProfileHash");
        this.required = Objects.requireNonNull(required, "required");
        this.offered = Objects.requireNonNull(offered, "offered");
        this.controls = Objects.requireNonNull(controls, "controls");
        this.activationSource = TargetSourcePosition.requireBounded(activationSource);
        if (!offered.canServe(required)
                || !controls.target().equals(offered.target())
                || !controls.sourceShard().equals(activationSource.shardId())) {
            throw new IllegalArgumentException("Target membership execution/control/source mismatch");
        }
        this.authorityPolicyRef = TargetCompatibilityCodec.assigned(authorityPolicyRef, 32, "authorityPolicyRef");
        this.authorityOperationId = TargetCompatibilityCodec.assigned(authorityOperationId, 32, "authorityOperationId");
        this.sourceMutationDigest = TargetCompatibilityCodec.assigned(sourceMutationDigest, 32, "sourceMutationDigest");
        registration = prepareRegistration(
                this.tenantScope,
                this.memberProfile,
                this.required,
                this.offered,
                this.controls,
                this.authorityPolicyRef,
                this.authorityOperationId);
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public ProfileRef memberProfile() {
        return memberProfile;
    }

    public TargetDispatchCompatibility required() {
        return required;
    }

    public TargetDispatchCompatibility offered() {
        return offered;
    }

    public TargetControlScope controls() {
        return controls;
    }

    public byte[] authorityPolicyRef() {
        return Bytes.copy(authorityPolicyRef);
    }

    public byte[] authorityOperationId() {
        return Bytes.copy(authorityOperationId);
    }

    public byte[] sourceMutationDigest() {
        return Bytes.copy(sourceMutationDigest);
    }

    public SourcePosition activationSource() {
        return activationSource;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.membershipGrant(digest);
    }

    /** Pre-append claims. Source mutation digest and application position are added only after source apply. */
    public byte[] registrationBytes() {
        return Bytes.copy(registration);
    }

    /** Creates the exact payload before its physical source position or enclosing mutation digest is known. */
    public static byte[] prepareRegistration(
            final byte[] tenantScope,
            final ProfileRef memberProfile,
            final TargetDispatchCompatibility required,
            final TargetDispatchCompatibility offered,
            final TargetControlScope controls,
            final byte[] authorityPolicyRef,
            final byte[] authorityOperationId) {
        final byte[] tenant = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantScope");
        final byte[] policy = TargetCompatibilityCodec.assigned(authorityPolicyRef, 32, "authorityPolicyRef");
        final byte[] operation = TargetCompatibilityCodec.assigned(authorityOperationId, 32, "authorityOperationId");
        if (memberProfile.profileKind() != ProfileKind.DESTINATION
                || memberProfile.profileId().length > TargetChannelIdentity.MAX_PROFILE_ID_BYTES
                || !offered.canServe(required)
                || !controls.target().equals(offered.target())) {
            throw new IllegalArgumentException("invalid Target membership registration claims");
        }
        TargetCompatibilityCodec.assigned(memberProfile.semanticHash(), 32, "memberProfileHash");
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, tenant);
            CanonicalProtobuf.bytes(out, 3, memberProfile.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, required.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, offered.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, controls.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, policy);
            CanonicalProtobuf.bytes(out, 8, operation);
        });
    }

    /** Called only after the enclosing mutation has passed authentication, authorization and source apply gates. */
    public static TargetMembershipGrant fromRegistration(
            final byte[] registration, final byte[] mutationDigest, final SourcePosition activationSource) {
        final var fields = TargetCompatibilityCodec.read(
                registration, MAX_REGISTRATION_BYTES, 8, false, "Target membership registration");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "Target membership registration");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target membership registration schema");
        }
        final byte[] profile = QueryCodecSupport.bytes(fields.get(2), 3);
        TargetCompatibilityCodec.read(
                profile, TargetChannelIdentity.MAX_PROFILE_REF_BYTES, 4, false, "Target membership ProfileRef");
        final var grant = new TargetMembershipGrant(
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                ProfileRef.decode(profile),
                TargetDispatchCompatibility.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                TargetDispatchCompatibility.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                TargetControlScope.decode(QueryCodecSupport.bytes(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(6), 7, 32),
                QueryCodecSupport.fixed(fields.get(7), 8, 32),
                mutationDigest,
                activationSource);
        QueryCodecSupport.requireCanonical(registration, grant.registrationBytes(), "Target membership registration");
        return grant;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(registration);
            CanonicalProtobuf.bytes(out, 9, sourceMutationDigest);
            CanonicalProtobuf.bytes(out, 10, activationSource.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 11, digest);
        });
    }

    public static TargetMembershipGrant decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 11, false, "TargetMembershipGrant");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "TargetMembershipGrant");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target membership schema");
        }
        final byte[] profile = QueryCodecSupport.bytes(fields.get(2), 3);
        TargetCompatibilityCodec.read(
                profile, TargetChannelIdentity.MAX_PROFILE_REF_BYTES, 4, false, "Target membership ProfileRef");
        final var result = new TargetMembershipGrant(
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                ProfileRef.decode(profile),
                TargetDispatchCompatibility.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                TargetDispatchCompatibility.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                TargetControlScope.decode(QueryCodecSupport.bytes(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(6), 7, 32),
                QueryCodecSupport.fixed(fields.get(7), 8, 32),
                QueryCodecSupport.fixed(fields.get(8), 9, 32),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(9), 10)));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 11, 32))) {
            throw new IllegalArgumentException("Target membership digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetMembershipGrant");
        return result;
    }

    public static TargetMembershipGrant decodeReferenced(final byte[] reference, final byte[] encoded) {
        final var result = decode(encoded);
        if (!Bytes.constantTimeEquals(reference, result.digest)) {
            throw new IllegalArgumentException("Target membership reference mismatch");
        }
        return result;
    }

    public static TargetMembershipGrant decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId expectedSourceShard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.encodedKey())
                || !expectedSourceShard.equals(result.activationSource.shardId())) {
            throw new IllegalArgumentException("Target membership Store key/Shard mismatch");
        }
        return result;
    }

    /** Exact registration payload, operation, source mutation and physical application position are all required. */
    public void requireSourceRegistration(
            final byte[] registration,
            final byte[] operationId,
            final byte[] mutationDigest,
            final SourcePosition appliedAt) {
        if (!Arrays.equals(registration, registrationBytes())
                || !Arrays.equals(operationId, authorityOperationId)
                || !Arrays.equals(mutationDigest, sourceMutationDigest)
                || !Arrays.equals(appliedAt.canonicalBytes(), activationSource.canonicalBytes())) {
            throw new IllegalArgumentException("Target membership authenticated source registration mismatch");
        }
    }

    /** Historical immutable linkage. First-binding closure and authenticated tenant provenance are external gates. */
    public void requireBindingProjection(final TargetScheduleBinding binding, final byte[] authenticatedTenantScope) {
        if (!Arrays.equals(tenantScope, authenticatedTenantScope)
                || !memberProfile.equals(binding.intent().profile())
                || !Arrays.equals(digest, binding.membershipGrantRef())
                || !offered.target().equals(binding.target())
                || !Arrays.equals(required.digest(), binding.requiredDispatchRef())
                || !Arrays.equals(offered.digest(), binding.offeredDispatchRef())
                || !Arrays.equals(controls.digest(), binding.controlScopeRef())
                || binding.bindingSource().compareTo(activationSource) <= 0) {
            throw new IllegalArgumentException("Target binding is outside its membership grant");
        }
    }

    /** The provider may rotate; the exact channel still needs live credential and resource authority. */
    public void requireChannelProjection(final TargetScheduleBinding binding, final TargetChannelIdentity channel) {
        requireBindingProjection(binding, tenantScope);
        final var context = channel.context();
        if (!binding.domain().equals(context.domain())
                || !Arrays.equals(binding.accountingIncarnation(), context.accountingIncarnation())
                || !controls.sourceShard().equals(context.sourceShard())
                || !offered.target().equals(context.target())
                || !Arrays.equals(offered.digest(), context.dispatchCompatibilityRef())
                || !Arrays.equals(controls.digest(), context.controlScopeRef())) {
            throw new IllegalArgumentException("Target channel is outside the binding membership domain");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetMembershipGrant that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
