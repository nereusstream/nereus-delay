package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Complete immutable policy approved by the control authority; decoding alone does not approve it. */
public final class TargetMembershipPolicy {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 23;
    public static final int COMMON_AUTHORIZED_PROVIDER = 1;
    public static final int MAX_CANONICAL_BYTES = 2
            + 34
            + 3
            + TargetChannelIdentity.MAX_PROFILE_REF_BYTES
            + 34
            + 4
            + TargetDispatchCompatibility.MAX_CANONICAL_BYTES
            + 3
            + TargetControlScope.MAX_CANONICAL_BYTES
            + 2
            + 34
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-membership-policy\0");
    private final byte[] tenantScope;
    private final ProfileRef memberProfile;
    private final byte[] requiredDispatchRef;
    private final TargetDispatchCompatibility offered;
    private final TargetControlScope controls;
    private final byte[] controlResourceScope;
    private final byte[] digest;

    public TargetMembershipPolicy(
            final byte[] tenantScope,
            final ProfileRef memberProfile,
            final byte[] requiredDispatchRef,
            final TargetDispatchCompatibility offered,
            final TargetControlScope controls,
            final byte[] controlResourceScope) {
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantScope");
        this.memberProfile = Objects.requireNonNull(memberProfile, "memberProfile");
        if (memberProfile.profileKind() != ProfileKind.DESTINATION
                || memberProfile.profileId().length > TargetChannelIdentity.MAX_PROFILE_ID_BYTES) {
            throw new IllegalArgumentException("membership policy requires a bounded Destination Profile");
        }
        TargetCompatibilityCodec.assigned(memberProfile.semanticHash(), 32, "memberProfileHash");
        this.requiredDispatchRef = TargetCompatibilityCodec.assigned(requiredDispatchRef, 32, "requiredDispatchRef");
        this.offered = Objects.requireNonNull(offered, "offered");
        this.controls = Objects.requireNonNull(controls, "controls");
        if (!offered.target().equals(controls.target())) {
            throw new IllegalArgumentException("membership policy execution/control target mismatch");
        }
        this.controlResourceScope = TargetCompatibilityCodec.assigned(controlResourceScope, 32, "controlResourceScope");
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public ProfileRef memberProfile() {
        return memberProfile;
    }

    public byte[] requiredDispatchRef() {
        return Bytes.copy(requiredDispatchRef);
    }

    public TargetDispatchCompatibility offered() {
        return offered;
    }

    public TargetControlScope controls() {
        return controls;
    }

    public byte[] controlResourceScope() {
        return Bytes.copy(controlResourceScope);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.membershipPolicy(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, tenantScope);
            CanonicalProtobuf.bytes(out, 3, memberProfile.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, requiredDispatchRef);
            CanonicalProtobuf.bytes(out, 5, offered.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, controls.canonicalBytes());
            CanonicalProtobuf.uint32(out, 7, COMMON_AUTHORIZED_PROVIDER);
            CanonicalProtobuf.bytes(out, 8, controlResourceScope);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetMembershipPolicy decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "TargetMembershipPolicy");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "TargetMembershipPolicy");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION
                || QueryCodecSupport.uint32(fields.get(6), 7) != COMMON_AUTHORIZED_PROVIDER) {
            throw new IllegalArgumentException("unknown membership policy schema or credential mode");
        }
        final byte[] profile = QueryCodecSupport.bytes(fields.get(2), 3);
        TargetCompatibilityCodec.read(
                profile, TargetChannelIdentity.MAX_PROFILE_REF_BYTES, 4, false, "membership policy ProfileRef");
        final var result = new TargetMembershipPolicy(
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                ProfileRef.decode(profile),
                QueryCodecSupport.fixed(fields.get(3), 4, 32),
                TargetDispatchCompatibility.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                TargetControlScope.decode(QueryCodecSupport.bytes(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(7), 8, 32));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 9, 32))) {
            throw new IllegalArgumentException("membership policy digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetMembershipPolicy");
        return result;
    }

    /** Validates the complete pre-append claims, and returns their exact operation ID. */
    public byte[] requireRegistration(final byte[] registration) {
        final var fields = TargetCompatibilityCodec.read(
                registration, TargetMembershipGrant.MAX_REGISTRATION_BYTES, 8, false, "Target membership registration");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "Target membership registration");
        final var required = TargetDispatchCompatibility.decode(QueryCodecSupport.bytes(fields.get(3), 4));
        final byte[] operation = QueryCodecSupport.fixed(fields.get(7), 8, 32);
        final byte[] expected = TargetMembershipGrant.prepareRegistration(
                tenantScope, memberProfile, required, offered, controls, digest, operation);
        if (!Arrays.equals(requiredDispatchRef, required.digest()) || !Arrays.equals(registration, expected)) {
            throw new IllegalArgumentException("registration differs from complete approved membership policy");
        }
        return operation;
    }

    public void requireGrant(final TargetMembershipGrant grant) {
        requireRegistration(grant.registrationBytes());
    }

    public static TargetMembershipPolicy decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.encodedKey()) || !shard.equals(result.controls.sourceShard())) {
            throw new IllegalArgumentException("membership policy Store key/Shard mismatch");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetMembershipPolicy that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
