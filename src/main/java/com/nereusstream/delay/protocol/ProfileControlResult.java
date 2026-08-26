package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Public-safe result for one Profile control target. */
public final class ProfileControlResult {
    private final ProfileRef profile;
    private final ProfileAcceptance acceptance;
    private final Long currentSecretGeneration;

    public ProfileControlResult(
            final ProfileRef profile, final ProfileAcceptance acceptance, final Long currentSecretGeneration) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.acceptance = Objects.requireNonNull(acceptance, "acceptance");
        final boolean secretProfile =
                profile.profileKind() == ProfileKind.DESTINATION || profile.profileKind() == ProfileKind.OBJECT_STORE;
        if (secretProfile != (currentSecretGeneration != null)) {
            throw new IllegalArgumentException("current secret generation presence does not match Profile kind");
        }
        if (currentSecretGeneration != null && currentSecretGeneration == 0) {
            throw new IllegalArgumentException("currentSecretGeneration must be non-zero");
        }
        this.currentSecretGeneration = currentSecretGeneration;
    }

    public ProfileRef profile() {
        return profile;
    }

    public ProfileAcceptance acceptance() {
        return acceptance;
    }

    public Long currentSecretGeneration() {
        return currentSecretGeneration;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, acceptance.wireValue());
            if (currentSecretGeneration != null) {
                CanonicalProtobuf.uint64Bits(output, 3, currentSecretGeneration);
            }
        });
    }

    public static ProfileControlResult decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ProfileControlResult");
        if (fields.size() != 2 && fields.size() != 3) {
            throw new IllegalArgumentException("invalid ProfileControlResult field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || (fields.size() == 3 && fields.get(2).number() != 3)) {
            throw new IllegalArgumentException("invalid ProfileControlResult field order");
        }
        final ProfileControlResult result = new ProfileControlResult(
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ProfileAcceptance.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                fields.size() == 3 ? QueryCodecSupport.uint(fields.get(2), 3) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileControlResult");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileControlResult that
                && profile.equals(that.profile)
                && acceptance == that.acceptance
                && Objects.equals(currentSecretGeneration, that.currentSecretGeneration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, acceptance, currentSecretGeneration);
    }
}
