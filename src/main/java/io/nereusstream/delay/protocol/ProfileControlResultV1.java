package io.nereusstream.delay.protocol;

import java.util.Objects;

/** Public-safe result for one Profile control target. */
public final class ProfileControlResultV1 {
    private final ProfileRefV1 profile;
    private final ProfileAcceptanceV1 acceptance;
    private final Long currentSecretGeneration;

    public ProfileControlResultV1(final ProfileRefV1 profile, final ProfileAcceptanceV1 acceptance,
                                  final Long currentSecretGeneration) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.acceptance = Objects.requireNonNull(acceptance, "acceptance");
        final boolean secretProfile = profile.profileKind() == ProfileKindV1.DESTINATION
                || profile.profileKind() == ProfileKindV1.OBJECT_STORE;
        if (secretProfile != (currentSecretGeneration != null)) {
            throw new IllegalArgumentException("current secret generation presence does not match Profile kind");
        }
        if (currentSecretGeneration != null && currentSecretGeneration <= 0) {
            throw new IllegalArgumentException("currentSecretGeneration must be positive");
        }
        this.currentSecretGeneration = currentSecretGeneration;
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public ProfileAcceptanceV1 acceptance() {
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
                CanonicalProtobuf.uint64(output, 3, currentSecretGeneration);
            }
        });
    }

    public static ProfileControlResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ProfileControlResultV1");
        if (fields.size() != 2 && fields.size() != 3) {
            throw new IllegalArgumentException("invalid ProfileControlResultV1 field count");
        }
        if (fields.get(0).number() != 1 || fields.get(1).number() != 2
                || (fields.size() == 3 && fields.get(2).number() != 3)) {
            throw new IllegalArgumentException("invalid ProfileControlResultV1 field order");
        }
        final ProfileControlResultV1 result = new ProfileControlResultV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ProfileAcceptanceV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                fields.size() == 3 ? QueryCodecSupport.uint(fields.get(2), 3) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileControlResultV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileControlResultV1 that && profile.equals(that.profile)
                && acceptance == that.acceptance && Objects.equals(currentSecretGeneration,
                that.currentSecretGeneration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, acceptance, currentSecretGeneration);
    }
}
