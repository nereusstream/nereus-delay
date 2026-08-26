package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 2: activate one immutable Profile version for new bindings. */
public final class ProfileBindingActivatePayload {
    private final ProfileRef profile;

    public ProfileBindingActivatePayload(final ProfileRef profile) {
        this.profile = Objects.requireNonNull(profile, "profile");
    }

    public ProfileRef profile() {
        return profile;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes()));
    }

    public static ProfileBindingActivatePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ProfileBindingActivatePayload");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "ProfileBindingActivatePayload");
        final ProfileBindingActivatePayload result =
                new ProfileBindingActivatePayload(ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileBindingActivatePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileBindingActivatePayload that && profile.equals(that.profile);
    }

    @Override
    public int hashCode() {
        return profile.hashCode();
    }
}
