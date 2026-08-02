package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 2: activate one immutable Profile version for new bindings. */
public final class ProfileBindingActivatePayloadV1 {
    private final ProfileRefV1 profile;

    public ProfileBindingActivatePayloadV1(final ProfileRefV1 profile) {
        this.profile = Objects.requireNonNull(profile, "profile");
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes()));
    }

    public static ProfileBindingActivatePayloadV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ProfileBindingActivatePayloadV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "ProfileBindingActivatePayloadV1");
        final ProfileBindingActivatePayloadV1 result = new ProfileBindingActivatePayloadV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileBindingActivatePayloadV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileBindingActivatePayloadV1 that && profile.equals(that.profile);
    }

    @Override
    public int hashCode() {
        return profile.hashCode();
    }
}
