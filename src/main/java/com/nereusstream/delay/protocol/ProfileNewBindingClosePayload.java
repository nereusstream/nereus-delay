package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 3: close one immutable Profile version for new bindings. */
public final class ProfileNewBindingClosePayload {
    private final ProfileRef profile;
    private final ControlReason reason;

    public ProfileNewBindingClosePayload(final ProfileRef profile, final ControlReason reason) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ProfileRef profile() {
        return profile;
    }

    public ControlReason reason() {
        return reason;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
    }

    public static ProfileNewBindingClosePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ProfileNewBindingClosePayload");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "ProfileNewBindingClosePayload");
        final ProfileNewBindingClosePayload result = new ProfileNewBindingClosePayload(
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ControlReason.decode(QueryCodecSupport.nested(fields.get(1), 2)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileNewBindingClosePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileNewBindingClosePayload that
                && profile.equals(that.profile)
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, reason);
    }
}
