package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 3: close one immutable Profile version for new bindings. */
public final class ProfileNewBindingClosePayloadV1 {
    private final ProfileRefV1 profile;
    private final ControlReasonV1 reason;

    public ProfileNewBindingClosePayloadV1(final ProfileRefV1 profile, final ControlReasonV1 reason) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
    }

    public static ProfileNewBindingClosePayloadV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ProfileNewBindingClosePayloadV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "ProfileNewBindingClosePayloadV1");
        final ProfileNewBindingClosePayloadV1 result = new ProfileNewBindingClosePayloadV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(1), 2)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileNewBindingClosePayloadV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileNewBindingClosePayloadV1 that
                && profile.equals(that.profile)
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, reason);
    }
}
