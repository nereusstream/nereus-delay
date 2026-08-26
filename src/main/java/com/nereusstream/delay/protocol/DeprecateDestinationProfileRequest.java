package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 request that source-orders deprecation of one Destination Profile version. */
public final class DeprecateDestinationProfileRequest implements ControlOperationRequestBranch {
    private final ProfileRef profile;
    private final ControlReason reason;

    public DeprecateDestinationProfileRequest(final ProfileRef profile, final ControlReason reason) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKind.DESTINATION) {
            throw new IllegalArgumentException("deprecated Profile must have DESTINATION kind");
        }
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ProfileRef profile() {
        return profile;
    }

    public ControlReason reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
    }

    public static DeprecateDestinationProfileRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "DeprecateDestinationProfileRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "DeprecateDestinationProfileRequest");
        final DeprecateDestinationProfileRequest result = new DeprecateDestinationProfileRequest(
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ControlReason.decode(QueryCodecSupport.nested(fields.get(1), 2)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DeprecateDestinationProfileRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DeprecateDestinationProfileRequest that
                && profile.equals(that.profile)
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, reason);
    }
}
