package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 request that source-orders deprecation of one Destination Profile version. */
public final class DeprecateDestinationProfileRequestV1 implements ControlOperationRequestBranchV1 {
    private final ProfileRefV1 profile;
    private final ControlReasonV1 reason;

    public DeprecateDestinationProfileRequestV1(final ProfileRefV1 profile, final ControlReasonV1 reason) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION) {
            throw new IllegalArgumentException("deprecated Profile must have DESTINATION kind");
        }
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, reason.canonicalBytes());
        });
    }

    public static DeprecateDestinationProfileRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "DeprecateDestinationProfileRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "DeprecateDestinationProfileRequestV1");
        final DeprecateDestinationProfileRequestV1 result = new DeprecateDestinationProfileRequestV1(
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(1), 2)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                "DeprecateDestinationProfileRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DeprecateDestinationProfileRequestV1 that
                && profile.equals(that.profile)
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, reason);
    }
}
