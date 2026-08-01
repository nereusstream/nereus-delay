package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Bare Message ID projection with explicit source-fence eligibility evidence. */
public final class UnknownMessageViewV1 implements QueryResponseBranchV1 {
    private final FirstScheduleEligibilityV1 firstScheduleEligibility;

    public UnknownMessageViewV1(final FirstScheduleEligibilityV1 firstScheduleEligibility) {
        this.firstScheduleEligibility = Objects.requireNonNull(firstScheduleEligibility, "firstScheduleEligibility");
    }

    public FirstScheduleEligibilityV1 firstScheduleEligibility() {
        return firstScheduleEligibility;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.uint32(output, 1, firstScheduleEligibility.wireValue()));
    }

    public static UnknownMessageViewV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "UnknownMessageViewV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "UnknownMessageViewV1");
        final UnknownMessageViewV1 result = new UnknownMessageViewV1(
                FirstScheduleEligibilityV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "UnknownMessageViewV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof UnknownMessageViewV1 that
                && firstScheduleEligibility == that.firstScheduleEligibility;
    }

    @Override
    public int hashCode() {
        return firstScheduleEligibility.hashCode();
    }
}
