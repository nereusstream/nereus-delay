package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Bare Message ID projection with explicit source-fence eligibility evidence. */
public final class UnknownMessageView implements QueryResponseBranch {
    private final FirstScheduleEligibility firstScheduleEligibility;

    public UnknownMessageView(final FirstScheduleEligibility firstScheduleEligibility) {
        this.firstScheduleEligibility = Objects.requireNonNull(firstScheduleEligibility, "firstScheduleEligibility");
    }

    public FirstScheduleEligibility firstScheduleEligibility() {
        return firstScheduleEligibility;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.uint32(output, 1, firstScheduleEligibility.wireValue()));
    }

    public static UnknownMessageView decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "UnknownMessageView");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "UnknownMessageView");
        final UnknownMessageView result =
                new UnknownMessageView(FirstScheduleEligibility.fromWire(QueryCodecSupport.uint(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "UnknownMessageView");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof UnknownMessageView that && firstScheduleEligibility == that.firstScheduleEligibility;
    }

    @Override
    public int hashCode() {
        return firstScheduleEligibility.hashCode();
    }
}
