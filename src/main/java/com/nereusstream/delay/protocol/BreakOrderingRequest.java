package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch that explicitly breaks an Ordering Domain. */
public final class BreakOrderingRequest implements ControlOperationRequestBranch {
    private final AcknowledgementSet acknowledgements;

    public BreakOrderingRequest(final AcknowledgementSet acknowledgements) {
        this.acknowledgements = Objects.requireNonNull(acknowledgements, "acknowledgements");
        if (!acknowledgements.has(AcknowledgementKind.ORDER_LOSS)
                || !acknowledgements.has(AcknowledgementKind.POSSIBLE_DUPLICATE)) {
            throw new IllegalArgumentException("breaking ordering requires ORDER_LOSS and POSSIBLE_DUPLICATE");
        }
    }

    public AcknowledgementSet acknowledgements() {
        return acknowledgements;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, acknowledgements.canonicalBytes()));
    }

    public static BreakOrderingRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "BreakOrderingRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "BreakOrderingRequest");
        final BreakOrderingRequest result =
                new BreakOrderingRequest(AcknowledgementSet.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "BreakOrderingRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof BreakOrderingRequest that && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledgements);
    }
}
