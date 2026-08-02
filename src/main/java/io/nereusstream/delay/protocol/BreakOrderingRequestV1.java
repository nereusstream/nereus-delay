package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch that explicitly breaks an Ordering Domain. */
public final class BreakOrderingRequestV1 implements ControlOperationRequestBranchV1 {
    private final AcknowledgementSetV1 acknowledgements;

    public BreakOrderingRequestV1(final AcknowledgementSetV1 acknowledgements) {
        this.acknowledgements = Objects.requireNonNull(acknowledgements, "acknowledgements");
        if (!acknowledgements.has(AcknowledgementKindV1.ORDER_LOSS)
                || !acknowledgements.has(AcknowledgementKindV1.POSSIBLE_DUPLICATE)) {
            throw new IllegalArgumentException("breaking ordering requires ORDER_LOSS and POSSIBLE_DUPLICATE");
        }
    }

    public AcknowledgementSetV1 acknowledgements() {
        return acknowledgements;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, acknowledgements.canonicalBytes()));
    }

    public static BreakOrderingRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "BreakOrderingRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "BreakOrderingRequestV1");
        final BreakOrderingRequestV1 result = new BreakOrderingRequestV1(
                AcknowledgementSetV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "BreakOrderingRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof BreakOrderingRequestV1 that && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledgements);
    }
}
