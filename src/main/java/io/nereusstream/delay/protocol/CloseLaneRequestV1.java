package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for closing one or more Destination Lanes. */
public final class CloseLaneRequestV1 implements ControlOperationRequestBranchV1 {
    private final ControlReasonV1 reason;
    private final ClosePolicyV1 closePolicy;
    private final boolean allowOrderBreak;
    private final AcknowledgementSetV1 acknowledgements;

    public CloseLaneRequestV1(final ControlReasonV1 reason, final ClosePolicyV1 closePolicy,
                              final boolean allowOrderBreak, final AcknowledgementSetV1 acknowledgements) {
        this.reason = Objects.requireNonNull(reason, "reason");
        this.closePolicy = Objects.requireNonNull(closePolicy, "closePolicy");
        if (closePolicy != ClosePolicyV1.V1_FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED) {
            throw new IllegalArgumentException("unsupported V1 close policy");
        }
        this.allowOrderBreak = allowOrderBreak;
        this.acknowledgements = Objects.requireNonNull(acknowledgements, "acknowledgements");
        if (allowOrderBreak && (!acknowledgements.has(AcknowledgementKindV1.ORDER_LOSS)
                || !acknowledgements.has(AcknowledgementKindV1.POSSIBLE_DUPLICATE))) {
            throw new IllegalArgumentException("allowOrderBreak requires ORDER_LOSS and POSSIBLE_DUPLICATE");
        }
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    public ClosePolicyV1 closePolicy() {
        return closePolicy;
    }

    public boolean allowOrderBreak() {
        return allowOrderBreak;
    }

    public AcknowledgementSetV1 acknowledgements() {
        return acknowledgements;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reason.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, closePolicy.wireValue());
            CanonicalProtobuf.uint32(output, 3, allowOrderBreak ? 1 : 0);
            CanonicalProtobuf.bytes(output, 4, acknowledgements.canonicalBytes());
        });
    }

    public static CloseLaneRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CloseLaneRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "CloseLaneRequestV1");
        final CloseLaneRequestV1 result = new CloseLaneRequestV1(
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ClosePolicyV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                QueryCodecSupport.bool(fields.get(2), 3),
                AcknowledgementSetV1.decode(QueryCodecSupport.nested(fields.get(3), 4)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CloseLaneRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CloseLaneRequestV1 that && allowOrderBreak == that.allowOrderBreak
                && reason.equals(that.reason) && closePolicy == that.closePolicy
                && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason, closePolicy, allowOrderBreak, acknowledgements);
    }
}
