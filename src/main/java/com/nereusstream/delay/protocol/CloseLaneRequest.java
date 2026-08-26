package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for closing one or more Destination Lanes. */
public final class CloseLaneRequest implements ControlOperationRequestBranch {
    private final ControlReason reason;
    private final ClosePolicy closePolicy;
    private final boolean allowOrderBreak;
    private final AcknowledgementSet acknowledgements;

    public CloseLaneRequest(
            final ControlReason reason,
            final ClosePolicy closePolicy,
            final boolean allowOrderBreak,
            final AcknowledgementSet acknowledgements) {
        this.reason = Objects.requireNonNull(reason, "reason");
        this.closePolicy = Objects.requireNonNull(closePolicy, "closePolicy");
        if (closePolicy != ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED) {
            throw new IllegalArgumentException("unsupported close policy");
        }
        this.allowOrderBreak = allowOrderBreak;
        this.acknowledgements = Objects.requireNonNull(acknowledgements, "acknowledgements");
        if (allowOrderBreak
                && (!acknowledgements.has(AcknowledgementKind.ORDER_LOSS)
                        || !acknowledgements.has(AcknowledgementKind.POSSIBLE_DUPLICATE))) {
            throw new IllegalArgumentException("allowOrderBreak requires ORDER_LOSS and POSSIBLE_DUPLICATE");
        }
    }

    public ControlReason reason() {
        return reason;
    }

    public ClosePolicy closePolicy() {
        return closePolicy;
    }

    public boolean allowOrderBreak() {
        return allowOrderBreak;
    }

    public AcknowledgementSet acknowledgements() {
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

    public static CloseLaneRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CloseLaneRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "CloseLaneRequest");
        final CloseLaneRequest result = new CloseLaneRequest(
                ControlReason.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ClosePolicy.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                QueryCodecSupport.bool(fields.get(2), 3),
                AcknowledgementSet.decode(QueryCodecSupport.nested(fields.get(3), 4)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CloseLaneRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CloseLaneRequest that
                && allowOrderBreak == that.allowOrderBreak
                && reason.equals(that.reason)
                && closePolicy == that.closePolicy
                && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason, closePolicy, allowOrderBreak, acknowledgements);
    }
}
