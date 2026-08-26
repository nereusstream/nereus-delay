package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for explicit replay of a terminal Dead Letter generation. */
public final class ReplayDeadLetterRequest implements ControlOperationRequestBranch {
    private final long deliverAt;
    private final long expireAt;
    private final RetryPolicyRef retryPolicy;
    private final boolean allowPossibleDuplicate;
    private final AcknowledgementSet acknowledgements;

    public ReplayDeadLetterRequest(
            final long deliverAt,
            final long expireAt,
            final RetryPolicyRef retryPolicy,
            final boolean allowPossibleDuplicate,
            final AcknowledgementSet acknowledgements) {
        if (deliverAt < 0 || expireAt < 0 || expireAt < deliverAt) {
            throw new IllegalArgumentException("replay timing must satisfy 0 <= deliverAt <= expireAt");
        }
        this.deliverAt = deliverAt;
        this.expireAt = expireAt;
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.allowPossibleDuplicate = allowPossibleDuplicate;
        this.acknowledgements = Objects.requireNonNull(acknowledgements, "acknowledgements");
        if (allowPossibleDuplicate && !acknowledgements.has(AcknowledgementKind.POSSIBLE_DUPLICATE)) {
            throw new IllegalArgumentException("possible duplicate replay requires POSSIBLE_DUPLICATE");
        }
    }

    public long deliverAt() {
        return deliverAt;
    }

    public long expireAt() {
        return expireAt;
    }

    public RetryPolicyRef retryPolicy() {
        return retryPolicy;
    }

    public boolean allowPossibleDuplicate() {
        return allowPossibleDuplicate;
    }

    public AcknowledgementSet acknowledgements() {
        return acknowledgements;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.int64(output, 1, deliverAt);
            CanonicalProtobuf.int64(output, 2, expireAt);
            CanonicalProtobuf.bytes(output, 3, retryPolicy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 4, allowPossibleDuplicate ? 1 : 0);
            CanonicalProtobuf.bytes(output, 5, acknowledgements.canonicalBytes());
        });
    }

    public static ReplayDeadLetterRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ReplayDeadLetterRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "ReplayDeadLetterRequest");
        final ReplayDeadLetterRequest result = new ReplayDeadLetterRequest(
                QueryCodecSupport.uint(fields.get(0), 1),
                QueryCodecSupport.uint(fields.get(1), 2),
                RetryPolicyRef.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.bool(fields.get(3), 4),
                AcknowledgementSet.decode(QueryCodecSupport.nested(fields.get(4), 5)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ReplayDeadLetterRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ReplayDeadLetterRequest that
                && deliverAt == that.deliverAt
                && expireAt == that.expireAt
                && allowPossibleDuplicate == that.allowPossibleDuplicate
                && retryPolicy.equals(that.retryPolicy)
                && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deliverAt, expireAt, retryPolicy, allowPossibleDuplicate, acknowledgements);
    }
}
