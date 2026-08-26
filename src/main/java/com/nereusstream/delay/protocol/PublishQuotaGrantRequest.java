package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch that publishes one immutable quota grant. */
public final class PublishQuotaGrantRequest implements ControlOperationRequestBranch {
    private final QuotaGrantRef quotaGrant;
    private final QuotaTransferPlanRef transferPlan;

    public PublishQuotaGrantRequest(final QuotaGrantRef quotaGrant, final QuotaTransferPlanRef transferPlan) {
        this.quotaGrant = Objects.requireNonNull(quotaGrant, "quotaGrant");
        this.transferPlan = transferPlan;
    }

    public QuotaGrantRef quotaGrant() {
        return quotaGrant;
    }

    public QuotaTransferPlanRef transferPlan() {
        return transferPlan;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, quotaGrant.canonicalBytes());
            if (transferPlan != null) {
                CanonicalProtobuf.bytes(output, 2, transferPlan.canonicalBytes());
            }
        });
    }

    public static PublishQuotaGrantRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PublishQuotaGrantRequest");
        if (fields.size() < 1
                || fields.size() > 2
                || fields.get(0).number() != 1
                || (fields.size() == 2 && fields.get(1).number() != 2)) {
            throw new IllegalArgumentException("invalid PublishQuotaGrantRequest fields");
        }
        final PublishQuotaGrantRequest result = new PublishQuotaGrantRequest(
                QuotaGrantRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                fields.size() == 2 ? QuotaTransferPlanRef.decode(QueryCodecSupport.nested(fields.get(1), 2)) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublishQuotaGrantRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PublishQuotaGrantRequest that
                && quotaGrant.equals(that.quotaGrant)
                && Objects.equals(transferPlan, that.transferPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quotaGrant, transferPlan);
    }
}
