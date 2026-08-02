package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch that publishes one immutable quota grant. */
public final class PublishQuotaGrantRequestV1 implements ControlOperationRequestBranchV1 {
    private final QuotaGrantRefV1 quotaGrant;
    private final QuotaTransferPlanRefV1 transferPlan;

    public PublishQuotaGrantRequestV1(final QuotaGrantRefV1 quotaGrant,
                                      final QuotaTransferPlanRefV1 transferPlan) {
        this.quotaGrant = Objects.requireNonNull(quotaGrant, "quotaGrant");
        this.transferPlan = transferPlan;
    }

    public QuotaGrantRefV1 quotaGrant() {
        return quotaGrant;
    }

    public QuotaTransferPlanRefV1 transferPlan() {
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

    public static PublishQuotaGrantRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PublishQuotaGrantRequestV1");
        if (fields.size() < 1 || fields.size() > 2 || fields.get(0).number() != 1
                || (fields.size() == 2 && fields.get(1).number() != 2)) {
            throw new IllegalArgumentException("invalid PublishQuotaGrantRequestV1 fields");
        }
        final PublishQuotaGrantRequestV1 result = new PublishQuotaGrantRequestV1(
                QuotaGrantRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                fields.size() == 2
                        ? QuotaTransferPlanRefV1.decode(QueryCodecSupport.nested(fields.get(1), 2)) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublishQuotaGrantRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PublishQuotaGrantRequestV1 that && quotaGrant.equals(that.quotaGrant)
                && Objects.equals(transferPlan, that.transferPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quotaGrant, transferPlan);
    }
}
