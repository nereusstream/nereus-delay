package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Complete prior/next grant request; transfer references name an independently registered parent plan. */
public final class TargetQuotaGrantControlRequest implements ControlOperationRequestBranch {
    public static final int VERSION = 1;
    public static final int MAX_TRANSFER_BYTES = 34 + 34 + 11 + 34;
    public static final int MAX_CANONICAL_BYTES =
            2 + 2 * (3 + TargetQuotaGrant.MAX_CANONICAL_BYTES) + 2 + MAX_TRANSFER_BYTES;
    public static final int CONTROL_KIND = 17;
    private final TargetQuotaGrant next;
    private final TargetQuotaGrant prior;
    private final QuotaTransferPlanRef transfer;

    public TargetQuotaGrantControlRequest(
            final TargetQuotaGrant next, final TargetQuotaGrant prior, final QuotaTransferPlanRef transfer) {
        this.next = Objects.requireNonNull(next, "next");
        next.requireSuccessor(prior);
        this.prior = prior;
        if (transfer != null) {
            TargetCompatibilityCodec.assigned(transfer.requestHash(), 32, "transferRequestHash");
            TargetCompatibilityCodec.assigned(transfer.planHash(), 32, "transferPlanHash");
            if (transfer.tenantPolicyVersion() != next.tenantPolicyVersion()) {
                throw new IllegalArgumentException("grant transfer policy version mismatch");
            }
        }
        this.transfer = transfer;
    }

    public TargetQuotaGrant next() {
        return next;
    }

    public TargetQuotaGrant prior() {
        return prior;
    }

    public QuotaTransferPlanRef transfer() {
        return transfer;
    }

    public ControlOperationKind operationKind() {
        return ControlOperationKind.PUBLISH_TARGET_QUOTA_GRANT;
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operationKind(), this);
    }

    public void requireControlRef(final ControlRef ref) {
        TargetCompatibilityCodec.assigned(ref.operationId(), 32, "quotaGrantOperationId");
        if (ref.targetIndex() != 0
                || !Arrays.equals(
                        ref.requestHash(), PreparedControlOperation.requestHash(operationKind(), operationRequest()))
                || (transfer != null && Arrays.equals(ref.operationId(), transfer.controlOperationId()))) {
            throw new IllegalArgumentException("quota grant request/control/parent transfer binding mismatch");
        }
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, next.canonicalBytes());
            if (prior != null) {
                CanonicalProtobuf.bytes(out, 3, prior.canonicalBytes());
            }
            if (transfer != null) {
                CanonicalProtobuf.bytes(out, 4, transfer.canonicalBytes());
            }
        });
    }

    public static TargetQuotaGrantControlRequest decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "Target quota grant request");
        if (fields.size() < 2 || QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota grant request version");
        }
        final boolean prior = fields.size() > 2 && fields.get(2).number() == 3;
        final boolean transfer = fields.getLast().number() == 4;
        QueryCodecSupport.requireNumbers(
                fields,
                prior
                        ? transfer ? new int[] {1, 2, 3, 4} : new int[] {1, 2, 3}
                        : transfer ? new int[] {1, 2, 4} : new int[] {1, 2},
                "Target quota grant request");
        QuotaTransferPlanRef plan = null;
        if (transfer) {
            final byte[] raw = QueryCodecSupport.bytes(fields.getLast(), 4);
            TargetCompatibilityCodec.read(raw, MAX_TRANSFER_BYTES, 4, false, "quota transfer reference");
            plan = QuotaTransferPlanRef.decode(raw);
        }
        final var result = new TargetQuotaGrantControlRequest(
                TargetQuotaGrant.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                prior ? TargetQuotaGrant.decode(QueryCodecSupport.bytes(fields.get(2), 3)) : null,
                plan);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target quota grant request");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaGrantControlRequest that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
