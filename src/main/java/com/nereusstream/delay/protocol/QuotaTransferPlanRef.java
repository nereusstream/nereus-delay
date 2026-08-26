package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact tenant-policy transfer plan reference used by quota controls. */
public final class QuotaTransferPlanRef {
    public static final int HASH_LENGTH = 32;

    private final byte[] controlOperationId;
    private final byte[] requestHash;
    private final long tenantPolicyVersion;
    private final byte[] planHash;

    public QuotaTransferPlanRef(
            final byte[] controlOperationId,
            final byte[] requestHash,
            final long tenantPolicyVersion,
            final byte[] planHash) {
        this.controlOperationId = nonZero(controlOperationId, "controlOperationId");
        this.requestHash = fixed(requestHash, "requestHash");
        if (tenantPolicyVersion == 0) {
            throw new IllegalArgumentException("tenantPolicyVersion must be nonzero");
        }
        this.tenantPolicyVersion = tenantPolicyVersion;
        this.planHash = fixed(planHash, "planHash");
    }

    public byte[] controlOperationId() {
        return Bytes.copy(controlOperationId);
    }

    public byte[] requestHash() {
        return Bytes.copy(requestHash);
    }

    public long tenantPolicyVersion() {
        return tenantPolicyVersion;
    }

    public byte[] planHash() {
        return Bytes.copy(planHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, controlOperationId);
            CanonicalProtobuf.bytes(output, 2, requestHash);
            CanonicalProtobuf.uint64Bits(output, 3, tenantPolicyVersion);
            CanonicalProtobuf.bytes(output, 4, planHash);
        });
    }

    public static QuotaTransferPlanRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "QuotaTransferPlanRef");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "QuotaTransferPlanRef");
        final QuotaTransferPlanRef result = new QuotaTransferPlanRef(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(2), 3),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "QuotaTransferPlanRef");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof QuotaTransferPlanRef that
                && tenantPolicyVersion == that.tenantPolicyVersion
                && Arrays.equals(controlOperationId, that.controlOperationId)
                && Arrays.equals(requestHash, that.requestHash)
                && Arrays.equals(planHash, that.planHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(controlOperationId),
                Arrays.hashCode(requestHash),
                tenantPolicyVersion,
                Arrays.hashCode(planHash));
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, name);
        for (byte current : result) {
            if (current != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
