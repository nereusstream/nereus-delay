package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Public-safe result for one Quota control target. */
public final class QuotaControlResultV1 {
    private final QuotaGrantRefV1 quotaGrant;
    private final byte[] persistedUsageDigest;

    public QuotaControlResultV1(final QuotaGrantRefV1 quotaGrant, final byte[] persistedUsageDigest) {
        this.quotaGrant = Objects.requireNonNull(quotaGrant, "quotaGrant");
        Bytes.requireLength(persistedUsageDigest, 32, "persistedUsageDigest");
        this.persistedUsageDigest = Bytes.copy(persistedUsageDigest);
    }

    public QuotaGrantRefV1 quotaGrant() {
        return quotaGrant;
    }

    public byte[] persistedUsageDigest() {
        return Bytes.copy(persistedUsageDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, quotaGrant.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, persistedUsageDigest);
        });
    }

    public static QuotaControlResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "QuotaControlResultV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "QuotaControlResultV1");
        final QuotaControlResultV1 result = new QuotaControlResultV1(
                QuotaGrantRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, 32));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "QuotaControlResultV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof QuotaControlResultV1 that
                && quotaGrant.equals(that.quotaGrant)
                && Arrays.equals(persistedUsageDigest, that.persistedUsageDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quotaGrant, Arrays.hashCode(persistedUsageDigest));
    }
}
