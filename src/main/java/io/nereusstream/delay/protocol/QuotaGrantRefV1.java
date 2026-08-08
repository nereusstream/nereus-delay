package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable semantic quota-grant reference used by a shard capacity envelope. */
public final class QuotaGrantRefV1 {
    public static final int HASH_LENGTH = 32;
    private static final byte[] SEMANTIC_DOMAIN = Bytes.utf8("nereus-delay-quota-grant-semantic-v1\0");

    private final byte[] grantId;
    private final long grantVersion;
    private final byte[] grantSemanticHash;
    private final PublishAdmissionBody.ChargeVector limit;

    public QuotaGrantRefV1(final byte[] grantId, final long grantVersion,
                           final PublishAdmissionBody.ChargeVector limit) {
        this.grantId = fixedNonZero(grantId, "grantId");
        if (grantVersion == 0) {
            throw new IllegalArgumentException("grantVersion must be nonzero");
        }
        this.grantVersion = grantVersion;
        this.limit = Objects.requireNonNull(limit, "limit");
        this.grantSemanticHash = semanticHash(this.grantId, grantVersion, limit);
    }

    private QuotaGrantRefV1(final byte[] grantId, final long grantVersion,
                            final byte[] grantSemanticHash, final PublishAdmissionBody.ChargeVector limit) {
        this.grantId = Bytes.copy(grantId);
        this.grantVersion = grantVersion;
        this.grantSemanticHash = Bytes.copy(grantSemanticHash);
        this.limit = limit;
    }

    public byte[] grantId() {
        return Bytes.copy(grantId);
    }

    public long grantVersion() {
        return grantVersion;
    }

    public byte[] grantSemanticHash() {
        return Bytes.copy(grantSemanticHash);
    }

    public PublishAdmissionBody.ChargeVector limit() {
        return limit;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, grantId);
            CanonicalProtobuf.uint64Bits(output, 2, grantVersion);
            CanonicalProtobuf.bytes(output, 3, grantSemanticHash);
            CanonicalProtobuf.bytes(output, 4, limit.canonicalBytes());
        });
    }

    public static QuotaGrantRefV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "QuotaGrantRefV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "QuotaGrantRefV1");
        final byte[] grantId = QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH);
        if (isZero(grantId)) {
            throw new IllegalArgumentException("QuotaGrantRefV1 grantId must be non-zero");
        }
        final long grantVersion = QueryCodecSupport.uint64Bits(fields.get(1), 2);
        if (grantVersion == 0) {
            throw new IllegalArgumentException("QuotaGrantRefV1 grantVersion must be nonzero");
        }
        final byte[] semanticHash = QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH);
        final PublishAdmissionBody.ChargeVector limit = PublishAdmissionBody.ChargeVector.decodeCanonical(
                QueryCodecSupport.nested(fields.get(3), 4));
        final QuotaGrantRefV1 result = new QuotaGrantRefV1(grantId, grantVersion, semanticHash, limit);
        if (!Arrays.equals(semanticHash, semanticHash(grantId, grantVersion, limit))) {
            throw new IllegalArgumentException("QuotaGrantRefV1 semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "QuotaGrantRefV1");
        return result;
    }

    private static byte[] semanticHash(final byte[] grantId, final long version,
                                       final PublishAdmissionBody.ChargeVector limit) {
        return Bytes.sha256(SEMANTIC_DOMAIN, grantId, Bytes.u64beBits(version), limit.canonicalBytes());
    }

    private static byte[] fixedNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        if (isZero(value)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    private static boolean isZero(final byte[] value) {
        for (byte current : value) {
            if (current != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof QuotaGrantRefV1 that && grantVersion == that.grantVersion
                && Arrays.equals(grantId, that.grantId)
                && Arrays.equals(grantSemanticHash, that.grantSemanticHash)
                && limit.equals(that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(grantId), grantVersion, Arrays.hashCode(grantSemanticHash), limit);
    }
}
