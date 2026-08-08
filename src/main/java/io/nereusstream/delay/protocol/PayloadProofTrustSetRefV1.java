package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable reference to the source-ordered payload-proof verifier set. */
public final class PayloadProofTrustSetRefV1 {
    private final long version;
    private final byte[] semanticHash;

    public PayloadProofTrustSetRefV1(final long version, final byte[] semanticHash) {
        if (version == 0) {
            throw new IllegalArgumentException("trust set version must be nonzero");
        }
        Bytes.requireLength(semanticHash, 32, "trustSetSemanticHash");
        this.version = version;
        this.semanticHash = Bytes.copy(semanticHash);
    }

    public long version() {
        return version;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64Bits(output, 1, version);
            CanonicalProtobuf.bytes(output, 2, semanticHash);
        });
    }

    public static PayloadProofTrustSetRefV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PayloadProofTrustSetRefV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "PayloadProofTrustSetRefV1");
        final PayloadProofTrustSetRefV1 result = new PayloadProofTrustSetRefV1(
                QueryCodecSupport.uint64Bits(fields.get(0), 1), QueryCodecSupport.fixed(fields.get(1), 2, 32));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofTrustSetRefV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PayloadProofTrustSetRefV1 that)) {
            return false;
        }
        return version == that.version && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, Arrays.hashCode(semanticHash));
    }
}
