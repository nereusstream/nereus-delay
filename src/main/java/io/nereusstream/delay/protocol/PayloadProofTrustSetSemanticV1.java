package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable, hash-addressed payload-proof verifier set from Registry §5.1.1. */
public final class PayloadProofTrustSetSemanticV1 {
    private static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-payload-trust-set-semantic-v1\0";

    private final long version;
    private final List<PayloadProofVerifierKeyV1> keys;
    private final byte[] semanticHash;

    public PayloadProofTrustSetSemanticV1(final long version,
                                          final List<PayloadProofVerifierKeyV1> keys) {
        if (version == 0) {
            throw new IllegalArgumentException("trust set version must be nonzero");
        }
        this.version = version;
        this.keys = sortedUnique(keys);
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException("trust set must contain a verifier key");
        }
        this.semanticHash = computeSemanticHash();
    }

    public long version() {
        return version;
    }

    public List<PayloadProofVerifierKeyV1> keys() {
        return keys;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public PayloadProofTrustSetRefV1 ref() {
        return new PayloadProofTrustSetRefV1(version, semanticHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64Bits(output, 1, version);
            for (PayloadProofVerifierKeyV1 key : keys) {
                CanonicalProtobuf.bytes(output, 2, key.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 3, semanticHash);
        });
    }

    public static PayloadProofTrustSetSemanticV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 3 || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 3) {
            throw new IllegalArgumentException("invalid PayloadProofTrustSetSemanticV1 field order");
        }
        final List<PayloadProofVerifierKeyV1> keys = new ArrayList<>();
        for (int index = 1; index < fields.size() - 1; index++) {
            if (fields.get(index).number() != 2) {
                throw new IllegalArgumentException("trust-set verifier keys must use field 2");
            }
            keys.add(PayloadProofVerifierKeyV1.decode(QueryCodecSupport.nested(fields.get(index), 2)));
        }
        final PayloadProofTrustSetSemanticV1 result = new PayloadProofTrustSetSemanticV1(
                QueryCodecSupport.uint64Bits(fields.get(0), 1), keys);
        if (!Bytes.constantTimeEquals(result.semanticHash,
                QueryCodecSupport.fixed(fields.get(fields.size() - 1), 3, HASH_LENGTH))) {
            throw new IllegalArgumentException("PayloadProofTrustSetSemanticV1 semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofTrustSetSemanticV1");
        return result;
    }

    private byte[] computeSemanticHash() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), Bytes.u64beBits(version), canonicalKeyList());
    }

    private byte[] canonicalKeyList() {
        return CanonicalProtobuf.message(output -> {
            for (PayloadProofVerifierKeyV1 key : keys) {
                CanonicalProtobuf.bytes(output, 2, key.canonicalBytes());
            }
        });
    }

    private static List<PayloadProofVerifierKeyV1> sortedUnique(
            final List<PayloadProofVerifierKeyV1> values) {
        Objects.requireNonNull(values, "keys");
        final List<PayloadProofVerifierKeyV1> result = new ArrayList<>(values.size());
        PayloadProofVerifierKeyV1 previous = null;
        for (PayloadProofVerifierKeyV1 value : values) {
            Objects.requireNonNull(value, "verifier key");
            if (previous != null && Integer.compareUnsigned(previous.keyVersion(), value.keyVersion()) >= 0) {
                throw new IllegalArgumentException("trust-set verifier keys must be sorted and unique");
            }
            result.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofTrustSetSemanticV1 that && version == that.version
                && keys.equals(that.keys) && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, keys, Arrays.hashCode(semanticHash));
    }
}
