package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable, hash-addressed payload-proof verifier set from Registry §5.1.1. */
public final class PayloadProofTrustSetSemantic {
    private static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-payload-trust-set-semantic\0";

    private final long version;
    private final List<PayloadProofVerifierKey> keys;
    private final byte[] semanticHash;

    public PayloadProofTrustSetSemantic(final long version, final List<PayloadProofVerifierKey> keys) {
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

    public List<PayloadProofVerifierKey> keys() {
        return keys;
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public PayloadProofTrustSetRef ref() {
        return new PayloadProofTrustSetRef(version, semanticHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64Bits(output, 1, version);
            for (PayloadProofVerifierKey key : keys) {
                CanonicalProtobuf.bytes(output, 2, key.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 3, semanticHash);
        });
    }

    public static PayloadProofTrustSetSemantic decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 3
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 3) {
            throw new IllegalArgumentException("invalid PayloadProofTrustSetSemantic field order");
        }
        final List<PayloadProofVerifierKey> keys = new ArrayList<>();
        for (int index = 1; index < fields.size() - 1; index++) {
            if (fields.get(index).number() != 2) {
                throw new IllegalArgumentException("trust-set verifier keys must use field 2");
            }
            keys.add(PayloadProofVerifierKey.decode(QueryCodecSupport.nested(fields.get(index), 2)));
        }
        final PayloadProofTrustSetSemantic result =
                new PayloadProofTrustSetSemantic(QueryCodecSupport.uint64Bits(fields.get(0), 1), keys);
        if (!Bytes.constantTimeEquals(
                result.semanticHash, QueryCodecSupport.fixed(fields.get(fields.size() - 1), 3, HASH_LENGTH))) {
            throw new IllegalArgumentException("PayloadProofTrustSetSemantic semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofTrustSetSemantic");
        return result;
    }

    private byte[] computeSemanticHash() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), Bytes.u64beBits(version), canonicalKeyList());
    }

    private byte[] canonicalKeyList() {
        return CanonicalProtobuf.message(output -> {
            for (PayloadProofVerifierKey key : keys) {
                CanonicalProtobuf.bytes(output, 2, key.canonicalBytes());
            }
        });
    }

    private static List<PayloadProofVerifierKey> sortedUnique(final List<PayloadProofVerifierKey> values) {
        Objects.requireNonNull(values, "keys");
        final List<PayloadProofVerifierKey> result = new ArrayList<>(values.size());
        PayloadProofVerifierKey previous = null;
        for (PayloadProofVerifierKey value : values) {
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
        return other instanceof PayloadProofTrustSetSemantic that
                && version == that.version
                && keys.equals(that.keys)
                && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, keys, Arrays.hashCode(semanticHash));
    }
}
