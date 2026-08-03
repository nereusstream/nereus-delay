package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Canonical meta/FIXED ingress-fence state shared by the fence and Store layers. */
public record IngressFenceState(long closedThroughEpochMs, byte[] proofId) {
    public static final long OPEN = -1;
    public static final int PROOF_ID_LENGTH = 32;
    public static final int MAX_CANONICAL_BYTES = 256;

    public IngressFenceState {
        if (closedThroughEpochMs < OPEN) {
            throw new IllegalArgumentException("closed ingress deadline must be -1 or non-negative");
        }
        if (proofId != null) {
            Bytes.requireLength(proofId, PROOF_ID_LENGTH, "proofId");
            if (isZero(proofId)) {
                throw new IllegalArgumentException("proofId must be non-zero");
            }
            proofId = Bytes.copy(proofId);
        }
    }

    @Override
    public byte[] proofId() {
        return proofId == null ? null : Bytes.copy(proofId);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            if (closedThroughEpochMs >= 0) {
                CanonicalProtobuf.uint64(output, 1, closedThroughEpochMs);
            }
            if (proofId != null) {
                CanonicalProtobuf.bytes(output, 2, proofId);
            }
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("IngressFenceState is too large");
        }
        return encoded;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof IngressFenceState that
                && closedThroughEpochMs == that.closedThroughEpochMs
                && Arrays.equals(proofId, that.proofId);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(closedThroughEpochMs) + Arrays.hashCode(proofId);
    }

    public static IngressFenceState decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid IngressFenceState length");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        int index = 0;
        long closedThrough = OPEN;
        byte[] proofId = null;
        if (index < fields.size() && fields.get(index).number() == 1) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            if (field.wireType() != 0) {
                throw new IllegalArgumentException("IngressFenceState deadline must be a uint64");
            }
            closedThrough = field.unsignedValue();
            if (closedThrough < 0) {
                throw new IllegalArgumentException("IngressFenceState deadline is outside signed V1 range");
            }
        }
        if (index < fields.size()) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            if (field.number() != 2 || field.wireType() != 2) {
                throw new IllegalArgumentException("IngressFenceState proof field is invalid");
            }
            proofId = field.rawValue();
        }
        if (index != fields.size()) {
            throw new IllegalArgumentException("IngressFenceState has unexpected fields");
        }
        final IngressFenceState result = new IngressFenceState(closedThrough, proofId);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical IngressFenceState");
        }
        return result;
    }

    private static boolean isZero(final byte[] value) {
        for (byte current : value) {
            if (current != 0) {
                return false;
            }
        }
        return true;
    }
}
