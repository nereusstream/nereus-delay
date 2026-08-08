package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable, non-borrowable capacity-grant projection. */
public final class CapacityGrantV1 {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-capacity-grant-v1\0");

    private final CapacityGrantKindV1 kind;
    private final byte[] grantId;
    private final long reserveSourceVersion;
    private final CapacityVectorV1 vector;
    private final byte[] digest;

    public CapacityGrantV1(final CapacityGrantKindV1 kind, final byte[] grantId,
                           final long reserveSourceVersion, final CapacityVectorV1 vector) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.grantId = fixed(grantId, "grantId");
        if (reserveSourceVersion == 0) {
            throw new IllegalArgumentException("reserveSourceVersion must be nonzero");
        }
        this.reserveSourceVersion = reserveSourceVersion;
        this.vector = Objects.requireNonNull(vector, "vector");
        this.digest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToFour());
    }

    private CapacityGrantV1(final CapacityGrantKindV1 kind, final byte[] grantId,
                            final long reserveSourceVersion, final CapacityVectorV1 vector,
                            final byte[] digest) {
        this.kind = kind;
        this.grantId = Bytes.copy(grantId);
        this.reserveSourceVersion = reserveSourceVersion;
        this.vector = vector;
        this.digest = Bytes.copy(digest);
    }

    public CapacityGrantKindV1 kind() {
        return kind;
    }

    public byte[] grantId() {
        return Bytes.copy(grantId);
    }

    public long reserveSourceVersion() {
        return reserveSourceVersion;
    }

    public CapacityVectorV1 vector() {
        return vector;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFour());
            CanonicalProtobuf.bytes(output, 5, digest);
        });
    }

    public static CapacityGrantV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "CapacityGrantV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5}, "CapacityGrantV1");
        final CapacityGrantKindV1 kind = CapacityGrantKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final byte[] grantId = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        if (isZero(grantId)) {
            throw new IllegalArgumentException("CapacityGrantV1 grantId must be non-zero");
        }
        final long sourceVersion = QueryCodecSupport.uint64Bits(fields.get(2), 3);
        if (sourceVersion == 0) {
            throw new IllegalArgumentException("CapacityGrantV1 reserveSourceVersion must be nonzero");
        }
        final CapacityVectorV1 vector = CapacityVectorV1.decode(QueryCodecSupport.nested(fields.get(3), 4));
        final byte[] digest = QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH);
        final CapacityGrantV1 result = new CapacityGrantV1(kind, grantId, sourceVersion, vector, digest);
        if (!Arrays.equals(digest, Bytes.sha256(DIGEST_DOMAIN, result.fieldsOneToFour()))) {
            throw new IllegalArgumentException("CapacityGrantV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CapacityGrantV1");
        return result;
    }

    private byte[] fieldsOneToFour() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.bytes(output, 2, grantId);
            CanonicalProtobuf.uint64Bits(output, 3, reserveSourceVersion);
            CanonicalProtobuf.bytes(output, 4, vector.canonicalBytes());
        });
    }

    private static byte[] fixed(final byte[] value, final String name) {
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
        return other instanceof CapacityGrantV1 that && kind == that.kind
                && reserveSourceVersion == that.reserveSourceVersion
                && Arrays.equals(grantId, that.grantId) && vector.equals(that.vector)
                && Arrays.equals(digest, that.digest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(grantId), reserveSourceVersion, vector,
                Arrays.hashCode(digest));
    }
}
