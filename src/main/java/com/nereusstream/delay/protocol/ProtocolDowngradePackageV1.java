package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable downgrade package bound to an already activated protocol marker.
 *
 * <p>A downgrade package never removes an activation marker.  It proves that
 * the fallback tuple was already active and that the package was built from
 * the exact schema, writer and reader bytes associated with the activated
 * tuple.  This keeps a writer rollback from silently turning into a protocol
 * or deduplication rollback.</p>
 */
public final class ProtocolDowngradePackageV1 {
    public static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-protocol-downgrade-package-v1\0");

    private final ProtocolTupleV1 fallbackTuple;
    private final ProtocolTupleV1 activatedTuple;
    private final byte[] canonicalSchemaHash;
    private final byte[] activationStateDigest;
    private final byte[] writerBinaryDigest;
    private final byte[] readerBinaryDigest;
    private final byte[] packageDigest;

    public ProtocolDowngradePackageV1(
            final ProtocolTupleV1 fallbackTuple,
            final ProtocolTupleV1 activatedTuple,
            final byte[] canonicalSchemaHash,
            final byte[] activationStateDigest,
            final byte[] writerBinaryDigest,
            final byte[] readerBinaryDigest) {
        this.fallbackTuple = Objects.requireNonNull(fallbackTuple, "fallbackTuple");
        this.activatedTuple = Objects.requireNonNull(activatedTuple, "activatedTuple");
        if (fallbackTuple.equals(activatedTuple)) {
            throw new IllegalArgumentException("downgrade fallback and activated tuples must differ");
        }
        this.canonicalSchemaHash = fixed(canonicalSchemaHash, "canonicalSchemaHash");
        this.activationStateDigest = fixed(activationStateDigest, "activationStateDigest");
        this.writerBinaryDigest = fixed(writerBinaryDigest, "writerBinaryDigest");
        this.readerBinaryDigest = fixed(readerBinaryDigest, "readerBinaryDigest");
        this.packageDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToSix());
    }

    public ProtocolTupleV1 fallbackTuple() {
        return fallbackTuple;
    }

    public ProtocolTupleV1 activatedTuple() {
        return activatedTuple;
    }

    public byte[] canonicalSchemaHash() {
        return Bytes.copy(canonicalSchemaHash);
    }

    public byte[] activationStateDigest() {
        return Bytes.copy(activationStateDigest);
    }

    public byte[] writerBinaryDigest() {
        return Bytes.copy(writerBinaryDigest);
    }

    public byte[] readerBinaryDigest() {
        return Bytes.copy(readerBinaryDigest);
    }

    public byte[] packageDigest() {
        return Bytes.copy(packageDigest);
    }

    /**
     * Verifies the package against the durable activation projection.  The
     * activated marker remains present after a successful validation.
     */
    public void validateAgainst(final ProtocolActivationStateV1 state) {
        Objects.requireNonNull(state, "state");
        if (state.activation(fallbackTuple) == null) {
            throw new IllegalStateException("downgrade fallback tuple is not active");
        }
        final ProtocolActivationStateV1.Activation activation = state.activation(activatedTuple);
        if (activation == null) {
            throw new IllegalStateException("downgrade package is for an inactive tuple");
        }
        if (!Bytes.constantTimeEquals(activation.canonicalSchemaHash(), canonicalSchemaHash)) {
            throw new IllegalStateException("downgrade package schema hash is stale");
        }
        if (!Bytes.constantTimeEquals(state.stateDigest(), activationStateDigest)) {
            throw new IllegalStateException("downgrade package activation state is stale");
        }
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, fallbackTuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, activatedTuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, canonicalSchemaHash);
            CanonicalProtobuf.bytes(output, 5, activationStateDigest);
            CanonicalProtobuf.bytes(output, 6, writerBinaryDigest);
            CanonicalProtobuf.bytes(output, 7, readerBinaryDigest);
            CanonicalProtobuf.bytes(output, 8, packageDigest);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("protocol downgrade package is too large");
        }
        return encoded;
    }

    public static ProtocolDowngradePackageV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid protocol downgrade package length");
        }
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ProtocolDowngradePackageV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "ProtocolDowngradePackageV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported protocol downgrade package version");
        }
        final ProtocolDowngradePackageV1 result = new ProtocolDowngradePackageV1(
                ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH));
        final byte[] suppliedDigest = QueryCodecSupport.fixed(fields.get(7), 8, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(suppliedDigest, result.packageDigest)) {
            throw new IllegalArgumentException("protocol downgrade package digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolDowngradePackageV1");
        return result;
    }

    private byte[] fieldsOneToSix() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, fallbackTuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, activatedTuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, canonicalSchemaHash);
            CanonicalProtobuf.bytes(output, 5, activationStateDigest);
            CanonicalProtobuf.bytes(output, 6, writerBinaryDigest);
            CanonicalProtobuf.bytes(output, 7, readerBinaryDigest);
        });
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolDowngradePackageV1 that
                && fallbackTuple.equals(that.fallbackTuple)
                && activatedTuple.equals(that.activatedTuple)
                && Arrays.equals(canonicalSchemaHash, that.canonicalSchemaHash)
                && Arrays.equals(activationStateDigest, that.activationStateDigest)
                && Arrays.equals(writerBinaryDigest, that.writerBinaryDigest)
                && Arrays.equals(readerBinaryDigest, that.readerBinaryDigest)
                && Arrays.equals(packageDigest, that.packageDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fallbackTuple,
                activatedTuple,
                Arrays.hashCode(canonicalSchemaHash),
                Arrays.hashCode(activationStateDigest),
                Arrays.hashCode(writerBinaryDigest),
                Arrays.hashCode(readerBinaryDigest),
                Arrays.hashCode(packageDigest));
    }
}
