package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** ControlPayload field 1: activate one exact protocol tuple for eligible readers. */
public final class ProtocolVersionActivatePayload {
    private static final int HASH_LENGTH = 32;

    private final ProtocolTuple tuple;
    private final byte[] canonicalSchemaHash;
    private final byte[] compatibleReaderSetEvidenceHash;

    public ProtocolVersionActivatePayload(
            final ProtocolTuple tuple, final byte[] canonicalSchemaHash, final byte[] compatibleReaderSetEvidenceHash) {
        this.tuple = Objects.requireNonNull(tuple, "tuple");
        Bytes.requireLength(canonicalSchemaHash, HASH_LENGTH, "canonicalSchemaHash");
        Bytes.requireLength(compatibleReaderSetEvidenceHash, HASH_LENGTH, "compatibleReaderSetEvidenceHash");
        this.canonicalSchemaHash = Bytes.copy(canonicalSchemaHash);
        this.compatibleReaderSetEvidenceHash = Bytes.copy(compatibleReaderSetEvidenceHash);
    }

    public ProtocolTuple tuple() {
        return tuple;
    }

    public byte[] canonicalSchemaHash() {
        return Bytes.copy(canonicalSchemaHash);
    }

    public byte[] compatibleReaderSetEvidenceHash() {
        return Bytes.copy(compatibleReaderSetEvidenceHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, canonicalSchemaHash);
            CanonicalProtobuf.bytes(output, 3, compatibleReaderSetEvidenceHash);
        });
    }

    public static ProtocolVersionActivatePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ProtocolVersionActivatePayload");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "ProtocolVersionActivatePayload");
        final ProtocolVersionActivatePayload result = new ProtocolVersionActivatePayload(
                ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolVersionActivatePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolVersionActivatePayload that
                && tuple.equals(that.tuple)
                && Arrays.equals(canonicalSchemaHash, that.canonicalSchemaHash)
                && Arrays.equals(compatibleReaderSetEvidenceHash, that.compatibleReaderSetEvidenceHash);
    }

    @Override
    public int hashCode() {
        int result = tuple.hashCode();
        result = 31 * result + Arrays.hashCode(canonicalSchemaHash);
        result = 31 * result + Arrays.hashCode(compatibleReaderSetEvidenceHash);
        return result;
    }
}
