package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** ControlPayload field 1: activate one exact protocol tuple for eligible readers. */
public final class ProtocolVersionActivatePayloadV1 {
    private static final int HASH_LENGTH = 32;

    private final ProtocolTupleV1 tuple;
    private final byte[] canonicalSchemaHash;
    private final byte[] compatibleReaderSetEvidenceHash;

    public ProtocolVersionActivatePayloadV1(final ProtocolTupleV1 tuple, final byte[] canonicalSchemaHash,
                                            final byte[] compatibleReaderSetEvidenceHash) {
        this.tuple = Objects.requireNonNull(tuple, "tuple");
        Bytes.requireLength(canonicalSchemaHash, HASH_LENGTH, "canonicalSchemaHash");
        Bytes.requireLength(compatibleReaderSetEvidenceHash, HASH_LENGTH,
                "compatibleReaderSetEvidenceHash");
        this.canonicalSchemaHash = Bytes.copy(canonicalSchemaHash);
        this.compatibleReaderSetEvidenceHash = Bytes.copy(compatibleReaderSetEvidenceHash);
    }

    public ProtocolTupleV1 tuple() {
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

    public static ProtocolVersionActivatePayloadV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ProtocolVersionActivatePayloadV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "ProtocolVersionActivatePayloadV1");
        final ProtocolVersionActivatePayloadV1 result = new ProtocolVersionActivatePayloadV1(
                ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolVersionActivatePayloadV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolVersionActivatePayloadV1 that
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
