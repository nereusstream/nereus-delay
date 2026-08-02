package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One exact acknowledgement hash/scope entry in a Control request. */
public final class AcknowledgementV1 {
    public static final int HASH_LENGTH = 32;

    private final AcknowledgementKindV1 kind;
    private final byte[] acknowledgementHash;
    private final byte[] ticketScopeHash;

    public AcknowledgementV1(final AcknowledgementKindV1 kind, final byte[] acknowledgementHash,
                             final byte[] ticketScopeHash) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.acknowledgementHash = fixed(acknowledgementHash, "acknowledgementHash");
        this.ticketScopeHash = fixed(ticketScopeHash, "ticketScopeHash");
    }

    public AcknowledgementKindV1 kind() {
        return kind;
    }

    public byte[] acknowledgementHash() {
        return Bytes.copy(acknowledgementHash);
    }

    public byte[] ticketScopeHash() {
        return Bytes.copy(ticketScopeHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.bytes(output, 2, acknowledgementHash);
            CanonicalProtobuf.bytes(output, 3, ticketScopeHash);
        });
    }

    public static AcknowledgementV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "AcknowledgementV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "AcknowledgementV1");
        final AcknowledgementV1 result = new AcknowledgementV1(
                AcknowledgementKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "AcknowledgementV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof AcknowledgementV1 that && kind == that.kind
                && Arrays.equals(acknowledgementHash, that.acknowledgementHash)
                && Arrays.equals(ticketScopeHash, that.ticketScopeHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(acknowledgementHash), Arrays.hashCode(ticketScopeHash));
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
