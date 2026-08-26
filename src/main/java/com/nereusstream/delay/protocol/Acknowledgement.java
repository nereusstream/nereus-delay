package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One exact acknowledgement hash/scope entry in a Control request. */
public final class Acknowledgement {
    public static final int HASH_LENGTH = 32;

    private final AcknowledgementKind kind;
    private final byte[] acknowledgementHash;
    private final byte[] ticketScopeHash;

    public Acknowledgement(
            final AcknowledgementKind kind, final byte[] acknowledgementHash, final byte[] ticketScopeHash) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.acknowledgementHash = fixed(acknowledgementHash, "acknowledgementHash");
        this.ticketScopeHash = fixed(ticketScopeHash, "ticketScopeHash");
    }

    public AcknowledgementKind kind() {
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

    public static Acknowledgement decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "Acknowledgement");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "Acknowledgement");
        final Acknowledgement result = new Acknowledgement(
                AcknowledgementKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Acknowledgement");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof Acknowledgement that
                && kind == that.kind
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
