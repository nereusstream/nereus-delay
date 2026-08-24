package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical, non-text control reason marker. */
public final class ControlReasonV1 {
    private static final int HASH_LENGTH = 32;

    private final ControlReasonKindV1 kind;
    private final byte[] ticketReferenceHash;
    private final byte[] boundedDetailHash;

    public ControlReasonV1(
            final ControlReasonKindV1 kind, final byte[] ticketReferenceHash, final byte[] boundedDetailHash) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.ticketReferenceHash = optionalHash(ticketReferenceHash, "ticketReferenceHash");
        this.boundedDetailHash = optionalHash(boundedDetailHash, "boundedDetailHash");
    }

    public ControlReasonKindV1 kind() {
        return kind;
    }

    public byte[] ticketReferenceHash() {
        return copyOptional(ticketReferenceHash);
    }

    public byte[] boundedDetailHash() {
        return copyOptional(boundedDetailHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (ticketReferenceHash != null) {
                CanonicalProtobuf.bytes(output, 2, ticketReferenceHash);
            }
            if (boundedDetailHash != null) {
                CanonicalProtobuf.bytes(output, 3, boundedDetailHash);
            }
        });
    }

    public static ControlReasonV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ControlReasonV1");
        if (fields.size() < 1 || fields.size() > 3 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("invalid ControlReasonV1 fields");
        }
        int index = 1;
        byte[] ticket = null;
        byte[] detail = null;
        if (index < fields.size() && fields.get(index).number() == 2) {
            ticket = QueryCodecSupport.fixed(fields.get(index++), 2, HASH_LENGTH);
        }
        if (index < fields.size() && fields.get(index).number() == 3) {
            detail = QueryCodecSupport.fixed(fields.get(index++), 3, HASH_LENGTH);
        }
        if (index != fields.size()) {
            throw new IllegalArgumentException("invalid ControlReasonV1 optional field order");
        }
        final ControlReasonV1 result = new ControlReasonV1(
                ControlReasonKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)), ticket, detail);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlReasonV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlReasonV1 that
                && kind == that.kind
                && Arrays.equals(ticketReferenceHash, that.ticketReferenceHash)
                && Arrays.equals(boundedDetailHash, that.boundedDetailHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(ticketReferenceHash), Arrays.hashCode(boundedDetailHash));
    }

    private static byte[] optionalHash(final byte[] value, final String name) {
        if (value == null) {
            return null;
        }
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] copyOptional(final byte[] value) {
        return value == null ? null : Bytes.copy(value);
    }
}
