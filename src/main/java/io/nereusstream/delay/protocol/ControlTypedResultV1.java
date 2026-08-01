package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed typed-result oneof envelope.  The operation-specific payload is
 * retained as canonical bytes here; its branch codec is owned by the
 * corresponding control-result module and is never treated as a free-form
 * map or diagnostic string.
 */
public final class ControlTypedResultV1 {
    private final ControlResultKindV1 kind;
    private final byte[] payload;

    public ControlTypedResultV1(final ControlResultKindV1 kind, final byte[] payload) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.payload = canonicalPayload(payload);
    }

    public ControlResultKindV1 kind() {
        return kind;
    }

    public byte[] payload() {
        return Bytes.copy(payload);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, kind.wireValue(), payload));
    }

    public static ControlTypedResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ControlTypedResultV1");
        if (fields.size() != 1 || fields.get(0).wireType() != 2) {
            throw new IllegalArgumentException("ControlTypedResultV1 must select exactly one branch");
        }
        final ControlResultKindV1 kind = ControlResultKindV1.fromWire(fields.get(0).number());
        final ControlTypedResultV1 result = new ControlTypedResultV1(kind, fields.get(0).rawValue());
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlTypedResultV1");
        return result;
    }

    private static byte[] canonicalPayload(final byte[] value) {
        Objects.requireNonNull(value, "payload");
        if (value.length == 0) {
            throw new IllegalArgumentException("typed control result payload must not be empty");
        }
        // A branch payload is itself a canonical protobuf message.  The
        // branch-specific decoder performs the closed field validation.
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(value);
        if (!reader.hasRemaining()) {
            throw new IllegalArgumentException("typed control result payload is empty");
        }
        while (reader.hasRemaining()) {
            reader.next();
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlTypedResultV1 that && kind == that.kind
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(payload));
    }
}
