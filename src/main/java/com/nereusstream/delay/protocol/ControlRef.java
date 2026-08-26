package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical registry {@code ControlRef}. */
public final class ControlRef {
    public static final int HASH_LENGTH = 32;

    private final byte[] operationId;
    private final byte[] requestHash;
    private final long targetIndex;

    public ControlRef(final byte[] operationId, final byte[] requestHash, final long targetIndex) {
        this.operationId = fixed(operationId, "operationId");
        this.requestHash = fixed(requestHash, "requestHash");
        if (targetIndex < 0 || targetIndex > 0xffff_ffffL) {
            throw new IllegalArgumentException("targetIndex must be an unsigned uint32");
        }
        this.targetIndex = targetIndex;
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public byte[] requestHash() {
        return Bytes.copy(requestHash);
    }

    public long targetIndex() {
        return targetIndex;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, operationId);
            CanonicalProtobuf.bytes(output, 2, requestHash);
            CanonicalProtobuf.uint32(output, 3, targetIndex);
        });
    }

    /** Computes the closed System Mutation logical identity for a control target. */
    public byte[] logicalOperationIdentity(final SystemMutationType type) {
        Objects.requireNonNull(type, "type");
        if (type != SystemMutationType.APPLY_SHARD_CONTROL
                && type != SystemMutationType.REPLAY_DEAD_LETTER
                && type != SystemMutationType.RESOLVE_UNCERTAIN) {
            throw new IllegalArgumentException("control ref is not valid for " + type);
        }
        return logicalOperationIdentity(type.wireValue());
    }

    /** Computes the identity when the applicable value is a ControlKind number. */
    public byte[] logicalOperationIdentity(final int applicableTypeOrControlKind) {
        if (applicableTypeOrControlKind <= 0 || applicableTypeOrControlKind > 0xffff) {
            throw new IllegalArgumentException("applicable control kind must fit uint16");
        }
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-control-target-logical-id\0"),
                operationId,
                Bytes.u32be(targetIndex),
                Bytes.u16be(applicableTypeOrControlKind));
    }

    public static ControlRef decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(encoded));
        if (fields.size() != 3) {
            throw new IllegalArgumentException("ControlRef fields are incomplete or unknown");
        }
        final byte[] operationId = fixed(fields.get(0), 1);
        final byte[] requestHash = fixed(fields.get(1), 2);
        final long targetIndex = unsigned(fields.get(2), 3);
        final ControlRef result = new ControlRef(operationId, requestHash, targetIndex);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical ControlRef");
        }
        return result;
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid ControlRef bytes field " + number);
        }
        final byte[] value = field.rawValue();
        Bytes.requireLength(value, HASH_LENGTH, "ControlRef field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number
                || field.wireType() != 0
                || field.unsignedValue() < 0
                || field.unsignedValue() > 0xffff_ffffL) {
            throw new IllegalArgumentException("invalid ControlRef uint32 field " + number);
        }
        return field.unsignedValue();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlRef that
                && targetIndex == that.targetIndex
                && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(requestHash, that.requestHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetIndex, Arrays.hashCode(operationId), Arrays.hashCode(requestHash));
    }
}
