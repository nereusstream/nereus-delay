package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.transport.Digest32;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable persisted projection of one Gateway physical attempt. */
public final class GatewayPhysicalAttemptV1 {
    private final int attemptNo;
    private final PhysicalEnqueueAttemptId physicalAttemptId;
    private final GatewayPhysicalAttemptStateV1 state;
    private final byte[] outcomeBytes;
    private final long startedAtEpochMs;
    private final long uncertaintyAtEpochMs;
    private final PhysicalEnqueueAttemptId retryRequestId;
    private final io.nereusstream.delay.transport.Digest32 retryRequestHash;
    private final long revision;
    private final long ownershipNotAfterEpochMs;

    public GatewayPhysicalAttemptV1(final int attemptNo, final PhysicalEnqueueAttemptId physicalAttemptId,
                                    final GatewayPhysicalAttemptStateV1 state, final byte[] outcomeBytes,
                                    final long startedAtEpochMs, final long uncertaintyAtEpochMs,
                                    final long revision, final long ownershipNotAfterEpochMs) {
        this(attemptNo, physicalAttemptId, state, outcomeBytes, startedAtEpochMs, uncertaintyAtEpochMs,
                null, null, revision, ownershipNotAfterEpochMs);
    }

    public GatewayPhysicalAttemptV1(final int attemptNo, final PhysicalEnqueueAttemptId physicalAttemptId,
                                    final GatewayPhysicalAttemptStateV1 state, final byte[] outcomeBytes,
                                    final long startedAtEpochMs, final long uncertaintyAtEpochMs,
                                    final PhysicalEnqueueAttemptId retryRequestId,
                                    final io.nereusstream.delay.transport.Digest32 retryRequestHash,
                                    final long revision, final long ownershipNotAfterEpochMs) {
        if (attemptNo <= 0 || startedAtEpochMs < 0 || uncertaintyAtEpochMs < startedAtEpochMs || revision <= 0
                || ownershipNotAfterEpochMs < startedAtEpochMs || ownershipNotAfterEpochMs > uncertaintyAtEpochMs) {
            throw new IllegalArgumentException("invalid Gateway physical attempt bounds");
        }
        if ((retryRequestId == null) != (retryRequestHash == null)) {
            throw new IllegalArgumentException("retry request id/hash must be present together");
        }
        this.attemptNo = attemptNo;
        this.physicalAttemptId = Objects.requireNonNull(physicalAttemptId, "physicalAttemptId");
        this.state = Objects.requireNonNull(state, "state");
        this.outcomeBytes = outcomeBytes == null ? null : Bytes.copy(outcomeBytes);
        this.startedAtEpochMs = startedAtEpochMs;
        this.uncertaintyAtEpochMs = uncertaintyAtEpochMs;
        this.retryRequestId = retryRequestId;
        this.retryRequestHash = retryRequestHash;
        this.revision = revision;
        this.ownershipNotAfterEpochMs = ownershipNotAfterEpochMs;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public PhysicalEnqueueAttemptId physicalAttemptId() {
        return physicalAttemptId;
    }

    public GatewayPhysicalAttemptStateV1 state() {
        return state;
    }

    public byte[] outcomeBytes() {
        return outcomeBytes == null ? null : Bytes.copy(outcomeBytes);
    }

    public long startedAtEpochMs() {
        return startedAtEpochMs;
    }

    public long uncertaintyAtEpochMs() {
        return uncertaintyAtEpochMs;
    }

    public PhysicalEnqueueAttemptId retryRequestId() {
        return retryRequestId;
    }

    public io.nereusstream.delay.transport.Digest32 retryRequestHash() {
        return retryRequestHash;
    }

    public long revision() {
        return revision;
    }

    public long ownershipNotAfterEpochMs() {
        return ownershipNotAfterEpochMs;
    }

    /** Strict decoder for the persisted attempt projection. */
    public static GatewayPhysicalAttemptV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 7) {
            throw new IllegalArgumentException("Gateway physical attempt fields are incomplete");
        }
        int index = 0;
        final int attemptNo = positiveInt(uint(field(fields, index++, 1), 1), "attemptNo");
        final PhysicalEnqueueAttemptId physicalAttemptId = PhysicalEnqueueAttemptId.require(
                fixed(field(fields, index++, 2), 2, 16));
        final GatewayPhysicalAttemptStateV1 state = GatewayPhysicalAttemptStateV1.fromWire(
                uint(field(fields, index++, 3), 3));
        final byte[] outcomeBytes;
        if (index < fields.size() && fields.get(index).number() == 4) {
            outcomeBytes = bytes(field(fields, index++, 4), 4);
        } else {
            outcomeBytes = null;
        }
        final long startedAt = nonNegative(uint(field(fields, index++, 5), 5), "startedAtEpochMs");
        final long uncertaintyAt = nonNegative(uint(field(fields, index++, 6), 6), "uncertaintyAtEpochMs");
        final PhysicalEnqueueAttemptId retryRequestId;
        final Digest32 retryRequestHash;
        if (index < fields.size() && fields.get(index).number() == 7) {
            retryRequestId = PhysicalEnqueueAttemptId.require(fixed(field(fields, index++, 7), 7, 16));
            retryRequestHash = new Digest32(fixed(field(fields, index++, 8), 8, Digest32.LENGTH));
        } else {
            retryRequestId = null;
            retryRequestHash = null;
        }
        final long revision = positive(uint(field(fields, index++, 9), 9), "revision");
        final long ownershipNotAfter = nonNegative(uint(field(fields, index++, 10), 10),
                "ownershipNotAfterEpochMs");
        if (index != fields.size()) {
            throw new IllegalArgumentException("Gateway physical attempt has unknown fields");
        }
        final GatewayPhysicalAttemptV1 result = new GatewayPhysicalAttemptV1(attemptNo, physicalAttemptId, state,
                outcomeBytes, startedAt, uncertaintyAt, retryRequestId, retryRequestHash, revision,
                ownershipNotAfter);
        if (!java.util.Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Gateway physical attempt is not canonical");
        }
        return result;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, attemptNo);
            CanonicalProtobuf.bytes(output, 2, physicalAttemptId.bytes());
            CanonicalProtobuf.uint32(output, 3, state.ordinal() + 1);
            if (outcomeBytes != null) {
                CanonicalProtobuf.bytes(output, 4, outcomeBytes);
            }
            CanonicalProtobuf.int64(output, 5, startedAtEpochMs);
            CanonicalProtobuf.int64(output, 6, uncertaintyAtEpochMs);
            if (retryRequestId != null) {
                CanonicalProtobuf.bytes(output, 7, retryRequestId.bytes());
                CanonicalProtobuf.bytes(output, 8, retryRequestHash.bytes());
            }
            CanonicalProtobuf.uint64(output, 9, revision);
            CanonicalProtobuf.int64(output, 10, ownershipNotAfterEpochMs);
        });
    }

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                         final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("unexpected Gateway physical attempt field at " + number);
        }
        return fields.get(index);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 2) {
            throw new IllegalArgumentException("Gateway physical attempt field " + number + " is not bytes");
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        if (value.length != length) {
            throw new IllegalArgumentException("Gateway physical attempt field " + number + " has invalid length");
        }
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("Gateway physical attempt field " + number + " is not uint");
        }
        return field.unsignedValue();
    }

    private static int positiveInt(final long value, final String name) {
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " is outside the signed Java bound");
        }
        return (int) value;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
