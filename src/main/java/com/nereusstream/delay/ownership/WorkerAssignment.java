package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact placement decision accepted by a Worker.
 *
 * <p>The assignment is separate from the ephemeral Owner Lease.  The
 * authority publishes this immutable projection first; a Worker must reread
 * the exact projection before it opens a Store or acquires the lease.</p>
 */
public record WorkerAssignment(
        String workerId,
        SourceAssignment sourceAssignment,
        long placementEpoch,
        byte[] capacityEnvelopeDigest,
        byte[] routeSnapshotDigest) {
    private static final int VERSION = 1;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-worker-assignment-v1\0");

    /** Creates an assignment that is not yet bound to a Route snapshot. */
    public WorkerAssignment(
            final String workerId,
            final SourceAssignment sourceAssignment,
            final long placementEpoch,
            final byte[] capacityEnvelopeDigest) {
        this(workerId, sourceAssignment, placementEpoch, capacityEnvelopeDigest, new byte[0]);
    }

    public WorkerAssignment {
        workerId = canonicalText(workerId, "workerId");
        Objects.requireNonNull(sourceAssignment, "sourceAssignment");
        if (placementEpoch == 0) {
            throw new IllegalArgumentException("placementEpoch must be non-zero");
        }
        Bytes.requireLength(capacityEnvelopeDigest, 32, "capacityEnvelopeDigest");
        if (allZero(capacityEnvelopeDigest)) {
            throw new IllegalArgumentException("capacityEnvelopeDigest must be non-zero");
        }
        capacityEnvelopeDigest = Bytes.copy(capacityEnvelopeDigest);
        routeSnapshotDigest = routeDigest(routeSnapshotDigest);
    }

    @Override
    public byte[] capacityEnvelopeDigest() {
        return Bytes.copy(capacityEnvelopeDigest);
    }

    @Override
    public byte[] routeSnapshotDigest() {
        return Bytes.copy(routeSnapshotDigest);
    }

    /** Whether this assignment was published from an authenticated Route snapshot. */
    public boolean routeBound() {
        return routeSnapshotDigest.length == 32;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof WorkerAssignment that && sameIdentity(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                workerId,
                sourceAssignment,
                placementEpoch,
                Arrays.hashCode(capacityEnvelopeDigest),
                Arrays.hashCode(routeSnapshotDigest));
    }

    /** Digest-bound canonical projection stored by the assignment authority. */
    public byte[] canonicalBytes() {
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, workerId.getBytes(StandardCharsets.UTF_8));
            CanonicalProtobuf.uint64Bits(output, 3, placementEpoch);
            CanonicalProtobuf.bytes(output, 4, capacityEnvelopeDigest);
            CanonicalProtobuf.bytes(output, 5, sourceAssignment.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, routeSnapshotDigest);
        });
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(body);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(DIGEST_DOMAIN, body));
        });
    }

    /** Decodes and verifies a placement projection without accepting aliases. */
    public static WorkerAssignment decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = fields(encoded);
        requireNumbers(fields, 1, 2, 3, 4, 5, 6, 7);
        if (uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported WorkerAssignment version");
        }
        final byte[] body = CanonicalProtobuf.message(output -> {
            for (int index = 0; index < 6; index++) {
                final CanonicalProtobuf.Reader.Field field = fields.get(index);
                writeField(output, field);
            }
        });
        final byte[] digest = fixed(fields.get(6), 7, 32);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, body))) {
            throw new IllegalArgumentException("WorkerAssignment digest mismatch");
        }
        final WorkerAssignment result = new WorkerAssignment(
                text(bytes(fields.get(1), 2), "workerId"),
                SourceAssignment.decode(bytes(fields.get(4), 5)),
                uint64Bits(fields.get(2), 3),
                fixed(fields.get(3), 4, 32),
                routeDigest(bytes(fields.get(5), 6)));
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical WorkerAssignment");
        }
        return result;
    }

    /** Returns true only when every placement identity is byte-identical. */
    public boolean sameIdentity(final WorkerAssignment other) {
        return other != null && Arrays.equals(canonicalBytes(), other.canonicalBytes());
    }

    private static void writeField(
            final java.io.ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else if (field.wireType() == 2) {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        } else {
            throw new IllegalArgumentException("unsupported WorkerAssignment field wire type");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> fields(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> result = new ArrayList<>();
        while (reader.hasRemaining()) {
            result.add(reader.next());
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("WorkerAssignment is empty");
        }
        return result;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int... numbers) {
        if (fields.size() != numbers.length) {
            throw new IllegalArgumentException("WorkerAssignment field count mismatch");
        }
        for (int index = 0; index < numbers.length; index++) {
            if (fields.get(index).number() != numbers[index]) {
                throw new IllegalArgumentException("WorkerAssignment field order mismatch");
            }
        }
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid WorkerAssignment bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "WorkerAssignment field " + number);
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid WorkerAssignment uint field " + number);
        }
        return field.unsignedValue();
    }

    private static long uint64Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        return uint(field, number);
    }

    private static String text(final byte[] value, final String name) {
        final String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
        }
        return canonicalText(result, name);
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(java.text.Normalizer.normalize(value, java.text.Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }

    private static byte[] routeDigest(final byte[] value) {
        Objects.requireNonNull(value, "routeSnapshotDigest");
        if (value.length != 0 && value.length != 32) {
            throw new IllegalArgumentException("routeSnapshotDigest must be empty or 32 bytes");
        }
        if (value.length == 32 && allZero(value)) {
            throw new IllegalArgumentException("routeSnapshotDigest must be non-zero");
        }
        return Bytes.copy(value);
    }
}
