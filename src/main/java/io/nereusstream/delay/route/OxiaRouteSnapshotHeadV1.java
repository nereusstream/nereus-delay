package io.nereusstream.delay.route;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;

/** Oxia CAS head that makes orphaned Route event writes invisible to readers. */
public final class OxiaRouteSnapshotHeadV1 {
    public static final int VERSION = 1;

    private final long publishedRevision;
    private final byte[] eventDigest;

    public OxiaRouteSnapshotHeadV1(final long publishedRevision, final byte[] eventDigest) {
        if (publishedRevision <= 0) {
            throw new IllegalArgumentException("publishedRevision must be positive");
        }
        if (publishedRevision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("publishedRevision cannot be exhausted");
        }
        Bytes.requireLength(eventDigest, 32, "eventDigest");
        for (byte value : eventDigest) {
            if (value != 0) {
                this.publishedRevision = publishedRevision;
                this.eventDigest = Bytes.copy(eventDigest);
                return;
            }
        }
        throw new IllegalArgumentException("eventDigest must be non-zero");
    }

    public static OxiaRouteSnapshotHeadV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        if (uint(version, 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Route head version");
        }
        final long revision = positive(uint(next(reader, 2), 2), "publishedRevision");
        final byte[] digest = bytes(next(reader, 3), 3);
        if (reader.hasRemaining()) {
            throw new IllegalArgumentException("Route head has unknown fields");
        }
        final OxiaRouteSnapshotHeadV1 result = new OxiaRouteSnapshotHeadV1(revision, digest);
        if (!Bytes.constantTimeEquals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Route head is not canonical");
        }
        return result;
    }

    public long publishedRevision() {
        return publishedRevision;
    }

    public byte[] eventDigest() {
        return Bytes.copy(eventDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint64(output, 2, publishedRevision);
            CanonicalProtobuf.bytes(output, 3, eventDigest);
        });
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalArgumentException("missing Route head field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalArgumentException("unexpected Route head field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Route head bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid Route head uint field " + number);
        }
        return field.unsignedValue();
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
