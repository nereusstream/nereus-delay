package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import java.security.PublicKey;
import java.util.Objects;

/** Canonical immutable Route event stored under the Oxia Route stream. */
public final class OxiaRouteSnapshotRecord {
    public static final int VERSION = 1;
    private static final int MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-route-event\0");

    private final long revision;
    private final long previousRevision;
    private final RouteSelectionHint route;
    private final RouteSnapshot snapshot;

    private OxiaRouteSnapshotRecord(
            final long revision,
            final long previousRevision,
            final RouteSelectionHint route,
            final RouteSnapshot snapshot) {
        if (revision <= 0
                || previousRevision < 0
                || previousRevision == Long.MAX_VALUE
                || revision != previousRevision + 1) {
            throw new IllegalArgumentException("Route event revision is not a contiguous positive successor");
        }
        this.revision = revision;
        this.previousRevision = previousRevision;
        this.route = Objects.requireNonNull(route, "route");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        if (route.adapterKind() != snapshot.ingress().adapterKind()) {
            throw new IllegalArgumentException("Route selector adapter does not match signed snapshot");
        }
        if (snapshot.signature().length == 0) {
            throw new IllegalArgumentException("Route event requires a signed snapshot");
        }
        if (snapshot.canonicalBytes().length > MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("Route snapshot exceeds bounded Oxia event size");
        }
    }

    public static OxiaRouteSnapshotRecord create(
            final long revision,
            final long previousRevision,
            final RouteSelectionHint route,
            final RouteSnapshot snapshot) {
        return new OxiaRouteSnapshotRecord(revision, previousRevision, route, snapshot);
    }

    public static OxiaRouteSnapshotRecord decode(final byte[] encoded, final PublicKey verificationKey) {
        Objects.requireNonNull(verificationKey, "verificationKey");
        if (encoded == null || encoded.length > MAX_SNAPSHOT_BYTES + 256) {
            throw new IllegalArgumentException("Route event exceeds bounded size");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        if (uint(version, 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Route event version");
        }
        final long revision = positive(uint(next(reader, 2), 2), "revision");
        final long previousRevision = nonNegative(uint(next(reader, 3), 3), "previousRevision");
        final RouteSelectionHint route = new RouteSelectionHint(
                com.nereusstream.delay.protocol.AdapterKind.fromWire(uint(next(reader, 4), 4)),
                bytes(next(reader, 5), 5));
        final byte[] snapshotBytes = bytes(next(reader, 6), 6);
        final byte[] digest = bytes(next(reader, 7), 7);
        Bytes.requireLength(digest, 32, "recordDigest");
        if (reader.hasRemaining()
                || !Bytes.constantTimeEquals(
                        digest,
                        Bytes.sha256(
                                DIGEST_DOMAIN,
                                canonicalWithoutDigest(revision, previousRevision, route, snapshotBytes)))) {
            throw new IllegalArgumentException("Route event digest or field order is invalid");
        }
        final RouteSnapshot snapshot = RouteSnapshot.decode(snapshotBytes, verificationKey);
        final OxiaRouteSnapshotRecord result = new OxiaRouteSnapshotRecord(revision, previousRevision, route, snapshot);
        if (!Bytes.constantTimeEquals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Route event is not canonical");
        }
        return result;
    }

    public long revision() {
        return revision;
    }

    public long previousRevision() {
        return previousRevision;
    }

    public RouteSelectionHint route() {
        return route;
    }

    public RouteSnapshot snapshot() {
        return snapshot;
    }

    public byte[] recordDigest() {
        return Bytes.sha256(
                DIGEST_DOMAIN, canonicalWithoutDigest(revision, previousRevision, route, snapshot.canonicalBytes()));
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(canonicalWithoutDigest(revision, previousRevision, route, snapshot.canonicalBytes()));
            CanonicalProtobuf.bytes(output, 7, recordDigest());
        });
    }

    private static byte[] canonicalWithoutDigest(
            final long revision,
            final long previousRevision,
            final RouteSelectionHint route,
            final byte[] snapshotBytes) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint64(output, 2, revision);
            CanonicalProtobuf.uint64(output, 3, previousRevision);
            CanonicalProtobuf.uint32(output, 4, route.adapterKind().wireValue());
            CanonicalProtobuf.bytes(output, 5, route.routeAliasUtf8Nfc());
            CanonicalProtobuf.bytes(output, 6, snapshotBytes);
        });
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalArgumentException("missing Route event field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalArgumentException("unexpected Route event field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Route event bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid Route event uint field " + number);
        }
        return field.unsignedValue();
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }
}
