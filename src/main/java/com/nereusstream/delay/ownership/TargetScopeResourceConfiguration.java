package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetScopeResourcePolicy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Public expected deployment data. Loading this file never creates an owned allocation. */
public record TargetScopeResourceConfiguration(TargetScopeResourcePolicy policy, ExpectedOwners expectedOwners,
        Limits limits) {
    public static final int MAX_CANONICAL_BYTES = TargetScopeResourcePolicy.MAX_CANONICAL_BYTES + 16_384;

    public TargetScopeResourceConfiguration {
        Objects.requireNonNull(policy); Objects.requireNonNull(expectedOwners); Objects.requireNonNull(limits);
        if (limits.maximumRetainedBytes() < policy.canonicalBytes().length)
            throw new IllegalArgumentException("Scope retained-byte limit cannot retain its complete policy");
    }

    /** Exact external owner's original parent allocation and durable restart identity. */
    public record ExpectedOwner(String ownerId, String backendId, byte[] parentAllocationId,
            byte[] allocationIncarnation, long birthVersion, byte[] trustRootDigest) {
        public ExpectedOwner {
            ownerId = text(ownerId, 1024, "original owner ID");
            backendId = text(backendId, 1024, "original backend ID");
            parentAllocationId = assigned(parentAllocationId, 32, "original parent allocation ID");
            allocationIncarnation = assigned(allocationIncarnation, 16, "durable allocation incarnation");
            nonnegative(birthVersion, "durable allocation birth version");
            trustRootDigest = assigned(trustRootDigest, 32, "original owner trust root digest");
        }
        @Override public byte[] parentAllocationId() { return parentAllocationId.clone(); }
        @Override public byte[] allocationIncarnation() { return allocationIncarnation.clone(); }
        @Override public byte[] trustRootDigest() { return trustRootDigest.clone(); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, Bytes.utf8(ownerId));
                CanonicalProtobuf.bytes(out, 2, Bytes.utf8(backendId));
                CanonicalProtobuf.bytes(out, 3, parentAllocationId);
                CanonicalProtobuf.bytes(out, 4, allocationIncarnation);
                CanonicalProtobuf.uint64(out, 5, birthVersion);
                CanonicalProtobuf.bytes(out, 6, trustRootDigest);
            });
        }
        static ExpectedOwner decode(byte[] encoded) {
            final var f = fields(encoded, 2304, 6, "expected original owner");
            final var value = new ExpectedOwner(utf8(f.get(0), 1), utf8(f.get(1), 2),
                    QueryCodecSupport.bytes(f.get(2), 3), QueryCodecSupport.bytes(f.get(3), 4),
                    QueryCodecSupport.uint64Bits(f.get(4), 5), QueryCodecSupport.bytes(f.get(5), 6));
            canonical(encoded, value.canonicalBytes()); return value;
        }
        public boolean matches(ExpectedOwner other) {
            return other != null && Arrays.equals(canonicalBytes(), other.canonicalBytes());
        }
    }

    public record ExpectedOwners(ExpectedOwner source, ExpectedOwner registry) {
        public ExpectedOwners { Objects.requireNonNull(source); Objects.requireNonNull(registry); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, source.canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, registry.canonicalBytes());
            });
        }
        static ExpectedOwners decode(byte[] encoded) {
            final var f = fields(encoded, 4700, 2, "expected original owners");
            final var value = new ExpectedOwners(ExpectedOwner.decode(QueryCodecSupport.bytes(f.get(0), 1)),
                    ExpectedOwner.decode(QueryCodecSupport.bytes(f.get(1), 2)));
            canonical(encoded, value.canonicalBytes()); return value;
        }
    }

    public record Limits(long maximumLocalReadBytes, long maximumRetainedBytes, int maximumLocalBorrows,
            long requestTimeoutNanos, long maximumSourceFrameBytes, long minimumRetainedUntilEpochMs) {
        public Limits {
            positive(maximumLocalReadBytes, "local allocation read bytes");
            positive(maximumRetainedBytes, "local allocation retained bytes");
            if (maximumLocalReadBytes > maximumRetainedBytes || maximumLocalBorrows < 1
                    || maximumLocalBorrows > TargetScopeResourcePolicy.MAX_PROCESS_LOANS)
                throw new IllegalArgumentException("explicit finite allocation read/borrow bounds required");
            positive(requestTimeoutNanos, "allocation request timeout");
            positive(maximumSourceFrameBytes, "complete Source frame bytes");
            if (maximumSourceFrameBytes >= Integer.MAX_VALUE)
                throw new IllegalArgumentException("complete Source frame must fit a finite native frame");
            positive(minimumRetainedUntilEpochMs, "minimum original retention horizon");
        }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint64(out, 1, maximumLocalReadBytes);
                CanonicalProtobuf.uint64(out, 2, maximumRetainedBytes);
                CanonicalProtobuf.uint32(out, 3, maximumLocalBorrows);
                CanonicalProtobuf.uint64(out, 4, requestTimeoutNanos);
                CanonicalProtobuf.uint64(out, 5, maximumSourceFrameBytes);
                CanonicalProtobuf.uint64(out, 6, minimumRetainedUntilEpochMs);
            });
        }
        static Limits decode(byte[] encoded) {
            final var f = fields(encoded, 68, 6, "allocation limits");
            final var value = new Limits(QueryCodecSupport.uint64Bits(f.get(0), 1),
                    QueryCodecSupport.uint64Bits(f.get(1), 2), QueryCodecSupport.uint32(f.get(2), 3),
                    QueryCodecSupport.uint64Bits(f.get(3), 4), QueryCodecSupport.uint64Bits(f.get(4), 5),
                    QueryCodecSupport.uint64Bits(f.get(5), 6));
            canonical(encoded, value.canonicalBytes()); return value;
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, policy.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, expectedOwners.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, limits.canonicalBytes());
        });
    }

    public static TargetScopeResourceConfiguration decode(byte[] encoded) {
        final var f = fields(encoded, MAX_CANONICAL_BYTES, 4, "Scope deployment configuration");
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1)
            throw new IllegalArgumentException("unknown Scope deployment configuration schema");
        final var value = new TargetScopeResourceConfiguration(
                TargetScopeResourcePolicy.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                ExpectedOwners.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                Limits.decode(QueryCodecSupport.bytes(f.get(3), 4)));
        canonical(encoded, value.canonicalBytes()); return value;
    }

    /** Closed canonical-protobuf schema; missing, unknown, duplicate and unbounded fields are rejected. */
    public static TargetScopeResourceConfiguration load(Path path) throws IOException {
        final var file = Objects.requireNonNull(path).toAbsolutePath().normalize();
        final var before = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!before.isRegularFile() || before.size() > MAX_CANONICAL_BYTES)
            throw new IOException("Scope configuration requires a bounded ordinary canonical file");
        final byte[] image;
        try (var input = Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            image = input.readNBytes(MAX_CANONICAL_BYTES + 1);
        }
        final var after = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (image.length > MAX_CANONICAL_BYTES || !after.isRegularFile() || image.length != before.size()
                || before.size() != after.size() || !Objects.equals(before.fileKey(), after.fileKey())
                || !before.lastModifiedTime().equals(after.lastModifiedTime()))
            throw new IOException("Scope configuration changed during bounded loading");
        return decode(image);
    }

    static List<CanonicalProtobuf.Reader.Field> fields(byte[] encoded, int bound, int count, String name) {
        if (encoded == null || encoded.length == 0 || encoded.length > bound)
            throw new IllegalArgumentException(name + " exceeds its finite canonical bound");
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final var fields = new java.util.ArrayList<CanonicalProtobuf.Reader.Field>(count);
        while (reader.hasRemaining()) {
            if (fields.size() == count) throw new IllegalArgumentException("unknown " + name + " field");
            final var field = reader.next();
            if (field.number() != fields.size() + 1) throw new IllegalArgumentException("missing " + name + " field");
            fields.add(field);
        }
        if (fields.size() != count) throw new IllegalArgumentException("incomplete " + name);
        return fields;
    }

    static void canonical(byte[] actual, byte[] expected) {
        if (!Arrays.equals(actual, expected)) throw new IllegalArgumentException("noncanonical Scope deployment data");
    }
    static byte[] assigned(byte[] bytes, int length, String name) {
        Bytes.requireLength(bytes, length, name);
        boolean assigned = false;
        for (byte value : bytes) assigned |= value != 0;
        if (!assigned) throw new IllegalArgumentException(name + " must be assigned");
        return bytes.clone();
    }
    static String text(String value, int maximum, String name) {
        Objects.requireNonNull(value, name);
        final var bytes = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || bytes.length > maximum || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)
                || !value.equals(new String(bytes, StandardCharsets.UTF_8)))
            throw new IllegalArgumentException(name + " requires bounded canonical UTF-8");
        return value;
    }
    private static String utf8(CanonicalProtobuf.Reader.Field field, int number) {
        final var bytes = QueryCodecSupport.bytes(field, number);
        final var value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8)))
            throw new IllegalArgumentException("invalid original owner UTF-8");
        return value;
    }
    static void nonnegative(long value, String name) {
        if (value < 0 || value == Long.MAX_VALUE) throw new IllegalArgumentException(name + " must be finite nonnegative");
    }
    static void positive(long value, String name) {
        if (value < 1 || value == Long.MAX_VALUE) throw new IllegalArgumentException(name + " must be finite positive");
    }
}
