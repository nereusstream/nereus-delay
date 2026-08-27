package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Exact resolved bytes used to construct a physical Pulsar record. */
public final class ResolvedPayload {
    public static final int HASH_LENGTH = 32;

    private final byte[] bytes;
    private final byte[] sha256;

    public ResolvedPayload(final byte[] bytes, final byte[] sha256) {
        this.bytes = Bytes.copy(Objects.requireNonNull(bytes, "bytes"));
        Bytes.requireLength(sha256, HASH_LENGTH, "sha256");
        this.sha256 = Bytes.copy(sha256);
        if (!Arrays.equals(this.sha256, Bytes.sha256(this.bytes))) {
            throw new IllegalArgumentException("resolved payload hash mismatch");
        }
    }

    public static ResolvedPayload of(final byte[] bytes) {
        return new ResolvedPayload(bytes, Bytes.sha256(bytes));
    }

    public byte[] bytes() {
        return Bytes.copy(bytes);
    }

    public long length() {
        return bytes.length;
    }

    public byte[] sha256() {
        return Bytes.copy(sha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, bytes.length);
            CanonicalProtobuf.bytes(output, 2, sha256);
            CanonicalProtobuf.bytes(output, 3, bytes);
        });
    }

    public static ResolvedPayload decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ResolvedPayload");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "ResolvedPayload");
        final long length = QueryCodecSupport.uint(fields.get(0), 1);
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("resolved payload is too large");
        }
        final byte[] bytes = QueryCodecSupport.bytes(fields.get(2), 3);
        if (bytes.length != length) {
            throw new IllegalArgumentException("resolved payload length mismatch");
        }
        final ResolvedPayload result =
                new ResolvedPayload(bytes, QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ResolvedPayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ResolvedPayload that
                && Arrays.equals(bytes, that.bytes)
                && Arrays.equals(sha256, that.sha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bytes), Arrays.hashCode(sha256));
    }
}
