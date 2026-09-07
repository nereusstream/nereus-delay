package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Stable generation/expiry projection, independent of reversible work and admitted-attempt obligations. */
public final class TargetExpiryRef {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 16;
    public static final int MAX_CANONICAL_BYTES = 2 + 3 + TargetMessageLocator.MAX_CANONICAL_BYTES + 11 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-expiry\0");
    private final TargetMessageLocator locator;
    private final long expireAtEpochMs;
    private final byte[] digest;

    public TargetExpiryRef(final TargetMessageLocator locator, final long expireAtEpochMs) {
        this.locator = Objects.requireNonNull(locator, "locator");
        if (expireAtEpochMs < 0) {
            throw new IllegalArgumentException("negative Target expiry time");
        }
        this.expireAtEpochMs = expireAtEpochMs;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetMessageLocator locator() {
        return locator;
    }

    public long expireAtEpochMs() {
        return expireAtEpochMs;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.expiry(expireAtEpochMs, locator.target(), locator.messageId(), locator.generation());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, expireAtEpochMs);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 4, digest);
        });
    }

    public static TargetExpiryRef decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target expiry exceeds byte bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "TargetExpiryRef");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "TargetExpiryRef");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target expiry schema");
        }
        final TargetExpiryRef ref = new TargetExpiryRef(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.uint(fields.get(2), 3));
        if (!Bytes.constantTimeEquals(ref.digest, QueryCodecSupport.fixed(fields.get(3), 4, 32))) {
            throw new IllegalArgumentException("Target expiry digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, ref.canonicalBytes(), "TargetExpiryRef");
        return ref;
    }

    public static TargetExpiryRef decodeForMessage(
            final byte[] key, final byte[] value, final TargetMessageRecord message) {
        final TargetExpiryRef ref = decode(value);
        if (!Arrays.equals(key, ref.encodedKey())
                || !ref.locator.equals(message.locator())
                || ref.expireAtEpochMs != message.expireAtEpochMs()
                || message.runtime().terminal()) {
            throw new IllegalArgumentException("Target expiry differs from its live Message generation");
        }
        return ref;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetExpiryRef that
                && expireAtEpochMs == that.expireAtEpochMs
                && locator.equals(that.locator);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
