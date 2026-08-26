package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Canonical payload projection used by a Claim and a prepared publish.
 *
 * <p>The payload bytes are opaque to Nereus Delay. The projection records the
 * exact length and SHA-256 digest and selects either inline bytes or a committed
 * immutable Object Store descriptor. The selected branch is part of the
 * canonical bytes and cannot be changed during replay.</p>
 */
public final class PayloadForPublish {
    public static final int HASH_LENGTH = 32;

    private final long length;
    private final byte[] payloadSha256;
    private final byte[] inlinePayload;
    private final CommittedPayloadDescriptor object;

    private PayloadForPublish(
            final long length,
            final byte[] payloadSha256,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptor object) {
        if (length < 0) {
            throw new IllegalArgumentException("payload length must be non-negative");
        }
        if ((inlinePayload == null) == (object == null)) {
            throw new IllegalArgumentException("PayloadForPublish must select exactly one branch");
        }
        Bytes.requireLength(payloadSha256, HASH_LENGTH, "payloadSha256");
        this.length = length;
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.inlinePayload = inlinePayload == null ? null : Bytes.copy(inlinePayload);
        this.object = object;
        if (inlinePayload != null) {
            if (inlinePayload.length != length || !Arrays.equals(this.payloadSha256, Bytes.sha256(inlinePayload))) {
                throw new IllegalArgumentException("inline payload length/hash mismatch");
            }
        } else if (object.length() != length || !Arrays.equals(this.payloadSha256, object.payloadSha256())) {
            throw new IllegalArgumentException("object payload length/hash mismatch");
        }
    }

    /** Creates the inline branch, including an intentional empty payload. */
    public static PayloadForPublish inline(final byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        return new PayloadForPublish(payload.length, Bytes.sha256(payload), payload, null);
    }

    /** Creates the committed immutable Object Store branch. */
    public static PayloadForPublish object(final CommittedPayloadDescriptor descriptor) {
        return new PayloadForPublish(
                Objects.requireNonNull(descriptor, "descriptor").length(),
                descriptor.payloadSha256(),
                null,
                descriptor);
    }

    public long length() {
        return length;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public boolean hasInlinePayload() {
        return inlinePayload != null;
    }

    public byte[] inlinePayload() {
        if (inlinePayload == null) {
            throw new IllegalStateException("PayloadForPublish has no inline branch");
        }
        return Bytes.copy(inlinePayload);
    }

    public boolean hasObject() {
        return object != null;
    }

    public CommittedPayloadDescriptor object() {
        if (object == null) {
            throw new IllegalStateException("PayloadForPublish has no object branch");
        }
        return object;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, length);
            CanonicalProtobuf.bytes(output, 2, payloadSha256);
            if (inlinePayload != null) {
                CanonicalProtobuf.bytes(output, 3, inlinePayload);
            } else {
                CanonicalProtobuf.bytes(output, 4, object.canonicalBytes());
            }
        });
    }

    /** Decodes and validates canonical PayloadForPublish bytes. */
    public static PayloadForPublish decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PayloadForPublish");
        if (fields.size() != 3) {
            throw new IllegalArgumentException("PayloadForPublish must contain exactly three fields");
        }
        final long length = QueryCodecSupport.uint(fields.get(0), 1);
        final byte[] hash = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        final PayloadForPublish result;
        if (fields.get(2).number() == 3) {
            result = new PayloadForPublish(length, hash, QueryCodecSupport.bytes(fields.get(2), 3), null);
        } else if (fields.get(2).number() == 4) {
            result = objectWithDeclaredValues(
                    length, hash, CommittedPayloadDescriptor.decode(QueryCodecSupport.nested(fields.get(2), 4)));
        } else {
            throw new IllegalArgumentException("PayloadForPublish has an unknown payload branch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadForPublish");
        return result;
    }

    private static PayloadForPublish objectWithDeclaredValues(
            final long length, final byte[] hash, final CommittedPayloadDescriptor descriptor) {
        return new PayloadForPublish(length, hash, null, descriptor);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadForPublish that
                && length == that.length
                && Arrays.equals(payloadSha256, that.payloadSha256)
                && Arrays.equals(inlinePayload, that.inlinePayload)
                && Objects.equals(object, that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(length, Arrays.hashCode(payloadSha256), Arrays.hashCode(inlinePayload), object);
    }
}
