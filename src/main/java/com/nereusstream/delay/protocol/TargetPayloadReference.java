package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Bounds and committed-identity requirements for object payloads in the Target Message format. */
public final class TargetPayloadReference {
    public static final int MAX_COMPONENT_BYTES = 1 << 20;
    public static final int MAX_CANONICAL_BYTES = 4 + 32 + 4 * (4 + MAX_COMPONENT_BYTES) + 8 + 32 + 32 + 32;

    private TargetPayloadReference() {}

    public static PayloadReference decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target payload reference exceeds encoding bound");
        }
        try {
            return requireBounded(PayloadReference.decode(encoded));
        } catch (java.nio.BufferUnderflowException truncated) {
            throw new IllegalArgumentException("truncated Target payload reference", truncated);
        }
    }

    public static PayloadReference requireBounded(final PayloadReference reference) {
        Objects.requireNonNull(reference, "reference");
        if (reference.maximumIdentityComponentBytes() > MAX_COMPONENT_BYTES) {
            throw new IllegalArgumentException("Target object identity component exceeds bound");
        }
        if (!reference.hasCommitIdentity() || Arrays.equals(reference.objectStoreProfileHash(), new byte[32])) {
            throw new IllegalArgumentException("Target object payload lacks its committed resource identity");
        }
        return reference;
    }
}
