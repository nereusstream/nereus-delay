package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Objects;

/**
 * Small storage seam used by the local payload adapter.
 *
 * <p>The reservation/handle/proof state machine remains the authority in
 * {@link InMemoryPayloadObjectStore}; this backend owns only immutable payload
 * bytes. A missing value is represented by {@code null}. Implementations must
 * return a defensive copy and must accept a repeated put only when the bytes
 * are identical.</p>
 */
@FunctionalInterface
interface PayloadObjectBackend {
    /** Reads the immutable bytes for one service-owned object identity. */
    byte[] read(String objectIdentity);

    /**
     * Stores one immutable object, or accepts an exact byte-identical retry.
     *
     * @param objectIdentity service-owned opaque identity
     * @param payload payload bytes
     * @param maxBytes configured object-size bound
     */
    default void putIfAbsent(final String objectIdentity, final byte[] payload, final long maxBytes) {
        throw new UnsupportedOperationException("payload backend is read-only");
    }

    /**
     * Returns the immutable provider version retained in the payload proof.
     * Local backends use the content-addressed fallback; remote backends must
     * override this when the Profile requires a provider-issued version.
     */
    default byte[] immutableObjectVersion(final String objectIdentity, final byte[] payloadSha256) {
        Objects.requireNonNull(objectIdentity, "objectIdentity");
        Objects.requireNonNull(payloadSha256, "payloadSha256");
        return Bytes.concat(Bytes.utf8("sha256-"), Bytes.utf8(Bytes.hex(payloadSha256)));
    }

    /** Returns an optional provider ETag or the deterministic local digest. */
    default byte[] etag(final String objectIdentity, final byte[] payloadSha256) {
        Objects.requireNonNull(objectIdentity, "objectIdentity");
        return Bytes.copy(Objects.requireNonNull(payloadSha256, "payloadSha256"));
    }
}
