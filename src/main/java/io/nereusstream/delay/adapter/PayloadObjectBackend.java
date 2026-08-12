package io.nereusstream.delay.adapter;

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
}
