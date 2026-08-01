package io.nereusstream.delay.runtime;

/** Safe payload-presence projection used by the local query surface. */
public enum PayloadAvailability {
    UPLOAD_PENDING,
    INLINE_RETAINED,
    OBJECT_RETAINED,
    PAYLOAD_RECLAIMED,
    NOT_APPLICABLE
}
