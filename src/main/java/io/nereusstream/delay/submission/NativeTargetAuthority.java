package io.nereusstream.delay.submission;

import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;

import java.util.Objects;

/** Frozen target/capability authority for a native prepared delivery. */
public record NativeTargetAuthority(NativePreparedDeliveryV1 prepared)
        implements SubmissionRouteAuthority {
    public NativeTargetAuthority {
        Objects.requireNonNull(prepared, "prepared");
    }
}
