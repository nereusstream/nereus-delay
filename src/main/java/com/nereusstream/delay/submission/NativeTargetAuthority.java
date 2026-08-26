package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.NativePreparedDelivery;
import java.util.Objects;

/** Frozen target/capability authority for a native prepared delivery. */
public record NativeTargetAuthority(NativePreparedDelivery prepared) implements SubmissionRouteAuthority {
    public NativeTargetAuthority {
        Objects.requireNonNull(prepared, "prepared");
    }
}
