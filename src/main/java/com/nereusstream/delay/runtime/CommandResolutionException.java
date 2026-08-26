package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;

/**
 * Fail-closed error returned by the local Profile/Adapter/Lane resolver.
 *
 * <p>The resolver is the seam at which source-position-pinned external
 * authority is supplied to the shard. A missing or stale snapshot must keep
 * its stable protocol meaning; it must not be collapsed into a malformed
 * legacy command.</p>
 */
public final class CommandResolutionException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    private final StableCode stableCode;

    public CommandResolutionException(final StableCode stableCode, final String message) {
        super(Objects.requireNonNull(message, "message"));
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
    }

    public StableCode stableCode() {
        return stableCode;
    }
}
