package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.DelayMessageId;

/** Test-classpath-only bridge for package-local physical mutation seams. */
public final class DelayShardTestSupport {
    private DelayShardTestSupport() {
    }

    public static RetiredMessageIdentityRecord retireMessageIdentity(
            final DelayShard shard, final DelayMessageId messageId,
            final long messageIdentityReuseUntilEpochMs) {
        return shard.retireMessageIdentity(messageId, messageIdentityReuseUntilEpochMs);
    }
}
