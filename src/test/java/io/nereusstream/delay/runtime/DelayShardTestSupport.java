package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;

/** Test-classpath-only bridge for package-local physical mutation seams. */
public final class DelayShardTestSupport {
    private DelayShardTestSupport() {
    }

    public static RetiredMessageIdentityRecord retireMessageIdentity(
            final DelayShard shard, final DelayMessageId messageId,
            final long messageIdentityReuseUntilEpochMs) {
        return shard.retireMessageIdentity(messageId, messageIdentityReuseUntilEpochMs);
    }

    public static LaneRecord updateLaneReadiness(final DelayShard shard,
                                                 final DestinationLaneId laneId,
                                                 final RuntimeReadiness readiness) {
        return shard.updateLaneReadiness(laneId, readiness);
    }
}
