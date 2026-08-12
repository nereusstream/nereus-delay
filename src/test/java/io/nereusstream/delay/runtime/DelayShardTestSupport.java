package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AuthorIdentity;
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

    public static ClaimRecord claimForPublish(final DelayShard shard,
                                              final DelayMessageId messageId,
                                              final AuthorIdentity owner,
                                              final long claimDeadlineEpochMs,
                                              final byte[] materialization,
                                              final byte[] claimedCharge) {
        return shard.claimForPublish(messageId, owner, claimDeadlineEpochMs, materialization, claimedCharge);
    }
}
