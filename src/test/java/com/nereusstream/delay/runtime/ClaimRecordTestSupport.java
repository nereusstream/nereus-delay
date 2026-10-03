package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.store.ShardStore;

/** Package bridge for tests that need a persisted Claim from the real runtime path. */
public final class ClaimRecordTestSupport {
    private ClaimRecordTestSupport() {}

    public static ClaimRecord claimScheduled(
            final ShardStore store,
            final PreparedCommand schedule,
            final KafkaSourcePosition sourcePosition,
            final DestinationLaneId laneId,
            final AuthorIdentity owner,
            final long nowEpochMs,
            final byte[] chargeVector) {
        final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
        shard.apply(schedule, sourcePosition);
        shard.updateLaneReadiness(laneId, RuntimeReadiness.READY);
        return shard.claimForPublish(schedule.delayMessageId(), owner, nowEpochMs, new byte[0], chargeVector);
    }
}
