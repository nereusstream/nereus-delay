package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.ShardId;
import java.util.Objects;

/** Builds a Worker source assignment from the exact signed Route partition policy. */
public final class RouteSourceAssignmentFactory {
    private RouteSourceAssignmentFactory() {}

    public static SourceAssignment fromRoute(
            final RouteSnapshot route, final int partition, final byte[] assignmentId, final long assignmentEpoch) {
        Objects.requireNonNull(route, "route");
        final RoutePartitionPolicy policy = route.partitionPolicy(partition);
        final ShardId shard = new ShardId(route.routeIncarnation(), partition);
        Bytes.requireLength(assignmentId, SourceAssignment.ID_LENGTH, "assignmentId");
        return new SourceAssignment(
                shard,
                assignmentId,
                assignmentEpoch,
                policy.activationBarrier().toSourceBarrier(route.routeIncarnation()));
    }
}
