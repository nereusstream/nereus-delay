package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;

/** Builds a Worker source assignment from the exact signed Route partition policy. */
public final class RouteSourceAssignmentFactory {
    private RouteSourceAssignmentFactory() {
    }

    public static SourceAssignment fromRoute(final RouteSnapshotV1 route, final int partition,
                                             final byte[] assignmentId, final long assignmentEpoch) {
        Objects.requireNonNull(route, "route");
        final RoutePartitionPolicyV1 policy = route.partitionPolicy(partition);
        final ShardId shard = new ShardId(route.routeIncarnation(), partition);
        Bytes.requireLength(assignmentId, SourceAssignment.ID_LENGTH, "assignmentId");
        return new SourceAssignment(shard, assignmentId, assignmentEpoch,
                policy.activationBarrier().toSourceBarrier(route.routeIncarnation()));
    }
}
