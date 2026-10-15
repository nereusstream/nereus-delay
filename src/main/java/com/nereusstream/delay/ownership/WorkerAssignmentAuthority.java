package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteSnapshot;
import java.util.Objects;
import java.util.Optional;

/** Durable desired-assignment boundary between placement and Worker startup. */
public interface WorkerAssignmentAuthority {
    /**
     * Publishes one exact assignment at the caller's expected record revision.
     * Revision zero means that no assignment record may exist yet.
     */
    Publication publish(WorkerAssignment assignment, long expectedRevision);

    /** Exact source renewal CAS; legacy authorities cannot silently discard its restart evidence. */
    default Publication publishReactivation(Publication previous, WorkerAssignment successor, RouteSnapshot route) {
        throw new UnsupportedOperationException("assignment authority has no durable source reactivation receipt");
    }

    /** Reads the exact current assignment projection for one shard. */
    Optional<Publication> current(ShardId shardId);

    /** Removes only the exact assignment publication that the caller owns. */
    boolean withdraw(Publication expected);

    record Publication(long revision, WorkerAssignment assignment, PulsarSourceReactivationReceipt reactivation) {
        public Publication(long revision, WorkerAssignment assignment) { this(revision, assignment, null); }

        public Publication {
            if (revision <= 0) {
                throw new IllegalArgumentException("assignment publication revision must be positive");
            }
            Objects.requireNonNull(assignment, "assignment");
            if (reactivation != null) reactivation.requireSuccessor(revision, assignment);
        }

        public byte[] digest() {
            return Bytes.sha256(Bytes.utf8("nereus-delay-worker-assignment-publication\0"), Bytes.u64beBits(revision),
                    Bytes.lp32(assignment.canonicalBytes()), Bytes.lp32(reactivation == null ? new byte[0] : reactivation.canonicalBytes()));
        }

        public boolean sameIdentity(Publication other) {
            return other != null && revision == other.revision() && assignment.sameIdentity(other.assignment())
                    && Bytes.constantTimeEquals(digest(), other.digest());
        }

        /** Only the actual authority publication may select the explicit renewal branch. */
        public void requireRouteProjection(RouteSnapshot route) {
            if (!assignment.routeBound() || !Bytes.constantTimeEquals(assignment.routeSnapshotDigest(), route.snapshotDigest())) {
                throw new IllegalStateException("assignment changed its signed Route binding");
            }
            if (reactivation != null) { reactivation.requireRoute(route); return; }
            final var source = assignment.sourceAssignment();
            if (!RouteSourceAssignmentFactory.fromRoute(route, source.shardId().partition(), source.assignmentId(),
                    source.assignmentEpoch()).sameIdentity(source)) {
                throw new IllegalStateException("assignment is not the strict signed Route projection");
            }
        }
    }
}
