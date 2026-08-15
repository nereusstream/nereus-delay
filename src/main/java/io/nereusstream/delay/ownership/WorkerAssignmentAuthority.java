package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;
import java.util.Optional;

/** Durable desired-assignment boundary between placement and Worker startup. */
public interface WorkerAssignmentAuthority {
    /**
     * Publishes one exact assignment at the caller's expected record revision.
     * Revision zero means that no assignment record may exist yet.
     */
    Publication publish(WorkerAssignment assignment, long expectedRevision);

    /** Reads the exact current assignment projection for one shard. */
    Optional<Publication> current(ShardId shardId);

    /** Removes only the exact assignment publication that the caller owns. */
    boolean withdraw(Publication expected);

    record Publication(long revision, WorkerAssignment assignment) {
        public Publication {
            if (revision <= 0) {
                throw new IllegalArgumentException("assignment publication revision must be positive");
            }
            Objects.requireNonNull(assignment, "assignment");
        }
    }
}
