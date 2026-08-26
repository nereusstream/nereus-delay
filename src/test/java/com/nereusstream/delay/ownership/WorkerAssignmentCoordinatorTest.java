package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerAssignmentCoordinatorTest {
    @Test
    void scorerPublicationAndWorkerAcceptanceUseTheSameExactAssignment() {
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final SourceAssignment source = sourceAssignment();
        final WorkerAssignmentCoordinator.PlacementResult result = coordinator.place(
                source,
                Bytes.sha256(Bytes.utf8("capacity-envelope")),
                1,
                List.of(candidate("worker-a", 2)),
                capacity(CapacityDimension.DB_INSTANCES, 1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                100,
                0,
                0);

        assertEquals(
                WorkerPlacementPolicy.DecisionReason.SELECTED, result.decision().reason());
        final WorkerAssignmentAuthority.Publication publication =
                result.publication().orElseThrow();
        assertEquals("worker-a", publication.assignment().workerId());
        assertEquals(
                publication.assignment(),
                coordinator.requireAccepted(source.shardId(), publication.revision(), publication.assignment()));
    }

    @Test
    void noCapacityDoesNotPublishAnAssignment() {
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final SourceAssignment source = sourceAssignment();
        final WorkerAssignmentCoordinator.PlacementResult result = coordinator.place(
                source,
                Bytes.sha256(Bytes.utf8("capacity-envelope")),
                1,
                List.of(candidate("worker-a", 0)),
                capacity(CapacityDimension.DB_INSTANCES, 1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                100,
                0,
                0);

        assertEquals(
                WorkerPlacementPolicy.DecisionReason.NO_CAPACITY,
                result.decision().reason());
        assertTrue(result.publication().isEmpty());
        assertTrue(authority.current(source.shardId()).isEmpty());
    }

    @Test
    void acceptanceRejectsAStaleRevisionAfterReplacement() {
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentCoordinator coordinator = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final SourceAssignment source = sourceAssignment();
        final WorkerAssignmentCoordinator.PlacementResult first = coordinator.place(
                source,
                Bytes.sha256(Bytes.utf8("capacity-envelope")),
                1,
                List.of(candidate("worker-a", 2)),
                capacity(CapacityDimension.DB_INSTANCES, 1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                100,
                0,
                0);
        final WorkerAssignmentCoordinator.PlacementResult replacement = coordinator.place(
                source,
                Bytes.sha256(Bytes.utf8("capacity-envelope-2")),
                2,
                List.of(candidate("worker-b", 2)),
                capacity(CapacityDimension.DB_INSTANCES, 1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                100,
                0,
                first.publication().orElseThrow().revision());

        assertEquals(2, replacement.publication().orElseThrow().revision());
        assertThrows(
                IllegalStateException.class,
                () -> coordinator.requireAccepted(
                        source.shardId(),
                        first.publication().orElseThrow().revision(),
                        first.publication().orElseThrow().assignment()));
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate(final String workerId, final long dbCapacity) {
        return new WorkerPlacementPolicy.WorkerCandidate(
                workerId,
                capacity(CapacityDimension.DB_INSTANCES, dbCapacity),
                CapacityVector.empty(),
                0,
                10,
                0,
                10,
                WorkerLoadVector.empty(),
                WorkerLoadVector.empty(),
                100,
                true,
                0);
    }

    private static CapacityVector capacity(final CapacityDimension dimension, final long amount) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[dimension.wireValue() - 1] = amount;
        return new CapacityVector(values);
    }

    private static SourceAssignment sourceAssignment() {
        final ShardId shard =
                new ShardId(RouteIncarnation.fromUuid(UUID.fromString("10213243-5465-7687-98a9-bacbdcedfe0f")), 2);
        return new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("source")),
                1,
                new KafkaActivationBarrier(
                        shard, "cluster-a", UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), 0));
    }
}
