package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerPlacementPolicyTest {
    @Test
    void hardFilterRejectsOverCapacityInsteadOfChoosingLeastOverfullWorker() {
        final WorkerPlacementPolicy policy = new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(
                1_000, 0, 0, 1, 0));
        final WorkerPlacementPolicy.Decision decision = policy.select(List.of(
                candidate("overfull", 1, 1, 1_000, false, 0),
                candidate("fit", 4, 0, 1_000, false, 0)),
                vector(CapacityDimensionV1.DB_INSTANCES, 1), CapacityVectorV1.empty(), CapacityVectorV1.empty(),
                null, 1_000, 0);
        assertEquals("fit", decision.workerId());
        assertEquals(WorkerPlacementPolicy.DecisionReason.SELECTED, decision.reason());
    }

    @Test
    void dominantObservedLoadBreaksEqualShardCountTie() {
        final WorkerPlacementPolicy policy = new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(
                1_000, 0, 0, 1, 0));
        final WorkerPlacementPolicy.Decision decision = policy.select(List.of(
                candidate("busy", 8, 0, 1_000, false, 0, 80),
                candidate("quiet", 8, 0, 1_000, false, 0, 20)),
                vector(CapacityDimensionV1.DB_INSTANCES, 1), CapacityVectorV1.empty(), CapacityVectorV1.empty(),
                null, 1_000, 0);
        assertEquals("quiet", decision.workerId());
    }

    @Test
    void hysteresisPreventsChurn() {
        final WorkerPlacementPolicy policy = new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(
                100, 0, 0.20, 0.50, 0));
        final WorkerPlacementPolicy.Decision decision = policy.select(List.of(
                candidate("current", 8, 0, 1_000, false, 500, 40),
                candidate("new", 8, 0, 1_000, false, 1_000, 30)),
                vector(CapacityDimensionV1.DB_INSTANCES, 1), CapacityVectorV1.empty(), CapacityVectorV1.empty(),
                "current", 1_050, 0);
        assertEquals("current", decision.workerId());
        assertEquals(WorkerPlacementPolicy.DecisionReason.HYSTERESIS, decision.reason());
    }

    @Test
    void staleTelemetryIsPenalizedWithoutBecomingAnUnrepresentableDecision() {
        final WorkerPlacementPolicy policy = new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(
                100, 0, 0, 0.50, 0));
        final WorkerPlacementPolicy.Decision decision = policy.select(List.of(
                candidate("stale", 8, 0, 0, false, 0, 10),
                candidate("fresh", 8, 0, 1_000, false, 0, 20)),
                vector(CapacityDimensionV1.DB_INSTANCES, 1), CapacityVectorV1.empty(), CapacityVectorV1.empty(),
                null, 1_050, 0);
        assertEquals("fresh", decision.workerId());
    }

    @Test
    void noCapacityIsExplicitAndArithmeticOverflowFailsClosed() {
        final WorkerPlacementPolicy policy = new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(
                1_000, 0, 0, 1, 0));
        final WorkerPlacementPolicy.Decision decision = policy.select(List.of(
                candidate("full", 1, 1, 1_000, false, 0)),
                vector(CapacityDimensionV1.DB_INSTANCES, 1), CapacityVectorV1.empty(), CapacityVectorV1.empty(),
                null, 1_000, 0);
        assertEquals(WorkerPlacementPolicy.DecisionReason.NO_CAPACITY, decision.reason());
        assertThrows(ArithmeticException.class, () -> policy.select(List.of(
                        candidate("overflow", Long.MAX_VALUE, Long.MAX_VALUE, 1_000, false, 0)),
                vector(CapacityDimensionV1.DB_INSTANCES, 0), vector(CapacityDimensionV1.DB_INSTANCES, 1),
                CapacityVectorV1.empty(),
                null, 1_000, 0));
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate(final String worker, final long hardDb,
                                                                   final long committedDb, final long observedAt,
                                                                   final boolean stale, final long residence) {
        return candidate(worker, hardDb, committedDb, observedAt, stale, residence, 10);
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate(final String worker, final long hardDb,
                                                                   final long committedDb, final long observedAt,
                                                                   final boolean stale, final long residence,
                                                                   final long activeMessages) {
        final CapacityVectorV1 hard = vector(CapacityDimensionV1.DB_INSTANCES, hardDb);
        final CapacityVectorV1 committed = vector(CapacityDimensionV1.DB_INSTANCES, committedDb);
        final WorkerLoadVector load = new WorkerLoadVector(activeMessages, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0);
        final WorkerLoadVector ceilings = new WorkerLoadVector(100, 1_000, 1_000, 1_000, 1_000, 1_000, 1_000,
                1_000, 1_000, 1_000, 1_000, 1_000, 1_000, 1_000, 1_000, 1_000);
        return new WorkerPlacementPolicy.WorkerCandidate(worker, hard, committed, 0, 4, 0, 4, load, ceilings,
                stale ? Math.max(0, observedAt - 1_000) : observedAt, true, residence);
    }

    private static CapacityVectorV1 vector(final CapacityDimensionV1 dimension, final long amount) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[dimension.wireValue() - 1] = amount;
        return new CapacityVectorV1(values);
    }
}
