package io.nereusstream.delay.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Bounded local scheduler bridge for durable Lane-close cursors.
 *
 * <p>The source-ordered Close marker remains the only semantic decision. This
 * helper merely discovers validated {@code timeline/SYSTEM(kind=2)} entries
 * and advances each cursor through {@link DelayShard#materializeClosedLane}.
 * It has no Owner Lease, Oxia, or cross-shard authority and therefore must be
 * invoked only by the owner/worker orchestration that supplies those gates.</p>
 */
final class LaneCloseMaterializer {
    TurnResult runTurn(final DelayShard shard, final int maxLanes, final int maxRecordsPerLane) {
        Objects.requireNonNull(shard, "shard");
        if (maxLanes <= 0 || maxRecordsPerLane <= 0) {
            throw new IllegalArgumentException("materializer bounds must be positive");
        }
        final List<DelayShard.LaneCloseMaterializationWork> work =
                shard.discoverLaneCloseMaterialization(maxLanes);
        final List<DelayShard.LaneCloseMaterializationResult> results = new ArrayList<>(work.size());
        for (DelayShard.LaneCloseMaterializationWork item : work) {
            results.add(shard.materializeClosedLane(item.laneId(), maxRecordsPerLane));
        }
        return new TurnResult(results);
    }

    public record TurnResult(List<DelayShard.LaneCloseMaterializationResult> laneResults) {
        public TurnResult {
            Objects.requireNonNull(laneResults, "laneResults");
            laneResults = List.copyOf(laneResults);
        }

        public int laneCount() {
            return laneResults.size();
        }

        public int scannedRecords() {
            return checkedSum(DelayShard.LaneCloseMaterializationResult::scannedRecords, "scanned records");
        }

        public int materializedMessages() {
            return checkedSum(DelayShard.LaneCloseMaterializationResult::materializedMessages,
                    "materialized messages");
        }

        public int materializedReservations() {
            return checkedSum(DelayShard.LaneCloseMaterializationResult::materializedReservations,
                    "materialized reservations");
        }

        public boolean complete() {
            return laneResults.stream().allMatch(DelayShard.LaneCloseMaterializationResult::complete);
        }

        private int checkedSum(final ToIntFunction<DelayShard.LaneCloseMaterializationResult> extractor,
                               final String label) {
            int total = 0;
            try {
                for (DelayShard.LaneCloseMaterializationResult result : laneResults) {
                    total = Math.addExact(total, extractor.applyAsInt(result));
                }
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("Lane close materializer " + label + " overflow", exception);
            }
            return total;
        }
    }
}
