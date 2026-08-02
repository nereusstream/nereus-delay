package io.nereusstream.delay.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Bounded local scheduler bridge for durable Lane-close cursors.
 *
 * <p>The source-ordered Close marker remains the only semantic decision. This
 * helper merely discovers validated {@code timeline/SYSTEM(kind=2)} entries
 * and advances each cursor through {@link DelayShard#materializeClosedLane}.
 * It has no Owner Lease, Oxia, or cross-shard authority and therefore must be
 * invoked only by the owner/worker orchestration that supplies those gates.</p>
 */
public final class LaneCloseMaterializer {
    public TurnResult runTurn(final DelayShard shard, final int maxLanes, final int maxRecordsPerLane) {
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
            return laneResults.stream().mapToInt(DelayShard.LaneCloseMaterializationResult::scannedRecords).sum();
        }

        public int materializedMessages() {
            return laneResults.stream()
                    .mapToInt(DelayShard.LaneCloseMaterializationResult::materializedMessages).sum();
        }

        public int materializedReservations() {
            return laneResults.stream()
                    .mapToInt(DelayShard.LaneCloseMaterializationResult::materializedReservations).sum();
        }

        public boolean complete() {
            return laneResults.stream().allMatch(DelayShard.LaneCloseMaterializationResult::complete);
        }
    }
}
