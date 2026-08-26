package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Deterministic local placement scoring seam for a multi-DB Worker.
 *
 * <p>The policy is deliberately not an Oxia/Kafka/Pulsar authority. It
 * filters candidates using committed hard capacity and DB slots, then ranks
 * the survivors using a dominant-resource score and bounded load telemetry.
 * Ownership, assignment and lease CAS remain outside this class.</p>
 */
public final class WorkerPlacementPolicy {
    private final Configuration configuration;

    public WorkerPlacementPolicy(final Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
    }

    public Decision select(
            final List<WorkerCandidate> candidates,
            final CapacityVector incomingShardCapacity,
            final CapacityVector workerFixedCost,
            final CapacityVector transitionDemand,
            final String currentWorkerId,
            final long nowEpochMs,
            final long movementBytes) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(incomingShardCapacity, "incomingShardCapacity");
        Objects.requireNonNull(workerFixedCost, "workerFixedCost");
        Objects.requireNonNull(transitionDemand, "transitionDemand");
        if (nowEpochMs < 0 || movementBytes < 0) {
            throw new IllegalArgumentException("placement time and movement bytes must be non-negative");
        }
        final CapacityVector required =
                incomingShardCapacity.add(workerFixedCost).add(transitionDemand);
        final List<ScoredCandidate> eligible = candidates.stream()
                .filter(candidate -> candidate.eligible(required))
                .map(candidate -> score(candidate, required, currentWorkerId, nowEpochMs, movementBytes))
                .sorted(Comparator.comparingDouble(ScoredCandidate::score)
                        .thenComparing(scored -> scored.candidate().workerId()))
                .toList();
        if (eligible.isEmpty()) {
            return Decision.noCapacity();
        }
        final ScoredCandidate best = eligible.get(0);
        final ScoredCandidate currentScore = eligible.stream()
                .filter(scored -> Objects.equals(scored.candidate().workerId(), currentWorkerId))
                .findFirst()
                .orElse(null);
        if (currentScore != null) {
            final long residence = nowEpochMs < currentScore.candidate().residenceSinceEpochMs()
                    ? Long.MAX_VALUE
                    : nowEpochMs - currentScore.candidate().residenceSinceEpochMs();
            if (residence < configuration.minimumResidenceMs()) {
                return Decision.keep(currentScore, DecisionReason.MINIMUM_RESIDENCE);
            }
            if (!Objects.equals(best.candidate().workerId(), currentWorkerId)
                    && best.score() + configuration.hysteresisMargin() >= currentScore.score()) {
                return Decision.keep(currentScore, DecisionReason.HYSTERESIS);
            }
        }
        return new Decision(best.candidate().workerId(), DecisionReason.SELECTED, best.score());
    }

    private ScoredCandidate score(
            final WorkerCandidate candidate,
            final CapacityVector required,
            final String currentWorkerId,
            final long nowEpochMs,
            final long movementBytes) {
        final CapacityVector projectedCapacity = candidate.committedCapacity().add(required);
        double score = dominantCapacityUtilization(candidate.hardCapacity(), projectedCapacity)
                + candidate.load().dominantUtilization(candidate.loadCeilings());
        if (candidate.telemetryStale(nowEpochMs, configuration.telemetryMaxAgeMs())) {
            score += configuration.staleTelemetryPenalty();
        }
        if (!Objects.equals(candidate.workerId(), currentWorkerId)) {
            score += movementBytes * configuration.movementCostPerByte();
        }
        // A zero soft ceiling with a non-zero observation is conservatively
        // ranked last, but it must not make the result unrepresentable when
        // every candidate is equally unmeasured.
        if (Double.isInfinite(score)) {
            score = Double.MAX_VALUE;
        }
        return new ScoredCandidate(candidate, score);
    }

    private static double dominantCapacityUtilization(
            final CapacityVector hardCapacity, final CapacityVector required) {
        double result = 0.0d;
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final long capacity = hardCapacity.amount(dimension);
            final long amount = required.amount(dimension);
            if (capacity == 0) {
                if (amount != 0) {
                    return Double.POSITIVE_INFINITY;
                }
                continue;
            }
            result = Math.max(result, (double) amount / (double) capacity);
        }
        return result;
    }

    public record Configuration(
            long telemetryMaxAgeMs,
            long minimumResidenceMs,
            double hysteresisMargin,
            double staleTelemetryPenalty,
            double movementCostPerByte) {
        public Configuration {
            if (telemetryMaxAgeMs < 0
                    || minimumResidenceMs < 0
                    || hysteresisMargin < 0
                    || staleTelemetryPenalty < 0
                    || movementCostPerByte < 0
                    || !Double.isFinite(hysteresisMargin)
                    || !Double.isFinite(staleTelemetryPenalty)
                    || !Double.isFinite(movementCostPerByte)) {
                throw new IllegalArgumentException("invalid placement policy configuration");
            }
        }
    }

    public record WorkerCandidate(
            String workerId,
            CapacityVector hardCapacity,
            CapacityVector committedCapacity,
            long ownedShardDbs,
            long maxOwnedShardDbs,
            long openShardDbs,
            long maxOpenShardDbs,
            WorkerLoadVector load,
            WorkerLoadVector loadCeilings,
            long telemetryObservedAtEpochMs,
            boolean healthy,
            long residenceSinceEpochMs) {
        public WorkerCandidate {
            Objects.requireNonNull(workerId, "workerId");
            Objects.requireNonNull(hardCapacity, "hardCapacity");
            Objects.requireNonNull(committedCapacity, "committedCapacity");
            Objects.requireNonNull(load, "load");
            Objects.requireNonNull(loadCeilings, "loadCeilings");
            if (workerId.isBlank()
                    || ownedShardDbs < 0
                    || maxOwnedShardDbs <= 0
                    || openShardDbs < 0
                    || maxOpenShardDbs <= 0
                    || telemetryObservedAtEpochMs < 0
                    || residenceSinceEpochMs < 0) {
                throw new IllegalArgumentException("invalid Worker placement candidate");
            }
        }

        public boolean eligible(final CapacityVector required) {
            return healthy
                    && ownedShardDbs < maxOwnedShardDbs
                    && openShardDbs < maxOpenShardDbs
                    && hardCapacity.covers(committedCapacity.add(required));
        }

        public boolean telemetryStale(final long nowEpochMs, final long maxAgeMs) {
            if (nowEpochMs < telemetryObservedAtEpochMs) {
                return true;
            }
            return nowEpochMs - telemetryObservedAtEpochMs > maxAgeMs;
        }
    }

    public record Decision(String workerId, DecisionReason reason, double score) {
        public Decision {
            Objects.requireNonNull(reason, "reason");
            if (reason == DecisionReason.NO_CAPACITY) {
                if (workerId != null || !Double.isInfinite(score)) {
                    throw new IllegalArgumentException("NO_CAPACITY must not carry a worker or finite score");
                }
            } else if (workerId == null || workerId.isBlank() || !Double.isFinite(score)) {
                throw new IllegalArgumentException("selected placement must carry a finite worker score");
            }
        }

        private static Decision noCapacity() {
            return new Decision(null, DecisionReason.NO_CAPACITY, Double.POSITIVE_INFINITY);
        }

        private static Decision keep(final ScoredCandidate current, final DecisionReason reason) {
            return new Decision(current.candidate().workerId(), reason, current.score());
        }
    }

    public enum DecisionReason {
        SELECTED,
        MINIMUM_RESIDENCE,
        HYSTERESIS,
        NO_CAPACITY
    }

    private record ScoredCandidate(WorkerCandidate candidate, double score) {}
}
