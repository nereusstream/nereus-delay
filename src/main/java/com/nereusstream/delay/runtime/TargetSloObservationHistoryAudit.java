package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Independent retained mode-history completeness; this never certifies SLO sample coverage. */
public final class TargetSloObservationHistoryAudit {
    private TargetSloObservationHistoryAudit() {}

    public static void requireComplete(List<TargetSloObservationState> states,
            List<TargetSloObservationGeneration> generations, TargetQuotaScope scope, byte[] lineage,
            TargetQuotaMutation frontier) {
        if (states.isEmpty() && generations.isEmpty()) return;
        if (states.size() != 1 || generations.isEmpty()) throw new IllegalStateException("Target SLO history lacks its sole current head or generations");
        final var ordered = new TreeMap<Long, TargetSloObservationGeneration>(Long::compareUnsigned);
        for (var generation : generations) {
            if (!scope.equals(generation.policy().scope()) || !Arrays.equals(lineage, generation.policy().lineage())
                    || ordered.putIfAbsent(generation.generation(), generation) != null) throw new IllegalStateException("Target SLO retained generations changed scope or repeat an ordinal");
            generation.mutation().requireAtOrBefore(frontier);
            generation.policy().requireSource(frontier.source());
            if (Long.compareUnsigned(generation.generation(), generation.policy().limits().maximumConcurrentGenerations()) > 0) {
                throw new IllegalStateException("Target SLO generation exceeded its original retained-generation window");
            }
        }
        long expected = 1;
        TargetSloObservationGeneration previous = null;
        for (var generation : ordered.values()) {
            if (generation.generation() != expected) throw new IllegalStateException("Target SLO retained generation history has a gap");
            if (previous != null) {
                if (previous.active() || !Arrays.equals(TargetSloObservationState.from(previous).digest(), generation.enable().body().request().expectedPriorDigest())) {
                    throw new IllegalStateException("Target SLO enable lost its complete stopped predecessor");
                }
                generation.enable().appliedAt().requireAfter(previous.control().appliedAt());
            }
            previous = generation;
            expected = TargetQuotaMutation.increment(expected);
        }
        final var state = states.getFirst();
        state.requireGeneration(Objects.requireNonNull(previous));
        state.mutation().requireAtOrBefore(frontier);
    }

    /** Every applied mode input and every stored endpoint must correlate, including when all mode rows are missing. */
    public static void requireControlInputs(List<TargetSloObservationGeneration> generations,
            List<TargetCheckpointDependencies.ControlImage> images, List<TargetResultRecord> firsts) {
        final var pending = new HashMap<String, TargetSloObservationGeneration.Control>();
        for (var generation : generations) {
            put(pending, generation.enable());
            if (generation.stop() != null) put(pending, generation.stop());
        }
        final var actual = new HashMap<String, TargetResultRecord>();
        for (var first : firsts) actual.put(Bytes.hex(first.logicalId()), first);
        for (var image : images) {
            if (!TargetSloObservationControlBody.isControl(image.image())) continue;
            final var key = Bytes.hex(image.image().systemMutationId());
            final var first = Objects.requireNonNull(actual.get(key), "original Target SLO Control first");
            final var endpoint = pending.remove(key);
            if (first.appliedFirst()) {
                if (endpoint == null || !Arrays.equals(endpoint.input().canonicalEnvelope(), image.image().canonicalEnvelope())
                        || !Arrays.equals(endpoint.appliedAt().source().canonicalBytes(), image.source().canonicalBytes())) {
                    throw new IllegalStateException("APPLIED Target SLO Control lost its exact retained generation endpoint");
                }
            } else if (endpoint != null) throw new IllegalStateException("rejected Target SLO Control appears as an applied generation");
        }
        if (!pending.isEmpty()) throw new IllegalStateException("Target SLO generation has missing original Source Control inputs");
    }

    private static void put(HashMap<String, TargetSloObservationGeneration.Control> values, TargetSloObservationGeneration.Control control) {
        if (values.putIfAbsent(Bytes.hex(control.input().systemMutationId()), control) != null) throw new IllegalStateException("duplicate Target SLO generation Control endpoint");
    }
}
