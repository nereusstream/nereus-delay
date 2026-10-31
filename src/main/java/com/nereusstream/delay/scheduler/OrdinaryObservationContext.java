package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Immutable ordinary telemetry boundary; a diagnostic record, never an activation or certification proof. */
public record OrdinaryObservationContext(
        OrdinaryObservationConfiguration configuration,
        UUID processInstance,
        BoundedAsyncMetricExporter.Limits queue,
        long retainedHeapBytes,
        long nativeBytes,
        List<SourceBoundary> effectiveAfterSources) {
    public static final int CANONICAL_HEADER_BYTES = 128;

    /** Neither a new process nor a bounded metric queue can prove a complete population. */
    public enum Coverage {
        UNKNOWN
    }

    public OrdinaryObservationContext {
        Objects.requireNonNull(configuration, "ordinary observation configuration");
        Objects.requireNonNull(processInstance, "actual process instance");
        Objects.requireNonNull(queue, "metric queue bounds");
        configuration.requireSourceCount(Objects.requireNonNull(effectiveAfterSources, "actual Source boundaries")
                .size());
        if (retainedHeapBytes != configuration.maximumRetainedHeapBytes(queue)
                || nativeBytes <= 0
                || nativeBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("ordinary context differs from its original process reservation");
        }
        long bytes = CANONICAL_HEADER_BYTES;
        for (var source : effectiveAfterSources) {
            bytes = Math.addExact(bytes, source.canonicalPayloadBytes());
        }
        if (bytes > configuration.maximumContextBytes()) {
            throw new IllegalArgumentException("actual ordinary observation context exceeds its byte bound");
        }
        effectiveAfterSources = List.copyOf(effectiveAfterSources);
    }

    public OrdinaryObservationConfiguration.Mode mode() {
        return configuration.mode();
    }

    public long generation() {
        return configuration.generation();
    }

    public Coverage priorProcessCoverage() {
        return Coverage.UNKNOWN;
    }

    public Coverage populationCoverage() {
        return Coverage.UNKNOWN;
    }

    /** A cut captured and checked by the original Process; full canonical position is retained only once. */
    public static final class SourceBoundary {
        private static final int FIXED_CANONICAL_BYTES = 4 + 3 * Long.BYTES + 1;
        private final byte[] appliedSourcePosition;
        private final long assignmentEpoch;
        private final long ownerEpoch;
        private final Long guardedSourceConnectionGeneration;

        public SourceBoundary(
                SourcePosition applied, long assignmentEpoch, long ownerEpoch, Long guardedSourceConnectionGeneration) {
            appliedSourcePosition = TargetSourcePosition.requireBounded(applied).canonicalBytes();
            if (assignmentEpoch <= 0
                    || ownerEpoch == 0
                    || guardedSourceConnectionGeneration != null && guardedSourceConnectionGeneration == 0) {
                throw new IllegalArgumentException(
                        "ordinary Source boundary requires an actual assignment/Owner generation");
            }
            this.assignmentEpoch = assignmentEpoch;
            this.ownerEpoch = ownerEpoch;
            this.guardedSourceConnectionGeneration = guardedSourceConnectionGeneration;
        }

        public byte[] appliedSourcePosition() {
            return appliedSourcePosition.clone();
        }

        public long assignmentEpoch() {
            return assignmentEpoch;
        }

        public long ownerEpoch() {
            return ownerEpoch;
        }

        public Long guardedSourceConnectionGeneration() {
            return guardedSourceConnectionGeneration;
        }

        public int canonicalPayloadBytes() {
            return Math.addExact(FIXED_CANONICAL_BYTES, appliedSourcePosition.length);
        }

        /** Checks the caller's context budget before allocating the full canonical position. */
        public static long maximumCanonicalPayloadBytes(SourcePosition applied) {
            Objects.requireNonNull(applied, "actual applied Source");
            final long positionBytes = applied instanceof PulsarSourcePosition pulsar
                    ? Math.min(
                            TargetSourcePosition.MAX_PULSAR_CANONICAL_BYTES,
                            Math.addExact(
                                    128L,
                                    Math.multiplyExact(
                                            3L, pulsar.physicalTopic().length())))
                    : TargetSourcePosition.MAX_KAFKA_CANONICAL_BYTES;
            return Math.addExact(FIXED_CANONICAL_BYTES, positionBytes);
        }
    }
}
