package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SloObservationOutbox;
import com.nereusstream.delay.protocol.SloThresholdDirection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Deterministic local merge projection for at-least-once SLO outbox export.
 *
 * <p>The collector keys records by the canonical sample ID and requires the
 * exact Start bytes to remain stable. Final observations use the protocol's
 * conservative direction-aware merge, so a retry cannot improve or erase a
 * bad/evidence-gap result. An optional {@link SloObservationCollectorLimits}
 * bounds the retained projection; exceeding it is an explicit evidence-capacity
 * failure and never a silent sample drop. This class is a local merge seam;
 * production collector durability, authorization and metric publication remain
 * outside it.</p>
 */
public final class SloObservationCollector {
    private final SloObservationCollectorLimits limits;
    private final TreeMap<String, SloObservationOutbox> samples = new TreeMap<>();
    private long canonicalBytes;

    /** Compatibility constructor for embedded callers without a capacity envelope. */
    public SloObservationCollector() {
        this(null);
    }

    /** Creates a collector projection with an explicit sample/byte envelope. */
    public SloObservationCollector(final SloObservationCollectorLimits limits) {
        this.limits = limits;
    }

    /** Merges one exported outbox record and returns the current projection. */
    public synchronized SloObservationOutbox merge(
            final SloObservationOutbox incoming, final SloThresholdDirection direction) {
        Objects.requireNonNull(incoming, "incoming");
        Objects.requireNonNull(direction, "direction");
        final String key = Bytes.hex(incoming.sampleId());
        final SloObservationOutbox current = samples.get(key);
        if (current == null) {
            final long incomingBytes = canonicalBytes(incoming);
            requireCapacity(1, incomingBytes, "new sample");
            samples.put(key, incoming);
            canonicalBytes = add(canonicalBytes, incomingBytes, "collector canonical bytes");
            return incoming;
        }
        if (!java.util.Arrays.equals(
                current.start().canonicalBytes(), incoming.start().canonicalBytes())) {
            throw new IllegalStateException("SLO collector received a different Start for one sample ID");
        }
        final SloObservationOutbox merged;
        if (incoming.finalObservation() == null) {
            merged = current;
        } else {
            merged = current.mergeFinal(incoming.finalObservation(), direction);
        }
        if (merged != current) {
            final long oldBytes = canonicalBytes(current);
            final long newBytes = canonicalBytes(merged);
            final long replacementBytes;
            try {
                replacementBytes = Math.addExact(Math.subtractExact(canonicalBytes, oldBytes), newBytes);
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("SLO collector byte usage overflow", exception);
            }
            requireCapacity(0, replacementBytes, "sample replacement");
            canonicalBytes = replacementBytes;
        }
        samples.put(key, merged);
        return merged;
    }

    /** Returns a deterministic sample projection, sorted by sample ID bytes. */
    public synchronized List<SloObservationOutbox> snapshot() {
        return List.copyOf(new ArrayList<>(samples.values()));
    }

    public synchronized SloObservationOutbox get(final byte[] sampleId) {
        Objects.requireNonNull(sampleId, "sampleId");
        return samples.get(Bytes.hex(sampleId));
    }

    public synchronized int size() {
        return samples.size();
    }

    /** Returns the current bounded projection usage. */
    public synchronized Usage usage() {
        return new Usage(samples.size(), canonicalBytes);
    }

    private void requireCapacity(final int addedSamples, final long nextBytes, final String operation) {
        if (limits == null) {
            return;
        }
        if (samples.size() + addedSamples > limits.maxSamples()) {
            throw new IllegalStateException("SLO collector sample capacity exceeded during " + operation);
        }
        if (nextBytes > limits.maxCanonicalBytes()) {
            throw new IllegalStateException("SLO collector byte capacity exceeded during " + operation);
        }
    }

    private static long canonicalBytes(final SloObservationOutbox value) {
        return value.canonicalBytes().length;
    }

    private static long add(final long left, final long right, final String name) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException exception) {
            throw new IllegalStateException(name + " overflow", exception);
        }
    }

    public record Usage(int sampleCount, long canonicalBytes) {
        public Usage {
            if (sampleCount < 0 || canonicalBytes < 0) {
                throw new IllegalArgumentException("SLO collector usage cannot be negative");
            }
        }
    }
}
