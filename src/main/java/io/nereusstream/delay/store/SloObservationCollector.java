package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;

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
 * bad/evidence-gap result. This class is a local merge seam; production
 * collector durability, authorization and metric publication remain outside
 * it.</p>
 */
public final class SloObservationCollector {
    private final TreeMap<String, SloObservationOutboxV1> samples = new TreeMap<>();

    /** Merges one exported outbox record and returns the current projection. */
    public synchronized SloObservationOutboxV1 merge(final SloObservationOutboxV1 incoming,
                                                      final SloThresholdDirectionV1 direction) {
        Objects.requireNonNull(incoming, "incoming");
        Objects.requireNonNull(direction, "direction");
        final String key = Bytes.hex(incoming.sampleId());
        final SloObservationOutboxV1 current = samples.get(key);
        if (current == null) {
            samples.put(key, incoming);
            return incoming;
        }
        if (!java.util.Arrays.equals(current.start().canonicalBytes(), incoming.start().canonicalBytes())) {
            throw new IllegalStateException("SLO collector received a different Start for one sample ID");
        }
        final SloObservationOutboxV1 merged;
        if (incoming.finalObservation() == null) {
            merged = current;
        } else {
            merged = current.mergeFinal(incoming.finalObservation(), direction);
        }
        samples.put(key, merged);
        return merged;
    }

    /** Returns a deterministic sample projection, sorted by sample ID bytes. */
    public synchronized List<SloObservationOutboxV1> snapshot() {
        return List.copyOf(new ArrayList<>(samples.values()));
    }

    public synchronized SloObservationOutboxV1 get(final byte[] sampleId) {
        Objects.requireNonNull(sampleId, "sampleId");
        return samples.get(Bytes.hex(sampleId));
    }

    public synchronized int size() {
        return samples.size();
    }
}
