package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Persistent remaining observation duties; actual Root/tenant and physical qualification remain independent. */
public final class TargetSloObservationReservations {
    private TargetSloObservationReservations() {}

    /** Legacy Counts1/2 are readable evidence without a completion reservation, never reserved zero. */
    public static TargetSloObservationGeneration.Counts.Reservation requireReserved(
            TargetSloObservationGeneration generation) {
        return Objects.requireNonNull(
                generation.counts().reservation(), "original SLO generation has no persistent completion reservation");
    }

    public static TargetSloObservationGeneration.Counts.Reservation forSample(
            TargetSloObservationGeneration generation, TargetSloObservationSample sample, boolean currentCustodied) {
        requireReserved(generation);
        if (generation.generation() != sample.generation()
                || !generation.policy().scope().equals(sample.scope())
                || !Arrays.equals(generation.policy().lineage(), sample.lineage())
                || !Arrays.equals(generation.policy().digest(), sample.policyDigest())) {
            throw new IllegalArgumentException("SLO reservation changed frozen sample generation");
        }
        if (sample.maximumRecordBytes() > generation.policy().limits().maximumRecordBytes()) {
            throw new IllegalStateException("original SLO single-record bound cannot retain the required sample Final");
        }
        final boolean open = sample.observation().finalObservation() == null;
        long records = 0, bytes = 0;
        if (open) {
            records = 2; // The Final and its future current-revision custody remain required after an open-Start ACK.
            bytes = Math.addExact(
                    Math.subtractExact(sample.maximumStoredBytes(), sample.storedBytes()),
                    Math.addExact(
                            maximumFinalStoredBytes(generation, sample),
                            maximumCustodyStoredBytes(generation, sample)));
        }
        if (!currentCustodied) {
            records = Math.addExact(records, 1);
            bytes = Math.addExact(bytes, maximumCustodyStoredBytes(generation, sample));
        }
        return new TargetSloObservationGeneration.Counts.Reservation(records, bytes);
    }

    /** The original declared cap is a conservative allowed-record upper bound, not an invented default. */
    public static long maximumFinalStoredBytes(
            TargetSloObservationGeneration generation, TargetSloObservationSample sample) {
        return generation.policy().limits().maximumRecordBytes();
    }

    public static long maximumCustodyStoredBytes(
            TargetSloObservationGeneration generation, TargetSloObservationSample sample) {
        return generation.policy().limits().maximumRecordBytes();
    }

    /** One immutable exact-digest key; an old-version receipt never discharges today's revision. */
    public static boolean currentCustodied(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloObservationSample sample) {
        final var key = TargetSloCustodyRecord.key(sample.observation().sampleId(), sample.digest());
        final var raw = view.projected(ColumnFamily.META, key, overlay);
        if (raw == null) {
            return false;
        }
        final var custody = TargetSloCustodyRecord.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetSloCustodyRecord.VALUE_TYPE)
                        .payload(),
                scope,
                lineage);
        if (!Arrays.equals(custody.sample().canonicalBytes(), sample.canonicalBytes())) {
            throw new IllegalStateException("SLO custody key changed exact exported sample bytes");
        }
        return true;
    }
}
