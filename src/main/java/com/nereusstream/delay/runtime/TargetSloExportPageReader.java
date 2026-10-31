package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationExport;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Live bounded delivery census of retained Target samples; a completed sweep is not population coverage. */
public final class TargetSloExportPageReader {
    private TargetSloExportPageReader() {}

    public record Limits(int maximumRows, long maximumCanonicalBytes, int maximumEventsPerExport) {
        public Limits {
            if (maximumRows <= 0
                    || maximumRows == Integer.MAX_VALUE
                    || maximumCanonicalBytes <= 0
                    || maximumCanonicalBytes == Long.MAX_VALUE
                    || maximumEventsPerExport <= 0
                    || maximumEventsPerExport == Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "Target export page requires explicit finite row/byte/history bounds");
            }
        }
    }

    public record Cursor(TargetQuotaScope scope, byte[] lineage, SourcePosition sweepSource, byte[] afterSampleKey) {
        public Cursor {
            Objects.requireNonNull(scope);
            TargetSourcePosition.requireBounded(sweepSource);
            Bytes.requireLength(lineage, 16, "SLO census lineage");
            lineage = Bytes.copy(lineage);
            afterSampleKey = Bytes.copy(Objects.requireNonNull(afterSampleKey));
            if (scope.target() != null
                    || !scope.shard().equals(sweepSource.shardId())
                    || afterSampleKey.length != 34
                    || afterSampleKey[0] != TargetKeyCodec.SLO_SAMPLE_TAG
                    || afterSampleKey[1] != TargetKeyCodec.KEY_FORMAT) {
                throw new IllegalArgumentException("SLO census cursor changed original scope or primary key");
            }
        }

        @Override
        public byte[] lineage() {
            return Bytes.copy(lineage);
        }

        @Override
        public byte[] afterSampleKey() {
            return Bytes.copy(afterSampleKey);
        }
    }

    public record Page(
            TargetQuotaMutation cut,
            List<TargetSloObservationExport> exports,
            Cursor nextCursor,
            boolean sweepComplete,
            long canonicalBytes) {
        public Page {
            Objects.requireNonNull(cut).requireSourceApplied();
            exports = List.copyOf(exports);
            if (canonicalBytes <= 0 || sweepComplete && nextCursor != null || !sweepComplete && nextCursor == null) {
                throw new IllegalArgumentException("SLO delivery page has an inconsistent cursor/bound");
            }
        }
    }

    public static Page read(
            TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage, Cursor after, Limits limits) {
        Objects.requireNonNull(reader);
        Objects.requireNonNull(scope);
        Objects.requireNonNull(limits);
        Bytes.requireLength(lineage, 16, "SLO census lineage");
        if (scope.target() != null || !scope.shard().equals(reader.shardId())) {
            throw new IllegalArgumentException("SLO census changed Shard scope");
        }
        final var aggregate = reader.aggregate();
        final var source = Objects.requireNonNull(reader.source(), "actual SLO census Source cut");
        final var position = result(reader, TargetSloObservationStore.positionKey(source));
        final var cut = position.mutation();
        cut.requireSourceApplied();
        cut.requireAtOrBefore(aggregate.mutation());
        final var root = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null);
        if (!position.kind().position()
                || !root.equals(position.primaryIdentity())
                || !Arrays.equals(scope.tenantScope(), position.tenantScope())
                || !Arrays.equals(lineage, position.recoveryLineage())
                || !Arrays.equals(cut.source().canonicalBytes(), source.canonicalBytes())) {
            throw new IllegalStateException("SLO census cut is not the actual guarded Source POSITION");
        }
        if (after != null
                && (!scope.equals(after.scope())
                        || !Arrays.equals(lineage, after.lineage())
                        || !source.sameSourceIdentity(after.sweepSource())
                        || after.sweepSource().compareTo(source) > 0)) {
            throw new IllegalArgumentException("SLO census cursor crossed scope, lineage or physical Source");
        }
        final var sweepSource = after == null ? source : after.sweepSource();
        final var prefix = new byte[] {TargetKeyCodec.SLO_SAMPLE_TAG, TargetKeyCodec.KEY_FORMAT};
        final var upper = prefixUpper(prefix);
        byte[] last = after == null ? null : after.afterSampleKey();
        byte[] lower = last == null ? prefix : Bytes.concat(last, new byte[] {0});
        final var exports = new ArrayList<TargetSloObservationExport>();
        long bytes = cut.canonicalBytes().length;
        if (bytes >= limits.maximumCanonicalBytes()) {
            throw new IllegalArgumentException("SLO page cannot retain its original cut");
        }
        boolean advanced = false;
        for (int visited = 0; visited < limits.maximumRows(); visited++) {
            try {
                reader.requireWithinElapsedBudget();
                final var row = reader.first(ColumnFamily.META, lower, upper, List.of());
                if (row == null) {
                    return new Page(cut, exports, null, true, bytes);
                }
                final var sample = TargetSloObservationSample.decodeForStore(
                        row.key(),
                        TargetValueEnvelope.decode(row.value(), TargetSloObservationSample.VALUE_TYPE)
                                .payload(),
                        scope,
                        lineage);
                if (!sample.primary(row.key())) {
                    throw new IllegalStateException("SLO census selected a non-primary sample");
                }
                TargetSloObservationSamples.requireOrigin(reader, List.of(), scope, lineage, root, sample, cut);
                if (!hasCustody(reader, scope, lineage, root, sample, cut)) {
                    final var generation = Objects.requireNonNull(
                            TargetSloObservationVerifier.readGeneration(reader, scope, lineage, sample.generation()),
                            "original export generation");
                    final var events =
                            history(reader, scope, lineage, root, sample, cut, limits.maximumEventsPerExport());
                    final var exported = new TargetSloObservationExport(generation, sample, cut, events);
                    final long next = Math.addExact(bytes, exported.canonicalBytes().length);
                    if (next > limits.maximumCanonicalBytes()) {
                        if (!advanced) {
                            throw new IllegalArgumentException(
                                    "one complete SLO export cannot fit its qualified page envelope");
                        }
                        return new Page(cut, exports, new Cursor(scope, lineage, sweepSource, last), false, bytes);
                    }
                    exports.add(exported);
                    bytes = next;
                }
                last = row.key();
                lower = Bytes.concat(last, new byte[] {0});
                advanced = true;
            } catch (ReadIncompleteException incomplete) {
                // Only complete prior rows advance delivery; an incomplete sample/history is retried from its original
                // key.
                if (!advanced) {
                    throw incomplete;
                }
                return new Page(cut, exports, new Cursor(scope, lineage, sweepSource, last), false, bytes);
            }
        }
        return new Page(cut, exports, new Cursor(scope, lineage, sweepSource, last), false, bytes);
    }

    private static boolean hasCustody(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloObservationSample sample,
            TargetQuotaMutation cut) {
        final var key = TargetSloCustodyRecord.key(sample.observation().sampleId(), sample.digest());
        final var raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            return false;
        }
        final var record = TargetSloCustodyRecord.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetSloCustodyRecord.VALUE_TYPE)
                        .payload(),
                scope,
                lineage);
        TargetSloObservationEventStore.requireCustodyOrigin(reader, List.of(), scope, lineage, root, record, cut);
        if (!Arrays.equals(record.sample().canonicalBytes(), sample.canonicalBytes())) {
            throw new IllegalStateException("SLO delivery custody changed the exact current sample version");
        }
        return true;
    }

    private static List<TargetSloObservationEventRecord> history(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloObservationSample sample,
            TargetQuotaMutation cut,
            int maximumEvents) {
        final var prefix = Bytes.concat(
                new byte[] {TargetKeyCodec.SLO_EVENT_TAG, TargetKeyCodec.KEY_FORMAT},
                sample.observation().sampleId());
        final var upper = prefixUpper(prefix);
        byte[] lower = prefix;
        final var events = new ArrayList<TargetSloObservationEventRecord>();
        for (int count = 0; ; count++) {
            reader.requireWithinElapsedBudget();
            final var row = reader.first(ColumnFamily.META, lower, upper, List.of());
            if (row == null) {
                return List.copyOf(events);
            }
            if (count == maximumEvents) {
                throw new IllegalStateException("complete SLO Source history exceeds its qualified export envelope");
            }
            final var event = TargetSloObservationEventRecord.decodeForStore(
                    row.key(),
                    TargetValueEnvelope.decode(row.value(), TargetSloObservationEventRecord.VALUE_TYPE)
                            .payload(),
                    scope,
                    lineage);
            if (!Arrays.equals(event.body().sampleId(), sample.observation().sampleId())) {
                throw new IllegalStateException("SLO history changed sample");
            }
            event.appliedAt().requireAtOrBefore(sample.mutation());
            TargetSloObservationEventStore.requireOrigin(reader, List.of(), scope, lineage, root, event, cut);
            events.add(event);
            lower = Bytes.concat(row.key(), new byte[] {0});
        }
    }

    private static TargetResultRecord result(TargetStoreBackend.Reader reader, byte[] key) {
        final var raw = Objects.requireNonNull(reader.get(ColumnFamily.DEDUPE, key), "actual Source POSITION");
        final var payload =
                TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload();
        final var value = TargetResultRecord.decode(payload);
        value.requireStored(key, TargetResultRecord.VALUE_TYPE, payload);
        return value;
    }

    private static byte[] prefixUpper(byte[] prefix) {
        final var upper = Bytes.copy(prefix);
        for (int index = upper.length - 1; index >= 0; index--) {
            if (++upper[index] != 0) {
                return upper;
            }
        }
        throw new IllegalArgumentException("SLO export prefix lacks a finite upper bound");
    }
}
