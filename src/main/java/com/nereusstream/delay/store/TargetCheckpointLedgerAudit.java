package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetResultLedgerAudit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/** Recovery-only fold of actual Target business records against their independently encoded counters. */
final class TargetCheckpointLedgerAudit {
    private TargetCheckpointLedgerAudit() {}

    static void audit(
            final RocksDB db,
            final Map<ColumnFamily, ColumnFamilyHandle> handles,
            final ColumnFamilyHandle defaultHandle,
            final TargetCheckpointRootVerifier.RootProof proof,
            final TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
        final var budget = new Budget(limits);
        try (RocksIterator empty = db.newIterator(defaultHandle)) {
            empty.seekToFirst();
            if (empty.isValid()) {
                throw new IllegalArgumentException("Target checkpoint default column family is not empty");
            }
            empty.status();
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot scan Target checkpoint default column family", failure);
        }
        final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
            @Override
            public ShardId shardId() {
                return proof.metadata().shardId();
            }

            @Override
            public byte[] projected(
                    final ColumnFamily family, final byte[] key, final List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) {
                    throw new IllegalArgumentException("recovery ledger audit cannot use a write overlay");
                }
                try {
                    final byte[] raw = db.get(handles.get(family), key);
                    budget.point(key.length, raw == null ? 0 : raw.length);
                    return raw;
                } catch (RocksDBException failure) {
                    throw new IllegalArgumentException("cannot read Target checkpoint accounting dependency", failure);
                }
            }
        };
        final var accounting = new TargetRecordAccounting(
                view,
                List.of(),
                proof.root().scope(),
                proof.root().recoveryLineage(),
                proof.aggregate().mutation(),
                proof.bookkeeping(),
                TargetQueueState.MAX_DOMAIN_SLOTS);
        final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt = new HashMap<>();
        final Map<TargetQuotaIdentity, CapacityVector> resultContributions = new HashMap<>();
        final List<TargetResultLedgerAudit.Stored> resultRows = new ArrayList<>();
        final List<TargetQuotaCounter> counters = new ArrayList<>();
        final var rootCharge =
                TargetRecordAccounting.resources(proof.bookkeeping().charge());
        merge(rebuilt, proof.root().identity(), rootCharge);
        merge(rebuilt, proof.root().tenantIdentity(), TargetRecordAccounting.resources(rootCharge.resources()));
        for (ColumnFamily family : ColumnFamily.values()) {
            try (RocksIterator iterator = db.newIterator(handles.get(family))) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    final byte[] key = iterator.key();
                    final byte[] raw = iterator.value();
                    budget.scan(key.length, raw.length);
                    if (!skipInfrastructure(family, key, raw, proof, counters, rebuilt)) {
                        final int type = TargetStoreBackend.businessType(family, key);
                        final byte[] payload =
                                TargetValueEnvelope.decode(raw, type).payload();
                        if (family == ColumnFamily.DEDUPE) {
                            resultRows.add(new TargetResultLedgerAudit.Stored(key, type, payload));
                        }
                        final var charge = accounting.charge(family, key, raw);
                        if (charge != null) {
                            merge(rebuilt, charge.owner().identity(), charge.primary());
                            merge(rebuilt, charge.owner().tenantIdentity(), charge.mirror());
                            if (family == ColumnFamily.DEDUPE) {
                                resultContributions.merge(
                                        charge.owner().identity(),
                                        charge.primary().resources(),
                                        CapacityVector::add);
                                resultContributions.merge(
                                        charge.owner().tenantIdentity(),
                                        charge.mirror().resources(),
                                        CapacityVector::add);
                            }
                        }
                    }
                    iterator.next();
                }
                iterator.status();
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot scan Target checkpoint business ledger", failure);
            }
        }
        final long resultBytes = limits.maxKeyValueBytes() > Long.MAX_VALUE - limits.maxPointReadBytes()
                ? Long.MAX_VALUE
                : limits.maxKeyValueBytes() + limits.maxPointReadBytes();
        final var resultSummary = TargetResultLedgerAudit.auditRows(
                proof.root().scope(),
                proof.root().recoveryLineage(),
                proof.mutationSequence(),
                proof.source(),
                new TargetResultLedgerAudit.Limits(limits.maxRecords(), limits.maxPointReads(), resultBytes),
                resultRows,
                key -> {
                    final byte[] raw = view.projected(ColumnFamily.META, key, List.of());
                    if (raw == null) {
                        return null;
                    }
                    final var value = TargetValueEnvelope.decodeAny(raw);
                    return new TargetResultLedgerAudit.Stored(key, value.valueType(), value.payload());
                });
        if (resultSummary.resultRecords() != resultRows.size()
                || !resultSummary.contributions().equals(resultContributions)) {
            throw new IllegalStateException("Target checkpoint result ledger differs from actual DEDUPE accounting");
        }
        TargetQuotaDelta.audit(proof.aggregate(), counters, rebuilt);
    }

    private static boolean skipInfrastructure(
            final ColumnFamily family,
            final byte[] key,
            final byte[] raw,
            final TargetCheckpointRootVerifier.RootProof proof,
            final List<TargetQuotaCounter> counters,
            final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt) {
        if (family != ColumnFamily.META || key.length < 2 || key[1] != TargetKeyCodec.KEY_FORMAT) {
            return false;
        }
        final int tag = Byte.toUnsignedInt(key[0]);
        if (tag == 1) {
            if (key.length != 3 || Byte.toUnsignedInt(key[2]) == 0 || Byte.toUnsignedInt(key[2]) > 14) {
                throw new IllegalArgumentException("unknown Target checkpoint fixed metadata key");
            }
            final var value = TargetValueEnvelope.decodeAny(raw);
            if (Byte.toUnsignedInt(key[2]) == 4) {
                if (value.valueType() != 1) {
                    throw new IllegalArgumentException("Target checkpoint ingress fence has another value type");
                }
                final byte[] payload = value.payload();
                IngressFenceState.decode(payload);
                final var charge = TargetRecordAccounting.resources(proof.root()
                        .accounting()
                        .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length));
                merge(rebuilt, proof.root().identity(), charge);
                merge(rebuilt, proof.root().tenantIdentity(), charge);
            }
            return true;
        }
        if (tag == 7) {
            if (key.length != 3 || Byte.toUnsignedInt(key[2]) == 0 || Byte.toUnsignedInt(key[2]) > 4) {
                throw new IllegalArgumentException("unknown Target checkpoint recovery metadata key");
            }
            TargetValueEnvelope.decode(raw, 1);
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_COUNTER_TAG) {
            counters.add(TargetQuotaCounter.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetQuotaCounter.VALUE_TYPE)
                            .payload(),
                    proof.metadata().shardId()));
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_TOTAL_TAG) {
            TargetValueEnvelope.decode(raw, TargetQuotaTotal.VALUE_TYPE);
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_AGGREGATE_TAG) {
            if (!Arrays.equals(key, proof.aggregate().key())
                    || !Bytes.constantTimeEquals(
                            TargetValueEnvelope.decode(raw, TargetQuotaAggregate.VALUE_TYPE)
                                    .payload(),
                            proof.aggregate().canonicalBytes())) {
                throw new IllegalStateException("Target checkpoint has an extra or changed aggregate");
            }
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_BOOKKEEPING_TAG) {
            if (!Arrays.equals(key, proof.bookkeeping().key())
                    || !Bytes.constantTimeEquals(
                            TargetValueEnvelope.decode(raw, TargetQuotaBookkeeping.VALUE_TYPE)
                                    .payload(),
                            proof.bookkeeping().canonicalBytes())) {
                throw new IllegalStateException("Target checkpoint has an extra or changed bookkeeping root");
            }
            return true;
        }
        return false;
    }

    private static void merge(
            final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt,
            final TargetQuotaIdentity identity,
            final TargetQuotaUsage contribution) {
        rebuilt.merge(identity, contribution, TargetQuotaUsage::add);
    }

    private static final class Budget {
        private final TargetCheckpointRootVerifier.LedgerAuditLimits limits;
        private int records;
        private long bytes;
        private int pointReads;
        private long pointBytes;

        private Budget(final TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
            this.limits = limits;
        }

        private void scan(final int keyBytes, final int valueBytes) {
            final long size = (long) keyBytes + valueBytes;
            if (records >= limits.maxRecords() || size > limits.maxKeyValueBytes() - bytes) {
                throw new IllegalArgumentException("Target checkpoint ledger scan budget exceeded");
            }
            records++;
            bytes += size;
        }

        private void point(final int keyBytes, final int valueBytes) {
            final long size = (long) keyBytes + valueBytes;
            if (pointReads >= limits.maxPointReads() || size > limits.maxPointReadBytes() - pointBytes) {
                throw new IllegalArgumentException("Target checkpoint ledger point-read budget exceeded");
            }
            pointReads++;
            pointBytes += size;
        }
    }
}
