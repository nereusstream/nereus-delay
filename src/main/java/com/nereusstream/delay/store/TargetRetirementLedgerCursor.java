package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource;
import com.nereusstream.delay.protocol.TargetExternalEvidenceReference;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetRetirementPreparation;
import com.nereusstream.delay.runtime.TargetSloWriterOrigin;
import com.nereusstream.delay.runtime.TargetTerminalGenerationRecord;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/**
 * Complete physical Native inventory for one held retirement view; independent recovery audits keep their original
 * path.
 */
public final class TargetRetirementLedgerCursor {
    private final TargetCheckpointRootVerifier.RootProof root;
    private final long nativeSequence;
    private final EnumMap<ColumnFamily, NavigableMap<String, ShardStore.KeyValue>> rows =
            new EnumMap<>(ColumnFamily.class);
    private final Map<String, NavigableMap<String, ShardStore.KeyValue>> references = new HashMap<>();
    private final Map<String, NavigableMap<String, ShardStore.KeyValue>> dependents = new HashMap<>();
    private int family;
    private byte[] lower;
    private boolean inventoryComplete;
    private final TargetCheckpointLedgerAudit.RetirementCursor fold;

    public TargetRetirementLedgerCursor(
            TargetStoreBackend.Reader reader, TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
        root = reader.retirementRootProof();
        nativeSequence = reader.nativeSequence();
        for (var value : ColumnFamily.values()) {
            rows.put(value, new TreeMap<>());
        }
        fold = new TargetCheckpointLedgerAudit.RetirementCursor(root, limits);
    }

    /** All rows, aliases, unknown keys and actual CF EOF are read before any complete-fold conclusion exists. */
    public boolean advance(
            TargetStoreBackend.Reader reader, int maximumRows, LongConsumer retained, LongConsumer copied) {
        final byte[] metadata = reader.metadata().encode(),
                source = Objects.requireNonNull(reader.source()).canonicalBytes();
        copied.accept((long) metadata.length + source.length);
        if (nativeSequence != reader.nativeSequence()
                || root.mutationSequence() != reader.sourceSequence()
                || !Arrays.equals(root.metadata().encode(), metadata)
                || !Arrays.equals(root.source().canonicalBytes(), source)) {
            throw new TargetRetirementPreparation.ViewChanged();
        }
        int used = 0;
        while (!inventoryComplete && used < maximumRows) {
            final var current = ColumnFamily.values()[family];
            final var row = reader.retirementNext(current, lower);
            if (row == null) {
                family++;
                lower = null;
                if (family == ColumnFamily.values().length) {
                    inventoryComplete = true;
                }
                continue;
            }
            final byte[] key = row.key(), raw = row.value();
            copied.accept(Math.addExact(key.length, raw.length));
            if (lower != null && Arrays.compareUnsigned(key, lower) < 0) {
                throw new IllegalStateException("retirement ledger continuation regressed");
            }
            retained.accept(Math.addExact(key.length, raw.length));
            copied.accept(Math.addExact(key.length, raw.length));
            if (rows.get(current).putIfAbsent(Bytes.hex(key), new ShardStore.KeyValue(key, raw)) != null) {
                throw new IllegalStateException("retirement ledger repeats a physical Native key");
            }
            indexReference(current, key, raw);
            lower = Bytes.concat(key, new byte[] {0});
            copied.accept(lower.length);
            used++;
            reader.requireWithinElapsedBudget();
        }
        if (!inventoryComplete) {
            return false;
        }
        return fold.advance(new InventoryView(this, reader, null, null), maximumRows - used, retained, copied);
    }

    public TargetCheckpointDependencies result() {
        if (!inventoryComplete) {
            throw new IllegalStateException("retirement ledger lacks physical EOF");
        }
        return fold.result();
    }

    public TargetQuotaAttemptBudget projectedBudget(byte[] id) {
        return fold.projectedBudget(id);
    }

    public TargetQuotaMutation settlement(byte[] attempt, byte[] share) {
        return fold.settlement(attempt, share);
    }

    public TargetResultRecord resultAt(TargetQuotaMutation at) {
        return fold.resultAt(at);
    }

    public long grantCount(TargetQuotaIdentity owner) {
        return fold.grantCount(owner);
    }

    public Collection<TargetCheckpointDependencies.RetryUse> retryUses(DelayMessageId id) {
        return fold.retryUses(id);
    }

    public List<TargetTerminalGenerationRecord> terminals(DelayMessageId id) {
        return fold.terminals(id);
    }

    public List<TargetCheckpointDependencies.Admission> messageAdmissions(DelayMessageId id) {
        return fold.messageAdmissions(id);
    }

    public boolean hasMessageAdmission(DelayMessageId id) {
        return !messageAdmissions(id).isEmpty();
    }

    public TargetTerminalGenerationRecord terminal(byte[] key) {
        dependenciesForClosure();
        final var row = Objects.requireNonNull(
                rows.get(ColumnFamily.TERMINAL).get(Bytes.hex(key)), "inline current terminal metadata");
        final var terminal = TargetTerminalGenerationRecord.decode(
                TargetValueEnvelope.decode(row.value(), TargetTerminalGenerationRecord.VALUE_TYPE)
                        .payload());
        if (!Arrays.equals(key, terminal.key())) {
            throw new IllegalStateException("inline current terminal key changed");
        }
        return terminal;
    }
    /**
     * Internal exact joins while the final original historical predicates are still advancing; not a completion
     * receipt.
     */
    public TargetCheckpointDependencies dependenciesForClosure() {
        return fold.dependenciesForClosure();
    }

    public TargetKafkaReceiptMapping mapping(byte[] id) {
        return fold.mapping(id);
    }

    public void requireUnreferenced(byte[] resultKey) {
        dependenciesForClosure();
        final byte[] prefix = TargetSloObservationSample.referencePrefix(resultKey);
        final byte[] upper = Bytes.copy(prefix);
        for (int i = upper.length - 1; i >= 0; i--) {
            if (++upper[i] != 0) {
                break;
            }
        }
        final var first = rows.get(ColumnFamily.META).ceilingEntry(Bytes.hex(prefix));
        if (first != null && Arrays.compareUnsigned(first.getValue().key(), upper) < 0) {
            throw new IllegalStateException("original Source record still has retained Target certification samples");
        }
    }

    public void clear() {
        for (var values : rows.values()) {
            values.clear();
        }
        references.clear();
        dependents.clear();
        fold.clear();
    }

    private void indexReference(ColumnFamily family, byte[] key, byte[] raw) {
        if (family != ColumnFamily.META || key.length < 2) {
            return;
        }
        final int tag = Byte.toUnsignedInt(key[0]);
        final int subtype;
        final byte[] digest;
        if (tag == TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG) {
            final var ref = TargetExternalEvidenceReference.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetExternalEvidenceReference.VALUE_TYPE)
                            .payload(),
                    root.metadata().shardId(),
                    root.root().recoveryLineage());
            subtype = ref.kind();
            digest = ref.artifactDigest();
        } else if (tag == TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG) {
            final var ref = TargetRecoveryEvidenceReference.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetRecoveryEvidenceReference.VALUE_TYPE)
                            .payload(),
                    root.metadata().shardId(),
                    root.root().recoveryLineage());
            subtype = TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF.wireValue();
            digest = ref.proofDigest();
        } else if (tag == Byte.toUnsignedInt((byte) TargetKeyCodec.RECOVERY_EVIDENCE_OWNER_TAG)) {
            final var proof = TargetRecoveryEvidenceOwner.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetRecoveryEvidenceOwner.VALUE_TYPE)
                            .payload(),
                    root.metadata().shardId(),
                    root.root().tenantScope(),
                    root.root().recoveryLineage());
            proof.adoptedAt().requireAtOrBefore(root.aggregate().mutation());
            final byte[] mappingKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG, TargetKeyCodec.KEY_FORMAT},
                    proof.reference().mappingId());
            final var mappingRow = Objects.requireNonNull(
                    rows.get(ColumnFamily.META).get(Bytes.hex(mappingKey)),
                    "retirement recovery owner original mapping");
            final var mapping = TargetKafkaReceiptMapping.decodeForStore(
                    mappingKey,
                    TargetValueEnvelope.decode(mappingRow.value(), TargetKafkaReceiptMapping.VALUE_TYPE)
                            .payload(),
                    root.metadata().shardId());
            mapping.mutation().requireAtOrBefore(root.aggregate().mutation());
            for (int kind :
                    List.of(TargetKafkaReceiptControlArtifact.PROTECTION, TargetKafkaReceiptControlArtifact.ANCHOR)) {
                final byte[] dependency = kind == TargetKafkaReceiptControlArtifact.PROTECTION
                        ? mapping.admission().kafkaReceipt().retentionProtectionDigest()
                        : mapping.admission().kafkaReceipt().scanObservationDigest();
                dependents
                        .computeIfAbsent(kind + ":" + Bytes.hex(dependency), ignored -> new TreeMap<>())
                        .put(Bytes.hex(key), rows.get(family).get(Bytes.hex(key)));
            }
            return;
        } else {
            return;
        }
        references
                .computeIfAbsent(subtype + ":" + Bytes.hex(digest), ignored -> new TreeMap<>())
                .put(Bytes.hex(key), rows.get(family).get(Bytes.hex(key)));
    }

    /** Privately constructed only after this cursor's physical all-CF inventory. It grants no Source authority. */
    public static final class InventoryView implements TargetRecordAccounting.View {
        private final TargetRetirementLedgerCursor owner;
        private final TargetStoreBackend.Reader reader;
        private final NavigableMap<String, ShardStore.KeyValue> selected;
        private final NavigableMap<String, ShardStore.KeyValue> selectedDependents;

        private InventoryView(
                TargetRetirementLedgerCursor owner,
                TargetStoreBackend.Reader reader,
                NavigableMap<String, ShardStore.KeyValue> selected,
                NavigableMap<String, ShardStore.KeyValue> selectedDependents) {
            if (!owner.inventoryComplete) {
                throw new IllegalStateException("retirement inventory view requires every original CF EOF");
            }
            this.owner = owner;
            this.reader = reader;
            this.selected = selected;
            this.selectedDependents = selectedDependents;
        }

        public ShardId shardId() {
            return reader.shardId();
        }

        void requireWithinElapsedBudget() {
            reader.requireWithinElapsedBudget();
        }

        TargetRetirementLedgerCursor ledger() {
            return owner;
        }

        ShardStore.KeyValue fullNext(ColumnFamily family, byte[] lower) {
            final var values = owner.rows.get(family);
            final var entry = lower == null ? values.firstEntry() : values.ceilingEntry(Bytes.hex(lower));
            return entry == null ? null : entry.getValue();
        }

        public TargetSloWriterOrigin.Context writerOriginContext() {
            return reader.writerOriginContext();
        }

        public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
            if (!overlay.isEmpty()) {
                throw new IllegalArgumentException("full retirement audit cannot use a mutation overlay");
            }
            // Actual Native point reads retain the original physical read/copy budget and current row semantics.
            return reader.get(family, key);
        }

        public ShardStore.KeyValue first(
                ColumnFamily family, byte[] lower, byte[] upper, List<TargetStoreBackend.Edit> overlay) {
            if (!overlay.isEmpty() || lower == null || upper == null || Arrays.compareUnsigned(lower, upper) >= 0) {
                throw new IllegalArgumentException("finite same-view retirement inventory range required");
            }
            final boolean reference = family == ColumnFamily.META
                    && (lower[0] == (byte) TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG
                            || lower[0] == (byte) TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG);
            final var values = family == ColumnFamily.META
                            && lower[0] == (byte) TargetKeyCodec.RECOVERY_EVIDENCE_OWNER_TAG
                            && selectedDependents != null
                    ? selectedDependents
                    : selected == null || !reference ? owner.rows.get(family) : selected;
            final var entry = values.ceilingEntry(Bytes.hex(lower));
            return entry == null || Arrays.compareUnsigned(entry.getValue().key(), upper) >= 0
                    ? null
                    : entry.getValue();
        }
        /**
         * Original full reference scans may use the complete inventory's exact object index; all other point joins stay
         * original.
         */
        public TargetRecordAccounting.View evidence(int subtype, byte[] digest) {
            final var selected = owner.references.get(subtype + ":" + Bytes.hex(digest));
            final var dependents = owner.dependents.get(subtype + ":" + Bytes.hex(digest));
            return new InventoryView(
                    owner,
                    reader,
                    selected == null ? Collections.emptyNavigableMap() : selected,
                    dependents == null ? Collections.emptyNavigableMap() : dependents);
        }
    }
}
