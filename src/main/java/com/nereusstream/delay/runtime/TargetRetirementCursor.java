package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody;
import com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody;
import com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource;
import com.nereusstream.delay.protocol.TargetExternalEvidenceOwner;
import com.nereusstream.delay.protocol.TargetExternalEvidenceReference;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/**
 * Repository-only continuation for the exact original retirement predicates, including aliases and hidden generations.
 */
final class TargetRetirementCursor {
    private record Range(ColumnFamily family, byte tag) {
        byte[] lower() {
            return new byte[] {tag, TargetKeyCodec.KEY_FORMAT};
        }

        byte[] upper() {
            return new byte[] {tag, (byte) (TargetKeyCodec.KEY_FORMAT + 1)};
        }
    }

    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final TargetExternalEvidenceObjectResource resource;
    private final Set<String> attempts;
    private final List<Range> ranges;
    private final Map<Byte, NavigableMap<String, ShardStore.KeyValue>> selected = new HashMap<>();
    private final int maximumSelected;
    private int range;
    private byte[] lower;

    private TargetRetirementCursor(
            TargetQuotaScope scope,
            byte[] lineage,
            TargetExternalEvidenceObjectResource resource,
            Set<String> attempts,
            List<Range> ranges,
            int maximumSelected) {
        this.scope = scope;
        this.lineage = lineage;
        this.resource = resource;
        this.attempts = attempts;
        this.ranges = ranges;
        this.maximumSelected = maximumSelected;
    }

    static TargetRetirementCursor forInput(TargetQuotaScope scope, byte[] lineage, SystemMutation input) {
        if (input.type() == SystemMutationType.RESOURCE_RETIRE_INTENT
                || input.type() == SystemMutationType.RESOURCE_DELETE_CONFIRMED) {
            final var intent = input.type() == SystemMutationType.RESOURCE_RETIRE_INTENT
                    ? TargetEvidenceRetireIntentBody.decode(input.canonicalBody())
                    : null;
            final var confirm = intent == null ? TargetEvidenceDeleteConfirmedBody.decode(input.canonicalBody()) : null;
            final var resource = intent == null ? confirm.resource() : intent.resource();
            final var context = intent == null ? confirm.context() : intent.context();
            if (!scope.shard().equals(resource.shard())
                    || !Arrays.equals(scope.tenantScope(), resource.tenantScope())
                    || !Arrays.equals(lineage, resource.recoveryLineage())) {
                throw new IllegalArgumentException("retirement inventory changed resource scope/lineage");
            }
            final var ranges = new ArrayList<Range>();
            final boolean recovery = resource.subtype() == TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF;
            final boolean control = resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection
                    || resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor;
            final boolean confirming = input.type() == SystemMutationType.RESOURCE_DELETE_CONFIRMED;
            if (resource.provenance() instanceof TargetExternalEvidenceObjectResource.StagingProvenance
                    || control
                    || recovery && confirming) {
                ranges.add(new Range(ColumnFamily.META, (byte)
                        (recovery
                                ? TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG
                                : TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG)));
            }
            if (control && confirming) {
                ranges.add(new Range(ColumnFamily.META, (byte) TargetKeyCodec.RECOVERY_EVIDENCE_OWNER_TAG));
            }
            return new TargetRetirementCursor(
                    scope,
                    lineage,
                    resource,
                    null,
                    List.copyOf(ranges),
                    Math.addExact(4, Math.multiplyExact(2, context.uses().size())));
        }
        if (input.type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
            throw new IllegalArgumentException("exact retirement/reclaim input required");
        }
        final var body = TargetResourceReclaimBody.decode(input.canonicalBody());
        if (!scope.equals(body.scope()) || !Arrays.equals(lineage, body.lineage())) {
            throw new IllegalArgumentException("reclaim inventory changed scope/lineage");
        }
        if (body.inlinePayloadRelease() || body.nativeRetainedReclaim() || body.objectPayloadReclaim()) {
            return new TargetRetirementCursor(scope, lineage, null, null, List.of(), 0);
        }
        final var attempts = new HashSet<String>();
        for (var operation : body.operations()) {
            attempts.add(Bytes.hex(((TargetResourceReclaimBody.UnusedAttemptReserve) operation)
                    .expected()
                    .publishAttemptId()));
        }
        return new TargetRetirementCursor(
                scope,
                lineage,
                null,
                Set.copyOf(attempts),
                List.of(
                        new Range(ColumnFamily.ID, (byte) TargetKeyCodec.MESSAGE_TAG),
                        new Range(ColumnFamily.TERMINAL, (byte) TargetKeyCodec.TERMINAL_GENERATION_TAG),
                        new Range(ColumnFamily.META, (byte) TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG),
                        new Range(ColumnFamily.META, (byte) TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG),
                        new Range(ColumnFamily.META, (byte) TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG)),
                0);
    }

    boolean unused() {
        return attempts != null && range == ranges.size();
    }

    boolean evidence(TargetExternalEvidenceObjectResource value) {
        return resource != null
                && range == ranges.size()
                && Arrays.equals(resource.canonicalBytes(), value.canonicalBytes());
    }

    boolean evidence(int subtype, byte[] digest, String exactKey) {
        return resource != null
                && range == ranges.size()
                && resource.subtype().wireValue() == subtype
                && Arrays.equals(resource.objectDigest(), digest)
                && resource.exactKey().equals(exactKey);
    }

    boolean advance(TargetStoreBackend.Reader reader, int maximumRows, LongConsumer retain, LongConsumer copy) {
        int visited = 0;
        while (range < ranges.size() && visited < maximumRows) {
            final var current = ranges.get(range);
            if (lower == null) {
                lower = current.lower();
            }
            final byte[] upper = current.upper();
            final var row = reader.first(current.family(), lower, upper, List.of());
            if (row == null) {
                range++;
                lower = null;
                continue;
            }
            final byte[] key = row.key(), raw = row.value();
            copy.accept(Math.addExact(key.length, raw.length));
            if (Arrays.compareUnsigned(key, lower) < 0 || Arrays.compareUnsigned(key, upper) >= 0) {
                throw new IllegalStateException("retirement continuation escaped its original finite range");
            }
            if (attempts != null) {
                requireUnusedRow(current, key, raw);
            } else if (relevant(reader, current, key, raw)) {
                final var values = selected.computeIfAbsent(current.tag(), ignored -> new TreeMap<>());
                if (values.size() >= maximumSelected) {
                    throw new IllegalStateException("retirement aliases exceed the exact sealed-use inventory");
                }
                retain.accept(Math.addExact(key.length, raw.length));
                copy.accept(Math.addExact(key.length, raw.length));
                values.put(Bytes.hex(key), new ShardStore.KeyValue(key, raw));
            }
            // A budget failure in a row's joins retries this row. Only a fully processed row advances the cursor.
            lower = Bytes.concat(key, new byte[] {0});
            copy.accept(lower.length);
            visited++;
            reader.requireWithinElapsedBudget();
        }
        return range == ranges.size();
    }

    private void requireUnusedRow(Range current, byte[] key, byte[] raw) {
        if (current.family() == ColumnFamily.ID || current.family() == ColumnFamily.TERMINAL) {
            final TargetGenerationRuntimeIndex runtime;
            if (current.family() == ColumnFamily.ID) {
                runtime = TargetMessageRecord.decodeForStore(
                                key,
                                TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE)
                                        .payload(),
                                scope.shard())
                        .runtime();
            } else {
                final var terminal = TargetTerminalGenerationRecord.decode(
                        TargetValueEnvelope.decode(raw, TargetTerminalGenerationRecord.VALUE_TYPE)
                                .payload());
                if (!Arrays.equals(terminal.key(), key) || !Arrays.equals(terminal.recoveryLineage(), lineage)) {
                    throw new IllegalStateException("terminal alias changed original key/lineage");
                }
                runtime = terminal.runtime();
            }
            for (var reference : runtime.attemptObligations()) {
                if (attempts.contains(Bytes.hex(reference.publishAttemptId()))) {
                    throw new IllegalStateException("unused reserve still has an open Attempt or related alias");
                }
            }
            return;
        }
        final byte[] attempt = current.tag() == TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG
                ? TargetKafkaReceiptMapping.decodeForStore(
                                key,
                                TargetValueEnvelope.decode(raw, TargetKafkaReceiptMapping.VALUE_TYPE)
                                        .payload(),
                                scope.shard())
                        .admission()
                        .publishAttemptId()
                : current.tag() == TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG
                        ? TargetExternalEvidenceReference.decodeForStore(
                                        key,
                                        TargetValueEnvelope.decode(raw, TargetExternalEvidenceReference.VALUE_TYPE)
                                                .payload(),
                                        scope.shard(),
                                        lineage)
                                .attemptId()
                        : TargetRecoveryEvidenceReference.decodeForStore(
                                        key,
                                        TargetValueEnvelope.decode(raw, TargetRecoveryEvidenceReference.VALUE_TYPE)
                                                .payload(),
                                        scope.shard(),
                                        lineage)
                                .attemptId();
        if (attempts.contains(Bytes.hex(attempt))) {
            throw new IllegalStateException(
                    "BASELINE reserve has a mapping/proof obligation outside the supported closure");
        }
    }

    private boolean relevant(TargetStoreBackend.Reader reader, Range current, byte[] key, byte[] raw) {
        if (current.tag() == TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG) {
            final var ref = TargetExternalEvidenceReference.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetExternalEvidenceReference.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    lineage);
            if (ref.kind() != resource.subtype().wireValue()
                    || !Arrays.equals(ref.artifactDigest(), resource.objectDigest())) {
                return false;
            }
            return !stagingOwnedElsewhere(reader);
        }
        if (current.tag() == TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG) {
            final var ref = TargetRecoveryEvidenceReference.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetRecoveryEvidenceReference.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    lineage);
            if (resource.subtype() != TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF
                    || !Arrays.equals(ref.proofDigest(), resource.objectDigest())) {
                return false;
            }
            return !stagingOwnedElsewhere(reader);
        }
        final var proof = TargetRecoveryEvidenceOwner.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetRecoveryEvidenceOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope(),
                lineage);
        // The original dependency predicate validates even unrelated proof owners and their actual mapping/cut.
        proof.adoptedAt().requireAtOrBefore(reader.aggregate().mutation());
        final byte[] mappingKey = Bytes.concat(
                new byte[] {TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG, TargetKeyCodec.KEY_FORMAT},
                proof.reference().mappingId());
        final var mapping = TargetKafkaReceiptMapping.decodeForStore(
                mappingKey,
                TargetValueEnvelope.decode(
                                Objects.requireNonNull(
                                        reader.get(ColumnFamily.META, mappingKey), "recovery proof actual mapping"),
                                TargetKafkaReceiptMapping.VALUE_TYPE)
                        .payload(),
                scope.shard());
        mapping.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (!(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection)
                && !(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor)) {
            return false;
        }
        final byte[] digest = resource.subtype().wireValue() == TargetKafkaReceiptControlArtifact.PROTECTION
                ? mapping.admission().kafkaReceipt().retentionProtectionDigest()
                : mapping.admission().kafkaReceipt().scanObservationDigest();
        return Arrays.equals(digest, resource.objectDigest());
    }

    private boolean stagingOwnedElsewhere(TargetStoreBackend.Reader reader) {
        if (!(resource.provenance() instanceof TargetExternalEvidenceObjectResource.StagingProvenance)) {
            return false;
        }
        final boolean recovery = resource.subtype() == TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF;
        final byte[] key = recovery
                ? TargetRecoveryEvidenceOwner.key(resource.objectDigest())
                : TargetExternalEvidenceOwner.key(resource.subtype().wireValue(), resource.objectDigest());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            return false;
        }
        final String exact = recovery
                ? TargetRecoveryEvidenceOwner.decodeForStore(
                                key,
                                TargetValueEnvelope.decode(raw, TargetRecoveryEvidenceOwner.VALUE_TYPE)
                                        .payload(),
                                scope.shard(),
                                scope.tenantScope(),
                                lineage)
                        .reference()
                        .exactKey()
                : TargetExternalEvidenceOwner.decodeForStore(
                                key,
                                TargetValueEnvelope.decode(raw, TargetExternalEvidenceOwner.VALUE_TYPE)
                                        .payload(),
                                scope.shard(),
                                scope.tenantScope(),
                                lineage)
                        .exactKey();
        return !resource.exactKey().equals(exact);
    }

    TargetRecordAccounting.View view(
            TargetRecordAccounting.View actual, List<TargetStoreBackend.Edit> expectedOverlay) {
        final var allowed = List.copyOf(expectedOverlay);
        return new TargetRecordAccounting.View() {
            public ShardId shardId() {
                return actual.shardId();
            }

            public TargetRetirementPreparation.Proof retirementProof() {
                return actual.retirementProof();
            }

            public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                return actual.projected(family, key, overlay);
            }

            public ShardStore.KeyValue first(
                    ColumnFamily family, byte[] lower, byte[] upper, List<TargetStoreBackend.Edit> overlay) {
                if (!allowed.equals(overlay)) {
                    throw new IllegalArgumentException("retirement prepared inventory changed its accounting overlay");
                }
                if (family != ColumnFamily.META || lower.length == 0 || !coveredTag(lower[0])) {
                    return actual.first(family, lower, upper, overlay);
                }
                final byte tag = lower[0];
                final byte[] boundLower = {tag, TargetKeyCodec.KEY_FORMAT},
                        boundUpper = {tag, (byte) (TargetKeyCodec.KEY_FORMAT + 1)};
                if (Arrays.compareUnsigned(lower, boundLower) < 0 || Arrays.compareUnsigned(upper, boundUpper) > 0) {
                    throw new IllegalArgumentException("retirement proof does not cover this range");
                }
                final var candidates = new TreeMap<String, ShardStore.KeyValue>();
                final var retained = selected.get(tag);
                if (retained != null) {
                    candidates.putAll(retained);
                }
                final var changed = new HashSet<String>();
                for (var edit : overlay) {
                    if (edit.family() != family || edit.key().length == 0 || edit.key()[0] != tag) {
                        continue;
                    }
                    final String id = Bytes.hex(edit.key());
                    if (!changed.add(id)) {
                        throw new IllegalArgumentException("duplicate retirement overlay key");
                    }
                    candidates.remove(id);
                    if (edit.after() != null) {
                        candidates.put(id, new ShardStore.KeyValue(edit.key(), edit.after()));
                    }
                }
                for (var row : candidates.values()) {
                    if (Arrays.compareUnsigned(row.key(), lower) >= 0 && Arrays.compareUnsigned(row.key(), upper) < 0) {
                        return row;
                    }
                }
                return null;
            }
        };
    }

    private boolean coveredTag(byte tag) {
        return ranges.stream().anyMatch(r -> r.family() == ColumnFamily.META && r.tag() == tag);
    }

    void clear() {
        selected.clear();
    }
}
