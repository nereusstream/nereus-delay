package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetParentControlCredit;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScopeControlObligation;
import com.nereusstream.delay.protocol.TargetSloControlScopeHeader;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSloObservationState;
import com.nereusstream.delay.protocol.TargetSloScopeNativePlan;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Converts only actual retained Source/Native charges out of their original Scope's frozen future. */
public final class TargetScopeNativeSettlement {
    private TargetScopeNativeSettlement() {}

    /** Only original Scope fee material or frozen settlement capacity is currently unavailable. */
    public static final class Unavailable extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public Unavailable(String message) {
            super(message);
        }

        public Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class Change {
        final TargetScopeControlObligation original;
        CapacityVector removed = CapacityVector.empty();
        CapacityVector added = CapacityVector.empty();
        long occurrences, samples, writers, generations;

        Change(TargetScopeControlObligation original) {
            this.original = original;
        }
    }

    /** Returned business overlay contains the exclusively internal Native64 usage updates. */
    public static List<TargetStoreBackend.Edit> append(
            TargetStoreBackend.Reader reader,
            List<TargetStoreBackend.Edit> business,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaMutation stamp,
            TargetQuotaBookkeeping root,
            int maximumDomains,
            SystemMutation actualInput,
            TargetScopeFeeAuthority authority) {
        try {
            return appendAvailable(
                    reader, business, scope, lineage, stamp, root, maximumDomains, actualInput, authority);
        } catch (TargetSourceAuthorityPreparation.InputUnavailable unavailable) {
            throw new Unavailable(unavailable.getMessage(), unavailable);
        }
    }

    private static List<TargetStoreBackend.Edit> appendAvailable(
            TargetStoreBackend.Reader reader,
            List<TargetStoreBackend.Edit> business,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaMutation stamp,
            TargetQuotaBookkeeping root,
            int maximumDomains,
            SystemMutation actualInput,
            TargetScopeFeeAuthority authority) {
        Objects.requireNonNull(authority);
        final var before = new TargetRecordAccounting(reader, List.of(), scope, lineage, stamp, root, maximumDomains);
        final var after = new TargetRecordAccounting(reader, business, scope, lineage, stamp, root, maximumDomains);
        final Map<String, Change> changes = new LinkedHashMap<>();
        final var keys = new LinkedHashSet<String>();
        Boolean paidParent = null;
        boolean changedScopedNative = false;
        TargetResultRecord physical = null;
        for (var edit : business) {
            if (!keys.add(edit.family() + ":" + Bytes.hex(edit.key()))) {
                throw new IllegalArgumentException("duplicate Source business key before Scope settlement");
            }
            if (!Arrays.equals(edit.before(), reader.get(edit.family(), edit.key()))) {
                throw new IllegalStateException("Scope settlement before differs from actual Store");
            }
            rejectExternalSettlement(edit);
            if (edit.family() == ColumnFamily.DEDUPE && edit.before() != null && edit.after() == null) {
                final var value = TargetValueEnvelope.decodeAny(edit.before());
                if (value.valueType() == TargetResultRecord.VALUE_TYPE) {
                    final var row = TargetResultRecord.decode(value.payload());
                    row.requireStored(edit.key(), value.valueType(), value.payload());
                    if (row.kind() == TargetResultRecord.Kind.SYSTEM
                            || row.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                        if (paidParent == null) {
                            paidParent = hasPaidParent(reader, scope, lineage);
                        }
                        if (paidParent) {
                            requireRetainedFeeFact(reader, row);
                        }
                    }
                }
            }
            if (edit.after() != null && edit.family() == ColumnFamily.DEDUPE) {
                final var envelope = TargetValueEnvelope.decodeAny(edit.after());
                if (envelope.valueType() == TargetResultRecord.VALUE_TYPE) {
                    final var row = TargetResultRecord.decode(envelope.payload());
                    row.requireStored(edit.key(), envelope.valueType(), envelope.payload());
                    if (row.kind() == TargetResultRecord.Kind.POSITION_SYSTEM && edit.before() == null) {
                        if (physical != null) {
                            throw new IllegalStateException(
                                    "one Source plan created multiple physical SYSTEM positions");
                        }
                        if (!row.mutation().equals(stamp)) {
                            throw new IllegalStateException(
                                    "Scope settlement lost this Source occurrence's exact stamp");
                        }
                        physical = row;
                    }
                }
            }
            if (edit.family() != ColumnFamily.META) {
                continue;
            }
            TargetSloControlScopeHeader.Reference oldOrigin = null;
            TargetSloControlScopeHeader.Reference newOrigin = null;
            if (edit.before() != null) {
                final var value = TargetValueEnvelope.decodeAny(edit.before());
                oldOrigin = TargetScopeNativeRows.origin(
                        reader,
                        List.of(),
                        edit.family(),
                        edit.key(),
                        value.valueType(),
                        value.payload(),
                        scope,
                        lineage);
                if (oldOrigin != null) {
                    if (edit.after() == null) {
                        throw unavailable(("scoped Native history requires its dedicated complete retirement proof "
                                + "before deletion"));
                    }
                    final var change = require(changes, reader, scope, lineage, oldOrigin, authority);
                    requireRecord(change, value.valueType(), edit.key(), value.payload());
                    change.removed = change.removed.add(charge(before, edit, edit.before(), root));
                }
            }
            if (edit.after() != null) {
                final var value = TargetValueEnvelope.decodeAny(edit.after());
                newOrigin = TargetScopeNativeRows.origin(
                        reader,
                        business,
                        edit.family(),
                        edit.key(),
                        value.valueType(),
                        value.payload(),
                        scope,
                        lineage);
                if (newOrigin != null) {
                    final var change = require(changes, reader, scope, lineage, newOrigin, authority);
                    requireRecord(change, value.valueType(), edit.key(), value.payload());
                    change.added = change.added.add(charge(after, edit, edit.after(), root));
                    if (edit.before() == null) {
                        switch (value.valueType()) {
                            case TargetSloObservationSample.VALUE_TYPE -> {
                                final var sample = TargetSloObservationSample.decodeForStore(
                                        edit.key(), value.payload(), scope, lineage);
                                if (sample.primary(edit.key())) {
                                    change.samples = Math.incrementExact(change.samples);
                                }
                            }
                            case TargetSloWriterEpoch.VALUE_TYPE ->
                                change.writers = Math.incrementExact(change.writers);
                            case TargetSloObservationGeneration.VALUE_TYPE ->
                                change.generations = Math.incrementExact(change.generations);
                            default -> {}
                        }
                    }
                }
            }
            if ((oldOrigin != null || newOrigin != null) && !Arrays.equals(edit.before(), edit.after())) {
                changedScopedNative = true;
            }
        }

        if (actualInput != null) {
            if (!Arrays.equals(Bytes.sha256(actualInput.canonicalEnvelope()), stamp.mutationDigest())
                    || !scope.shard().equals(actualInput.shardId())) {
                throw new IllegalStateException("Scope Source input differs from this exact physical plan");
            }
            final var candidate = TargetScopeNativeRows.sourceCandidate(reader, business, scope, lineage, actualInput);
            if (candidate.occurrenceRequired()) {
                if (physical == null) {
                    throw unavailable("scoped Source input lacks its new physical POSITION");
                }
                final byte[] firstKey = Bytes.concat(
                        new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, physical.logicalId());
                final var first = result(reader, business, firstKey);
                physical.requireFirst(first);
                final var firstEdit = business.stream()
                        .filter(edit -> edit.family() == ColumnFamily.DEDUPE && Arrays.equals(edit.key(), firstKey))
                        .findFirst()
                        .orElse(null);
                final boolean newFirst = firstEdit != null && firstEdit.before() == null && firstEdit.after() != null;
                TargetScopeControlObligation payer = null;
                if (candidate.reference() != null) {
                    final byte[] key = obligationKey(candidate.reference());
                    // Positive absence may only be classified by the original occurrence authority below.
                    if (reader.get(ColumnFamily.META, key) != null) {
                        payer = TargetScopeControlObligationStore.requirePaidScope(
                                reader, scope, lineage, candidate.reference());
                    }
                }
                authority.requireCurrent();
                final var payment = Objects.requireNonNull(
                        authority.requireOccurrence(new TargetScopeFeeAuthority.Occurrence(
                                actualInput, actualInput, first, physical, payer)),
                        "original Scope occurrence payment");
                if (payment == TargetScopeFeeAuthority.Payment.INVALID_SHARED) {
                    if (changedScopedNative || newFirst && first.appliedFirst()) {
                        throw unavailable("shared invalid-input payment cannot fund APPLIED scoped Native effects");
                    }
                } else {
                    if (payer == null || candidate.reference() == null) {
                        throw unavailable("funded Source occurrence lacks its unique actual Native64 payer");
                    }
                    final var change = require(changes, reader, scope, lineage, candidate.reference(), authority);
                    final var plan = change.original.definition().plan();
                    plan.requireSource(physical.mutation().source());
                    if (actualInput.encodeFrame().length > plan.maximumSourceFrameBytes()) {
                        throw unavailable("complete Source occurrence exceeds its original frozen frame bound");
                    }
                    requireRecord(
                            change,
                            TargetSloScopeNativePlan.NativeClass.POSITION,
                            physical.key(),
                            physical.canonicalBytes());
                    change.added = change.added.add(physical.recordCharge());
                    if (newFirst) {
                        if (!first.mutation().equals(stamp)) {
                            throw new IllegalStateException(
                                    "new scoped SYSTEM first lost its actual creation Source stamp");
                        }
                        requireRecord(
                                change,
                                TargetSloScopeNativePlan.NativeClass.FIRST,
                                first.key(),
                                first.canonicalBytes());
                        change.added = change.added.add(first.recordCharge());
                    }
                    change.occurrences = Math.incrementExact(change.occurrences);
                }
                authority.requireCurrent();
            }
        }

        if (changes.isEmpty()) {
            return business;
        }
        final var complete = new ArrayList<>(business);
        for (var change : changes.values()) {
            final var prior = change.original;
            stamp.requireAfter(prior.mutation());
            prior.requireSource(stamp.source());
            if (!prior.usage().convertedNative().covers(change.removed)) {
                throw new IllegalStateException("retained Scope Native rows exceed its recorded converted charge");
            }
            final TargetScopeControlObligation next;
            try {
                final var converted =
                        prior.usage().convertedNative().subtract(change.removed).add(change.added);
                next = prior.settled(
                        converted, change.occurrences, change.samples, change.writers, change.generations, stamp);
            } catch (IllegalArgumentException | ArithmeticException exhausted) {
                throw new Unavailable(
                        ("original Scope cannot settle these actual charges within its frozen "
                                + "remaining/class/lifetime bounds"),
                        exhausted);
            }
            if (!prior.parentCommitment().equals(next.parentCommitment())) {
                throw new IllegalStateException("Scope settlement changed its original parent C0 payment");
            }
            final var raw = reader.get(ColumnFamily.META, prior.key());
            if (!Arrays.equals(
                    TargetValueEnvelope.decode(raw, TargetScopeControlObligation.VALUE_TYPE)
                            .payload(),
                    prior.canonicalBytes())) {
                throw new IllegalStateException("Scope settlement changed its original selected Native64 row");
            }
            complete.add(new TargetStoreBackend.Edit(
                    ColumnFamily.META,
                    prior.key(),
                    raw,
                    TargetValueEnvelope.encode(TargetScopeControlObligation.VALUE_TYPE, next.canonicalBytes())));
        }
        if (complete.size() > reader.maximumWriteRecords()) {
            throw unavailable("Scope settlement exceeds the original Source atomic write bound");
        }
        authority.requireCurrent();
        return List.copyOf(complete);
    }

    private static Change require(
            Map<String, Change> changes,
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloControlScopeHeader.Reference reference,
            TargetScopeFeeAuthority authority) {
        final var key = Bytes.hex(reference.canonicalBytes());
        final var existing = changes.get(key);
        if (existing != null) {
            return existing;
        }
        final var paid = TargetScopeControlObligationStore.requirePaidScope(reader, scope, lineage, reference);
        if (!paid.settlementEnabled()) {
            throw unavailable("legacy Scope has no original settlement contract");
        }
        authority.requireScope(paid);
        authority.requireCurrent();
        final var change = new Change(paid);
        changes.put(key, change);
        return change;
    }

    private static void rejectExternalSettlement(TargetStoreBackend.Edit edit) {
        if (edit.family() != ColumnFamily.META) {
            return;
        }
        final boolean before = edit.before() != null
                && TargetValueEnvelope.decodeAny(edit.before()).valueType() == TargetScopeControlObligation.VALUE_TYPE;
        final boolean after = edit.after() != null
                && TargetValueEnvelope.decodeAny(edit.after()).valueType() == TargetScopeControlObligation.VALUE_TYPE;
        if ((before || after) && edit.before() != null) {
            throw new IllegalArgumentException(
                    "existing Native64 settlement may only be generated from actual Source charges");
        }
        if (after) {
            final var row = TargetScopeControlObligation.decode(
                    TargetValueEnvelope.decode(edit.after(), TargetScopeControlObligation.VALUE_TYPE)
                            .payload());
            if (!row.usage().consumedNative().isZero()
                    || row.usage().sourceOccurrences() != 0
                    || row.usage().samples() != 0
                    || row.usage().writerEpochs() != 0
                    || row.usage().generations() != 0) {
                throw new IllegalArgumentException("new Native64 allocation cannot supply fabricated settlement usage");
            }
        }
    }

    /** A bounded original-view existence check; deleting a parent in the same proposal cannot erase protection. */
    private static boolean hasPaidParent(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage) {
        final byte[] prefix = new byte[] {TargetParentControlCredit.KEY_TAG, TargetKeyCodec.KEY_FORMAT};
        final byte[] upper = new byte[] {TargetParentControlCredit.KEY_TAG, (byte) (TargetKeyCodec.KEY_FORMAT + 1)};
        final var row = reader.first(ColumnFamily.META, prefix, upper, List.of());
        if (row == null) {
            return false;
        }
        TargetParentControlCredit.decodeForStore(
                row.key(),
                TargetValueEnvelope.decode(row.value(), TargetParentControlCredit.VALUE_TYPE)
                        .payload(),
                scope,
                lineage);
        return true;
    }

    /** Current generic reclaim has no per-occurrence Scope lifetime-history replacement or NoFuture proof. */
    private static void requireRetainedFeeFact(TargetRecordAccounting.View reader, TargetResultRecord row) {
        final TargetResultRecord first;
        if (row.kind() == TargetResultRecord.Kind.SYSTEM) {
            first = row;
        } else {
            final byte[] key = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, row.logicalId());
            first = result(reader, List.of(), key);
            row.requireFirst(first);
        }
        if (first.kind() != TargetResultRecord.Kind.SYSTEM) {
            throw new IllegalStateException("retained Scope fee POSITION lost its original SYSTEM first");
        }
        final var type = SystemMutationResult.decode(first.typedPayload()).mutationType();
        if (type == SystemMutationType.APPLY_SHARD_CONTROL || type == SystemMutationType.TARGET_SLO_OBSERVATION) {
            throw unavailable(("paid parent retains Control/SLO SYSTEM first and POSITION fee facts until "
                    + "dedicated history retirement"));
        }
    }

    private static CapacityVector charge(
            TargetRecordAccounting accounting, TargetStoreBackend.Edit edit, byte[] raw, TargetQuotaBookkeeping root) {
        final var charge = Objects.requireNonNull(accounting.charge(edit.family(), edit.key(), raw));
        if (!charge.owner().identity().equals(root.owner())) {
            throw new IllegalStateException("scoped Native row changed its original SHARD accounting owner");
        }
        return charge.primary().resources();
    }

    private static void requireRecord(Change change, int type, byte[] key, byte[] payload) {
        final var kind =
                switch (type) {
                    case TargetSloObservationState.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.HEAD;
                    case TargetSloObservationGeneration.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.GENERATION;
                    case TargetSloObservationSample.VALUE_TYPE ->
                        key[0] == TargetKeyCodec.SLO_SAMPLE_TAG
                                ? TargetSloScopeNativePlan.NativeClass.SAMPLE_PRIMARY
                                : TargetSloScopeNativePlan.NativeClass.SAMPLE_PROJECTION;
                    case TargetSloObservationEventRecord.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.EVENT;
                    case TargetSloCustodyRecord.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.CUSTODY;
                    case TargetSloWriterEpoch.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.WRITER_EPOCH;
                    default -> throw new IllegalArgumentException("unrecognized actual Scope record class");
                };
        requireRecord(change, kind, key, payload);
    }

    private static void requireRecord(
            Change change, TargetSloScopeNativePlan.NativeClass kind, byte[] key, byte[] payload) {
        try {
            change.original.definition().plan().requireNativeRecord(kind, key.length, payload.length);
        } catch (IllegalArgumentException exceeded) {
            throw unavailable("actual Native record exceeds its original frozen Scope format");
        }
    }

    private static TargetResultRecord result(
            TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, byte[] key) {
        final var raw = view.projected(ColumnFamily.DEDUPE, key, overlay);
        if (raw == null) {
            throw unavailable("Source occurrence lacks its retained original SYSTEM first");
        }
        final var payload =
                TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload();
        final var first = TargetResultRecord.decode(payload);
        first.requireStored(key, TargetResultRecord.VALUE_TYPE, payload);
        return first;
    }

    private static byte[] obligationKey(TargetSloControlScopeHeader.Reference reference) {
        return Bytes.concat(
                new byte[] {TargetScopeControlObligation.KEY_TAG, TargetKeyCodec.KEY_FORMAT}, reference.scopeId());
    }

    private static Unavailable unavailable(String message) {
        return new Unavailable(message);
    }
}
