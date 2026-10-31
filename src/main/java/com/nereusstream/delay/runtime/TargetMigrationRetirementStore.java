package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Same original Source retirement writer. Catalog and resource owners adopt these facts, never replace them. */
public final class TargetMigrationRetirementStore {
    private TargetMigrationRetirementStore() {}

    /**
     * The original held owner authenticates the complete frozen domain and actual eligible readers for tuples 32/33,
     * Native71 and their independently retained raw material. Confirmation authenticates the actual original Catalog
     * CAS capability, drained admissions and every resource owner's real exit/delete/receiver-custody/fee receipt.
     * All methods except requireCurrent execute before Native views; requireCurrent is local and performs no I/O.
     * A hash, callback completion, pin count or newly fabricated Target counter cannot satisfy this authority.
     */
    public interface OriginalInputs {
        void requireIntent(TargetMigrationRoot root, TargetMigrationRetirementIntentRequest intent,
                SystemMutation input, SourcePosition source);
        void requireConfirmation(TargetMigrationRoot root, TargetMigrationRetirementRecord prior,
                TargetMigrationRetirementConfirmRequest request, SystemMutation input, SourcePosition source);
        void requireManagementPayment(TargetMigrationRoot root, PreparedControlOperation control,
                SystemMutation input, SourcePosition source, TargetMigrationRetirementRecord next);
        void requireCurrent();
    }
    public static final class Prepared {
        private final TargetStoreBackend backend;
        private final TargetStoreBackend.Prepared batch;
        private final TargetMigrationRetirementRecord record;
        private final SystemMutationResult result;
        private final Runnable current;
        private Prepared(TargetStoreBackend backend, TargetStoreBackend.Prepared batch,
                TargetMigrationRetirementRecord record, SystemMutationResult result, Runnable current) {
            this.backend = backend; this.batch = batch; this.record = record; this.result = result; this.current = current;
        }
        public TargetMigrationRetirementRecord record() { return record; }
    }
    private record Capture(TargetMigrationRoot root, TargetMigrationRetirementRecord prior,
            TargetQuotaIncarnation payer, TargetQuotaMutation frontier, TargetQuotaMutation at) {}

    /** Opaque actual Root/intent/first/POS snapshot, rechecked under the caller's original Native read authority. */
    public static final class AppliedIntent {
        private final TargetStoreBackend backend;
        private final TargetQuotaScope scope;
        private final byte[] lineage;
        private final TargetMigrationPlan plan;
        private final TargetMigrationRoot migration;
        private final TargetMigrationRetirementRecord record;
        private final TargetStoreBackend.ReadAuthority reads;
        private AppliedIntent(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
                TargetMigrationPlan plan, TargetMigrationRoot migration, TargetMigrationRetirementRecord record,
                TargetStoreBackend.ReadAuthority reads) {
            this.backend = backend; this.scope = scope; this.lineage = Bytes.copy(lineage); this.plan = plan;
            this.migration = migration; this.record = record; this.reads = reads;
        }
        public TargetMigrationRoot migration() { return migration; }
        public TargetMigrationRetirementRecord record() { return record; }
        public TargetMigrationRetirementIntentRequest intention() { return record.intention(); }
        public TargetQuotaMutation intentAt() { return record.intentAt(); }
        public void requireCurrent() {
            final long bytes = Math.addExact(2L * TargetMigrationRetirementRecord.MAX_CANONICAL_BYTES,
                    4L * TargetResultRecord.MAX_CANONICAL_BYTES + 2L * TargetMigrationRoot.MAX_CANONICAL_BYTES
                            + 4L * TargetQuotaIncarnation.MAX_CANONICAL_BYTES + 4096);
            final var budget = new BoundedReadBudget(64, bytes, java.time.Duration.ofSeconds(5).toNanos(), System::nanoTime);
            final var actual = readAppliedIntent(backend, scope, lineage, plan, budget, reads);
            if (actual == null || !Arrays.equals(actual.record.intentInput().canonicalEnvelope(), record.intentInput().canonicalEnvelope())
                    || !actual.record.intentAt().equals(record.intentAt())
                    || record.retired() && !actual.record.equals(record))
                throw new IllegalStateException("actual retirement intent/confirmation changed or disappeared");
        }
    }
    public static final class AppliedConfirmation {
        private final AppliedIntent intent;
        private AppliedConfirmation(AppliedIntent intent) {
            this.intent = intent;
            if (!intent.record.retired()) throw new IllegalArgumentException("actual retirement confirmation required");
        }
        public AppliedIntent intent() { return intent; }
        public TargetMigrationRetirementIntentRequest intention() { return intent.intention(); }
        public TargetMigrationRoot migration() { return intent.migration(); }
        public TargetMigrationRetirementRecord record() { return intent.record(); }
        public TargetMigrationRetirementConfirmRequest confirmation() { return record().confirmation(); }
        public TargetQuotaMutation confirmedAt() { return record().confirmedAt(); }
        public void requireCurrent() { intent.requireCurrent(); }
    }

    /** Route retained-first/physical replay before this method, including after an UNKNOWN Native response. */
    public static Prepared prepareFirst(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationImporter.Limits limits, BoundedReadBudget budget, PreparedControlOperation control,
            SystemMutation input, SourcePosition source, TargetMigrationStore.Authority authority,
            OriginalInputs original) {
        Objects.requireNonNull(backend); Objects.requireNonNull(limits); Objects.requireNonNull(original);
        TargetSourcePosition.requireBounded(source);
        final var body = TargetMigrationControlBody.decode(input.canonicalBody()); body.requireInput(input);
        if (!scope.equals(body.request().scope()) || !(body.request() instanceof TargetMigrationRetirementIntentRequest
                || body.request() instanceof TargetMigrationRetirementConfirmRequest))
            throw new IllegalArgumentException("expected independent original migration retirement Control purpose");
        final Runnable authenticated = TargetMigrationStore.authenticate(control, input, source, body, authority);
        final var capture = backend.prepareRead(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0
                    || reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs())
                throw new IllegalStateException("retirement requires an unexpired later actual original Source input");
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var prior = selected(reader, List.of(), root, reader.aggregate().mutation());
            final byte[] ownerKey = root.owner().key();
            final var payer = TargetQuotaIncarnation.decodeForStore(ownerKey, TargetValueEnvelope.decode(
                    Objects.requireNonNull(reader.get(ColumnFamily.META, ownerKey), "actual original management payer"),
                    TargetQuotaIncarnation.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
            final var at = new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()), source, Bytes.sha256(input.canonicalEnvelope()));
            return new Capture(root, prior, payer, reader.aggregate().mutation(), at);
        }).value();
        final TargetMigrationRetirementRecord next;
        if (body.request() instanceof TargetMigrationRetirementIntentRequest intent) {
            intent.requireRoot(capture.root());
            if (capture.prior() != null) throw new IllegalStateException("old domain is already RETIRING/RETIRED; only its exact original flow may continue");
            original.requireIntent(capture.root(), intent, input, source);
            next = TargetMigrationRetirementRecord.retiring(capture.payer(), control, input, capture.at());
        } else {
            final var confirmation = (TargetMigrationRetirementConfirmRequest) body.request();
            final var prior = Objects.requireNonNull(capture.prior(), "actual original RETIRING Source fact");
            if (prior.retired()) throw new IllegalStateException("old domain is permanently retired; route original first replay");
            confirmation.requireIntent(prior.intention(), prior.intentAt());
            final var takeover = backend.prepareRead(budget, reader -> TargetMigrationArchiveStore.requireTakeover(reader,
                    List.of(), capture.root(), prior, reader.aggregate().mutation())).value();
            if (confirmation.takeoverAt() == null || !confirmation.takeoverAt().equals(takeover.managementAt()))
                throw new IllegalStateException("retirement confirmation requires its exact actual Source archive takeover");
            original.requireConfirmation(capture.root(), prior, confirmation, input, source);
            next = prior.confirm(control, input, capture.at());
        }
        next.requireRoot(capture.root());
        original.requireManagementPayment(capture.root(), control, input, source, next);
        final Runnable current = () -> { authenticated.run(); original.requireCurrent(); };
        current.run();
        final var result = SystemMutationResult.from(input, ApplyStatus.APPLIED, StableCode.OK, source.canonicalBytes());
        final var batch = backend.prepare(budget, reader -> {
            current.run();
            if (!reader.aggregate().mutation().equals(capture.frontier())
                    || reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs())
                throw new IllegalStateException("retirement actual Source/fee image changed while original owner materials were acquired");
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            if (!root.equals(capture.root())) throw new IllegalStateException("retirement original migration root changed");
            final var prior = selected(reader, List.of(), root, reader.aggregate().mutation());
            if (!Objects.equals(prior, capture.prior())) throw new IllegalStateException("retirement changed its exact frozen predecessor");
            if (body.request() instanceof TargetMigrationRetirementConfirmRequest confirmation) {
                final var takeover = TargetMigrationArchiveStore.requireTakeover(reader, List.of(), root, prior, reader.aggregate().mutation());
                if (!takeover.managementAt().equals(confirmation.takeoverAt())) throw new IllegalStateException("actual Source takeover changed before retirement confirmation");
            }
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            edits.add(reader.replace(ColumnFamily.META, next.key(), TargetMigrationRetirementRecord.VALUE_TYPE, next.canonicalBytes()));
            TargetMigrationStore.appendResults(reader, edits, capture.payer(), input, result, capture.at());
            return new TargetSourceAccounting(scope, lineage, source, capture.at().mutationDigest(), limits.maximumCounters(),
                    limits.maximumTargets(), limits.maximumDomains()).assemble(reader, edits);
        });
        return new Prepared(backend, batch, next, result, current);
    }
    public static SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared); prepared.current.run();
        prepared.backend.commit(prepared.batch, TargetMigrationStore.guarded(authority, prepared.current));
        return prepared.result;
    }

    public static AppliedIntent readAppliedIntent(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationPlan plan, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        Objects.requireNonNull(reads, "actual original Native read authority");
        if (!plan.scope().equals(scope) || !Arrays.equals(plan.newLineage(), lineage)) throw new IllegalArgumentException("retirement read changed original migration scope/lineage");
        final var image = backend.prepareRead(budget, reader -> {
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            if (!root.plan().equals(plan)) throw new IllegalStateException("retirement read changed exact original migration plan");
            final var record = selected(reader, List.of(), root, reader.aggregate().mutation());
            return record == null ? java.util.Optional.<AppliedIntent>empty()
                    : java.util.Optional.of(new AppliedIntent(backend, scope, lineage, plan, root, record, reads));
        });
        return backend.completeRead(image, reads).orElse(null);
    }
    public static AppliedConfirmation readAppliedConfirmation(TargetStoreBackend backend, TargetQuotaScope scope,
            byte[] lineage, TargetMigrationPlan plan, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        final var intent = readAppliedIntent(backend, scope, lineage, plan, budget, reads);
        return intent == null || !intent.record().retired() ? null : new AppliedConfirmation(intent);
    }
    public static TargetMigrationRetirementRecord decode(byte[] raw) {
        return raw == null ? null : TargetMigrationRetirementRecord.decode(TargetValueEnvelope.decode(raw, TargetMigrationRetirementRecord.VALUE_TYPE).payload());
    }
    public static TargetMigrationRetirementRecord selected(TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay, TargetMigrationRoot root, TargetQuotaMutation frontier) {
        final byte[] key = TargetMigrationRetirementRecord.keyFor(root.plan().migrationId());
        final var record = decode(view.projected(ColumnFamily.META, key, overlay));
        if (record == null) return null;
        record.requireRoot(root);
        if (!Arrays.equals(record.key(), key)) throw new IllegalStateException("retirement permanent key changed original migration");
        TargetMigrationStore.requireApplied(view, overlay, record.payer(), record.intentInput(), record.intentAt(), frontier);
        if (record.retired()) TargetMigrationStore.requireApplied(view, overlay, record.payer(), record.confirmationInput(), record.confirmedAt(), frontier);
        return record;
    }

    /** Newly paid State only. Old owners' physical/legacy shares never become synthetic new-Store counters. */
    public static TargetRecordAccounting.Charge nativeCharge(TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay, TargetQuotaScope scope, byte[] lineage, TargetQuotaMutation frontier,
            ColumnFamily family, byte[] key, byte[] payload) {
        if (family != ColumnFamily.META) throw new IllegalArgumentException("retirement Native fact belongs to META");
        final var root = TargetMigrationStore.requireActivated(view, overlay, scope, lineage, frontier);
        final var record = Objects.requireNonNull(selected(view, overlay, root, frontier), "permanent retirement Native fact");
        if (!Arrays.equals(key, record.key()) || !Arrays.equals(payload, record.canonicalBytes()))
            throw new IllegalStateException("retirement Native accounting changed selected key/content");
        final var frozen = record.payer();
        final var owner = TargetQuotaIncarnation.decodeForStore(frozen.key(), TargetValueEnvelope.decode(
                Objects.requireNonNull(view.projected(ColumnFamily.META, frozen.key(), overlay), "actual original management fee owner"),
                TargetQuotaIncarnation.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
        if (!owner.identity().equals(frozen.identity()) || !owner.accounting().equals(frozen.accounting())
                || !owner.allocation().equals(frozen.allocation())) throw new IllegalStateException("retirement management payer changed");
        owner.latestMutation().requireAtOrBefore(frontier);
        return new TargetRecordAccounting.Charge(owner, TargetRecordAccounting.resources(owner.accounting().recordCharge(
                TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
    }

    /** No generic GC, normal mutation or local shutdown may remove or rewrite the frozen old-domain fact. */
    public static void requireEdits(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits,
            TargetQuotaMutation source) {
        TargetMigrationRetirementRecord retained = null;
        boolean loaded = false;
        for (var edit : edits) if (edit.family() == ColumnFamily.DEDUPE && edit.before() != null
                && !Arrays.equals(edit.before(), edit.after())) {
            if (!loaded) {
                final var root = TargetMigrationStore.decodeRoot(reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId())));
                retained = root == null ? null : decode(reader.get(ColumnFamily.META,
                        TargetMigrationRetirementRecord.keyFor(root.plan().migrationId())));
                if (retained != null) retained.requireRoot(root);
                loaded = true;
            }
            if (retained != null && (isResultKey(edit.key(), retained.intentInput(), retained.intentAt())
                    || retained.retired() && isResultKey(edit.key(), retained.confirmationInput(), retained.confirmedAt())))
                throw new IllegalStateException("permanent retirement facts retain their original Source first/POS history");
        }
        for (var edit : edits) if (edit.family() == ColumnFamily.META && edit.key().length > 2
                && Byte.toUnsignedInt(edit.key()[0]) == TargetKeyCodec.MIGRATION_RETIREMENT_TAG
                && !TargetMigrationArchiveState.isKey(edit.key())) {
            if (edit.after() == null || source.isLocalMutation()) throw new IllegalStateException("retirement birth/history/tombstone is permanent and Source ordered");
            final var after = decode(edit.after());
            final var root = Objects.requireNonNull(TargetMigrationStore.decodeRoot(reader.get(ColumnFamily.META,
                    TargetMigrationRoot.keyFor(reader.shardId()))), "original migration root");
            after.requireRoot(root);
            if (!Arrays.equals(after.key(), edit.key()) || !after.mutation().equals(source))
                throw new IllegalStateException("retirement changed exact permanent key/actual Source decision");
            if (edit.before() == null) {
                if (after.retired()) throw new IllegalStateException("retirement confirmation cannot invent its Source intent");
            } else {
                final var before = decode(edit.before());
                if (before.retired() || !after.retired() || !before.payer().equals(after.payer())
                        || !Arrays.equals(before.intentControl().canonicalBytes(), after.intentControl().canonicalBytes())
                        || !Arrays.equals(before.intentInput().canonicalEnvelope(), after.intentInput().canonicalEnvelope())
                        || !before.intentAt().equals(after.intentAt()))
                    throw new IllegalStateException("retirement only advances its exact original RETIRING fact once");
            }
        }
    }
    private static boolean isResultKey(byte[] key, SystemMutation input, TargetQuotaMutation at) {
        return Arrays.equals(key, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, input.systemMutationId()))
                || Arrays.equals(key, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, at.source().canonicalBytes()));
    }
}
