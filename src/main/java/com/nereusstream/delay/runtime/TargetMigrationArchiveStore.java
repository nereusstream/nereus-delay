package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayOutcome;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Actual Source COPY/TAKEOVER and original-identity archive projections, under the original retirement root. */
public final class TargetMigrationArchiveStore {
    private TargetMigrationArchiveStore() {}
    private record OwnReplay(TargetStoreBackend backend, SourceReplayEntry actual) {}
    private static final ThreadLocal<OwnReplay> OWN_REPLAY = new ThreadLocal<>();

    private static TargetStoreBackend.Prepared planReplay(TargetStoreBackend backend, SourceReplayEntry actual,
            java.util.function.Supplier<TargetStoreBackend.Prepared> planner) {
        if (OWN_REPLAY.get() != null) throw new IllegalStateException("another original archive replay owns this Source plan");
        OWN_REPLAY.set(new OwnReplay(backend, actual));
        try { return planner.get(); } finally { OWN_REPLAY.remove(); }
    }

    /** Only this writer's authenticated actual replay plan can bypass ordinary prospective-tail acquisition. */
    public static boolean handlesSourceTail(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits, TargetQuotaMutation at) {
        final var owned = OWN_REPLAY.get(); if (owned == null) return false;
        reader.requireBackend(owned.backend());
        if (!Arrays.equals(at.source().canonicalBytes(), owned.actual().position().canonicalBytes())
                || !Arrays.equals(at.mutationDigest(), sourceDigest(owned.actual()))) throw new IllegalStateException("archive replay plan changed its actual Source input");
        TargetMigrationArchiveRow physical = null;
        TargetMigrationArchiveState head = null;
        for (var edit : edits) if (edit.family() == ColumnFamily.META && edit.after() != null) {
            if (edit.key().length > 2 && edit.key()[0] == TargetKeyCodec.MIGRATION_ARCHIVE_TAG) {
                final var row = decodeRow(edit.after());
                if (row.sourceBranch() == TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY && row.entry().namespace() == 3) {
                    if (physical != null || edit.before() != null) throw new IllegalStateException("archive replay cannot replace or multiply its physical first");
                    physical = row;
                }
            } else if (TargetMigrationArchiveState.isKey(edit.key())) {
                final var state = decodeState(edit.after()); if (!state.history()) head = state;
            }
        }
        if (physical == null || head == null || !physical.materializedAt().equals(at) || !head.mutation().equals(at)
                || !Arrays.equals(physical.sourceFrame(), frame(owned.actual()))) throw new IllegalStateException("actual archive replay lost its atomic physical/head update");
        requireEdits(reader, edits, at); return true;
    }
    /** Independently held original management payment. Acquired on owned I/O; all checks here consume local held data. */
    public interface OriginalInputs {
        void requireManagementPayment(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
                TargetMigrationArchiveState prior, TargetMigrationArchiveState next, List<TargetMigrationArchiveRow> rows,
                PreparedControlOperation control, SystemMutation input, SourcePosition source);
        void requireCurrent();
    }
    public static final class Prepared {
        private final TargetStoreBackend backend; private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result; private final Runnable current;
        private Prepared(TargetStoreBackend backend, TargetStoreBackend.Prepared batch, SystemMutationResult result, Runnable current) {
            this.backend = backend; this.batch = batch; this.result = result; this.current = current;
        }
    }
    private record Capture(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
            TargetMigrationArchiveState prior, TargetQuotaIncarnation payer, TargetQuotaMutation frontier, TargetQuotaMutation at) {}

    public static Prepared prepareFirst(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationImporter.Limits limits, BoundedReadBudget budget, PreparedControlOperation control,
            SystemMutation input, SourcePosition source, TargetMigrationStore.Authority authority,
            TargetMigrationArchivePreparation.PreparedBatch copy, TargetMigrationArchivePreparation.PreparedTakeover takeover,
            TargetMigrationArchiveReceived received, OriginalInputs original) {
        Objects.requireNonNull(original); final var body = TargetMigrationControlBody.decode(input.canonicalBody()); body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationArchiveRequest request) || !scope.equals(request.scope())
                || !Arrays.equals(lineage, request.lineage())) throw new IllegalArgumentException("exact archive management purpose required");
        final Runnable authenticated = TargetMigrationStore.authenticate(control, input, source, body, authority);
        final var capture = backend.prepareRead(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0
                    || reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()) throw new IllegalStateException("archive Source operation is not a later unexpired input");
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()), "actual RETIRE intent");
            request.requireRetirement(root, retirement);
            if (retirement.retired() || !TargetProtocolActivationStore.readerAuthorized(reader, scope, lineage, request.tuple()))
                throw new IllegalStateException("archive requires original RETIRING domain and actual reader36 activation");
            final var prior = head(reader, List.of(), root, retirement, reader.aggregate().mutation());
            requirePredecessor(request, prior, reader.aggregate().mutation());
            final var payer = currentPayer(reader, List.of(), root.owner(), scope, reader.aggregate().mutation());
            return new Capture(root, retirement, prior, payer, reader.aggregate().mutation(), new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()), source, Bytes.sha256(input.canonicalEnvelope())));
        }).value();
        if (request.takeover()) {
            Objects.requireNonNull(takeover, "actual complete archive takeover preparation").requireTakeover(capture.root(), capture.retirement(), capture.prior(), request, source);
            Objects.requireNonNull(received, "actual independently protected new archive owner").requireTakeover(capture.root(), capture.retirement(), request, source);
        } else Objects.requireNonNull(copy, "actual bounded archive copy preparation").requireCopy(capture.root(), capture.retirement(), capture.prior(), request, source);
        final long tailRows = capture.prior() == null ? 0 : capture.prior().tailRows();
        final long tailBytes = capture.prior() == null ? 0 : capture.prior().tailRawBytes();
        if (request.action() instanceof TargetMigrationArchiveRequest.Takeover transfer
                && (transfer.receipt().tailRows() != tailRows || transfer.receipt().tailRawBytes() != tailBytes))
            throw new IllegalStateException("archive takeover changed actual Source tail inventory");
        final var step = new TargetMigrationArchiveState(capture.payer(), request.migrationId(), request.planDigest(), request.intentDigest(), request.intentAt(),
                request.generation(), request.resultingRoot(), request.coverage(), request.shadowCut(), capture.at(), capture.at().mutationDigest(), capture.at(),
                request.takeover(), tailRows, tailBytes, control, input, capture.at(), null,
                Objects.requireNonNull(request.action() instanceof TargetMigrationArchiveRequest.Takeover transfer ? transfer.receipt().durableReceive()
                        : request.copyBatch().receivedInventory(), "actual received archive inventory reference"));
        final var rows = new ArrayList<TargetMigrationArchiveRow>();
        if (request.copyBatch() != null) for (var entry : request.copyBatch().rows()) {
            TargetMigrationArchiveSemantics.requireEntry(entry, capture.root().plan());
            rows.add(row(capture.root().plan(), capture.payer(), entry, TargetMigrationArchiveRow.SourceBranch.COPY_BATCH,
                    request.generation(), capture.at().mutationDigest(), capture.at(), null));
        }
        original.requireManagementPayment(capture.root(), capture.retirement(), capture.prior(), step, List.copyOf(rows), control, input, source);
        final Runnable current = () -> { authenticated.run(); original.requireCurrent();
            if (copy != null) copy.requireCurrent(); if (takeover != null) takeover.requireCurrent(); if (received != null) received.requireCurrent(); };
        current.run(); final var result = SystemMutationResult.from(input, ApplyStatus.APPLIED, StableCode.OK, source.canonicalBytes());
        final var batch = backend.prepare(budget, reader -> {
            current.run();
            if (!reader.aggregate().mutation().equals(capture.frontier())) throw new IllegalStateException("archive exact Native preparation cut changed");
            final var actualRoot = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var actualRetirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), actualRoot, reader.aggregate().mutation()));
            final var prior = head(reader, List.of(), actualRoot, actualRetirement, reader.aggregate().mutation());
            if (!actualRoot.equals(capture.root()) || !actualRetirement.equals(capture.retirement()) || !Objects.equals(prior, capture.prior()))
                throw new IllegalStateException("archive original root/intent/previous state changed");
            requirePredecessor(request, prior, reader.aggregate().mutation());
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            for (var row : rows) appendImmutable(reader, edits, row);
            edits.add(reader.replace(ColumnFamily.META, step.key(), TargetMigrationArchiveState.VALUE_TYPE, step.canonicalBytes()));
            final var next = step.head(); edits.add(reader.replace(ColumnFamily.META, next.key(), TargetMigrationArchiveState.VALUE_TYPE, next.canonicalBytes()));
            TargetMigrationStore.appendResults(reader, edits, capture.payer(), input, result, capture.at());
            return new TargetSourceAccounting(scope, lineage, source, capture.at().mutationDigest(), limits.maximumCounters(), limits.maximumTargets(), limits.maximumDomains()).assemble(reader, edits);
        });
        return new Prepared(backend, batch, result, current);
    }
    public static SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        prepared.current.run(); prepared.backend.commit(prepared.batch, TargetMigrationStore.guarded(authority, prepared.current)); return prepared.result;
    }
    private static void requirePredecessor(TargetMigrationArchiveRequest request, TargetMigrationArchiveState prior, TargetQuotaMutation frontier) {
        if (!request.coveredThrough().equals(frontier) || prior != null && prior.taken()
                || prior == null && (request.generation() != 1 || request.expectedStateDigest() != null
                    || !Arrays.equals(request.expectedArchiveRoot(), TargetMigrationArchiveRequest.genesisRoot(request.intentDigest())))
                || prior != null && (request.generation() != TargetQuotaMutation.increment(prior.generation())
                    || !Arrays.equals(request.expectedStateDigest(), prior.digest()) || !Arrays.equals(request.expectedArchiveRoot(), prior.archiveRoot())
                    || !prior.coveredThrough().equals(frontier))) throw new IllegalStateException("archive did not preserve exact original progress and complete actual Source frontier");
    }
    private static TargetMigrationArchiveRow row(TargetMigrationPlan plan, TargetQuotaIncarnation payer, TargetMigrationArchiveEntry entry,
            TargetMigrationArchiveRow.SourceBranch branch, long generation, byte[] digest, TargetQuotaMutation at, byte[] frame) {
        return new TargetMigrationArchiveRow(plan.migrationId(), plan.digest(), plan.oldStore(), plan.oldDbIdentity(), plan.oldLineage(), payer, entry, branch, generation, digest, at, frame);
    }
    private static void appendImmutable(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits, TargetMigrationArchiveRow row) {
        final var raw = reader.projected(ColumnFamily.META, row.key(), edits);
        if (raw != null) {
            final var original = decodeRow(raw); TargetMigrationArchiveSemantics.requireEquivalent(original.entry(), row.entry());
            return;
        }
        edits.add(reader.replace(ColumnFamily.META, row.key(), TargetMigrationArchiveRow.VALUE_TYPE, row.canonicalBytes()));
    }
    public static TargetMigrationArchiveRow decodeRow(byte[] raw) {
        return raw == null ? null : TargetMigrationArchiveRow.decode(TargetValueEnvelope.decode(raw, TargetMigrationArchiveRow.VALUE_TYPE).payload());
    }
    public static TargetMigrationArchiveState decodeState(byte[] raw) {
        return raw == null ? null : TargetMigrationArchiveState.decode(TargetValueEnvelope.decode(raw, TargetMigrationArchiveState.VALUE_TYPE).payload());
    }
    public static TargetMigrationArchiveState head(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetMigrationRoot root, TargetMigrationRetirementRecord retirement, TargetQuotaMutation frontier) {
        final var head = decodeState(view.projected(ColumnFamily.META, TargetMigrationArchiveState.headKey(root.plan().migrationId()), overlay));
        if (head == null) return null;
        head.requireRetirement(root, retirement); head.mutation().requireAtOrBefore(frontier);
        if (head.history()) throw new IllegalStateException("archive head cannot replace its immutable management history");
        final var step = Objects.requireNonNull(decodeState(view.projected(ColumnFamily.META,
                TargetMigrationArchiveState.historyKey(head.migrationId(), head.managementAt().sequence()), overlay)), "actual archive management history");
        requireStep(view, overlay, root, retirement, step, frontier);
        if (head.generation() != step.generation() || !head.managementAt().equals(step.managementAt()) || !Arrays.equals(head.managementDigest(), step.managementDigest())
                || head.taken() != step.taken() || !head.coverage().equals(step.coverage()) || !head.shadowCut().equals(step.shadowCut()))
            throw new IllegalStateException("archive head changed its exact management/coverage origin");
        if (head.mutation().equals(head.managementAt())) {
            if (!head.equals(step.head())) throw new IllegalStateException("archive management head changed its exact actual inventory/root");
        } else requireTailApplied(view, overlay, root.plan(), head.mutation(), head.tailFrame(), frontier);
        return head;
    }
    public static void requireStep(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, TargetMigrationRoot root,
            TargetMigrationRetirementRecord retirement, TargetMigrationArchiveState step, TargetQuotaMutation frontier) {
        step.requireRetirement(root, retirement);
        if (!step.history()) throw new IllegalStateException("actual archive management history required");
        step.request().requireRetirement(root, retirement);
        final var request = step.request();
        final var inventory = request.action() instanceof TargetMigrationArchiveRequest.Takeover transfer
                ? transfer.receipt().durableReceive() : request.copyBatch().receivedInventory();
        if (step.inventory() == null || !step.inventory().equals(inventory)) throw new IllegalStateException("archive Source step lost its exact received inventory reference");
        TargetMigrationStore.requireApplied(view, overlay, step.payer(), step.input(), step.managementAt(), frontier);
    }
    public static TargetMigrationArchiveState requireTakeover(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetMigrationRoot root, TargetMigrationRetirementRecord retirement, TargetQuotaMutation frontier) {
        final var head = Objects.requireNonNull(head(view, overlay, root, retirement, frontier), "actual archive takeover head");
        if (!head.taken()) throw new IllegalStateException("old interpretation domain has not actually transferred on Source");
        final var step = Objects.requireNonNull(decodeState(view.projected(ColumnFamily.META,
                TargetMigrationArchiveState.historyKey(head.migrationId(), head.managementAt().sequence()), overlay)));
        if (!step.request().takeover() || !step.coverage().complete()) throw new IllegalStateException("archive takeover lacks complete actual Source coverage");
        return step;
    }
    public static final class AppliedTakeover {
        private final TargetStoreBackend backend; private final TargetQuotaScope scope; private final byte[] lineage;
        private final TargetMigrationPlan plan; private final TargetMigrationRoot root; private final TargetMigrationRetirementRecord retirement;
        private final TargetMigrationArchiveState state; private final TargetStoreBackend.ReadAuthority reads;
        private AppliedTakeover(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage, TargetMigrationPlan plan,
                TargetMigrationRoot root, TargetMigrationRetirementRecord retirement, TargetMigrationArchiveState state, TargetStoreBackend.ReadAuthority reads) {
            this.backend = backend; this.scope = scope; this.lineage = Bytes.copy(lineage); this.plan = plan; this.root = root; this.retirement = retirement; this.state = state; this.reads = reads;
        }
        public TargetMigrationRoot migration() { return root; }
        public TargetMigrationRetirementRecord retirement() { return retirement; }
        public TargetMigrationArchiveState state() { return state; }
        public TargetMigrationArchiveRequest request() { return state.request(); }
        public TargetQuotaMutation takeoverAt() { return state.managementAt(); }
        public void requireCurrent() {
            final var actual = readAppliedTakeover(backend, scope, lineage, plan, new BoundedReadBudget(128,
                    4L * TargetMigrationArchiveState.MAX_CANONICAL_BYTES + 2L * TargetMigrationRetirementRecord.MAX_CANONICAL_BYTES
                            + 8L * TargetResultRecord.MAX_CANONICAL_BYTES + 8L * TargetQuotaIncarnation.MAX_CANONICAL_BYTES,
                    java.time.Duration.ofSeconds(5).toNanos(), System::nanoTime), reads);
            if (actual == null || !actual.state.equals(state)) throw new IllegalStateException("actual Source takeover changed or disappeared");
        }
    }
    public static AppliedTakeover readAppliedTakeover(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationPlan plan, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        final var read = backend.prepareRead(budget, reader -> {
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            if (!root.plan().equals(plan)) throw new IllegalStateException("archive takeover read changed original migration");
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
            final var head = head(reader, List.of(), root, retirement, reader.aggregate().mutation());
            if (head == null || !head.taken()) return java.util.Optional.<AppliedTakeover>empty();
            if (!TargetProtocolActivationStore.readerAuthorized(reader, scope, lineage, ProtocolTuple.targetMigrationLogicalFirstArchive()))
                throw new IllegalStateException("actual Native72 reader activation is unavailable");
            final var step = requireTakeover(reader, List.of(), root, retirement, reader.aggregate().mutation());
            return java.util.Optional.of(new AppliedTakeover(backend, scope, lineage, plan, root, retirement, step, reads));
        });
        return backend.completeRead(read, Objects.requireNonNull(reads)).orElse(null);
    }
    public static TargetMigrationArchiveState readProgress(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationPlan plan, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        final var read = backend.prepareRead(budget, reader -> {
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            if (!root.plan().equals(plan)) throw new IllegalStateException("archive progress changed original plan");
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
            return java.util.Optional.ofNullable(head(reader, List.of(), root, retirement, reader.aggregate().mutation()));
        });
        return backend.completeRead(read, reads).orElse(null);
    }

    /** Preview only, for reopening the original new owner's receipt before any access to the old shadow. */
    public static TargetMigrationArchiveState peekTakeover(ShardStore store, TargetMigrationPlan plan, BoundedReadBudget budget) {
        final var image = store.readWithBudget(budget, () -> {
            TargetMigrationStore.requireIdentity(store.metadata(), plan);
            final var root = Objects.requireNonNull(TargetMigrationStore.decodeRoot(store.get(ColumnFamily.META, TargetMigrationRoot.keyFor(plan.scope().shard()))));
            if (!root.plan().equals(plan)) throw new IllegalStateException("takeover preview changed original root");
            final var head = decodeState(store.get(ColumnFamily.META, TargetMigrationArchiveState.headKey(plan.migrationId())));
            if (head == null || !head.taken()) return null;
            final var step = Objects.requireNonNull(decodeState(store.get(ColumnFamily.META,
                    TargetMigrationArchiveState.historyKey(plan.migrationId(), head.managementAt().sequence()))), "persisted takeover head lost actual management history");
            if (!step.history() || !step.taken() || !step.managementAt().equals(head.managementAt())
                    || !Arrays.equals(step.migrationId(), plan.migrationId()) || !Arrays.equals(step.planDigest(), plan.digest()))
                throw new IllegalStateException("persisted takeover preview changed original management fact");
            return step;
        });
        return store.withReadView(image.view(), image::value);
    }
    public static TargetMigrationArchiveRow read(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage, byte[] originalKey,
            BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        final var plan = backend.prepareRead(budget, reader -> {
            final var raw = reader.get(ColumnFamily.META, TargetMigrationArchiveRow.keyFor(originalKey));
            if (raw == null) return java.util.Optional.<TargetMigrationArchiveRow>empty();
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
            final var row = decodeRow(raw); requireRow(reader, List.of(), root, retirement, row, reader.aggregate().mutation()); return java.util.Optional.of(row);
        });
        return backend.completeRead(plan, reads).orElse(null);
    }
    public static final class LogicalPair {
        private final TargetMigrationArchiveRow first, auxiliary;
        private LogicalPair(TargetMigrationArchiveRow first, TargetMigrationArchiveRow auxiliary) { this.first = first; this.auxiliary = auxiliary; }
        public TargetMigrationArchiveRow first() { return first; }
        public TargetMigrationArchiveRow auxiliary() { return auxiliary; }
    }
    /** RESULT remains an independent original query projection; it never substitutes for a missing COMMAND first. */
    public static LogicalPair readCommandPair(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            CommandId id, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        return readLogicalPair(backend, scope, lineage, KeyCodec.dedupeCommand(id), KeyCodec.dedupeResult(id), true, budget, reads);
    }
    /** A deterministic FENCE proof occupies its original namespace even if a different System ID is presented. */
    public static LogicalPair readSystemPair(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            byte[] systemId, byte[] fenceProofId, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        return readLogicalPair(backend, scope, lineage, KeyCodec.dedupeSystemMutation(systemId),
                fenceProofId == null ? null : KeyCodec.dedupeFence(fenceProofId), false, budget, reads);
    }
    private static LogicalPair readLogicalPair(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            byte[] firstKey, byte[] auxiliaryKey, boolean command, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        final var image = backend.prepareRead(budget, reader -> {
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
            final var first = decodeRow(reader.get(ColumnFamily.META, TargetMigrationArchiveRow.keyFor(firstKey)));
            final var auxiliary = auxiliaryKey == null ? null : decodeRow(reader.get(ColumnFamily.META, TargetMigrationArchiveRow.keyFor(auxiliaryKey)));
            if (first != null) requireRow(reader, List.of(), root, retirement, first, reader.aggregate().mutation());
            if (auxiliary != null) requireRow(reader, List.of(), root, retirement, auxiliary, reader.aggregate().mutation());
            if (command && auxiliary != null) {
                if (first == null) throw new IllegalStateException("archive query projection cannot manufacture a missing original logical first");
                final var original = TargetMigrationArchiveSemantics.decode(first.entry()).command();
                final var query = TargetMigrationArchiveSemantics.decode(auxiliary.entry()).command();
                if (original == null || query == null || !Arrays.equals(original.encode(), query.encode()))
                    throw new IllegalStateException("archive original COMMAND/RESULT decisions conflict");
            }
            return first == null && auxiliary == null ? java.util.Optional.<LogicalPair>empty() : java.util.Optional.of(new LogicalPair(first, auxiliary));
        });
        return backend.completeRead(image, reads).orElse(null);
    }
    public static CommandResult readCommandQueryResult(TargetMigrationArchiveRow result) {
        if (result.entry().namespace() != 2) throw new IllegalArgumentException("original RESULT namespace required");
        return Objects.requireNonNull(TargetMigrationArchiveSemantics.decode(result.entry()).command());
    }

    public static CommandResult readCommandOutcome(TargetMigrationArchiveRow row, PreparedCommand input, SourcePosition actual) {
        return TargetMigrationArchiveSemantics.commandOutcome(row.entry(), input, actual);
    }
    public static SystemMutationResult readSystemOutcome(TargetMigrationArchiveRow row, SystemMutation input, SourcePosition actual) {
        return TargetMigrationArchiveSemantics.systemOutcome(row.entry(), input, actual);
    }
    public static void requireRow(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, TargetMigrationRoot root,
            TargetMigrationRetirementRecord retirement, TargetMigrationArchiveRow row, TargetQuotaMutation frontier) {
        row.requirePlan(root.plan()); row.materializedAt().requireAtOrBefore(frontier); TargetMigrationArchiveSemantics.requireEntry(row.entry(), root.plan());
        if (row.sourceBranch() == TargetMigrationArchiveRow.SourceBranch.COPY_BATCH) {
            final var step = Objects.requireNonNull(decodeState(view.projected(ColumnFamily.META,
                    TargetMigrationArchiveState.historyKey(row.migrationId(), row.materializedAt().sequence()), overlay)), "archive row original actual COPY");
            requireStep(view, overlay, root, retirement, step, frontier);
            if (step.generation() != row.managementGeneration() || !step.managementAt().equals(row.materializedAt())
                    || !Arrays.equals(step.managementDigest(), row.managementDigest()) || step.request().copyBatch() == null
                    || !step.request().copyBatch().rows().contains(row.entry())) throw new IllegalStateException("archive row changed original COPY entry");
        } else requireTailApplied(view, overlay, root.plan(), row.materializedAt(), row.sourceFrame(), frontier);
    }
    private static void requireTailApplied(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetMigrationPlan plan, TargetQuotaMutation at, byte[] frame, TargetQuotaMutation frontier) {
        at.requireAtOrBefore(frontier); TargetMigrationArchiveState.requireFrame(frame, at);
        final byte[] prefixRaw = view.projected(ColumnFamily.META, TargetMigrationPrefixRecord.keyFor(plan.migrationId(), at.sequence()), overlay);
        if (prefixRaw != null) {
            final var prefix = TargetMigrationPrefixRecord.decode(TargetValueEnvelope.decode(prefixRaw, TargetMigrationPrefixRecord.VALUE_TYPE).payload()); prefix.requirePlan(plan);
            if (!prefix.at().equals(at)) throw new IllegalStateException("archive original legacy Source prefix changed"); return;
        }
        final byte[] positionRaw = view.projected(ColumnFamily.DEDUPE, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, at.source().canonicalBytes()), overlay);
        if (positionRaw != null) {
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(positionRaw, TargetResultRecord.VALUE_TYPE).payload());
            if (!position.mutation().equals(at)) throw new IllegalStateException("archive actual Target physical Source changed"); return;
        }
        final var physical = decodeRow(view.projected(ColumnFamily.META, TargetMigrationArchiveRow.keyFor(KeyCodec.dedupePosition(at.source().canonicalBytes())), overlay));
        if (physical == null || physical.sourceBranch() != TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY
                || !physical.materializedAt().equals(at) || !Arrays.equals(physical.sourceFrame(), frame)) throw new IllegalStateException("archive Source lacks actual original prefix/Target/Native72 physical proof");
        requirePhysical(view, overlay, physical, plan);
    }
    private static TargetQuotaIncarnation currentPayer(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaIncarnation frozen, TargetQuotaScope scope, TargetQuotaMutation frontier) {
        final var owner = TargetQuotaIncarnation.decodeForStore(frozen.key(), TargetValueEnvelope.decode(Objects.requireNonNull(
                view.projected(ColumnFamily.META, frozen.key(), overlay)), TargetQuotaIncarnation.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
        if (!owner.identity().equals(frozen.identity()) || !owner.accounting().equals(frozen.accounting()) || !owner.allocation().equals(frozen.allocation()))
            throw new IllegalStateException("archive actual new metadata payer changed"); owner.latestMutation().requireAtOrBefore(frontier); return owner;
    }
    public static TargetRecordAccounting.Charge nativeCharge(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaMutation frontier, byte[] key, byte[] payload, boolean archiveRow) {
        final var root = TargetMigrationStore.requireActivated(view, overlay, scope, lineage, frontier);
        final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(view, overlay, root, frontier));
        final TargetQuotaIncarnation payer;
        if (archiveRow) {
            final var row = TargetMigrationArchiveRow.decode(payload); requireRow(view, overlay, root, retirement, row, frontier);
            if (!Arrays.equals(key, row.key())) throw new IllegalStateException("archive original namespace/key changed"); payer = row.payer();
        } else {
            final var state = TargetMigrationArchiveState.decode(payload);
            if (!Arrays.equals(key, state.key())) throw new IllegalStateException("archive child key changed");
            if (state.history()) requireStep(view, overlay, root, retirement, state, frontier);
            else if (!state.equals(head(view, overlay, root, retirement, frontier))) throw new IllegalStateException("archive head accounting changed selected bytes"); payer = state.payer();
        }
        final var owner = currentPayer(view, overlay, payer, scope, frontier);
        return new TargetRecordAccounting.Charge(owner, TargetRecordAccounting.resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
    }
    /** Source caller supplies a privately prepared actual tail, in the same original Source Native batch. */
    public static List<TargetStoreBackend.Edit> appendTail(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> existing,
            TargetMigrationRoot root, TargetMigrationRetirementRecord retirement, SourceReplayEntry actual, TargetQuotaMutation at,
            TargetMigrationArchivePreparation.PreparedTail proof) {
        final var prior = head(reader, List.of(), root, retirement, reader.aggregate().mutation());
        if (prior == null) return List.copyOf(existing);
        Objects.requireNonNull(proof, "actual privately prepared original Source tail").requireTail(reader, root, retirement, actual, at);
        proof.requireCurrent(); final var edits = new ArrayList<>(existing); byte[] digest = prior.archiveRoot(); long rows = 0, bytes = 0;
        byte[] previous = null;
        for (var entry : proof.entries()) {
            if (previous != null && Arrays.compareUnsigned(previous, entry.originalKey()) >= 0) throw new IllegalStateException("archive actual Source tail keys are duplicate or unordered");
            previous = entry.originalKey(); TargetMigrationArchiveSemantics.requireEntry(entry, root.plan());
            final var next = row(root.plan(), prior.payer(), entry, proof.branch(), 0, null, at, frame(actual));
            if (reader.projected(ColumnFamily.META, next.key(), edits) == null) {
                appendImmutable(reader, edits, next); digest = TargetMigrationArchiveRequest.appendRoot(digest, entry);
                rows = Math.incrementExact(rows); bytes = Math.addExact(bytes, entry.rawValue().length);
            } else TargetMigrationArchiveSemantics.requireEquivalent(decodeRow(reader.projected(ColumnFamily.META, next.key(), edits)).entry(), entry);
        }
        final var next = prior.tail(digest, rows, bytes, at, frame(actual), proof.inventory());
        edits.add(reader.replace(ColumnFamily.META, next.key(), TargetMigrationArchiveState.VALUE_TYPE, next.canonicalBytes()));
        proof.requireCurrent(); return List.copyOf(edits);
    }
    /** Original new owner retains the real Source frame, frozen old contract and exact physical charge through commit. */
    public interface ReplayInputs {
        TargetMigrationRecordReference sourceRecord(SourceReplayEntry actual);
        /** Same material's captured original fence; no I/O or authority substitution. */
        default void requireFence(long actualFrozenFence) {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("actual archive replay material lacks its exact original fence binding");
        }
        /** Same material's exact pre-materialization Source/head; this check is local under the Native view. */
        default void requirePredecessor(TargetMigrationArchiveState actualHead, TargetQuotaMutation actualBefore) {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("actual archive replay material lacks its exact original Source/head predecessor");
        }
        default TargetMigrationObjectReference inventory() {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("actual new archive owner's physical replay inventory is unavailable");
        }
        void requireReplay(AppliedTakeover takeover, TargetMigrationArchiveRow logical, SourceReplayEntry actual, SourceReplayOutcome outcome);
        void requireCurrent();
    }
    public static final class PreparedReplay {
        private final TargetStoreBackend backend; private final TargetStoreBackend.Prepared batch; private final SourceReplayOutcome outcome; private final Runnable current;
        private PreparedReplay(TargetStoreBackend backend, TargetStoreBackend.Prepared batch, SourceReplayOutcome outcome, Runnable current) {
            this.backend = backend; this.batch = batch; this.outcome = outcome; this.current = current;
        }
    }
    public static PreparedReplay prepareReplay(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationImporter.Limits limits, BoundedReadBudget budget, AppliedTakeover takeover, TargetMigrationArchiveRow logical,
            SourceReplayEntry actual, ReplayInputs inputs) {
        Objects.requireNonNull(takeover).requireCurrent(); Objects.requireNonNull(inputs);
        final long originalFence = backend.prepareRead(budget, reader -> reader.closedIngressDeadlineThrough()).value();
        inputs.requireFence(originalFence);
        final SourceReplayOutcome outcome;
        final int recordKind; final byte[] result;
        if (actual instanceof SourceReplayRecord command) {
            final var first = TargetMigrationArchiveSemantics.commandOutcome(logical.entry(), command.command(), actual.position(), originalFence);
            final var value = new CommandResult(first.applyStatus(), first.stableCode(), first.generation(), first.stateVersion(), first.messageStatus(), actual.position().canonicalBytes());
            outcome = SourceReplayOutcome.command(actual.position(), value); result = value.encode(); recordKind = ProtocolTuple.CLIENT_COMMAND;
        } else {
            final var input = ((SourceReplayMutation) actual).mutation();
            final var first = TargetMigrationArchiveSemantics.systemOutcome(logical.entry(), input, actual.position(), originalFence);
            final var value = SystemMutationResult.from(input, first.applyStatus(), first.stableCode(), actual.position().canonicalBytes());
            outcome = SourceReplayOutcome.systemMutation(actual.position(), value); result = value.encode(); recordKind = ProtocolTuple.SYSTEM_MUTATION;
        }
        inputs.requireReplay(takeover, logical, actual, outcome);
        final var ref = Objects.requireNonNull(inputs.sourceRecord(actual), "actual protected original Source frame reference");
        final Runnable current = () -> inputs.requireCurrent(); current.run();
        final var batch = planReplay(backend, actual, () -> backend.prepare(budget, reader -> {
            current.run();
            if (reader.closedIngressDeadlineThrough() != originalFence) throw new IllegalStateException("archive physical replay changed original Source fence");
            if (reader.source() == null || actual.position().compareTo(reader.source()) <= 0) throw new IllegalStateException("route an already applied archive physical occurrence through readPhysicalReplay");
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
            final var applied = requireTakeover(reader, List.of(), root, retirement, reader.aggregate().mutation());
            if (!applied.equals(takeover.state())) throw new IllegalStateException("archive replay changed actual Source takeover");
            final var retained = decodeRow(reader.get(ColumnFamily.META, logical.key()));
            if (!logical.equals(retained)) throw new IllegalStateException("archive replay changed its exact original logical first");
            requireRow(reader, List.of(), root, retirement, logical, reader.aggregate().mutation());
            final var prior = Objects.requireNonNull(head(reader, List.of(), root, retirement, reader.aggregate().mutation()));
            // After actual T, ordinary Native first/POS own their Source effects independently; archive progress may be sparse.
            prior.coveredThrough().requireAtOrBefore(reader.aggregate().mutation());
            inputs.requirePredecessor(prior, reader.aggregate().mutation());
            final byte[] frame = frame(actual);
            final var at = new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()), actual.position(), sourceDigest(actual));
            final var physical = new TargetMigrationArchivePhysicalRecord(recordKind, logical.entry().originalKey(), logical.digest(), result, originalFence);
            final var entry = new TargetMigrationArchiveEntry(KeyCodec.dedupePosition(actual.position().canonicalBytes()),
                    TargetValueEnvelope.encode(TargetMigrationArchiveRow.VALUE_TYPE, physical.canonicalBytes()),
                    TargetMigrationArchiveEntry.Origin.ARCHIVE_PHYSICAL, null, at, ref);
            final var row = row(root.plan(), prior.payer(), entry, TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY, 0, null, at, frame);
            final var edits = new ArrayList<TargetStoreBackend.Edit>(); appendImmutable(reader, edits, row);
            final var next = prior.tail(TargetMigrationArchiveRequest.appendRoot(prior.archiveRoot(), entry), 1, entry.rawValue().length, at, frame, inputs.inventory());
            edits.add(reader.replace(ColumnFamily.META, next.key(), TargetMigrationArchiveState.VALUE_TYPE, next.canonicalBytes()));
            return new TargetSourceAccounting(scope, lineage, actual.position(), at.mutationDigest(), limits.maximumCounters(), limits.maximumTargets(), limits.maximumDomains()).assemble(reader, edits);
        }));
        return new PreparedReplay(backend, batch, outcome, current);
    }
    /** Pure derivative projection shared by actual custody, Source writing and cold verification; it mints no authority. */
    public static List<TargetMigrationArchiveEntry> closedEntries(AppliedTakeover takeover, TargetMigrationArchiveState prior,
            TargetMigrationArchiveClosedContract.Decision decision, TargetMigrationRecordReference packet) {
        Objects.requireNonNull(takeover); Objects.requireNonNull(prior); Objects.requireNonNull(decision); Objects.requireNonNull(packet);
        decision.requireCurrent(); final var actual = decision.actual(); final var at = decision.at();
        if (!prior.taken() || !Arrays.equals(prior.migrationId(), takeover.migration().plan().migrationId())
                || !Arrays.equals(prior.planDigest(), takeover.migration().plan().digest())) throw new IllegalArgumentException("closed projection changed its actual takeover/root");
        final var logical = List.copyOf(decision.logicalEntries(packet));
        if (decision.physicalOnly() != !decision.logicalFirst() || decision.logicalFirst() != !logical.isEmpty())
            throw new IllegalStateException("original closed-contract logical/physical classification is inconsistent");
        final byte[] firstKey = actual instanceof SourceReplayRecord command ? KeyCodec.dedupeCommand(command.command().commandId())
                : KeyCodec.dedupeSystemMutation(((SourceReplayMutation) actual).mutation().systemMutationId());
        TargetMigrationArchiveRow first = null; final var entries = new java.util.TreeMap<byte[],TargetMigrationArchiveEntry>(Arrays::compareUnsigned);
        for (var entry : logical) {
            if (entry.namespace() == 3 || !entry.provenanceCut().equals(at) || entry.originalFirst() == null
                    || !Arrays.equals(entry.originalFirst().canonicalBytes(), actual.position().canonicalBytes())
                    || entries.put(entry.originalKey(),entry) != null) throw new IllegalStateException("closed-contract logical first lost original namespace/actual Source");
            if (Arrays.equals(entry.originalKey(),firstKey)) first = row(takeover.migration().plan(), prior.payer(), entry,
                    TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY, 0, null, at, frame(actual));
        }
        if (decision.logicalFirst() != (first != null)) throw new IllegalStateException("closed-contract decision omitted its exact original Command/System first");
        final var outcome = decision.outcome();
        final var physical = new TargetMigrationArchivePhysicalRecord(outcome.isCommand() ? ProtocolTuple.CLIENT_COMMAND : ProtocolTuple.SYSTEM_MUTATION,
                first == null ? null : first.entry().originalKey(), first == null ? null : first.digest(),
                outcome.isCommand() ? outcome.commandResult().encode() : outcome.systemMutationResult().encode(), decision.fence());
        final var position = new TargetMigrationArchiveEntry(KeyCodec.dedupePosition(actual.position().canonicalBytes()),
                TargetValueEnvelope.encode(TargetMigrationArchiveRow.VALUE_TYPE, physical.canonicalBytes()),
                TargetMigrationArchiveEntry.Origin.ARCHIVE_PHYSICAL, null, at, packet);
        if (entries.put(position.originalKey(),position) != null) throw new IllegalStateException("closed contract supplied multiple physical occurrences");
        return List.copyOf(entries.values());
    }
    /** Consumes only the original interpreter's private decision and the new owner's actual immutable material. */
    public static PreparedReplay prepareClosed(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            TargetMigrationImporter.Limits limits, BoundedReadBudget budget, AppliedTakeover takeover,
            SourceReplayEntry actual, TargetMigrationArchiveClosedContract.Decision decision,
            TargetMigrationArchiveReplayMaterial inputs) {
        Objects.requireNonNull(takeover).requireCurrent(); Objects.requireNonNull(decision); Objects.requireNonNull(inputs);
        final var outcome = decision.outcome();
        if (!Arrays.equals(frame(actual), frame(decision.actual())) || !Arrays.equals(actual.position().canonicalBytes(), outcome.position().canonicalBytes())
                || outcome.isCommand() != (actual instanceof SourceReplayRecord))
            throw new IllegalArgumentException("closed contract changed its exact original frame/Source/result branch");
        final long fence = backend.prepareRead(budget, reader -> reader.closedIngressDeadlineThrough()).value();
        if (fence != decision.fence()) throw new IllegalStateException("original closed-contract Source fence changed");
        inputs.requireFence(fence); inputs.requireClosed(decision, actual, outcome);
        final var packet = Objects.requireNonNull(inputs.sourceRecord(actual), "actual protected closed-contract packet");
        final var inventory = Objects.requireNonNull(inputs.inventory(), "actual received closed-contract inventory");
        final Runnable current = () -> { decision.requireCurrent(); inputs.requireCurrent(); }; current.run();
        final var batch = planReplay(backend, actual, () -> backend.prepare(budget, reader -> {
            current.run(); decision.requireReader(reader, takeover, actual);
            if (reader.source() == null || actual.position().compareTo(reader.source()) <= 0)
                throw new IllegalStateException("route an already applied closed-contract occurrence through actual Native72 physical replay");
            if (reader.closedIngressDeadlineThrough() != fence || !decision.before().equals(reader.aggregate().mutation()))
                throw new IllegalStateException("original closed-contract material changed exact Source predecessor/fence");
            final var at = new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()), actual.position(), sourceDigest(actual));
            if (!at.equals(decision.at())) throw new IllegalStateException("closed-contract materialization changed its exact actual Source stamp");
            final var root = TargetMigrationStore.requireActivated(reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
            final var applied = requireTakeover(reader, List.of(), root, retirement, reader.aggregate().mutation());
            if (!applied.equals(takeover.state())) throw new IllegalStateException("closed-contract decision changed actual Source takeover");
            final var prior = Objects.requireNonNull(head(reader, List.of(), root, retirement, reader.aggregate().mutation()));
            inputs.requirePredecessor(prior, reader.aggregate().mutation());
            final byte[] rawFrame = frame(actual); final var edits = new ArrayList<TargetStoreBackend.Edit>();
            byte[] digest = prior.archiveRoot(); long rows = 0, bytes = 0;
            for (var entry : closedEntries(takeover, prior, decision, packet)) {
                TargetMigrationArchiveSemantics.requireEntry(entry, root.plan());
                final var next = row(root.plan(), prior.payer(), entry, TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY, 0, null, at, rawFrame);
                if (reader.projected(ColumnFamily.META, next.key(), edits) != null)
                    throw new IllegalStateException("closed-contract decision conflicts with an already retained original identity");
                appendImmutable(reader, edits, next); digest = TargetMigrationArchiveRequest.appendRoot(digest, entry);
                rows = Math.incrementExact(rows); bytes = Math.addExact(bytes, entry.rawValueLength());
            }
            final var next = prior.tail(digest, rows, bytes, at, rawFrame, inventory);
            edits.add(reader.replace(ColumnFamily.META, next.key(), TargetMigrationArchiveState.VALUE_TYPE, next.canonicalBytes()));
            return new TargetSourceAccounting(scope, lineage, actual.position(), at.mutationDigest(), limits.maximumCounters(), limits.maximumTargets(), limits.maximumDomains()).assemble(reader, edits);
        }));
        return new PreparedReplay(backend, batch, outcome, current);
    }
    public static SourceReplayOutcome commitClosed(PreparedReplay prepared, TargetStoreBackend.CommitAuthority authority) {
        return commitReplay(prepared, authority);
    }

    public static SourceReplayOutcome commitReplay(PreparedReplay prepared, TargetStoreBackend.CommitAuthority authority) {
        prepared.current.run(); prepared.backend.commit(prepared.batch, TargetMigrationStore.guarded(authority, prepared.current)); return prepared.outcome;
    }
    public static SourceReplayOutcome readPhysicalReplay(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            SourceReplayEntry actual, BoundedReadBudget budget, TargetStoreBackend.ReadAuthority reads) {
        final var row = read(backend, scope, lineage, KeyCodec.dedupePosition(actual.position().canonicalBytes()), budget, reads);
        if (row == null || row.sourceBranch() != TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY || !Arrays.equals(row.sourceFrame(), frame(actual)))
            throw new IllegalStateException("archive physical replay lost its exact already committed frame/result");
        final var value = TargetMigrationArchivePhysicalRecord.decode(TargetValueEnvelope.decode(row.entry().rawValue(), TargetMigrationArchiveRow.VALUE_TYPE).payload());
        return actual instanceof SourceReplayRecord ? SourceReplayOutcome.command(actual.position(), CommandResult.decode(value.outcome()))
                : SourceReplayOutcome.systemMutation(actual.position(), SystemMutationResult.decode(value.outcome()));
    }
    private static void requirePhysical(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, TargetMigrationArchiveRow row, TargetMigrationPlan plan) {
        final var physical = TargetMigrationArchivePhysicalRecord.decode(TargetValueEnvelope.decode(row.entry().rawValue(), TargetMigrationArchiveRow.VALUE_TYPE).payload());
        final var first = physical.firstOriginalKey() == null ? null : Objects.requireNonNull(decodeRow(view.projected(ColumnFamily.META,
                TargetMigrationArchiveRow.keyFor(physical.firstOriginalKey()), overlay)), "actual archived original logical first");
        requirePhysicalRow(row, first, plan, physical);
    }
    /** Actual archived physical first reference/frame/result; its independent Source/fence authority is retained by the new owner. */
    public static void requirePhysical(TargetCheckpointDependencies dependencies, TargetMigrationArchiveRow row) {
        if (row.sourceBranch() != TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY) return;
        final var physical = TargetMigrationArchivePhysicalRecord.decode(TargetValueEnvelope.decode(row.entry().rawValue(), TargetMigrationArchiveRow.VALUE_TYPE).payload());
        final var first = physical.firstOriginalKey() == null ? null : Objects.requireNonNull(dependencies.migrationArchive(physical.firstOriginalKey()),
                "archive physical occurrence lost its actual archived logical first");
        requirePhysicalRow(row, first, Objects.requireNonNull(dependencies.migrationRoot()).plan(), physical);
    }
    private static void requirePhysicalRow(TargetMigrationArchiveRow row, TargetMigrationArchiveRow first,
            TargetMigrationPlan plan, TargetMigrationArchivePhysicalRecord physical) {
        row.requirePlan(plan); TargetMigrationArchiveState.requireFrame(row.sourceFrame(), row.materializedAt());
        if (first != null) {
            first.requirePlan(plan);
            if (!Arrays.equals(physical.firstDigest(), first.digest())) throw new IllegalStateException("archive physical occurrence changed its exact logical first");
        } else if (physical.firstOriginalKey() != null || physical.firstDigest() != null)
            throw new IllegalStateException("archive physical-only occurrence cannot manufacture a logical first");
        if (!Arrays.equals(row.entry().originalKey(), KeyCodec.dedupePosition(row.materializedAt().source().canonicalBytes())))
            throw new IllegalStateException("archive physical occurrence changed actual Source position");
        if (physical.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var input = CommandCodec.decodeFrame(row.sourceFrame()); final var result = CommandResult.decode(physical.outcome());
            if (!Arrays.equals(result.appliedSourcePosition(), row.materializedAt().source().canonicalBytes())
                    || first != null && !Arrays.equals(physical.firstOriginalKey(), KeyCodec.dedupeCommand(input.commandId())))
                throw new IllegalStateException("archive physical Command identity/result changed");
            if (first != null) {
                final var expected = TargetMigrationArchiveSemantics.commandOutcome(first.entry(), input, row.materializedAt().source(), physical.closedIngressDeadlineThrough());
                if (result.applyStatus() != expected.applyStatus() || result.stableCode() != expected.stableCode() || result.generation() != expected.generation()
                        || result.stateVersion() != expected.stateVersion() || result.messageStatus() != expected.messageStatus())
                    throw new IllegalStateException("archive physical Command changed original decision");
            }
        } else {
            final var input = SystemMutation.decodeTargetFrame(row.sourceFrame()); final var result = SystemMutationResult.decode(physical.outcome());
            if (!Arrays.equals(result.appliedSourcePosition(), row.materializedAt().source().canonicalBytes()) || !Arrays.equals(result.mutationId(), input.systemMutationId())
                    || !Arrays.equals(result.mutationHash(), input.mutationHash()) || result.mutationType() != input.type()
                    || result.retryUntilEpochMs() != input.retryUntilEpochMs() || !Arrays.equals(result.authorIdentity(), input.authorIdentity())
                    || first != null && !Arrays.equals(physical.firstOriginalKey(), KeyCodec.dedupeSystemMutation(input.systemMutationId())))
                throw new IllegalStateException("archive physical System identity/result changed");
            if (first != null) {
                final var expected = TargetMigrationArchiveSemantics.systemOutcome(first.entry(), input, row.materializedAt().source(), physical.closedIngressDeadlineThrough());
                if (result.applyStatus() != expected.applyStatus() || result.stableCode() != expected.stableCode())
                    throw new IllegalStateException("archive physical System changed original decision");
            }
        }
    }
    private static byte[] frame(SourceReplayEntry actual) {
        return actual instanceof SourceReplayRecord command ? CommandCodec.encodeFrame(command.command()) : ((SourceReplayMutation) actual).mutation().encodeFrame();
    }
    private static byte[] sourceDigest(SourceReplayEntry actual) {
        return actual instanceof SourceReplayRecord command ? command.command().canonicalFrameDigest() : Bytes.sha256(((SourceReplayMutation) actual).mutation().canonicalEnvelope());
    }

    /** Backend-level permanent identity/history protection, including actual archive management first/POS. */
    public static void requireEdits(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits, TargetQuotaMutation at) {
        TargetMigrationRoot root = null; TargetMigrationRetirementRecord retirement = null;
        for (var edit : edits) {
            final boolean row = edit.family() == ColumnFamily.META && edit.key().length > 2 && edit.key()[0] == TargetKeyCodec.MIGRATION_ARCHIVE_TAG;
            final boolean state = edit.family() == ColumnFamily.META && TargetMigrationArchiveState.isKey(edit.key());
            if (row || state) {
                if (edit.after() == null || at.isLocalMutation()) throw new IllegalStateException("archive original identities/history are permanent Source facts");
                if (root == null) { root = Objects.requireNonNull(TargetMigrationStore.decodeRoot(reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId()))));
                    retirement = Objects.requireNonNull(TargetMigrationRetirementStore.decode(reader.get(ColumnFamily.META, TargetMigrationRetirementRecord.keyFor(root.plan().migrationId())))); }
                if (row) {
                    final var next = decodeRow(edit.after()); next.requirePlan(root.plan());
                    if (edit.before() != null || !next.materializedAt().equals(at) || !Arrays.equals(next.key(), edit.key())) throw new IllegalStateException("archive original namespace first cannot be overwritten");
                } else {
                    final var next = decodeState(edit.after()); next.requireRetirement(root, retirement);
                    if (!next.mutation().equals(at) || !Arrays.equals(next.key(), edit.key())) throw new IllegalStateException("archive progress changed actual Source/key");
                    if (next.history()) { if (edit.before() != null) throw new IllegalStateException("archive management history is immutable"); }
                    else {
                        final var prior = decodeState(edit.before());
                        if (prior == null || next.generation() != prior.generation()) {
                            final var step = Objects.requireNonNull(decodeState(reader.projected(ColumnFamily.META,
                                    TargetMigrationArchiveState.historyKey(next.migrationId(), at.sequence()), edits)), "actual same-batch archive management history");
                            if (!step.head().equals(next) || prior == null && step.request().expectedStateDigest() != null
                                    || prior != null && (!Arrays.equals(step.request().expectedStateDigest(), prior.digest()) || prior.taken()))
                                throw new IllegalStateException("archive management changed exact previous Source head");
                        } else {
                            if (!next.managementAt().equals(prior.managementAt()) || !Arrays.equals(next.managementDigest(), prior.managementDigest())
                                    || next.taken() != prior.taken() || !next.coverage().equals(prior.coverage())) throw new IllegalStateException("archive tail changed its original management/base coverage");
                            byte[] digest = prior.archiveRoot(); long count = 0, bytes = 0;
                            final var added = edits.stream().filter(e -> e.family() == ColumnFamily.META && e.key().length > 2 && e.key()[0] == TargetKeyCodec.MIGRATION_ARCHIVE_TAG && e.before() == null && e.after() != null)
                                    .map(e -> decodeRow(e.after())).sorted((a,b) -> Arrays.compareUnsigned(a.entry().originalKey(), b.entry().originalKey())).toList();
                            for (var addedRow : added) { digest = TargetMigrationArchiveRequest.appendRoot(digest, addedRow.entry()); count++; bytes = Math.addExact(bytes, addedRow.entry().rawValue().length); }
                            if (!Arrays.equals(digest, next.archiveRoot()) || next.tailRows() != Math.addExact(prior.tailRows(), count)
                                    || next.tailRawBytes() != Math.addExact(prior.tailRawBytes(), bytes)) throw new IllegalStateException("archive actual Source tail root/count/bytes changed");
                        }
                    }
                }
            }
            if (edit.family() == ColumnFamily.DEDUPE && edit.before() != null && !Arrays.equals(edit.before(), edit.after())) {
                final var envelope = TargetValueEnvelope.decodeAny(edit.before());
                if (envelope.valueType() == TargetResultRecord.VALUE_TYPE) {
                    final var record = TargetResultRecord.decode(envelope.payload());
                    final var migration = TargetMigrationStore.decodeRoot(reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId())));
                    if (migration != null) {
                        final var history = decodeState(reader.get(ColumnFamily.META, TargetMigrationArchiveState.historyKey(migration.plan().migrationId(), record.mutation().sequence())));
                        if (history != null && history.history() && (Arrays.equals(edit.key(), Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG,1}, history.input().systemMutationId()))
                                || Arrays.equals(edit.key(), Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG,1}, history.managementAt().source().canonicalBytes()))))
                            throw new IllegalStateException("archive management retains its original actual first/POS forever");
                    }
                }
            }
        }
    }
    /** Structural cold reconstruction; independent original material authentication remains with the new owner. */
    public static void requireCheckpoint(TargetCheckpointDependencies dependencies) {
        final var cursor = new RetirementCursor(dependencies);
        while (!cursor.advance(256, () -> {}, ignored -> {}, ignored -> {})) { }
    }

    /** Same complete archive fold for protected multi-turn preparation; construction does not copy or sort the inventory. */
    public static final class RetirementCursor {
        private final TargetCheckpointDependencies dependencies;
        private final TargetMigrationRoot root;
        private final TargetMigrationRetirementRecord retirement;
        private final java.util.Iterator<TargetMigrationArchiveState> states;
        private final java.util.Iterator<TargetMigrationArchiveRow> entries;
        private final java.util.NavigableMap<Long,TargetMigrationArchiveState> steps = new java.util.TreeMap<>(Long::compareUnsigned);
        private final java.util.NavigableMap<byte[],TargetMigrationArchiveRow> rows = new java.util.TreeMap<>(Arrays::compareUnsigned);
        private final java.util.NavigableMap<Long,java.util.NavigableMap<byte[],TargetMigrationArchiveRow>> tails = new java.util.TreeMap<>(Long::compareUnsigned);
        private TargetMigrationArchiveState head, reverseStep, eventStep, latest;
        private TargetMigrationArchiveRequest reverseRequest, eventRequest;
        private java.util.Iterator<TargetMigrationArchiveState> reverse;
        private int reverseIndex, eventIndex, phase;
        private java.util.Iterator<java.util.Map.Entry<Long,TargetMigrationArchiveState>> stepEvents;
        private java.util.Iterator<java.util.Map.Entry<Long,java.util.NavigableMap<byte[],TargetMigrationArchiveRow>>> tailEvents;
        private java.util.Map.Entry<Long,TargetMigrationArchiveState> nextStep;
        private java.util.Map.Entry<Long,java.util.NavigableMap<byte[],TargetMigrationArchiveRow>> nextTail;
        private java.util.Iterator<TargetMigrationArchiveRow> eventTails;
        private byte[] digest;
        private long generation, tailRows, tailBytes;
        private boolean any;
        public RetirementCursor(TargetCheckpointDependencies dependencies) {
            this.dependencies = dependencies;
            root = dependencies.migrationRoot(); retirement = dependencies.migrationRetirement();
            states = dependencies.migrationArchiveStateValues().iterator(); entries = dependencies.migrationArchiveEntryValues().iterator();
        }
        public boolean advance(int maximum, Runnable permit, java.util.function.LongConsumer retained, java.util.function.LongConsumer copied) {
            if (maximum < 1) throw new IllegalArgumentException("archive cursor turn must be bounded and positive");
            for (int work = 0; work < maximum; work++) {
                permit.run();
                if (phase == 0) {
                    if (!states.hasNext()) { phase = 1; continue; }
                    requireRoot(); any = true;
                    final var state = states.next(); state.requireRetirement(root, retirement);
                    if (state.history()) {
                        if (steps.put(state.managementAt().sequence(), state) != null) throw new IllegalStateException("duplicate actual archive management Source");
                        retained.accept(64);
                    } else if (head != null) throw new IllegalStateException("formal archive has multiple heads"); else head = state;
                } else if (phase == 1) {
                    if (!entries.hasNext()) { reverse = steps.values().iterator(); phase = 2; continue; }
                    requireRoot(); any = true;
                    final var row = entries.next(); row.requirePlan(root.plan()); TargetMigrationArchiveSemantics.requireEntry(row.entry(), root.plan());
                    requirePhysical(dependencies, row);
                    final byte[] key = row.key(); copied.accept(key.length); retained.accept(Math.addExact(key.length, 64));
                    if (rows.put(key,row) != null) throw new IllegalStateException("duplicate archive original namespace/key");
                    if (row.sourceBranch() == TargetMigrationArchiveRow.SourceBranch.COPY_BATCH) {
                        final var step = steps.get(row.materializedAt().sequence());
                        if (step == null || !step.history() || step.generation() != row.managementGeneration()
                                || !step.managementAt().equals(row.materializedAt()) || !Arrays.equals(step.managementDigest(), row.managementDigest()))
                            throw new IllegalStateException("archive row lost its original actual COPY management first");
                        final var request = step.request();
                        if (request.copyBatch() == null || !request.copyBatch().rows().contains(row.entry())) throw new IllegalStateException("archive row is outside original COPY statement");
                    } else {
                        var group = tails.get(row.materializedAt().sequence());
                        if (group == null) { group = new java.util.TreeMap<>(Arrays::compareUnsigned); tails.put(row.materializedAt().sequence(),group); retained.accept(64); }
                        group.put(key,row); retained.accept(48);
                    }
                } else if (phase == 2) {
                    if (reverseStep == null) {
                        if (!reverse.hasNext()) {
                            if (!any) { phase = 5; return true; }
                            if (head == null || steps.isEmpty()) throw new IllegalStateException("formal archive lacks unique head/actual management history");
                            digest = TargetMigrationArchiveRequest.genesisRoot(retirement.intention().digest());
                            stepEvents = steps.entrySet().iterator(); tailEvents = tails.entrySet().iterator();
                            nextStep = stepEvents.hasNext() ? stepEvents.next() : null;
                            nextTail = tailEvents.hasNext() ? tailEvents.next() : null;
                            phase = 3; continue;
                        }
                        reverseStep = reverse.next(); reverseRequest = reverseStep.request(); reverseIndex = 0;
                        if (reverseStep.inventory() == null) throw new IllegalStateException("archive management lost exact received inventory reference");
                        if (reverseRequest.copyBatch() == null) { reverseStep = null; reverseRequest = null; continue; }
                    }
                    final var requested = reverseRequest.copyBatch().rows();
                    if (reverseIndex == requested.size()) { reverseStep = null; reverseRequest = null; continue; }
                    final var entry = requested.get(reverseIndex++);
                    final byte[] key = TargetMigrationArchiveRow.keyFor(entry.originalKey()); copied.accept(key.length);
                    final var actual = Objects.requireNonNull(rows.get(key), "COPY statement lost its actual Native72 first");
                    TargetMigrationArchiveSemantics.requireEquivalent(actual.entry(), entry);
                    actual.materializedAt().requireAtOrBefore(reverseStep.managementAt());
                    if (!actual.payer().identity().equals(reverseStep.payer().identity()) || !actual.payer().accounting().equals(reverseStep.payer().accounting())
                            || !actual.payer().allocation().equals(reverseStep.payer().allocation()))
                        throw new IllegalStateException("COPY statement changed actual Native72 payer");
                } else if (phase == 3) {
                    if (eventStep != null) {
                        final var batch = eventRequest.copyBatch();
                        if (batch != null && eventIndex < batch.rows().size()) {
                            final var entry = batch.rows().get(eventIndex++); digest = TargetMigrationArchiveRequest.appendRoot(digest, entry); continue;
                        }
                        if (!Arrays.equals(eventStep.archiveRoot(), digest) || eventStep.tailRows() != tailRows || eventStep.tailRawBytes() != tailBytes)
                            throw new IllegalStateException("formal archive management lost actual root/tail count/bytes");
                        generation = eventStep.generation(); latest = eventStep; eventStep = null; eventRequest = null; continue;
                    }
                    if (eventTails != null) {
                        if (eventTails.hasNext()) {
                            final var row = eventTails.next(); digest = TargetMigrationArchiveRequest.appendRoot(digest, row.entry());
                            tailRows = Math.incrementExact(tailRows); tailBytes = Math.addExact(tailBytes, row.entry().rawValueLength()); continue;
                        }
                        eventTails = null; continue;
                    }
                    if (nextStep == null && nextTail == null) { phase = 4; continue; }
                    if (nextStep != null && (nextTail == null || Long.compareUnsigned(nextStep.getKey(), nextTail.getKey()) < 0)) {
                        eventStep = nextStep.getValue(); eventRequest = eventStep.request(); eventIndex = 0;
                        if (eventStep.generation() != TargetQuotaMutation.increment(generation) || !Arrays.equals(eventRequest.expectedArchiveRoot(), digest)
                                || latest != null && latest.taken()) throw new IllegalStateException("formal archive management changed original progress/root");
                        nextStep = stepEvents.hasNext() ? stepEvents.next() : null;
                    } else {
                        if (nextStep != null && nextStep.getKey().equals(nextTail.getKey())) throw new IllegalStateException("one Source event cannot be both COPY management and an unrelated archive tail");
                        eventTails = nextTail.getValue().values().iterator(); nextTail = tailEvents.hasNext() ? tailEvents.next() : null;
                    }
                } else if (phase == 4) {
                    if (latest == null || head.generation() != generation || !head.managementAt().equals(latest.managementAt()) || head.taken() != latest.taken()
                            || !Arrays.equals(head.archiveRoot(),digest) || head.tailRows()!=tailRows || head.tailRawBytes()!=tailBytes
                            || !head.coverage().equals(latest.coverage()) || head.inventory() == null)
                        throw new IllegalStateException("formal archive head does not reconstruct from actual management/tail rows");
                    phase = 5; return true;
                } else return true;
            }
            return phase == 5;
        }
        private void requireRoot() {
            if (root == null || retirement == null) throw new IllegalStateException("formal archive lost original migration/retirement root");
        }
    }
}
