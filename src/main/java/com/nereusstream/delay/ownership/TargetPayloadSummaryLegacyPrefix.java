package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetMigrationControlBody;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationSettlementRequest;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetPayloadObligationSummaryBootstrap;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** One protected finite old Target Source prefix. Neither capture nor a v1 permit initializes a summary. */
public final class TargetPayloadSummaryLegacyPrefix implements CheckpointInputFuture.Cleanup, AutoCloseable {
    public sealed interface Opening permits TargetWorkerSourceStartup.PayloadSummaryPrefixOpening {
        TargetWorkerProcessRuntime.Assembly graph();

        ShardStore store();

        TargetQuotaScope scope();

        byte[] lineage();

        SourceAssignment assignment();

        OwnerLease ownerLease();

        TargetQuotaIncarnation root();

        Limits limits();

        LongSupplier nanos();

        LongSupplier clock();

        void requireCurrent();

        void requireLocalCurrent();

        void retainCleanup(CheckpointInputFuture.Cleanup original);
    }

    @FunctionalInterface
    public interface Provider {
        Hold acquire(Opening original);
    }
    /** Real old-writer/management/Owner/Source/retention acquisition; complete Native folds are executed below. */
    public interface Hold extends CheckpointInputFuture.Cleanup, AutoCloseable {
        long retainedBytes();

        void requireFrozen(Opening original, SourcePosition actualC);

        void requireRange(Opening original, SourcePosition actualC, SourcePosition actualR);

        void requireOriginal(SourceReplayEntry original, TargetQuotaMutation actualBefore);

        TargetPayloadObligationSummaryBootstrap.Protection eventImage(
                SourceReplayEntry original, SourcePosition actualCut);
        /** Releases only this event's original local image/read acquisition; durable history/retention remains held. */
        void releaseEvent(TargetPayloadObligationSummaryBootstrap.Protection original);
        /**
         * All non-Native legacy/external responsibilities, under their original payer and exact retained protection.
         */
        default boolean closedLegacyRetain(TargetQuotaPayloadOwner owner, SourceReplayEntry event) {
            return false;
        }

        void requireCurrent();

        void requireLocalCurrent();

        void requestStop();

        boolean ioExited();

        @Override
        void close();
    }

    public record Limits(
            long maximumRetainedBytes,
            int maximumReadRecords,
            long maximumReadBytes,
            long maximumElapsedNanos,
            int rowsPerTurn,
            long taskBytes,
            TargetPayloadObligationSummaryBootstrap.Limits fold) {
        public Limits {
            Objects.requireNonNull(fold);
            if (maximumRetainedBytes < 1
                    || maximumRetainedBytes == Long.MAX_VALUE
                    || maximumReadRecords < 1
                    || maximumReadRecords == Integer.MAX_VALUE
                    || maximumReadBytes < 1
                    || maximumReadBytes == Long.MAX_VALUE
                    || maximumElapsedNanos < 1
                    || maximumElapsedNanos == Long.MAX_VALUE
                    || rowsPerTurn < 1
                    || rowsPerTurn > fold.maximumRecords()
                    || taskBytes < 1
                    || taskBytes > maximumRetainedBytes) {
                throw new IllegalArgumentException(
                        "finite original legacy-prefix frame/fold/retained/time bounds required");
            }
        }
    }

    private final Opening opening;
    private final ShardStore store;
    private final java.nio.file.Path physicalStore;
    private final StoreMetadata storeIdentity;
    private final TargetQuotaScope originalScope;
    private final byte[] originalLineage, originalRootIdentity;
    private final Limits limits;
    /** Deliberately shared across capture and every event: a new page/event cannot reset the paid total window. */
    private final BoundedReadBudget budget;

    private final SourcePosition c;
    private final OwnerLease originalOwner;
    private Hold hold;
    private CheckpointInputFuture.Cleanup failedAcquisition;
    private long turns;
    private TargetWorkerRecoverySource originalReader;
    private TargetWorkerRecoverySource replayReader;
    private SourceReplayCursor<SourceReplayEntry> captureCursor;
    private SourceReplaySuccessor captureSuccessor;
    private SourcePosition delivered, r;
    private QualifiedSource source;
    private boolean tailCaptured, stopping, closed;
    private Throwable failure;
    private WorkClassTask queued;
    private OwnedCheckpointIo<Boolean> io;
    private OwnedCheckpointIo<Boolean> cleanup;
    private TargetSourceApplyRuntime runtime;
    private int applied;
    private SourceReplayEntry selected;
    private SourcePosition completedAt;
    private byte[] completedDigest;
    private TargetPayloadObligationSummaryBootstrap.Protection protection;
    private TargetPayloadObligationSummaryBootstrap.Protection staleProtection;
    private TargetPayloadObligationSummaryBootstrap.Cursor foldCursor;
    private EventPermit permit;
    private final CompletableFuture<Void> cleaned = new CompletableFuture<>();

    private TargetPayloadSummaryLegacyPrefix(Opening opening, Limits limits, BoundedReadBudget carried) {
        this.opening = Objects.requireNonNull(opening);
        this.store = opening.store();
        this.limits = Objects.requireNonNull(limits);
        physicalStore = store.dbPath().toAbsolutePath().normalize();
        storeIdentity = store.metadata();
        originalScope = opening.scope();
        originalLineage = Bytes.copy(opening.lineage());
        originalRootIdentity = opening.root().identity().canonicalBytes();
        if (!limits.equals(opening.limits())) {
            throw new IllegalArgumentException("prefix changed its original Process-reserved limits");
        }
        c = Objects.requireNonNull(store.appliedShardLogPosition());
        delivered = c;
        originalOwner = opening.ownerLease();
        budget = carried == null
                ? new BoundedReadBudget(
                        limits.maximumReadRecords(),
                        limits.maximumReadBytes(),
                        limits.maximumElapsedNanos(),
                        opening.nanos())
                : carried;
        if (limits.taskBytes() < opening.graph().config().maximumSourceApplyTaskBytes()) {
            throw new IllegalArgumentException("legacy prefix omits the original decoded Source task envelope");
        }
    }
    /** Runs on the root's already reserved Owned I/O; retain before any external acquisition. */
    public static TargetPayloadSummaryLegacyPrefix open(Opening opening, Provider provider, Limits limits) {
        return acquire(opening, provider, limits, null);
    }
    /**
     * Another genuinely acquired finite range carries the original cumulative spent/elapsed window across Owner
     * changes.
     */
    public static TargetPayloadSummaryLegacyPrefix openNext(
            Opening opening, Provider provider, Limits limits, TargetPayloadSummaryLegacyPrefix previous) {
        Objects.requireNonNull(previous);
        final var actualC = opening.store().appliedShardLogPosition();
        if (!previous.cleanupComplete()
                || !previous.closed
                || previous.r == null
                || !limits.equals(previous.limits)
                || !opening.store().dbPath().toAbsolutePath().normalize().equals(previous.physicalStore)
                || !Arrays.equals(opening.store().metadata().encode(), previous.storeIdentity.encode())
                || !opening.scope().equals(previous.originalScope)
                || !Arrays.equals(opening.lineage(), previous.originalLineage)
                || !Arrays.equals(opening.root().identity().canonicalBytes(), previous.originalRootIdentity)
                || !(same(previous.c, previous.r) || same(previous.completedAt, previous.r))
                || !actualC.sameSourceIdentity(previous.r)
                || actualC.compareTo(previous.r) < 0) {
            throw new IllegalStateException(("new old-prefix range lacks the previous actual exit/apply and original "
                    + "Store/cumulative allocation"));
        }
        return acquire(opening, provider, limits, previous.budget);
    }

    private static TargetPayloadSummaryLegacyPrefix acquire(
            Opening opening, Provider provider, Limits limits, BoundedReadBudget carried) {
        opening.requireCurrent();
        final var result = new TargetPayloadSummaryLegacyPrefix(opening, limits, carried);
        opening.retainCleanup(result);
        try {
            result.hold = Objects.requireNonNull(provider.acquire(opening));
            if (result.hold.retainedBytes() < 0 || result.hold.retainedBytes() > limits.maximumRetainedBytes()) {
                throw new IllegalStateException("original prefix hold exceeds its reserved Process input window");
            }
            result.hold.requireFrozen(opening, result.c);
            result.requireCurrent();
            return result;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof CheckpointInputFuture.Cleanup owned) {
                result.failedAcquisition = owned;
            }
            result.failure = failure;
            result.stopping = true;
            throw failure;
        }
    }

    public boolean capture(TargetWorkerRecoverySource reader) {
        requireCurrent();
        if (r != null && originalReader == reader && reader.cleanupComplete()) {
            poll();
            return io == null && queued == null;
        }
        if (originalReader == null) {
            reader.requireStartup(
                    opening.assignment(),
                    opening.graph().config().maximumSourceFrameBytes(),
                    opening.graph().classes(),
                    opening.graph().resources());
            reader.requireReaderBinding(opening.assignment(), c);
            originalReader = reader;
            captureCursor = reader.replayCursor();
            captureSuccessor = reader.successorAfter(c);
        } else if (originalReader != reader) {
            throw new IllegalStateException("prefix changed its actual captured SDK reader");
        }
        advance(() -> {
            if (r != null) {
                reader.close();
                return;
            }
            if (!tailCaptured) {
                reader.captureReplayTail();
                tailCaptured = true;
            }
            if (captureCursor.hasNext()) {
                final var entry = captureCursor.next();
                reader.requireReaderEntry(entry, opening.assignment(), c);
                captureSuccessor.validate(delivered, entry.position());
                final byte[] raw = frame(entry);
                if (!budget.tryCharge(entry.position().canonicalBytes().length, raw.length)) {
                    throw new IllegalStateException(
                            "captured old prefix exceeds its original cumulative frame/read budget");
                }
                delivered = entry.position();
            } else {
                final var through = reader.capturedTailReadThrough();
                if (through.isEmpty()) {
                    if (!budget.beforeTimedWork()) {
                        throw budget.incomplete();
                    }
                    return;
                }
                final var actual = through.orElseThrow();
                if (!same(actual, delivered)) {
                    throw new SourceReplayGapException("captured prefix lost the exact native final Source frame");
                }
                hold.requireRange(opening, c, actual);
                r = actual;
            }
        });
        return r != null && reader.cleanupComplete() && io == null && queued == null;
    }

    public boolean captureReaderClosed(TargetWorkerRecoverySource actual) {
        return actual == originalReader && r != null && actual.cleanupComplete() && io == null && queued == null;
    }

    public TargetWorkerRecoverySource source(TargetWorkerRecoverySource actual) {
        requireCurrent();
        if (r == null
                || originalReader == null
                || !originalReader.cleanupComplete()
                || io != null
                || queued != null
                || actual == originalReader) {
            throw new IllegalStateException(
                    "second actual C reader requires complete capture and first native reader exit");
        }
        if (replayReader == null) {
            actual.requireStartup(
                    opening.assignment(),
                    opening.graph().config().maximumSourceFrameBytes(),
                    opening.graph().classes(),
                    opening.graph().resources());
            actual.requireReaderBinding(opening.assignment(), c);
            replayReader = actual;
            source = new QualifiedSource();
        } else if (replayReader != actual) {
            throw new IllegalStateException("finite prefix changed its second actual reader");
        }
        return source;
    }

    public SourcePosition start() {
        return c;
    }

    public SourcePosition through() {
        return Objects.requireNonNull(r, "actual captured R");
    }

    public boolean isSource(TargetWorkerRecoverySource original, TargetWorkerRecoverySource qualified) {
        return replayReader == original && source != null && source == qualified;
    }

    void bind(
            TargetSourceApplyRuntime actual,
            ShardStore store,
            TargetQuotaScope scope,
            byte[] lineage,
            SourceAssignment assignment,
            OwnerLease owner) {
        requireCurrent();
        if (runtime != null && runtime != actual
                || store != store
                || !scope.equals(opening.scope())
                || !Arrays.equals(lineage, opening.lineage())
                || !assignment.sameIdentity(opening.assignment())
                || !owner.sameIdentity(originalOwner)
                || !same(store.appliedShardLogPosition(), c)) {
            throw new IllegalStateException("legacy prefix changed its original Process/Store/Root/lineage/Owner/C");
        }
        runtime = actual;
    }

    boolean prepare(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireRuntime(actual);
        requireExpected(entry);
        if (selected != null && !SourceApplyCoordinator.sameEntry(selected, entry)) {
            throw new IllegalStateException("prefix retains another exact event");
        }
        selected = entry;
        advance(() -> {
            if (staleProtection != null) {
                hold.releaseEvent(staleProtection);
                staleProtection = null;
                return;
            }
            if (permit != null) {
                permit.requireOutside();
                return;
            }
            if (protection == null) {
                protection = Objects.requireNonNull(hold.eventImage(entry, store.appliedShardLogPosition()));
                protection.requireCurrent();
                final var dependencies = protection.image().payloadSummaryDependencies();
                final var read = store.readWithBudget(budget, () -> {
                    requireLocal();
                    final var root = dependencies.root();
                    final byte[] aggregate =
                            store.get(ColumnFamily.META, root.aggregate().key());
                    if (!root.metadata().equals(store.metadata())
                            || !Arrays.equals(
                                    root.root().canonicalBytes(), opening.root().canonicalBytes())
                            || !same(root.source(), store.appliedShardLogPosition())
                            || root.mutationSequence() != store.shardMutationSequence()
                            || !Arrays.equals(
                                    aggregate,
                                    TargetValueEnvelope.encode(
                                            TargetQuotaAggregate.VALUE_TYPE,
                                            root.aggregate().canonicalBytes()))) {
                        throw new IllegalStateException(
                                "event image differs from the exact actual Native view/accounting cut");
                    }
                    hold.requireOriginal(entry, root.aggregate().mutation());
                    return owner(entry, dependencies);
                });
                final var owner = read.value();
                if (owner == null || owner.summaryQualified()) {
                    permit = new EventPermit(
                            this,
                            entry,
                            read.view(),
                            owner,
                            null,
                            dependencies.root().aggregate().mutation());
                    return;
                }
                foldCursor =
                        TargetPayloadObligationSummaryBootstrap.begin(store, owner, protection, limits.fold(), budget);
            }
            final var complete = foldCursor.advance(budget, limits.rowsPerTurn());
            if (complete.isPresent()) {
                final var fold = complete.orElseThrow();
                fold.requireCurrent();
                permit = new EventPermit(this, entry, null, fold.owner(), fold, fold.cut());
            }
        });
        return permit != null && io == null && queued == null;
    }

    EventPermit permit(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        requireRuntime(actual);
        requireExpected(entry);
        if (permit == null || io != null || queued != null) {
            throw new TargetSourceAuthorityPreparation.Required();
        }
        return permit;
    }

    boolean complete(TargetSourceApplyRuntime actual, SourceReplayEntry entry) {
        if (same(completedAt, entry.position()) && Arrays.equals(completedDigest, eventDigest(entry))) {
            poll();
            return io == null && queued == null;
        }
        requireRuntime(actual);
        requireExpected(entry);
        if (permit == null) {
            return false;
        }
        advance(() -> store.readWithBudget(budget, () -> {
            requireLocal();
            final var key = TargetQuotaAggregate.genesis(
                            opening.scope().shard(), opening.root().identity().accountingIncarnation())
                    .key();
            final var aggregate = TargetQuotaAggregate.decode(
                    TargetValueEnvelope.decode(store.get(ColumnFamily.META, key), TargetQuotaAggregate.VALUE_TYPE)
                            .payload());
            if (!same(store.appliedShardLogPosition(), entry.position())
                    || !same(aggregate.mutation().source(), entry.position())
                    || aggregate.mutation().sequence() != TargetQuotaMutation.increment(permit.cut.sequence())
                    || !Arrays.equals(aggregate.mutation().mutationDigest(), eventDigest(entry))) {
                throw new IllegalStateException("old prefix event has no actual committed Source/accounting successor");
            }
            return true;
        }));
        if (io != null || queued != null) {
            return false;
        }
        advance(() -> {
            hold.releaseEvent(protection);
            applied++;
            completedAt = entry.position();
            completedDigest = eventDigest(entry);
            selected = null;
            protection = null;
            foldCursor = null;
            permit = null;
        });
        return same(completedAt, entry.position())
                && Arrays.equals(completedDigest, eventDigest(entry))
                && io == null
                && queued == null;
    }

    private static TargetQuotaPayloadOwner owner(SourceReplayEntry entry, TargetCheckpointDependencies dependencies) {
        DelayMessageId id = null;
        if (entry instanceof SourceReplayRecord command) {
            id = command.command().delayMessageId();
        } else {
            final var input = ((SourceReplayMutation) entry).mutation();
            if (input.type() == SystemMutationType.TARGET_PUBLISH_ADMISSION) {
                id = TargetPublishAdmissionBody.decode(input.canonicalBody())
                        .locator()
                        .messageId();
            } else if (input.type() == SystemMutationType.EXPIRE_GENERATION) {
                id = TargetExpireGenerationBody.decode(input.canonicalBody()).messageId();
            } else if (input.type() == SystemMutationType.PUBLISH_OUTCOME
                    || input.type() == SystemMutationType.EVIDENCE_RESOLUTION) {
                final var attempt =
                        PublishOutcomeBody.decode(input.canonicalBody()).publishAttemptId();
                for (var admission : dependencies.admissions()) {
                    if (Arrays.equals(admission.attempt().publishAttemptId(), attempt)) {
                        id = admission.attempt().locator().messageId();
                    }
                }
            } else if (TargetMigrationControlBody.isControl(input)
                    && TargetMigrationControlBody.decode(input.canonicalBody()).request()
                            instanceof TargetMigrationSettlementRequest settlement) {
                id = settlement.messageId();
            }
        }
        return id == null ? null : dependencies.payloadOwner(id);
    }

    private void advance(Runnable step) {
        requireCurrent();
        if (!budget.beforeTimedWork()) {
            throw new IllegalStateException("original prefix cumulative time window exhausted");
        }
        if (io != null) {
            io.retryCleanup();
            if (!io.operationExited() || !io.ioExited()) {
                return;
            }
            if (CheckpointInputFuture.ownedFailure(io.receipt())
                    instanceof TargetPayloadObligationSummaryBootstrap.ViewChanged) {
                staleProtection = protection;
                protection = null;
                foldCursor = null;
                permit = null;
                io = null;
                if (queued != null && opening.graph().classes().state(queued).isEmpty()) {
                    queued = null;
                }
                return;
            }
            try {
                io.receipt().join();
            } catch (RuntimeException failure) {
                this.failure = failure;
                stopping = true;
                throw failure;
            }
            io = null;
        }
        if (queued != null) {
            if (opening.graph().classes().state(queued).isPresent()) {
                return;
            }
            queued = null;
            return;
        }
        queued = new WorkClassTask(
                WorkClass.CHECKPOINT,
                "legacy-v1-prefix/" + Bytes.hex(opening.root().digest()) + '/' + ++turns,
                limits.taskBytes());
        opening.graph()
                .classes()
                .submit(
                        queued,
                        () -> io = OwnedCheckpointIo.submit(
                                ignored -> {
                                    requireCurrent();
                                    step.run();
                                    requireCurrent();
                                    return true;
                                },
                                this::requireCurrent,
                                opening.graph().io()));
    }

    private void poll() {
        if (io != null) {
            io.retryCleanup();
            if (!io.operationExited() || !io.ioExited()) {
                return;
            }
            io.receipt().join();
            io = null;
        }
        if (queued != null && opening.graph().classes().state(queued).isEmpty()) {
            queued = null;
        }
    }

    private void requireExpected(SourceReplayEntry entry) {
        if (source == null
                || source.current == null
                || !SourceApplyCoordinator.sameEntry(source.current, entry)
                || entry.position().compareTo(c) <= 0
                || entry.position().compareTo(r) > 0) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "event is outside its captured finite old prefix; reacquire original authority/range");
        }
    }

    private void requireRuntime(TargetSourceApplyRuntime actual) {
        if (runtime != actual) {
            throw new IllegalStateException("prefix belongs to another actual recovery runtime");
        }
        requireLocal();
    }

    private void requireCurrent() {
        if (stopping || failure != null) {
            throw new IllegalStateException("original legacy prefix is stopped", failure);
        }
        opening.requireCurrent();
        if (hold != null) {
            hold.requireCurrent();
        }
    }

    private void requireLocal() {
        if (stopping || failure != null) {
            throw new IllegalStateException("original legacy prefix is stopped", failure);
        }
        if (runtime == null) {
            opening.requireLocalCurrent();
        } else {
            runtime.requirePayloadSummaryPrefixLocal(
                    this, originalOwner, opening.clock().getAsLong());
        }
        hold.requireLocalCurrent();
    }

    private static boolean same(SourcePosition left, SourcePosition right) {
        return left != null && right != null && Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static byte[] frame(SourceReplayEntry entry) {
        return entry instanceof SourceReplayRecord command
                ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    private static byte[] eventDigest(SourceReplayEntry entry) {
        return entry instanceof SourceReplayRecord command
                ? command.command().canonicalFrameDigest()
                : Bytes.sha256(((SourceReplayMutation) entry).mutation().canonicalEnvelope());
    }

    /** Minted only after the exact event's complete actual Native fold. It never grants v2 or SDK send. */
    public static final class EventPermit {
        private final TargetPayloadSummaryLegacyPrefix prefix;
        private final SourceReplayEntry entry;
        private final ShardStore.ReadView view;
        private final TargetQuotaPayloadOwner owner;
        private final TargetPayloadObligationSummaryBootstrap.Fold fold;
        private final TargetQuotaMutation cut;

        private EventPermit(
                TargetPayloadSummaryLegacyPrefix prefix,
                SourceReplayEntry entry,
                ShardStore.ReadView view,
                TargetQuotaPayloadOwner owner,
                TargetPayloadObligationSummaryBootstrap.Fold fold,
                TargetQuotaMutation cut) {
            this.prefix = prefix;
            this.entry = entry;
            this.view = view;
            this.owner = owner;
            this.fold = fold;
            this.cut = cut;
        }

        private void requireOutside() {
            prefix.requireCurrent();
            if (fold != null) {
                fold.requireCurrent();
            } else {
                prefix.store.requirePayloadSummaryReadView(view);
            }
        }

        public void requireCurrent(TargetStoreBackend.Reader reader) {
            prefix.requireLocal();
            prefix.requireExpected(entry);
            if (prefix.permit != this
                    || !reader.metadata().equals(prefix.store.metadata())
                    || !reader.aggregate().mutation().equals(cut)) {
                throw new IllegalStateException("old prefix event lost its actual Store/root/accounting cut");
            }
            if (fold != null) {
                fold.requireEventView(reader);
            } else {
                prefix.store.requirePayloadSummaryReadView(view);
            }
        }

        public void requireCurrent(TargetStoreBackend.Reader reader, SystemMutation input, SourcePosition source) {
            requireCurrent(reader);
            if (!(entry instanceof SourceReplayMutation original)
                    || !same(source, entry.position())
                    || !Arrays.equals(
                            input.canonicalEnvelope(), original.mutation().canonicalEnvelope())) {
                throw new IllegalStateException("v1 permit changed its exact original System event");
            }
        }

        public void requireCurrent(TargetStoreBackend.Reader reader, PreparedCommand input, SourcePosition source) {
            requireCurrent(reader);
            if (!(entry instanceof SourceReplayRecord original)
                    || !same(source, entry.position())
                    || !Arrays.equals(
                            input.canonicalFrameDigest(), original.command().canonicalFrameDigest())) {
                throw new IllegalStateException("v1 permit changed its exact original Command event");
            }
        }

        public void requireSource(TargetStoreBackend.Reader reader, TargetQuotaMutation stamp) {
            requireCurrent(reader);
            stamp.requireStoreSuccessorOf(cut);
            if (!same(stamp.source(), entry.position()) || !Arrays.equals(stamp.mutationDigest(), eventDigest(entry))) {
                throw new IllegalStateException("v1 Source accounting changed its exact retained event/digest");
            }
        }

        public boolean noUnresolvedAfter(
                TargetStoreBackend.Reader reader,
                TargetQuotaPayloadOwner selected,
                List<TargetStoreBackend.Edit> edits) {
            requireCurrent(reader);
            requireOwner(selected);
            if (selected.summaryQualified()) {
                return selected.unresolvedCount() == 0;
            }
            long count = fold == null ? 0 : fold.unresolvedCount();
            for (var edit : edits) {
                if (edit.family() == ColumnFamily.META) {
                    final var before = attempt(edit.before());
                    final var after = attempt(edit.after());
                    if (before != null
                            && before.locator().messageId().equals(selected.messageId())
                            && unresolved(before)) {
                        count = Math.subtractExact(count, 1);
                    }
                    if (after != null
                            && after.locator().messageId().equals(selected.messageId())
                            && unresolved(after)) {
                        count = Math.addExact(count, 1);
                    }
                }
            }
            if (count < 0) {
                throw new IllegalStateException("legacy fold/actual Budget overlay underflow");
            }
            return count == 0;
        }

        public boolean noLegacyCustodyAfter(
                TargetStoreBackend.Reader reader,
                TargetQuotaPayloadOwner selected,
                List<TargetStoreBackend.Edit> edits) {
            requireCurrent(reader);
            requireOwner(selected);
            final var raw = reader.projected(
                    ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(selected.messageId()), edits);
            if (raw != null
                    && !TargetMigrationCustodyGuard.decode(
                                    TargetValueEnvelope.decode(raw, TargetMigrationCustodyGuard.VALUE_TYPE)
                                            .payload())
                            .settled()) {
                return false;
            }
            return prefix.hold.closedLegacyRetain(selected, entry);
        }

        public void requireLegacyOwner(
                TargetStoreBackend.Reader reader,
                TargetQuotaPayloadOwner before,
                TargetQuotaPayloadOwner after,
                List<TargetStoreBackend.Edit> edits) {
            requireCurrent(reader);
            if (before != null) {
                requireOwner(before);
            }
            final byte[] key = before != null ? before.key() : after == null ? null : after.key();
            boolean ownerEdited = false;
            if (key != null) {
                for (var edit : edits) {
                    if (edit.family() == ColumnFamily.META && Arrays.equals(edit.key(), key)) {
                        ownerEdited = true;
                    }
                }
            }
            if (!ownerEdited) {
                if (before == null && after == null) {
                    return;
                }
                if (before == null
                        || after == null
                        || !Arrays.equals(before.canonicalBytes(), after.canonicalBytes())) {
                    throw new IllegalStateException("unmodified legacy payload owner changed without an actual edit");
                }
                return;
            }
            if (before != null && before.summaryQualified() || after != null && after.summaryQualified()) {
                throw new IllegalStateException("v1 prefix cannot change qualified v2 summary rules");
            }
            if (after != null) {
                if (before != null && !before.messageId().equals(after.messageId())) {
                    throw new IllegalStateException("old event changed its payload identity");
                }
                if (before == null
                        && (!(entry instanceof SourceReplayRecord command)
                                || !command.command().delayMessageId().equals(after.messageId()))) {
                    throw new IllegalStateException("fresh legacy payload belongs to another original Command");
                }
                if (!same(after.mutation().source(), entry.position())) {
                    throw new IllegalStateException("v1 successor is not this actual Source event");
                }
            }
            if (after == null || after.phase() == TargetQuotaPayloadOwner.Phase.RETAINED) {
                if (before == null
                        || !noUnresolvedAfter(reader, before, edits)
                        || !noLegacyCustodyAfter(reader, before, edits)) {
                    throw new IllegalStateException(
                            "old payload retain/delete lacks full Native and original legacy closure");
                }
            }
        }

        private void requireOwner(TargetQuotaPayloadOwner selected) {
            if (owner == null || !Arrays.equals(owner.canonicalBytes(), selected.canonicalBytes())) {
                throw new IllegalStateException("old prefix fold belongs to another exact payload owner");
            }
        }

        private static TargetQuotaAttemptBudget attempt(byte[] raw) {
            if (raw == null) {
                return null;
            }
            final var value = TargetValueEnvelope.decodeAny(raw);
            return value.valueType() == TargetQuotaAttemptBudget.VALUE_TYPE
                    ? TargetQuotaAttemptBudget.decode(value.payload())
                    : null;
        }

        private static boolean unresolved(TargetQuotaAttemptBudget value) {
            return value.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                    || value.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN;
        }

        public void requireCommit(SourceReplayEntry actual) {
            prefix.requireLocal();
            prefix.requireExpected(actual);
            if (prefix.permit != this || !SourceApplyCoordinator.sameEntry(entry, actual)) {
                throw new IllegalStateException("v1 commit lost its exact retained event/hold");
            }
        }
    }

    /** The second real native reader supplies every delivery/successor; only the independently captured R limits it. */
    private final class QualifiedSource implements TargetWorkerRecoverySource {
        private SourceReplayEntry current;
        private SourcePosition deliveredCut = c;
        private SourceReplayCursor<SourceReplayEntry> cursor;

        public void requireLocalCurrent() {
            replayReader.requireLocalCurrent();
            requireLocal();
            replayReader.requireLocalCurrent();
        }

        public void requireStartup(
                SourceAssignment assignment,
                long bytes,
                WorkClassExecutionRegistry classes,
                SharedRocksDbResources resources) {
            replayReader.requireStartup(assignment, bytes, classes, resources);
            requireLocal();
        }

        public SourceReplayCursor<SourceReplayEntry> replayCursor() {
            if (cursor == null) {
                final var actual = replayReader.replayCursor();
                cursor = SourceReplayCursor.guarded(
                        new Iterator<>() {
                            public boolean hasNext() {
                                requireLocal();
                                if (same(deliveredCut, r)) {
                                    return false;
                                }
                                if (!actual.hasNext()) {
                                    throw new SourceReplayGapException(
                                            "second retained native reader ended before the captured R");
                                }
                                if (actual.peek().position().compareTo(r) > 0) {
                                    throw new SourceReplayGapException(
                                            "second native reader skipped the exact captured R");
                                }
                                return true;
                            }

                            public SourceReplayEntry next() {
                                if (!hasNext()) {
                                    throw new NoSuchElementException();
                                }
                                current = actual.next();
                                replayReader.requireReaderEntry(current, opening.assignment(), c);
                                final var raw = frame(current);
                                if (!budget.tryCharge(current.position().canonicalBytes().length, raw.length)) {
                                    throw new IllegalStateException(
                                            "second Source pass exhausted the original shared cumulative budget");
                                }
                                deliveredCut = current.position();
                                return current;
                            }
                        },
                        TargetPayloadSummaryLegacyPrefix.this::requireLocal,
                        this);
            }
            return cursor;
        }

        public SourceReplaySuccessor successorAfter(SourcePosition cut) {
            requireLocal();
            if (!same(cut, c)) {
                throw new IllegalStateException("second actual reader changed original C");
            }
            return replayReader.successorAfter(cut);
        }

        public void captureReplayTail() {
            requireLocal();
            hold.requireRange(opening, c, r);
            replayReader.captureReplayTail();
        }

        public boolean replayTailReached(SourcePosition actual) {
            requireLocal();
            return same(deliveredCut, r) && actual.sameSourceIdentity(r) && actual.compareTo(r) >= 0;
        }

        public Optional<SourcePosition> capturedTailReadThrough() {
            requireLocal();
            return same(deliveredCut, r) ? Optional.of(r) : Optional.empty();
        }

        public void requireReaderBinding(SourceAssignment assignment, SourcePosition cut) {
            requireLocal();
            replayReader.requireReaderBinding(assignment, c);
            if (!same(cut, c)) {
                throw new IllegalStateException("second reader changed original C");
            }
        }

        public void requireReaderEntry(SourceReplayEntry entry, SourceAssignment assignment, SourcePosition cut) {
            requireReaderBinding(assignment, cut);
            replayReader.requireReaderEntry(entry, assignment, cut);
            if (current != entry) {
                throw new IllegalStateException("entry was not delivered by the second actual native reader");
            }
        }

        public TargetOwnedBackendGroup backend() {
            return replayReader.backend();
        }

        public boolean cleanupComplete() {
            return replayReader.cleanupComplete();
        }

        public void close() {
            replayReader.close();
        }
    }

    public void requestStop() {
        stopping = true;
        if (io != null) {
            io.requestStop();
        }
        if (hold != null) {
            hold.requestStop();
        }
    }

    public boolean ioExited() {
        return (io == null || io.operationExited() && io.ioExited())
                && (cleanup == null || cleanup.operationExited() && cleanup.ioExited());
    }

    public Throwable pendingIoFailure() {
        return failure;
    }

    @Override
    public void close() {
        requestStop();
    }

    @Override
    public boolean cleanupComplete() {
        return cleaned.isDone();
    }

    @Override
    public CompletableFuture<Void> retryCleanup() {
        if (cleaned.isDone()) {
            return cleaned;
        }
        if (!stopping) {
            return cleaned;
        }
        if (queued != null) {
            opening.graph().classes().withdrawQueued(queued);
            opening.graph().classes().discardFailed(queued);
            if (opening.graph().classes().state(queued).isPresent()) {
                return cleaned;
            }
            queued = null;
        }
        if (io != null) {
            io.requestStop();
            io.retryCleanup();
            if (!io.operationExited() || !io.ioExited()) {
                return cleaned;
            }
            io = null;
        }
        if (cleanup != null) {
            cleanup.retryCleanup();
            if (!cleanup.operationExited() || !cleanup.ioExited()) {
                return cleaned;
            }
            boolean done = false;
            try {
                done = cleanup.receipt().join();
            } catch (RuntimeException retained) {
                if (failure == null) {
                    failure = retained;
                }
            }
            cleanup = null;
            if (done) {
                closed = true;
                cleaned.complete(null);
                return cleaned;
            }
        }
        try {
            cleanup = OwnedCheckpointIo.submit(
                    ignored -> {
                        if (originalReader != null) {
                            originalReader.close();
                            if (!originalReader.cleanupComplete()) {
                                return false;
                            }
                        }
                        if (replayReader != null) {
                            replayReader.close();
                            if (!replayReader.cleanupComplete()) {
                                return false;
                            }
                        }
                        if (failedAcquisition != null) {
                            failedAcquisition.retryCleanup();
                            if (!failedAcquisition.cleanupComplete()) {
                                return false;
                            }
                        }
                        if (hold != null) {
                            if (protection != null) {
                                hold.releaseEvent(protection);
                                protection = null;
                            }
                            if (staleProtection != null) {
                                hold.releaseEvent(staleProtection);
                                staleProtection = null;
                            }
                            hold.requestStop();
                            hold.close();
                            hold.retryCleanup();
                            if (!hold.ioExited() || !hold.cleanupComplete()) {
                                return false;
                            }
                        }
                        return true;
                    },
                    () -> {},
                    opening.graph().io());
        } catch (java.util.concurrent.RejectedExecutionException full) {
            if (opening.graph().io().isShutdown() && failure == null) {
                failure = full;
            }
        }
        return cleaned;
    }
}
