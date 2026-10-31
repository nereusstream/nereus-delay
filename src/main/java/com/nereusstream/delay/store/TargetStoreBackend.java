package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.runtime.TargetQueueHeadCache;
import com.nereusstream.delay.runtime.TargetQuotaTotalsDelta;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetResultLedgerAudit;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.rocksdb.RocksDB;

/** Real RocksDB adapter for Target plans; production composition must still supply business/lease authority. */
public final class TargetStoreBackend {
    /** Coalesced notification shared by the active Workers in one Target host. */
    public static final class TargetQueueChangeSignal {
        public record Changes(boolean inventoryDirty, boolean businessRecheck, Set<TargetPartitionId> dirtyTargets) {
            public Changes {
                dirtyTargets = Set.copyOf(Objects.requireNonNull(dirtyTargets, "dirtyTargets"));
            }
        }

        private final AtomicLong revision = new AtomicLong();
        private final Object monitor = new Object();
        private final Set<TargetPartitionId> registeredTargets = new HashSet<>();
        private final Set<TargetPartitionId> dirtyTargets = new HashSet<>();
        private int maximumTargets;
        private boolean inventoryDirty;
        private boolean businessRecheck;

        public long revision() {
            return revision.get();
        }

        /** Waits only while the caller's pre-scan revision is still current. */
        public boolean awaitChange(final long observedRevision, final java.time.Duration timeout)
                throws InterruptedException {
            final var exactTimeout = Objects.requireNonNull(timeout, "timeout");
            if (exactTimeout.isNegative()) {
                throw new IllegalArgumentException("Target queue change timeout cannot be negative");
            }
            final long timeoutNanos = exactTimeout.toNanos();
            final long deadline = System.nanoTime() + timeoutNanos;
            synchronized (monitor) {
                while (revision.get() == observedRevision) {
                    final long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                }
                return true;
            }
        }

        /** Applies the activated bound used by one Host's live Target registrations. */
        public void configureTargetLimit(final int maximumTargets) {
            if (maximumTargets <= 0) {
                throw new IllegalArgumentException("Target wake registration requires a finite positive bound");
            }
            synchronized (monitor) {
                if (this.maximumTargets != 0 && this.maximumTargets != maximumTargets) {
                    throw new IllegalStateException("Target wake registration limit is already configured");
                }
                this.maximumTargets = maximumTargets;
            }
        }

        /** Replaces the exact bounded set installed by a complete Host inventory. */
        public void registerTargets(final Collection<TargetPartitionId> targets) {
            final Set<TargetPartitionId> exact = Set.copyOf(Objects.requireNonNull(targets, "targets"));
            synchronized (monitor) {
                if (maximumTargets <= 0 || exact.size() > maximumTargets) {
                    throw new IllegalStateException("Target wake registrations exceed the activated bound");
                }
                registeredTargets.clear();
                registeredTargets.addAll(exact);
                if (dirtyTargets.stream().anyMatch(target -> !registeredTargets.contains(target))) {
                    inventoryDirty = true;
                }
                dirtyTargets.retainAll(registeredTargets);
            }
        }

        /** Removes a target after a bounded refresh proves that no active source Store contains it. */
        public void unregisterTarget(final TargetPartitionId target) {
            final var exact = Objects.requireNonNull(target, "target");
            synchronized (monitor) {
                registeredTargets.remove(exact);
                if (dirtyTargets.remove(exact)) {
                    inventoryDirty = true;
                }
            }
        }

        /** Takes coalesced changes once; events arriving after this cut stay queued for the next turn. */
        public Changes drainChanges() {
            synchronized (monitor) {
                final var result = new Changes(inventoryDirty, businessRecheck, dirtyTargets);
                inventoryDirty = false;
                businessRecheck = false;
                dirtyTargets.clear();
                return result;
            }
        }

        /** Publishes a committed business or host-membership change to bounded schedulers. */
        public void signal() {
            synchronized (monitor) {
                revision.incrementAndGet();
                inventoryDirty = true;
                monitor.notifyAll();
            }
        }

        /** Publishes changed queue keys without invalidating unrelated Target registrations. */
        public void signalChanges(final Set<TargetPartitionId> targets, final boolean businessRecheck) {
            final var exactTargets = Set.copyOf(Objects.requireNonNull(targets, "targets"));
            synchronized (monitor) {
                revision.incrementAndGet();
                this.businessRecheck |= businessRecheck;
                for (TargetPartitionId target : exactTargets) {
                    if (registeredTargets.contains(target)) {
                        dirtyTargets.add(target);
                    } else {
                        inventoryDirty = true;
                    }
                }
                monitor.notifyAll();
            }
        }
    }

    public record WriteLimits(int maximumRecords, long maximumEncodedBytes) {
        public WriteLimits {
            if (maximumRecords <= 0 || maximumEncodedBytes <= 0) {
                throw new IllegalArgumentException("positive finite Target write limits required");
            }
        }
    }

    /** Exact complete NV before/after, with null denoting absence/deletion. */
    public record Edit(ColumnFamily family, byte[] key, byte[] before, byte[] after) {
        public Edit {
            Objects.requireNonNull(family, "family");
            key = Bytes.copy(Objects.requireNonNull(key, "key"));
            before = before == null ? null : Bytes.copy(before);
            after = after == null ? null : Bytes.copy(after);
            if (before == null && after == null) {
                throw new IllegalArgumentException("empty Target edit");
            }
            final int type = businessType(family, key);
            if (before != null) {
                TargetValueEnvelope.decode(before, type);
            }
            if (after != null) {
                TargetValueEnvelope.decode(after, type);
            }
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }

        @Override
        public byte[] before() {
            return before == null ? null : Bytes.copy(before);
        }

        @Override
        public byte[] after() {
            return after == null ? null : Bytes.copy(after);
        }
    }

    /** Backend-owned fixed projection; its exact storage charge is assembled against the Shard root. */
    public record IngressFenceChange(byte[] before, IngressFenceState after) {
        public IngressFenceChange {
            before = before == null ? null : Bytes.copy(before);
            Objects.requireNonNull(after, "after");
            final var prior = before == null
                    ? new IngressFenceState(IngressFenceState.OPEN, null)
                    : IngressFenceState.decode(
                            TargetValueEnvelope.decode(before, 1).payload());
            if (after.closedThroughEpochMs() < 0
                    || after.proofId() == null
                    || after.closedThroughEpochMs() < prior.closedThroughEpochMs()) {
                throw new IllegalArgumentException("Target fence must retain a proof and never regress");
            }
        }

        @Override
        public byte[] before() {
            return before == null ? null : Bytes.copy(before);
        }

        public byte[] encodedAfter() {
            return TargetValueEnvelope.encode(1, after.canonicalBytes());
        }
    }

    /** Complete monotone cursor projection, owned by the Source batch rather than ordinary business edits. */
    public record EvidenceCursorChange(byte[] before, List<EvidenceCursor> after) {
        public EvidenceCursorChange {
            before = before == null ? null : Bytes.copy(before);
            after = List.copyOf(after);
            if (after.isEmpty()) {
                throw new IllegalArgumentException("Source cursor seed must retain a nonempty accepted frontier");
            }
            final byte[] encoded = StoreRuntimeMetadata.encodeEvidenceCursors(after);
            if (encoded.length > StoreRuntimeMetadata.MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("Target evidence cursor seed exceeds its fixed bound");
            }
            int successor = 0;
            for (final var prior : priorCursors(before)) {
                while (successor < after.size() && after.get(successor).compareTo(prior) < 0) {
                    successor++;
                }
                if (successor == after.size() || !after.get(successor).dominates(prior)) {
                    throw new IllegalArgumentException("Target Source cursor seed cannot drop or regress a frontier");
                }
            }
        }

        @Override
        public byte[] before() {
            return before == null ? null : Bytes.copy(before);
        }

        public byte[] encodedAfter() {
            return TargetValueEnvelope.encode(1, StoreRuntimeMetadata.encodeEvidenceCursors(after));
        }

        private static List<EvidenceCursor> priorCursors(final byte[] before) {
            return before == null ? List.of() : StoreRuntimeMetadata.decodeEvidenceCursors(
                    TargetValueEnvelope.decode(before, 1).payload());
        }

        private static EvidenceCursorChange advance(final byte[] before, final EvidenceCursor accepted) {
            final var cursors = new ArrayList<>(priorCursors(before));
            for (int index = 0; index < cursors.size(); index++) {
                final var prior = cursors.get(index);
                if (prior.sameIdentity(accepted)) {
                    if (prior.dominates(accepted)) {
                        return null;
                    }
                    if (!accepted.dominates(prior)) {
                        throw new IllegalArgumentException(
                                "authenticated cursor has an incomparable retained frontier");
                    }
                    cursors.set(index, accepted);
                    return new EvidenceCursorChange(before, cursors);
                }
            }
            cursors.add(accepted);
            cursors.sort(EvidenceCursor::compareTo);
            return new EvidenceCursorChange(before, cursors);
        }
    }

    public record Mutation(
            TargetQuotaTotalsDelta quota, List<Edit> business,
            IngressFenceChange ingressFence, EvidenceCursorChange evidenceCursors) {
        public Mutation(final TargetQuotaTotalsDelta quota, final List<Edit> business) {
            this(quota, business, null, null);
        }

        public Mutation(final TargetQuotaTotalsDelta quota, final List<Edit> business, final IngressFenceChange fence) {
            this(quota, business, fence, null);
        }

        public Mutation {
            Objects.requireNonNull(quota, "quota");
            business = List.copyOf(business);
            if (ingressFence != null && quota.counters().mutation().isLocalMutation()) {
                throw new IllegalArgumentException("only a source mutation may advance the Target fence");
            }
            if (evidenceCursors != null) {
                if (ingressFence != null || quota.counters().mutation().isLocalMutation()) {
                    throw new IllegalArgumentException("cursor seeds require a separate Source mutation");
                }
                StoreRuntimeMetadata.requireEvidenceCursorScope(evidenceCursors.after(), 2, quota.shardScope().shard());
            }
        }
    }

    /** Must hold source/Owner/Store/Route/grant and complete business semantics through native commit. */
    @FunctionalInterface
    public interface CommitAuthority {
        CommitGuard acquire(StoreMetadata store, TargetQuotaScope scope, Mutation mutation);
    }

    public interface CommitGuard extends AutoCloseable {
        void requireCurrent();

        @Override
        void close();
    }

    public static final class Prepared {
        private final TargetStoreBackend backend;
        private final ShardStore.ReadView view;
        private final Mutation mutation;
        private final int writeRecords;
        private final long encodedBytes;
        private final com.nereusstream.delay.runtime.TargetSloWriterOrigin.Context writerContext;
        private boolean attempted;

        private Prepared(
                final TargetStoreBackend backend,
                final ShardStore.ReadView view,
                final Mutation mutation,
                final int writeRecords,
                final long encodedBytes,
                final com.nereusstream.delay.runtime.TargetSloWriterOrigin.Context writerContext) {
            this.backend = backend;
            this.view = view;
            this.mutation = mutation;
            this.writeRecords = writeRecords;
            this.encodedBytes = encodedBytes;
            this.writerContext = writerContext;
        }

        public Mutation mutation() {
            return mutation;
        }

        public int writeRecords() {
            return writeRecords;
        }

        public long encodedBytes() {
            return encodedBytes;
        }
    }

    /** Read methods are valid only during prepare, under one Store view and shared read budget. */
    public final class Reader implements TargetRecordAccounting.View {
        private boolean active = true;
        private final BoundedReadBudget budget;
        private final com.nereusstream.delay.runtime.TargetSloWriterOrigin.Context writerContext;
        private final com.nereusstream.delay.runtime.TargetRetirementPreparation.Proof retirementProof;
        private final com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.PreparedTail archiveTail;

        /** Exact original maintenance acquisition, available only on the opaque candidate backend. */
        public com.nereusstream.delay.runtime.TargetMigrationImporter.PreparedCandidate migrationCandidate() {
            requireActive();
            if (migrationCandidate != null) migrationCandidate.requireStore(store);
            return migrationCandidate;
        }

        private Reader(final BoundedReadBudget budget) {
            this(budget, null);
        }

        private Reader(final BoundedReadBudget budget,
                final com.nereusstream.delay.runtime.TargetRetirementPreparation.Proof retirementProof) {
            this.budget = budget;
            this.retirementProof = retirementProof;
            this.archiveTail = archiveTailContext.get();
            final var origin = writerOrigin;
            writerContext = origin == null ? null : origin.openView();
        }

        @Override public com.nereusstream.delay.runtime.TargetSloWriterOrigin.Context writerOriginContext() {
            requireActive(); return writerContext;
        }

        @Override public com.nereusstream.delay.runtime.TargetRetirementPreparation.Proof retirementProof() {
            requireActive(); return retirementProof;
        }

        @Override public com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.PreparedTail archiveTailProof() {
            requireActive(); return archiveTail;
        }

        public void requireBackend(final TargetStoreBackend expected) {
            requireActive();
            if (expected != TargetStoreBackend.this) throw new IllegalArgumentException("retirement preparation belongs to another actual backend");
        }
        public void requireSourceFrameBinding(ShardStore actual,TargetQuotaScope expected,byte[] expectedLineage){
            requireActive();
            if(store!=actual||!scope.equals(expected)||!Arrays.equals(lineage,expectedLineage))
                throw new IllegalArgumentException("Source frame qualification changed its actual Native Store/root lineage");
        }
        public com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrameContract(){
            requireActive();
            final var inputs=sourceOperationInputs;
            if(inputs==null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("Native plan has no original complete Source frame qualification");
            final var actual=inputs.sourceFrameContract();actual.requireCurrent(this);return actual;
        }

        private void requireActive() {
            if (!active || !Thread.holdsLock(store)) {
                throw new IllegalStateException("Target reader escaped its bounded Store plan");
            }
        }

        public byte[] get(final ColumnFamily family, final byte[] key) {
            requireActive();
            return store.get(family, key);
        }

        /** Check elapsed work without invalidating completed inputs at an exact record/byte limit. */
        public void requireWithinElapsedBudget() {
            requireActive();
            if (!budget.beforeTimedWork()) {
                throw budget.incomplete();
            }
        }

        public StoreMetadata metadata() {
            requireActive();
            return store.metadata();
        }

        /** Complete bounded emptiness proof for first initialization, including unknown keys and the default CF. */
        public void requireUninitialized() {
            requireActive();
            store.requireUninitializedTarget(budget);
        }

        public long closedIngressDeadlineThrough() {
            requireActive();
            final byte[] raw = get(ColumnFamily.META, KeyCodec.metaFixed(4));
            return raw == null
                    ? IngressFenceState.OPEN
                    : IngressFenceState.decode(
                                    TargetValueEnvelope.decode(raw, 1).payload())
                            .closedThroughEpochMs();
        }

        /** Caller must have authenticated the accepted cursor against its exact Source evidence operation. */
        public EvidenceCursorChange advanceEvidenceCursor(final EvidenceCursor accepted) {
            requireActive();
            StoreRuntimeMetadata.requireEvidenceCursorScope(List.of(accepted), 2, store.shardId());
            return EvidenceCursorChange.advance(get(ColumnFamily.META, KeyCodec.metaFixed(6)), accepted);
        }

        public int maximumWriteRecords() {
            requireActive();
            return limits.maximumRecords();
        }

        public com.nereusstream.delay.protocol.ShardId shardId() {
            requireActive();
            return store.shardId();
        }

        /** Opaque same-view identity for privately preparing this exact prospective Source tail on owned I/O. */
        public ShardStore.ReadView archiveReadView() {
            requireActive(); return store.captureReadView();
        }

        /** Native Store revision for detecting changes between separately protected read pages. */
        public long nativeSequence() {
            requireActive();
            return store.latestSequenceNumber();
        }

        /** Exact overlay read; absence is explicit and never inferred from a bounded range scan. */
        public byte[] projected(final ColumnFamily family, final byte[] key, final List<Edit> overlay) {
            requireActive();
            requireOverlayLimit(overlay);
            Edit found = null;
            for (var edit : overlay) {
                if (edit.family == family && Arrays.equals(edit.key, key)) {
                    if (found != null) {
                        throw new IllegalArgumentException("duplicate overlay key");
                    }
                    found = edit;
                }
            }
            return found == null ? get(family, key) : found.after();
        }

        /** Finds the first surviving key using at most overlay.size()+1 persisted entries. */
        @Override
        public ShardStore.KeyValue first(
                final ColumnFamily family, final byte[] lower, final byte[] upper, final List<Edit> overlay) {
            requireActive();
            requireOverlayLimit(overlay);
            if (lower == null || upper == null || Arrays.compareUnsigned(lower, upper) >= 0) {
                throw new IllegalArgumentException("finite ordered range required");
            }
            final var replacements = new java.util.HashMap<String, Edit>();
            ShardStore.KeyValue selected = null;
            for (var edit : overlay) {
                if (edit.family != family
                        || Arrays.compareUnsigned(edit.key, lower) < 0
                        || Arrays.compareUnsigned(edit.key, upper) >= 0) {
                    continue;
                }
                if (replacements.putIfAbsent(HexFormat.of().formatHex(edit.key), edit) != null) {
                    throw new IllegalArgumentException("duplicate range overlay key");
                }
                if (edit.after != null && (selected == null || Arrays.compareUnsigned(edit.key, selected.key()) < 0)) {
                    selected = new ShardStore.KeyValue(edit.key, edit.after);
                }
            }
            final ShardStore.KeyValue[] persisted = {null};
            final var result = store.visitResult(
                    family, lower, upper, Math.incrementExact(replacements.size()), budget, (row, shared) -> {
                        if (replacements.containsKey(HexFormat.of().formatHex(row.key()))) {
                            return true;
                        }
                        persisted[0] = row;
                        return false;
                    });
            if (result.stop() == ShardStore.VisitStop.INCOMPLETE) {
                throw new ReadIncompleteException(result.reason());
            }
            if (result.stop() != ShardStore.VisitStop.RANGE_END
                    && result.stop() != ShardStore.VisitStop.VISITOR_STOPPED) {
                throw new IllegalStateException("Target overlay minimum was not established");
            }
            return persisted[0] != null
                            && (selected == null || Arrays.compareUnsigned(persisted[0].key(), selected.key()) < 0)
                    ? persisted[0]
                    : selected;
        }

        /** Reads one actual CF row, or proves its real EOF, without concealing unknown Native keys. */
        public ShardStore.KeyValue retirementNext(final ColumnFamily family, final byte[] lower) {
            requireActive();
            final ShardStore.KeyValue[] selected = {null};
            final var result = store.visitResult(family, lower, null, 1, budget, (row, shared) -> {
                selected[0] = row; return false;
            });
            if (result.stop() == ShardStore.VisitStop.INCOMPLETE) throw budget.incomplete();
            if (result.stop() != ShardStore.VisitStop.VISITOR_STOPPED && result.stop() != ShardStore.VisitStop.RANGE_END)
                throw new IllegalStateException("retirement preparation did not establish the next actual CF row or EOF");
            return selected[0];
        }

        private void requireOverlayLimit(final List<Edit> overlay) {
            if (Objects.requireNonNull(overlay, "overlay").size() > limits.maximumRecords()) {
                throw new IllegalArgumentException("Target overlay exceeds the write record limit");
            }
        }

        public SourcePosition source() {
            requireActive();
            return store.appliedShardLogPosition();
        }

        public long sourceSequence() {
            requireActive();
            return store.shardMutationSequence();
        }

        public TargetQuotaAggregate aggregate() {
            requireActive();
            return readAggregate();
        }
        /** Same selected root/accounting binding for a protected paged fold; this does not audit any CF. */
        public TargetCheckpointRootVerifier.RootProof retirementRootProof() {
            requireActive();final var aggregate=aggregate();
            final var bookkeeping=com.nereusstream.delay.protocol.TargetQuotaBookkeeping.decode(TargetValueEnvelope.decode(
                    get(ColumnFamily.META,com.nereusstream.delay.protocol.TargetQuotaBookkeeping.keyFor(scope.shard())),
                    com.nereusstream.delay.protocol.TargetQuotaBookkeeping.VALUE_TYPE).payload());
            final byte[] rootKey=bookkeeping.owner().key();rootKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final var root=com.nereusstream.delay.protocol.TargetQuotaIncarnation.decodeForStore(rootKey,
                    TargetValueEnvelope.decode(get(ColumnFamily.META,rootKey),com.nereusstream.delay.protocol.TargetQuotaIncarnation.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope());
            if(!root.identity().equals(bookkeeping.owner())||!java.util.Arrays.equals(root.tenantScope(),scope.tenantScope())
                    ||!java.util.Arrays.equals(root.recoveryLineage(),lineage)
                    ||!java.util.Arrays.equals(root.accounting().canonicalBytes(),bookkeeping.accounting().canonicalBytes())
                    ||!java.util.Arrays.equals(root.identity().accountingIncarnation(),aggregate.accountingIncarnation()))
                throw new IllegalStateException("Native ledger changed original root/bookkeeping lineage");
            final var proof=new TargetCheckpointRootVerifier.RootProof(metadata(),source(),sourceSequence(),root,bookkeeping,aggregate);
            root.latestMutation().requireAtOrBefore(aggregate.mutation());bookkeeping.mutation().requireAtOrBefore(aggregate.mutation());
            return proof;
        }
        /** Complete cold-path fold of this exact guarded view, sharing its original finite Native budget. */
        public TargetCheckpointDependencies auditLedger(TargetCheckpointRootVerifier.LedgerAuditLimits auditLimits){
            final var proof = retirementRootProof();
            final var result=TargetCheckpointRootVerifier.auditLiveStoreDependencies(store,proof,auditLimits,budget);
            requireWithinElapsedBudget();return result;
        }
        public TargetCheckpointDependencies auditLedger(){
            return auditLedger(new TargetCheckpointRootVerifier.LedgerAuditLimits(budget.maxRecords(),budget.maxBytes(),budget.maxRecords(),budget.maxBytes()));
        }

        public TargetQuotaCounter counter(final TargetQuotaIdentity identity) {
            requireActive();
            return readCounter(identity);
        }

        public TargetQuotaTotal total(final TargetQuotaScope targetScope) {
            requireActive();
            return readTotal(targetScope);
        }

        /** Reads the durable active-message count for one exact Target without enumerating its records. */
        public OptionalLong activeTargetMessages(final TargetPartitionId target) {
            requireActive();
            final TargetQuotaTotal total = readTotal(scope.forTarget(Objects.requireNonNull(target, "target")));
            if (total == null) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(total.usage().resources().amount(CapacityDimension.ACTIVE_MESSAGES));
        }

        public Edit replace(final ColumnFamily family, final byte[] key, final int type, final byte[] payload) {
            requireActive();
            return new Edit(
                    family, key, get(family, key), payload == null ? null : TargetValueEnvelope.encode(type, payload));
        }
    }

    private final ShardStore store;
    private final TargetQuotaScope scope;
    private final TargetQuotaAggregate genesis;
    private final byte[] lineage;
    private final WriteLimits limits;
    private final com.nereusstream.delay.runtime.TargetMigrationImporter.PreparedCandidate migrationCandidate;
    private final AtomicReference<ShardStore.NativeWriteOperation> nextNativeWriteForTesting = new AtomicReference<>();
    private volatile TargetQueueChangeSignal targetQueueChangeSignal;
    private volatile TargetQueueHeadCache targetQueueHeadCache;
    private volatile com.nereusstream.delay.runtime.TargetSloWriterOrigin writerOrigin;
    private volatile com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.SourcePlanInputs sourceOperationInputs;
    private volatile TargetStoreBackend sourceOperationEnvelopeParent;
    private volatile boolean sourceOperationEnvelopeReady;
    private final ThreadLocal<com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.PreparedTail> archiveTailContext = new ThreadLocal<>();

    public TargetStoreBackend(
            final ShardStore store,
            final TargetQuotaScope scope,
            final byte[] shardAccountingIncarnation,
            final byte[] lineage,
            final WriteLimits limits) {
        this(store, scope, shardAccountingIncarnation, lineage, limits, null);
    }

    /** Only the opaque, independently sealed migration cut can operate while its Root is PREPARED. */
    public static TargetStoreBackend forMigration(final ShardStore store, final TargetQuotaScope scope,
            final byte[] shardAccountingIncarnation, final byte[] lineage, final WriteLimits limits,
            final com.nereusstream.delay.runtime.TargetMigrationImporter.PreparedCandidate candidate) {
        Objects.requireNonNull(candidate, "sealed migration candidate").requireStore(store);
        if (!candidate.plan().scope().equals(scope) || !Arrays.equals(candidate.plan().newLineage(), lineage)
                || !Arrays.equals(candidate.root().identity().accountingIncarnation(), shardAccountingIncarnation)) {
            throw new IllegalArgumentException("migration backend changed its actual candidate root");
        }
        return new TargetStoreBackend(store, scope, shardAccountingIncarnation, lineage, limits, candidate);
    }

    private TargetStoreBackend(final ShardStore store, final TargetQuotaScope scope,
            final byte[] shardAccountingIncarnation, final byte[] lineage, final WriteLimits limits,
            final com.nereusstream.delay.runtime.TargetMigrationImporter.PreparedCandidate migration) {
        this.store = Objects.requireNonNull(store, "store");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.limits = Objects.requireNonNull(limits, "limits");
        migrationCandidate = migration;
        Bytes.requireLength(lineage, 16, "lineage");
        this.lineage = Bytes.copy(lineage);
        if (store.metadata().storeFormatVersion() != 2
                || scope.target() != null
                || !store.shardId().equals(scope.shard())
                || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("Target backend requires explicit format and Shard/lineage binding");
        }
        genesis = TargetQuotaAggregate.genesis(scope.shard(), shardAccountingIncarnation);
        if (migration == null) com.nereusstream.delay.runtime.TargetMigrationStore.requireServiceable(store);
    }

    /** Binds production composition to this exact native Store object, not merely a matching Shard ID. */
    public void requireStore(final ShardStore actual) {
        if (store != Objects.requireNonNull(actual, "store")) {
            throw new IllegalArgumentException("Target backend belongs to another Store instance");
        }
    }

    /** The configured Source binds the actual Process reservation after reading this Native Root. */
    public void bindSourceOperationEnvelope(
            com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.SourcePlanInputs actual) {
        Objects.requireNonNull(actual).requireStore(store);
        synchronized(store) {
            if(sourceOperationInputs!=null&&sourceOperationInputs!=actual)
                throw new IllegalStateException("actual Native backend changed its Source plan reservation");
            sourceOperationInputs=actual;
        }
    }

    public boolean sourceOperationEnvelopeReady() {
        final var actual=sourceOperationInputs;
        final var parent=sourceOperationEnvelopeParent;
        if(!(sourceOperationEnvelopeReady||parent!=null&&parent.sourceOperationEnvelopeReady())||actual==null||store.nativeExposureFenceRaised())return false;
        actual.sourceFrameContract();return true;
    }
    /** Aliases the same actual Source reservation for its existing archive backend over this exact Store. */
    public void shareSourceOperationEnvelope(TargetStoreBackend original){
        Objects.requireNonNull(original);
        if(original==this||original.store!=store||!original.scope.equals(scope)||!Arrays.equals(original.lineage,lineage)
                ||original.sourceOperationEnvelopeParent!=null)
            throw new IllegalArgumentException("archive Source envelope changed its original Native backend");
        bindSourceOperationEnvelope(Objects.requireNonNull(original.sourceOperationInputs));
        sourceOperationEnvelopeParent=original;
    }

    /** Only the original Source owned-I/O caller invokes this, after its real kernel has been installed. */
    public void verifySourceOperationEnvelope(BoundedReadBudget budget, ReadAuthority reads) {
        final var actual=sourceOperationInputs;
        if(actual==null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                "Source lacks its original Process Native-plan reservation");
        actual.requireStore(store);
        actual.prepareSourceFrames();
        final var required=guardedRead(budget,reader->{
            final var frames=actual.sourceFrameContract();
            final var computed=com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.required(reader,frames);
            frames.requireService(reader,computed);return computed;
        },reads);
        actual.acquire(required);
        actual.requireStore(store);
        sourceOperationEnvelopeReady=true;
    }

    /** A process-local continuation for prospective APPLIED work; it cannot be restored by serialization. */
    public static final class SourceOperationEnvelopeRequired extends RuntimeException {
        private static final long serialVersionUID=1L;
        private final transient TargetStoreBackend backend;
        private final transient com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.SourcePlanInputs inputs;
        private final transient com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.Bounds required;
        private SourceOperationEnvelopeRequired(TargetStoreBackend backend,
                com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.SourcePlanInputs inputs,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.Bounds required) {
            super("actual APPLIED protocol family requires its complete Source service window");
            this.backend=backend;this.inputs=inputs;this.required=required;
        }
        @java.io.Serial
        private void readObject(java.io.ObjectInputStream ignored) throws java.io.InvalidObjectException {
            throw new java.io.InvalidObjectException("Source operation continuation requires its original live Process and cannot be restored");
        }
        public void requireBackend(TargetStoreBackend actual) {
            if(actual!=backend||backend.sourceOperationInputs!=inputs)
                throw new IllegalArgumentException("Source operation request changed its original backend/reservation");
        }
        public void acquire() {
            if(Thread.holdsLock(backend.store))throw new IllegalStateException("Source operation window cannot grow inside a Native plan");
            requireBackend(backend);inputs.acquire(required);
        }
    }

    private void requireSourceOperationEnvelope(Reader reader, Mutation mutation) {
        final var actual=sourceOperationInputs;
        if(actual==null)return; // Unconfigured bootstrap/maintenance is not an exposed Source runtime.
        if(!sourceOperationEnvelopeReady())throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                "Source Native operation family has not completed its actual Root service gate");
        actual.requireStore(store);
        actual.sourceFrameContract().requireService(reader,actual.operationBounds());
        for(final var edit:mutation.business()) {
            if(edit.family()!=ColumnFamily.META||edit.key.length<2
                    ||edit.key[0]!=TargetKeyCodec.PROTOCOL_ACTIVATION_TAG||edit.before!=null||edit.after==null)continue;
            final var applied=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.decodeForStore(edit.key,
                    TargetValueEnvelope.decode(edit.after,com.nereusstream.delay.protocol.TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
            final com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.Bounds required;
            try{required=com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.activating(reader,applied.readers().descriptor(),actual.operationBounds());}
            catch(com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable unavailable){
                throw new IllegalStateException("prospective Source protocol family lacks its original service capability",unavailable);
            }
            actual.sourceFrameContract().requireService(reader,required);
            if(!actual.includes(required))throw new SourceOperationEnvelopeRequired(this,actual,required);
        }
    }

    /** One actual Source action can expose only its privately prepared exact tail to newly opened Readers. */
    public <T> T withArchiveTail(final com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.PreparedTail proof,
            final java.util.function.Supplier<T> action) {
        final var previous = archiveTailContext.get();
        if (previous != null && previous != proof) throw new IllegalStateException("another actual Source tail already owns this action");
        if (proof != null) proof.requireCurrent();
        if (proof == null) archiveTailContext.remove(); else archiveTailContext.set(proof);
        try { return Objects.requireNonNull(action, "actual Source action").get(); }
        finally { if (previous == null) archiveTailContext.remove(); else archiveTailContext.set(previous); }
    }

    /** Installs the one original protected writer kernel for this actual Store; never grants a prefix by itself. */
    public void installWriterOrigin(final com.nereusstream.delay.runtime.TargetSloWriterOrigin original) {
        Objects.requireNonNull(original, "original writer origin").requireStore(store);
        synchronized (store) {
            if (writerOrigin != null && writerOrigin != original)
                throw new IllegalStateException("actual backend already has its original writer origin");
            writerOrigin = original;
        }
    }

    void injectNextNativeWriteForTesting(final ShardStore.NativeWriteOperation nativeWrite) {
        if (!nextNativeWriteForTesting.compareAndSet(null, Objects.requireNonNull(nativeWrite, "nativeWrite"))) {
            throw new IllegalStateException("a Target native write injection is already pending");
        }
    }

    /** Binds one host signal and requests a refresh for already persisted queue state. */
    public synchronized void bindTargetQueueChangeSignal(final TargetQueueChangeSignal signal) {
        final var exactSignal = Objects.requireNonNull(signal, "signal");
        if (targetQueueChangeSignal != null && targetQueueChangeSignal != exactSignal) {
            throw new IllegalStateException("Target backend is already bound to another host signal");
        }
        targetQueueChangeSignal = exactSignal;
        exactSignal.signal();
    }

    /** Clears a failed Host-construction binding without detaching a signal installed by another Host. */
    public synchronized void unbindTargetQueueChangeSignal(final TargetQueueChangeSignal signal) {
        if (targetQueueChangeSignal == Objects.requireNonNull(signal, "signal")) {
            targetQueueChangeSignal = null;
        }
    }

    /** Binds the active source runtime's Owner/Store-scoped cache for post-commit invalidation. */
    public void bindTargetQueueHeadCache(final TargetQueueHeadCache cache) {
        targetQueueHeadCache = Objects.requireNonNull(cache, "cache");
    }

    /** Authority for acknowledging an already durable source record without writing another batch. */
    @FunctionalInterface
    public interface ReadAuthority {
        CommitGuard acquire(StoreMetadata metadata, TargetQuotaScope scope);
    }

    /**
     * Checkpoint-only authority supplied by the original read/retention provider. Implementing the legacy
     * ReadAuthority does not supply this capability: its CommitGuard may perform remote checks.
     */
    @FunctionalInterface
    public interface CheckpointReadAuthority {
        /** Acquires the actual finite hold; an exceptional late acquisition must retain its Cleanup handle. */
        CheckpointReadHold acquireCheckpointRead(StoreMetadata metadata, TargetQuotaScope scope);
    }

    /**
     * The same actual acquisition protects the read, both external checks and final local delivery.
     * Original Store/retention/Pin protection remains held until release; a longer-lived consumer needs its
     * own retained original loan rather than relying on this completed projection's immutable bytes.
     */
    public interface CheckpointReadHold extends AutoCloseable,
            com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup {
        /** Complete original Owner/session/acquisition and retention validation; called outside the Store monitor. */
        void requireExternalCurrent();

        /**
         * Validates this acquisition's actual local token, finite validity and revocation/stop state.
         * Must not perform RPC, wait for external I/O, or reconstruct authority from an epoch projection.
         */
        void requireLocalCurrent();

        /** Starts release of this exact hold; cleanupComplete must retain any actual late I/O/release obligation. */
        @Override
        void close();

        /** Local observation of actual acquisition/read/release exit, including failure and late completion. */
        @Override
        boolean cleanupComplete();

        @Override
        java.util.concurrent.CompletableFuture<Void> retryCleanup();
    }

    public static final class ReadPlan<T> {
        private final TargetStoreBackend backend;
        private final ShardStore.ReadView view;
        private final T value;
        private boolean completed;

        private ReadPlan(final TargetStoreBackend backend, final ShardStore.ReadView view, final T value) {
            this.backend = backend;
            this.view = view;
            this.value = value;
        }

        /** Inspection is not source acknowledgement; completeRead must first validate the retained view/guard. */
        public T value() {
            return value;
        }
    }

    /** The planner must return immutable values; the Reader itself becomes unusable when this method returns. */
    public <T> ReadPlan<T> prepareRead(final BoundedReadBudget budget, final Function<Reader, T> planner) {
        Objects.requireNonNull(planner, "planner");
        final var read = store.readWithBudget(budget, () -> {
            final var reader = new Reader(budget);
            try {
                return Objects.requireNonNull(planner.apply(reader), "read result");
            } finally {
                reader.active = false;
            }
        });
        return new ReadPlan<>(this, read.view(), read.value());
    }

    /** Prepare an opaque view under authority, then require a fresh guard again when completeRead consumes it. */
    public <T> ReadPlan<T> guardedPrepareRead(
            BoundedReadBudget budget, Function<Reader, T> planner, ReadAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        try (var guard = Objects.requireNonNull(authority.acquire(store.metadata(), scope), "read guard")) {
            guard.requireCurrent();
            final var plan = prepareRead(budget, planner);
            return store.withReadView(plan.view, () -> {
                guard.requireCurrent();
                return plan;
            });
        }
    }

    /** Holds current read authority before the first local read and until the verified value is delivered. */
    public <T> T guardedRead(BoundedReadBudget budget, Function<Reader, T> planner, ReadAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        try (var guard = Objects.requireNonNull(authority.acquire(store.metadata(), scope), "read guard")) {
            guard.requireCurrent();
            final var plan = prepareRead(budget, planner);
            plan.completed = true;
            return store.withReadView(plan.view, () -> {
                guard.requireCurrent();
                return plan.value;
            });
        }
    }

    /** Checkpoint projection with external validation outside the Store and only the same hold's local token inside. */
    public <T> T checkpointRead(
            final BoundedReadBudget budget, final Function<Reader, T> planner,
            final CheckpointReadAuthority authority) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(planner, "planner");
        Objects.requireNonNull(authority, "authority");
        requireCheckpointTime(budget);
        final var hold = Objects.requireNonNull(
                authority.acquireCheckpointRead(store.metadata(), scope), "checkpoint read hold");
        T value = null;
        Throwable failure = null;
        try {
            for (;;) {
                requireCheckpointTime(budget);
                hold.requireExternalCurrent();
                requireCheckpointTime(budget);
                final var plan = prepareRead(budget, reader -> {
                    hold.requireLocalCurrent();
                    final T projected = Objects.requireNonNull(planner.apply(reader), "checkpoint read result");
                    hold.requireLocalCurrent();
                    reader.requireWithinElapsedBudget();
                    return projected;
                });
                hold.requireExternalCurrent();
                requireCheckpointTime(budget);
                final var delivered = store.withCheckpointReadView(plan.view, () -> {
                    hold.requireLocalCurrent();
                    requireCheckpointTime(budget);
                    return plan.value;
                });
                if (delivered.isPresent()) {
                    value = delivered.orElseThrow();
                    break;
                }
                // Preserve this actual acquisition and the original cumulative byte/record/deadline budget.
            }
        } catch (RuntimeException | Error rejected) {
            failure = rejected;
        }
        final Throwable readFailure = failure;
        try {
            hold.close();
        } catch (RuntimeException | Error releaseFailure) {
            if (failure != null && releaseFailure != failure) releaseFailure.addSuppressed(failure);
            failure = releaseFailure;
        }
        if (!hold.cleanupComplete()
                || failure instanceof com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup cleanup
                        && !cleanup.cleanupComplete()
                || readFailure instanceof com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup readCleanupState
                        && !readCleanupState.cleanupComplete()) {
            throw new CheckpointReadCleanup(hold, failure, readFailure);
        }
        if (failure instanceof RuntimeException rejected) throw rejected;
        if (failure instanceof Error rejected) throw rejected;
        return Objects.requireNonNull(value, "completed checkpoint projection");
    }

    private static void requireCheckpointTime(final BoundedReadBudget budget) {
        if (!budget.beforeTimedWork()) throw budget.incomplete();
    }

    /** Keeps the original release/failure handles visible to the caller's OwnedCheckpointIo/PreparationIo. */
    private static final class CheckpointReadCleanup extends IllegalStateException
            implements com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient CheckpointReadHold hold;
        private final transient com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup failureCleanup;
        private final transient com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup readCleanup;

        private CheckpointReadCleanup(final CheckpointReadHold hold, final Throwable failure, final Throwable readFailure) {
            super("original checkpoint read hold has not completed actual cleanup", failure);
            this.hold = hold;
            failureCleanup = failure instanceof com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup cleanup
                    ? cleanup : null;
            readCleanup = readFailure != failure
                    && readFailure instanceof com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup cleanup
                    ? cleanup : null;
        }

        @Override public boolean cleanupComplete() {
            return hold.cleanupComplete() && (failureCleanup == null || failureCleanup.cleanupComplete())
                    && (readCleanup == null || readCleanup.cleanupComplete());
        }

        @Override public java.util.concurrent.CompletableFuture<Void> retryCleanup() {
            final var original = retry(hold);
            final var failed = retry(failureCleanup);
            final var read = retry(readCleanup);
            return java.util.concurrent.CompletableFuture.allOf(original, failed, read);
        }

        private static java.util.concurrent.CompletableFuture<Void> retry(
                final com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup cleanup) {
            try {
                return cleanup == null || cleanup.cleanupComplete()
                        ? java.util.concurrent.CompletableFuture.completedFuture(null)
                        : Objects.requireNonNull(cleanup.retryCleanup(), "checkpoint read cleanup future");
            } catch (RuntimeException | Error failure) {
                return java.util.concurrent.CompletableFuture.failedFuture(failure);
            }
        }
    }

    public <T> T completeRead(final ReadPlan<T> plan, final ReadAuthority authority) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(authority, "authority");
        synchronized (plan) {
            if (plan.backend != this || plan.completed) {
                throw new IllegalStateException("foreign or already completed Target read plan");
            }
            plan.completed = true;
        }
        try (var guard = Objects.requireNonNull(authority.acquire(store.metadata(), scope), "read guard")) {
            return store.withReadView(plan.view, () -> {
                guard.requireCurrent();
                return plan.value;
            });
        }
    }

    public Prepared prepare(final BoundedReadBudget budget, final Function<Reader, Mutation> planner) {
        return prepareWithRetirementProof(budget, null, planner);
    }

    public Prepared prepareRetirement(final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetRetirementPreparation.Proof proof,
            final Function<Reader, Mutation> planner) {
        return prepareWithRetirementProof(budget, Objects.requireNonNull(proof, "complete original retirement proof"), planner);
    }

    private Prepared prepareWithRetirementProof(final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetRetirementPreparation.Proof proof,
            final Function<Reader, Mutation> planner) {
        Objects.requireNonNull(planner, "planner");
        final long beforeRows = budget.actualRecords(), beforeBytes = budget.actualBytes();
        try {
        final var read = store.readWithBudget(budget, () -> {
            final var reader = new Reader(budget, proof);
            final Mutation mutation;
            try {
                if (proof != null) proof.requireReader(reader, this);
                mutation = Objects.requireNonNull(planner.apply(reader), "mutation");
                requireSourceOperationEnvelope(reader,mutation);
                com.nereusstream.delay.runtime.TargetMigrationRetirementStore.requireEdits(reader, mutation.business(),
                        mutation.quota().counters().mutation());
                com.nereusstream.delay.runtime.TargetMigrationArchiveStore.requireEdits(reader, mutation.business(),
                        mutation.quota().counters().mutation());
                if (!scope.equals(mutation.quota().shardScope())) {
                    throw new IllegalArgumentException("Target mutation belongs to another scope");
                }
                mutation.quota()
                        .requireCurrent(
                                reader.aggregate(),
                                reader.sourceSequence(),
                                reader.source(),
                                reader::counter,
                                reader::total);
                for (var edit : mutation.business()) {
                    if (!Arrays.equals(edit.before, reader.get(edit.family, edit.key))) {
                        throw new IllegalStateException("Target business read set changed");
                    }
                }
                if (mutation.ingressFence() != null
                        && !Arrays.equals(
                                mutation.ingressFence().before(),
                                reader.get(ColumnFamily.META, KeyCodec.metaFixed(4)))) {
                    throw new IllegalStateException("Target ingress fence read set changed");
                }
                if (mutation.evidenceCursors() != null
                        && !Arrays.equals(mutation.evidenceCursors().before(),
                                reader.get(ColumnFamily.META, KeyCodec.metaFixed(6)))) {
                    throw new IllegalStateException("Target cursor seed read set changed");
                }
                return measure(mutation, reader.writerOriginContext());
            } finally {
                reader.active = false;
            }
        });
        return new Prepared(this, read.view(), read.value().mutation, read.value().records, read.value().bytes,
                read.value().writerContext);
        } finally {
            if (proof != null) proof.debitReads(Math.subtractExact(budget.actualRecords(), beforeRows),
                    Math.subtractExact(budget.actualBytes(), beforeBytes));
        }
    }

    private record Measured(Mutation mutation, int records, long bytes,
            com.nereusstream.delay.runtime.TargetSloWriterOrigin.Context writerContext) {}

    private Measured measure(final Mutation mutation,
            final com.nereusstream.delay.runtime.TargetSloWriterOrigin.Context writerContext) {
        final var keys = new HashSet<String>();
        long bytes = 0;
        for (var edit : mutation.business()) {
            if (edit.family == ColumnFamily.META && edit.key.length > 2
                    && edit.key[0] == TargetKeyCodec.MIGRATION_ACTIVATION_ATTEMPT_TAG
                    && (edit.before != null || edit.after == null || migrationCandidate == null
                        || !mutation.quota().counters().mutation().migrationPreparation()))
                throw new IllegalArgumentException("durable migration append intent requires its original typed maintenance purpose");
            if (edit.family == ColumnFamily.META && edit.key.length > 2
                    && (edit.key[0] == TargetKeyCodec.MIGRATION_CUSTODY_GUARD_TAG
                        || edit.key[0] == TargetKeyCodec.MIGRATION_ORIGIN_TAG
                        || edit.key[0] == TargetKeyCodec.MIGRATION_PREFIX_TAG
                        || edit.key[0] == TargetKeyCodec.MIGRATION_ROOT_TAG
                        || edit.key[0] == TargetKeyCodec.MIGRATION_ACTIVATION_ATTEMPT_TAG
                        || edit.key[0] == TargetKeyCodec.MIGRATION_RETIREMENT_TAG
                        || edit.key[0] == TargetKeyCodec.MIGRATION_ARCHIVE_TAG)
                    && edit.after == null)
                throw new IllegalArgumentException("normal Native/GC cannot delete migration root, origin, custody or prefix");
            if (edit.family == ColumnFamily.META && Arrays.equals(edit.key, KeyCodec.metaFixed(10))) {
                final var stamp = mutation.quota().counters().mutation();
                if (edit.before != null || edit.after == null || stamp.isLocalMutation() || stamp.sequence() != 1) {
                    throw new IllegalArgumentException(
                            "Target initial control snapshot requires its first Source batch");
                }
                final var snapshot = com.nereusstream.delay.protocol.TargetInitialControlSnapshot.decode(
                        TargetValueEnvelope.decode(edit.after, 1).payload());
                if (!snapshot.mutation().equals(stamp)) {
                    throw new IllegalArgumentException("Target initial control snapshot changed its Source stamp");
                }
            }
            if (!keys.add(edit.family.name() + ':' + HexFormat.of().formatHex(edit.key))) {
                throw new IllegalArgumentException("duplicate Target business edit key");
            }
            bytes = Math.addExact(
                    bytes, Math.addExact((long) edit.key.length, edit.after == null ? 0 : edit.after.length));
        }
        int records = mutation.business().size();
        if (mutation.evidenceCursors() != null) {
            bytes = Math.addExact(bytes, Math.addExact((long) KeyCodec.metaFixed(6).length,
                    mutation.evidenceCursors().encodedAfter().length));
            records = Math.incrementExact(records);
        }
        if (mutation.ingressFence() != null) {
            bytes = Math.addExact(
                    bytes,
                    Math.addExact(
                            (long) KeyCodec.metaFixed(4).length,
                            mutation.ingressFence().encodedAfter().length));
            records = Math.incrementExact(records);
        }
        for (var change : mutation.quota().counters().changes()) {
            bytes = Math.addExact(
                    bytes,
                    encodedSize(change.next().identity().key(), change.next().canonicalBytes()));
            records = Math.incrementExact(records);
        }
        for (var change : mutation.quota().changes()) {
            bytes = Math.addExact(
                    bytes, encodedSize(change.next().key(), change.next().canonicalBytes()));
            records = Math.incrementExact(records);
        }
        final var aggregate = mutation.quota().counters().nextAggregate();
        bytes = Math.addExact(bytes, encodedSize(aggregate.key(), aggregate.canonicalBytes()));
        records = Math.incrementExact(records);
        final var stamp = mutation.quota().counters().mutation();
        if (!stamp.isLocalMutation()) {
            bytes = Math.addExact(
                    bytes, encodedSize(KeyCodec.metaFixed(3), stamp.source().canonicalBytes()));
            bytes = Math.addExact(bytes, encodedSize(KeyCodec.metaFixed(5), Bytes.u64beBits(stamp.sequence())));
            records = Math.addExact(records, 2);
        }
        if (records > limits.maximumRecords || bytes > limits.maximumEncodedBytes) {
            throw new IllegalStateException("Target atomic write exceeds its configured budget");
        }
        return new Measured(mutation, records, bytes, writerContext);
    }

    /** Consumes the plan once. Failed/unknown attempts are reconciled by a fresh read/recovery, never blind retry. */
    public void commit(final Prepared prepared, final CommitAuthority authority) {
        final var injected = nextNativeWriteForTesting.getAndSet(null);
        commit(prepared, authority, injected == null ? RocksDB::write : injected);
    }

    /** Package-local native boundary for verifying whole-batch failure and ambiguous-response recovery. */
    void commit(
            final Prepared prepared,
            final CommitAuthority authority,
            final ShardStore.NativeWriteOperation nativeWrite) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(nativeWrite, "nativeWrite");
        synchronized (prepared) {
            if (prepared.backend != this || prepared.attempted) {
                throw new IllegalStateException("foreign or already attempted Target plan");
            }
            prepared.attempted = true;
        }
        final boolean hasBusinessChanges =
                prepared.mutation.business().stream().anyMatch(edit -> !Arrays.equals(edit.before, edit.after));
        final var changedQueueTargets = changedQueueHeadTargets(prepared.mutation.business());
        final var writeCompleted = new AtomicBoolean();
        final var origin = writerOrigin;
        try (var guard =
                Objects.requireNonNull(authority.acquire(store.metadata(), scope, prepared.mutation), "guard")) {
            store.withReadView(prepared.view, () -> {
                guard.requireCurrent();
                store.write(batch -> {
                    batch.requireReadView(prepared.view);
                    guard.requireCurrent();
                    for (var edit : prepared.mutation.business()) {
                        if (edit.after == null) {
                            batch.delete(edit.family, edit.key);
                        } else {
                            batch.put(edit.family, edit.key, edit.after);
                        }
                    }
                    if (prepared.mutation.ingressFence() != null) {
                        batch.putTargetIngressFence(
                                prepared.mutation.ingressFence().after());
                    }
                    if (prepared.mutation.evidenceCursors() != null) {
                        batch.putTargetEvidenceCursors(prepared.mutation.evidenceCursors().after());
                    }
                    final var quota = prepared.mutation.quota();
                    for (var change : quota.counters().changes()) {
                        batch.put(
                                ColumnFamily.META,
                                change.next().identity().key(),
                                TargetValueEnvelope.encode(
                                        TargetQuotaCounter.VALUE_TYPE,
                                        change.next().canonicalBytes()));
                    }
                    for (var change : quota.changes()) {
                        batch.put(
                                ColumnFamily.META,
                                change.next().key(),
                                TargetValueEnvelope.encode(
                                        TargetQuotaTotal.VALUE_TYPE,
                                        change.next().canonicalBytes()));
                    }
                    final var aggregate = quota.counters().nextAggregate();
                    batch.put(
                            ColumnFamily.META,
                            aggregate.key(),
                            TargetValueEnvelope.encode(TargetQuotaAggregate.VALUE_TYPE, aggregate.canonicalBytes()));
                    final var stamp = quota.counters().mutation();
                    if (!stamp.isLocalMutation()) {
                        batch.putValue(
                                ColumnFamily.META,
                                1,
                                KeyCodec.metaFixed(3),
                                stamp.source().canonicalBytes());
                        batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(stamp.sequence()));
                    }
                }, nativeWrite);
                writeCompleted.set(true);
                if (origin != null) {
                    try { origin.committed(prepared.mutation.business(), prepared.writerContext); }
                    catch (RuntimeException | Error failure) { origin.invalidate(); throw failure; }
                }
                invalidateChangedQueueHeads(changedQueueTargets);
                return null;
            });
        } catch (RuntimeException | Error failure) {
            if (!writeCompleted.get() && origin != null) origin.invalidate();
            throw failure;
        } finally {
            if (writeCompleted.get() && hasBusinessChanges) {
                final var signal = targetQueueChangeSignal;
                if (signal != null) {
                    signal.signalChanges(changedQueueTargets, true);
                }
            }
        }
    }

    private static Set<TargetPartitionId> changedQueueHeadTargets(final List<Edit> edits) {
        final Set<TargetPartitionId> targets = new HashSet<>();
        for (Edit edit : edits) {
            if (edit.family != ColumnFamily.META || Arrays.equals(edit.before, edit.after)) {
                continue;
            }
            final byte[] key = edit.key;
            if (key.length == 2 + TargetPartitionId.LENGTH
                    && key[1] == TargetKeyCodec.KEY_FORMAT
                    && (Byte.toUnsignedInt(key[0]) == TargetKeyCodec.STATE_TAG
                            || Byte.toUnsignedInt(key[0]) == TargetKeyCodec.IDENTITY_TAG)) {
                targets.add(new TargetPartitionId(Arrays.copyOfRange(key, 2, key.length)));
            }
        }
        return Set.copyOf(targets);
    }

    private void invalidateChangedQueueHeads(final Set<TargetPartitionId> targets) {
        final var cache = targetQueueHeadCache;
        if (cache == null) {
            return;
        }
        for (TargetPartitionId target : targets) {
            cache.invalidate(target);
        }
    }

    public static final class AuditedResults {
        private final TargetStoreBackend backend;
        private final ShardStore.ReadView view;
        private final TargetResultLedgerAudit.Summary summary;

        private AuditedResults(
                final TargetStoreBackend backend, final ShardStore.ReadPlan<TargetResultLedgerAudit.Summary> read) {
            this.backend = backend;
            view = read.view();
            summary = read.value();
        }

        public TargetResultLedgerAudit.Summary summary() {
            return summary;
        }
    }

    /** Complete DEDUPE range and META dependency reads under the same Store monitor and shared budget. */
    public AuditedResults auditResults(
            final BoundedReadBudget budget,
            final TargetResultLedgerAudit.Limits auditLimits,
            final TargetResultLedgerAudit.CompletenessAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        final var read = store.readWithBudget(budget, () -> {
            final var rows = new ArrayList<TargetResultLedgerAudit.Stored>();
            // One extra row distinguishes an exact maximum from truncated traversal.
            final int scanLimit = Math.incrementExact(auditLimits.maximumResults());
            final long[] encoded = {0};
            final var traversal = store.visitResult(
                    ColumnFamily.DEDUPE, new byte[] {6}, new byte[] {10}, scanLimit, budget, (row, shared) -> {
                        if (rows.size() == auditLimits.maximumResults()) {
                            throw new IllegalStateException("Target result count limit reached before range end");
                        }
                        encoded[0] =
                                Math.addExact(encoded[0], Math.addExact((long) row.key().length, row.value().length));
                        if (encoded[0] > auditLimits.maximumEncodedBytes()) {
                            throw new IllegalStateException("Target result encoded byte limit reached");
                        }
                        final var value = TargetValueEnvelope.decode(row.value(), TargetResultRecord.VALUE_TYPE);
                        rows.add(new TargetResultLedgerAudit.Stored(row.key(), value.valueType(), value.payload()));
                        return true;
                    });
            if (traversal.stop() != ShardStore.VisitStop.RANGE_END) {
                throw new IllegalStateException("Target result Store traversal is incomplete: " + traversal.stop());
            }
            return TargetResultLedgerAudit.audit(
                    scope,
                    lineage,
                    store.shardMutationSequence(),
                    store.appliedShardLogPosition(),
                    auditLimits,
                    rows,
                    key -> {
                        final byte[] raw = store.get(ColumnFamily.META, key);
                        if (raw == null) {
                            return null;
                        }
                        final var value = TargetValueEnvelope.decodeAny(raw);
                        return new TargetResultLedgerAudit.Stored(key, value.valueType(), value.payload());
                    },
                    authority,
                    com.nereusstream.delay.runtime.TargetMigrationStore.decodeRoot(store.get(ColumnFamily.META,
                            com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard()))));
        });
        return new AuditedResults(this, read);
    }

    /** Publishes a completed audit only while its exact Store view still holds. */
    public <T> T withAuditView(
            final AuditedResults audit,
            final TargetResultLedgerAudit.CompletenessAuthority authority,
            final Function<TargetResultLedgerAudit.Summary, T> publisher) {
        Objects.requireNonNull(audit, "audit");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(publisher, "publisher");
        if (audit.backend != this) {
            throw new IllegalArgumentException("result audit belongs to another Target backend");
        }
        return store.withReadView(audit.view, () -> {
            authority.requireComplete(audit.summary);
            return publisher.apply(audit.summary);
        });
    }

    private TargetQuotaAggregate readAggregate() {
        final byte[] raw = store.get(ColumnFamily.META, genesis.key());
        if (raw == null) {
            if (store.shardMutationSequence() != 0 || store.appliedShardLogPosition() != null) {
                throw new IllegalStateException("nonempty Target source has no aggregate");
            }
            return genesis;
        }
        final var value = TargetQuotaAggregate.decodeForStore(
                genesis.key(),
                TargetValueEnvelope.decode(raw, TargetQuotaAggregate.VALUE_TYPE).payload(),
                scope.shard());
        if (!Arrays.equals(value.accountingIncarnation(), genesis.accountingIncarnation())) {
            throw new IllegalStateException("Target aggregate accounting incarnation changed");
        }
        return value;
    }

    private TargetQuotaCounter readCounter(final TargetQuotaIdentity identity) {
        if (!scope.shard().equals(identity.shard())
                || (identity.kind().isMirror() && !Arrays.equals(scope.tenantScope(), identity.tenantScope()))) {
            throw new IllegalArgumentException("Target counter lookup crossed scope");
        }
        final byte[] raw = store.get(ColumnFamily.META, identity.key());
        return raw == null
                ? null
                : TargetQuotaCounter.decodeForStore(
                        identity.key(),
                        TargetValueEnvelope.decode(raw, TargetQuotaCounter.VALUE_TYPE)
                                .payload(),
                        scope.shard());
    }

    private TargetQuotaTotal readTotal(final TargetQuotaScope targetScope) {
        if (targetScope.target() == null
                || !scope.forTarget(targetScope.target()).equals(targetScope)) {
            throw new IllegalArgumentException("Target total lookup crossed scope");
        }
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_TOTAL_TAG, 1}, targetScope.keySuffix());
        final byte[] raw = store.get(ColumnFamily.META, key);
        if (raw == null) {
            return null;
        }
        final var total = TargetQuotaTotal.decode(
                TargetValueEnvelope.decode(raw, TargetQuotaTotal.VALUE_TYPE).payload());
        if (!targetScope.equals(total.scope()) || !Arrays.equals(key, total.key())) {
            throw new IllegalStateException("Target total key/value mismatch");
        }
        return total;
    }

    private static long encodedSize(final byte[] key, final byte[] payload) {
        return Math.addExact(12L, Math.addExact((long) key.length, payload.length));
    }

    /** Excludes fixed metadata and derived counter/total/aggregate keys, owned only by this backend. */
    static int businessType(final ColumnFamily family, final byte[] key) {
        if (key.length < 3 || key[1] != 1) {
            throw new IllegalArgumentException("invalid Target business key");
        }
        if (family == ColumnFamily.META && Arrays.equals(key, KeyCodec.metaFixed(10))) { return 1; }
        final int tag = Byte.toUnsignedInt(key[0]);
        final int type =
                switch (family) {
                    case TERMINAL -> tag == TargetKeyCodec.TERMINAL_GENERATION_TAG ? 37 : 0;
                    case INFLIGHT -> tag == TargetKeyCodec.CLAIM_TAG ? 36 : 0;
                    case DEDUPE -> tag >= 6 && tag <= 9 ? 35 : 0;
                    case ID ->
                        switch (tag) {
                            case 5 -> 15;
                            case 6 -> 21;
                            case 7, 8, 9 -> 38;
                            default -> 0;
                        };
                    case TIMELINE ->
                        switch (tag) {
                            case 8, 9, 11, 12 -> 14;
                            case 10 -> 16;
                            case 13 -> 38;
                            default -> 0;
                        };
                    case META ->
                        switch (tag) {
                            case 9 -> 12;
                            case 10 -> 17;
                            case 11 -> 13;
                            case 12 -> 18;
                            case 13 -> 19;
                            case 14 -> 20;
                            case 15 -> 22;
                            case 16 -> 23;
                            case 17 -> 24;
                            case 18 -> 25;
                            case 21 -> 28;
                            case 23 -> 30;
                            case 24 -> 31;
                            case 25 -> 32;
                            case 26 -> 33;
                            case 27 -> 34;
                            case 28 -> 39;
                            case 29 -> 40;
                            case 30 -> 41;
                            case 31, 32, 33, 34, 35 -> 42;
                            case 36 -> 43;
                            case 37 -> 44;
                            case 38 -> 45;
                            case 39 -> 46;
                            case 40 -> 47;
                            case 41 -> 48;
                            case 42 -> 49;
                            case 43 -> 50;
                            case 44 -> 51;
                            case 45 -> 52;
                            case 46 -> 53;
                            case 47 -> 54;
                            case 48 -> 55;
                            case 49 -> 56;
                            case 50 -> 57;
                            case 51 -> 58;
                            case 52, 53 -> 59;
                            case 54 -> 60;
                            case 55 -> 61;
                            case 56 -> 62;
                            case 57 -> 63;
                            case 58 -> 64;
                            case 59 -> 65;
                            case 60 -> 66;
                            case 61 -> 67;
                            case 62 -> 68;
                            case 63 -> 69;
                            default -> 0;
                        };
                    default -> 0;
                };
        if (type == 0) {
            throw new IllegalArgumentException("unregistered or backend-owned Target key");
        }
        return type;
    }
}
