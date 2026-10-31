package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.TargetWorkerProcessRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRequest;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRow;
import com.nereusstream.delay.protocol.TargetMigrationArchiveState;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.runtime.TargetMigrationArchivePreparation;
import com.nereusstream.delay.runtime.TargetMigrationArchiveStore;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

/** A complete actual immutable inventory/packet audit, protected by the original new-owner allocation. */
public final class TargetMigrationArchiveHeld {
    private final TargetMigrationArchiveCustody custody;
    private final TargetMigrationArchiveReceipt receipt;
    private final TargetMigrationArchiveStore.AppliedTakeover takeover;
    private final TargetMigrationArchiveInventory head;
    private final boolean fullSourcePackets;
    private RuntimeHold runtime;
    private byte[] nativeHead;

    private TargetMigrationArchiveHeld(
            TargetMigrationArchiveCustody custody,
            TargetMigrationArchiveReceipt receipt,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetMigrationArchiveInventory head,
            boolean fullSourcePackets) {
        this.custody = custody;
        this.receipt = receipt;
        this.takeover = takeover;
        this.head = head;
        this.fullSourcePackets = fullSourcePackets;
    }

    static Cursor begin(
            TargetMigrationArchiveCustody custody,
            TargetMigrationArchiveReceipt receipt,
            TargetMigrationArchiveStore.AppliedTakeover takeover) {
        return new Cursor(custody, receipt, takeover);
    }

    static Cursor begin(TargetMigrationArchiveCustody custody, TargetMigrationArchiveReceived received) {
        return new Cursor(custody, received);
    }

    public TargetMigrationArchiveReceipt receipt() {
        requireCurrent();
        return Objects.requireNonNull(receipt, "partial archive is not takeover coverage");
    }

    public void requireCurrent() {
        if (runtime == null) {
            custody.requireLocalCurrent();
        } else {
            runtime.requireLocalCurrent();
        }
    }

    public void requireLocalCurrent() {
        requireCurrent();
    }

    public void requireReceipt(TargetMigrationArchiveReceipt exact) {
        requireCurrent();
        if (!receipt().equals(exact)) {
            throw new IllegalArgumentException("actual archive hold changed the complete received identity");
        }
    }

    public void requireDependencies(TargetCheckpointDependencies dependencies) {
        requireCurrent();
        final var root = Objects.requireNonNull(dependencies.migrationRoot());
        final var retirement = Objects.requireNonNull(dependencies.migrationRetirement());
        final var captured = Objects.requireNonNull(dependencies.migrationArchiveState(
                TargetMigrationArchiveState.headKey(root.plan().migrationId())));
        if (fullSourcePackets && captured.schemaVersion() != TargetMigrationArchiveState.SCHEMA_VERSION) {
            throw new IllegalStateException(
                    "full-Source archive packets require their actual new Native71 reader schema");
        }
        if (captured.schemaVersion() == TargetMigrationArchiveState.SCHEMA_VERSION) {
            TargetMigrationArchiveStore.requireCheckpointFormat(dependencies, captured, sourceFrames());
        }
        if (nativeHead == null || !Arrays.equals(nativeHead, captured.digest())) {
            throw new IllegalStateException(
                    "complete original Native72 Source/raw material audit is not current for this image");
        }
        if (!Arrays.equals(head.intentDigest(), retirement.intention().digest())
                || !head.intentAt().equals(retirement.intentAt())
                || !head.owner().equals(root.owner().identity())) {
            throw new IllegalStateException("cold archive changed its original/new protected owner");
        }
        if (receipt == null) {
            final var current = Objects.requireNonNull(dependencies.migrationArchiveState(
                    TargetMigrationArchiveState.headKey(root.plan().migrationId())));
            if (!Arrays.equals(current.archiveRoot(), head.archiveRoot())
                    || !current.coverage().equals(head.coverage())
                    || current.tailRows() != head.tailRows()
                    || current.tailRawBytes() != head.tailRawBytes()) {
                throw new IllegalStateException(
                        "partial cold archive differs from the actually read complete receiving inventory");
            }
            return;
        }
        final var actual = Objects.requireNonNull(
                dependencies.migrationArchiveState(TargetMigrationArchiveState.historyKey(
                        root.plan().migrationId(),
                        TargetQuotaMutation.increment(receipt.coveredThrough().sequence()))),
                "cold archive has no actual Source TAKEOVER history");
        if (!actual.taken()
                || !((TargetMigrationArchiveRequest.Takeover) actual.request().action())
                        .receipt()
                        .equals(receipt)) {
            throw new IllegalStateException("cold archive changed its actual Source receiving receipt");
        }
    }

    public byte[] readExact(TargetMigrationObjectReference exact, int maximumBytes) {
        requireCurrent();
        return runtime == null ? custody.readExact(exact, maximumBytes) : runtime.readExact(exact, maximumBytes);
    }

    public NativeCursor prepareNativeInputs(TargetCheckpointDependencies actual, BoundedReadBudget cumulative) {
        return new NativeCursor(this, actual, cumulative);
    }

    public boolean ioExited() {
        return custody.ioExited();
    }

    public long retainedBytes() {
        return Math.addExact(head.canonicalBytes().length, receipt == null ? 0 : receipt.canonicalBytes().length);
    }

    private com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames() {
        requireCurrent();
        return runtime == null ? custody.opening.sourceFrames() : runtime.opening.sourceFrames();
    }

    /** Full actual Native rows are already captured by the original collector; one raw dependency is read per turn. */
    public static final class NativeCursor {
        private final TargetMigrationArchiveHeld held;
        private final TargetCheckpointDependencies dependencies;
        private final BoundedReadBudget cumulative;
        private final java.util.Iterator<TargetMigrationArchiveRow> rows;
        private final byte[] head;
        private boolean failed, complete;

        private NativeCursor(
                TargetMigrationArchiveHeld held,
                TargetCheckpointDependencies dependencies,
                BoundedReadBudget cumulative) {
            this.held = held;
            this.dependencies = Objects.requireNonNull(dependencies);
            this.cumulative = Objects.requireNonNull(cumulative);
            if (cumulative.maxBytes() == Long.MAX_VALUE || cumulative.maxRecords() == Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "Native archive Source audit requires its actual cumulative input budget");
            }
            rows = dependencies.migrationArchiveEntryValues().iterator();
            head = Objects.requireNonNull(dependencies.migrationArchiveState(TargetMigrationArchiveState.headKey(
                            dependencies.migrationRoot().plan().migrationId())))
                    .digest();
        }

        public boolean advance() {
            if (failed) {
                throw new IllegalStateException("Native archive Source audit failed");
            }
            held.requireCurrent();
            if (complete) {
                return true;
            }
            try {
                if (!rows.hasNext()) {
                    held.nativeHead = Bytes.copy(head);
                    complete = true;
                    return true;
                }
                final var row = rows.next();
                if (!cumulative.beforeRead()) {
                    throw cumulative.incomplete();
                }
                if (row.schemaVersion() == TargetMigrationArchiveRow.SCHEMA_VERSION) {
                    TargetMigrationArchiveStore.requireCheckpointFormat(dependencies, row, held.sourceFrames());
                }
                if (row.sourceBranch() != TargetMigrationArchiveRow.SourceBranch.COPY_BATCH) {
                    final var object = row.entry().originalRecord().object();
                    final var raw = held.readExact(object, held.custody.limits.maximumPacketBytes());
                    if (!cumulative.tryCharge(row.key().length, raw.length)) {
                        throw cumulative.incomplete();
                    }
                    if (row.sourceBranch() == TargetMigrationArchiveRow.SourceBranch.ARCHIVE_REPLAY) {
                        TargetMigrationArchiveReplayMaterial.requirePacket(raw, row, dependencies, held.custody.owner);
                    } else {
                        final var packet = TargetMigrationArchivePreparation.decodePacket(raw, object);
                        final long ordinal = row.entry().originalRecord().ordinal();
                        if (packet.schemaVersion() > 1
                                        && row.schemaVersion() != TargetMigrationArchiveRow.SCHEMA_VERSION
                                || !packet.plan()
                                        .equals(dependencies.migrationRoot().plan())
                                || !packet.retirement()
                                        .intention()
                                        .equals(dependencies
                                                .migrationRetirement()
                                                .intention())
                                || !packet.retirement()
                                        .intentAt()
                                        .equals(dependencies
                                                .migrationRetirement()
                                                .intentAt())
                                || ordinal >= packet.entries().size()
                                || !packet.entries()
                                        .get(Math.toIntExact(ordinal))
                                        .equals(row.entry())) {
                            throw new IllegalStateException(
                                    "formal archive row differs from its actual complete original raw packet");
                        }
                        held.custody.owner.requireOriginalPacket(
                                raw,
                                packet.entries(),
                                Objects.requireNonNull(dependencies.migrationRoot()),
                                Objects.requireNonNull(dependencies.migrationRetirement()));
                    }
                } else if (!cumulative.tryCharge(row.key().length, 32)) {
                    throw cumulative.incomplete();
                }
                if (!cumulative.beforeTimedWork()) {
                    throw cumulative.incomplete();
                }
                return false;
            } catch (RuntimeException | Error rejected) {
                failed = true;
                throw rejected;
            }
        }

        public void requireCurrent() {
            held.requireCurrent();
            if (failed) {
                throw new IllegalStateException("Native archive raw audit is not current");
            }
        }
    }

    /** The implementation is a private actual Runtime/Driver acquisition, never a serializable permission DTO. */
    public interface RuntimeOpening {
        TargetWorkerProcessRuntime.Assembly graph();

        void requireCurrent();

        void requireLocalCurrent();

        void retainCleanup(CheckpointInputFuture.Cleanup actual);

        default com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames() {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                    "archive runtime lacks its original complete Source frame material");
        }
    }

    public synchronized RuntimeHold acquireRuntime(
            TargetMigrationArchiveStore.AppliedTakeover actual, RuntimeOpening opening) {
        Objects.requireNonNull(actual).requireCurrent();
        Objects.requireNonNull(opening).requireCurrent();
        requireReceipt(
                ((TargetMigrationArchiveRequest.Takeover) actual.request().action()).receipt());
        final var selected = opening.graph();
        final var original = custody.opening.graph();
        if (selected.classes() != original.classes()
                || selected.resources() != original.resources()
                || selected.io() != original.io()
                || selected.oxia() != original.oxia()
                || !selected.config().equals(original.config())) {
            throw new IllegalArgumentException("archive runtime changed its original physical resource graph");
        }
        if (runtime != null) {
            runtime.requireBinding(actual, opening);
            return runtime;
        }
        final var result = new RuntimeHold(this, actual, opening);
        opening.retainCleanup(result);
        custody.ownerCell.acquireRuntime();
        result.acquired = true;
        result.requireCurrent();
        runtime = result;
        return result;
    }
    /** Same actual native/provider loan transferred to normal Source before the T SDK ACK is required. */
    public static final class RuntimeHold implements CheckpointInputFuture.Cleanup, AutoCloseable {
        private final TargetMigrationArchiveHeld held;
        private final TargetMigrationArchiveStore.AppliedTakeover actual;
        private final RuntimeOpening opening;
        private boolean acquired, closed, stopping;
        private int calls;

        private RuntimeHold(
                TargetMigrationArchiveHeld held,
                TargetMigrationArchiveStore.AppliedTakeover actual,
                RuntimeOpening opening) {
            this.held = held;
            this.actual = actual;
            this.opening = opening;
        }

        void requireBinding(TargetMigrationArchiveStore.AppliedTakeover selected, RuntimeOpening exact) {
            if (opening != exact
                    || !actual.takeoverAt().equals(selected.takeoverAt())
                    || !actual.request().equals(selected.request())) {
                throw new IllegalArgumentException("archive runtime changed its actual Source takeover loan");
            }
            requireCurrent();
        }

        public void requireCurrent() {
            requireLocalCurrent();
        }

        public synchronized void requireLocalCurrent() {
            if (!acquired || closed || stopping) {
                throw new IllegalStateException("actual archive runtime loan is not current");
            }
            opening.requireLocalCurrent();
            held.custody.owner.requireLocalCurrent();
        }

        public TargetMigrationArchiveReceipt receipt() {
            requireCurrent();
            return held.receipt;
        }

        public TargetMigrationArchiveStore.AppliedTakeover takeover() {
            requireCurrent();
            return actual;
        }

        public byte[] readExact(TargetMigrationObjectReference object, int maximumBytes) {
            synchronized (this) {
                requireLocalCurrent();
                calls++;
            }
            try {
                held.custody.owner.requireObject(object.resource());
                final var value = held.custody.objects.readMigrationObject(object.resource(), maximumBytes);
                requireCurrent();
                return value;
            } finally {
                synchronized (this) {
                    calls--;
                }
            }
        }

        CheckpointResource putExact(String fixedName, byte[] bytes, int maximumBytes) {
            synchronized (this) {
                requireLocalCurrent();
                calls++;
            }
            try {
                final var value = held.custody.objects.putMigrationObject(
                        actual.migration().plan().newLineage(),
                        held.custody.checkpointId,
                        fixedName,
                        bytes,
                        maximumBytes);
                held.custody.owner.requireObject(value);
                if (!Arrays.equals(bytes, held.custody.objects.readMigrationObject(value, maximumBytes))) {
                    throw new IllegalStateException(
                            "actual runtime archive object differs from its complete immutable write");
                }
                requireCurrent();
                return value;
            } finally {
                synchronized (this) {
                    calls--;
                }
            }
        }

        TargetMigrationArchiveCustody custody() {
            return held.custody;
        }

        public TargetMigrationArchiveReplayMaterial prepareReplay(
                TargetMigrationArchiveStore.AppliedTakeover takeover,
                TargetMigrationArchiveState prior,
                TargetQuotaMutation before,
                com.nereusstream.delay.ownership.SourceReplayEntry entry,
                TargetMigrationArchiveRow logical,
                com.nereusstream.delay.ownership.SourceReplayOutcome outcome,
                long frozenFence) {
            requireBinding(takeover, opening);
            return TargetMigrationArchiveReplayMaterial.prepare(
                    this, takeover, prior, before, entry, logical, outcome, frozenFence);
        }

        public TargetMigrationArchiveReplayMaterial prepareClosed(
                TargetMigrationArchiveStore.AppliedTakeover takeover,
                TargetMigrationArchiveState prior,
                com.nereusstream.delay.runtime.TargetMigrationArchiveClosedContract.Decision decision) {
            requireBinding(takeover, opening);
            return TargetMigrationArchiveReplayMaterial.prepareClosed(this, takeover, prior, decision);
        }

        public long retainedBytes() {
            return Math.addExact(held.retainedBytes(), held.custody.owner.retainedBytes());
        }

        public synchronized boolean ioExited() {
            return calls == 0 && held.custody.owner.ioExited();
        }

        @Override
        public void close() {
            synchronized (this) {
                stopping = true;
                if (!ioExited()) {
                    return;
                }
                closed = true;
                if (acquired) {
                    held.custody.ownerCell.releaseRuntime();
                }
            }
        }

        @Override
        public boolean cleanupComplete() {
            return closed && (!acquired || held.custody.ownerCell.runtimeComplete());
        }

        @Override
        public CompletableFuture<Void> retryCleanup() {
            close();
            return cleanupComplete() ? CompletableFuture.completedFuture(null) : held.custody.owner.retryCleanup();
        }
    }

    /** One immutable link or one complete bounded packet per original owned-I/O turn. */
    public static final class Cursor implements CheckpointInputFuture.Cleanup, AutoCloseable {
        private final TargetMigrationArchiveCustody custody;
        private final TargetMigrationArchiveReceipt receipt;
        private final TargetMigrationArchiveStore.AppliedTakeover takeover;
        private TargetMigrationArchiveInventory head;
        private TreeSet<String> keys = new TreeSet<>();
        private TargetMigrationObjectReference next;
        private TargetMigrationArchiveInventory inventory;
        private byte[] expectedRoot;
        private BoundedReadBudget cumulative;
        private long rows, rawBytes, retained, physicalBytes;
        private int packets;
        private boolean failed, closed, fullSourcePackets;
        private TargetMigrationArchiveHeld complete;

        private Cursor(
                TargetMigrationArchiveCustody custody,
                TargetMigrationArchiveReceipt receipt,
                TargetMigrationArchiveStore.AppliedTakeover takeover) {
            this.custody = custody;
            this.receipt = Objects.requireNonNull(receipt);
            this.takeover = takeover;
            if (receipt.rawObjects().size() != 1
                    || !receipt.rawObjects().getFirst().equals(receipt.durableReceive())) {
                throw new IllegalArgumentException(
                        "archive receiving implementation requires its exact complete fixed inventory root");
            }
            next = receipt.durableReceive();
            expectedRoot = receipt.archiveRoot();
            retained = receipt.canonicalBytes().length;
        }

        private Cursor(TargetMigrationArchiveCustody custody, TargetMigrationArchiveReceived received) {
            this.custody = custody;
            this.takeover = null;
            this.head = received.inventory;
            this.receipt = head.coverage().complete() ? received.receipt() : null;
            next = received.inventoryObject;
            expectedRoot = head.archiveRoot();
            retained = head.canonicalBytes().length;
        }

        public Optional<TargetMigrationArchiveHeld> advance(BoundedReadBudget shared) {
            if (failed || closed) {
                throw new IllegalStateException("archive raw audit no longer admits work");
            }
            if (complete != null) {
                complete.requireCurrent();
                return Optional.of(complete);
            }
            if (cumulative == null) {
                cumulative = Objects.requireNonNull(shared);
                if (shared.maxBytes() == Long.MAX_VALUE || shared.maxRecords() == Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            "archive audit requires the original cumulative row/byte window");
                }
            } else if (cumulative != shared) {
                throw new IllegalArgumentException(
                        "archive audit cannot reset its original cumulative read budget each turn");
            }
            custody.requireCurrent();
            if (takeover != null) {
                takeover.requireCurrent();
            }
            try {
                if (inventory == null) {
                    final byte[] raw = read(next, custody.limits.maximumReceiptBytes());
                    inventory = TargetMigrationArchiveInventory.decode(raw);
                    if (head == null) {
                        inventory.requireReceipt(receipt);
                        head = inventory;
                    }
                    if (!Arrays.equals(inventory.archiveRoot(), expectedRoot)
                            || !Arrays.equals(inventory.intentDigest(), head.intentDigest())
                            || !inventory.intentAt().equals(head.intentAt())
                            || !inventory.owner().equals(head.owner())
                            || !inventory.allocation().equals(head.allocation())) {
                        throw new IllegalStateException(
                                "archive custody link changed its complete exact intent/root/receiver");
                    }
                    if (packets == 0 && receipt != null) {
                        inventory.requireReceipt(receipt);
                    }
                    if (inventory.packets() > custody.limits.maximumBatches()
                            || inventory.reservedBytes() > custody.limits.maximumRawBytes()) {
                        throw new IllegalStateException(
                                "archive custody history exceeds the actual retained physical allocation");
                    }
                    if (inventory.packets() != head.packets() - packets) {
                        throw new IllegalStateException(
                                "archive custody inventory skipped or repeated an exact fixed link");
                    }
                    if (packets == 0) {
                        custody.owner.requireCapacity(
                                custody.intent == null ? custody.takeover.migration() : custody.intent.migration(),
                                custody.intent == null ? custody.takeover.retirement() : custody.intent.record(),
                                head.reservedBytes(),
                                Math.multiplyExact(head.packets(), 2));
                    }
                    return Optional.empty();
                }
                final byte[] raw = read(inventory.packet(), custody.limits.maximumPacketBytes());
                final var packet = TargetMigrationArchivePreparation.decodePacket(raw, inventory.packet());
                if (packet.schemaVersion() > 1) {
                    fullSourcePackets = true;
                    if (takeover != null
                            && takeover.state().schemaVersion() != TargetMigrationArchiveState.SCHEMA_VERSION) {
                        throw new IllegalStateException(
                                "old archive Source takeover cannot adopt a full-Source packet format");
                    }
                }
                final var entries = packet.entries();
                final var root = custody.intent == null ? custody.takeover.migration() : custody.intent.migration();
                final var retirement = custody.intent == null ? custody.takeover.retirement() : custody.intent.record();
                if (!packet.plan().equals(root.plan())
                        || !packet.retirement().intention().equals(retirement.intention())
                        || !packet.retirement().intentAt().equals(retirement.intentAt())
                        || !packet.shadowCut().equals(inventory.shadowCut())
                        || !packet.coveredThrough().equals(inventory.coveredThrough())
                        || !Arrays.equals(packet.priorArchiveRoot(), inventory.priorRoot())
                        || !Objects.equals(packet.priorInventory(), inventory.priorInventory())
                        || !packet.coverage().equals(inventory.coverage())
                        || !Arrays.equals(packet.sourceCoverageDigest(), inventory.sourceCoverageDigest())
                        || packet.tailRows() != inventory.tailRows()
                        || packet.tailRawBytes() != inventory.tailRawBytes()) {
                    throw new IllegalStateException(
                            "actual full raw archive packet differs from the receiver's frozen complete material");
                }
                custody.owner.requireOriginalPacket(raw, entries, root, retirement);
                if (entries.size() != inventory.records()) {
                    throw new IllegalStateException("archive custody packet omitted original logical records");
                }
                byte[] calculated = inventory.priorRoot();
                for (var entry : entries) {
                    com.nereusstream.delay.runtime.TargetMigrationArchiveSemantics.requireEntry(entry, root.plan());
                    if (!keys.add(Bytes.hex(entry.originalKey()))) {
                        throw new IllegalStateException("archive custody chain duplicates an original first identity");
                    }
                    retained = Math.addExact(
                            retained, Math.addExact(64L, Math.multiplyExact(2L, entry.originalKey().length)));
                    if (retained > custody.limits.maximumRetainedBytes()) {
                        throw new IllegalStateException(
                                "complete archive duplicate/provenance audit exceeds original input reservation");
                    }
                    calculated = TargetMigrationArchiveRequest.appendRoot(calculated, entry);
                    rows = Math.incrementExact(rows);
                    rawBytes = Math.addExact(rawBytes, entry.rawValue().length);
                }
                if (!Arrays.equals(calculated, inventory.archiveRoot())) {
                    throw new IllegalStateException(
                            "complete archive packet does not reproduce its actual Native rolling root");
                }
                packets = Math.incrementExact(packets);
                expectedRoot = inventory.priorRoot();
                next = inventory.priorInventory();
                inventory = null;
                custody.requireCurrent();
                if (next != null) {
                    return Optional.empty();
                }
                if (!Arrays.equals(expectedRoot, TargetMigrationArchiveRequest.genesisRoot(head.intentDigest()))
                        || rows != Math.addExact(head.coverage().totalRows(), head.tailRows())
                        || rawBytes != Math.addExact(head.coverage().totalRawBytes(), head.tailRawBytes())
                        || packets != head.packets()
                        || physicalBytes > head.reservedBytes()) {
                    throw new IllegalStateException(
                            "archive receiving inventory lacks complete original namespace/tail coverage");
                }
                keys = null;
                complete = new TargetMigrationArchiveHeld(custody, receipt, takeover, head, fullSourcePackets);
                return Optional.of(complete);
            } catch (RuntimeException | Error rejected) {
                failed = true;
                throw rejected;
            }
        }

        private byte[] read(TargetMigrationObjectReference object, int maximumBytes) {
            if (!cumulative.beforeRead()) {
                throw cumulative.incomplete();
            }
            final byte[] raw = custody.readExact(object, maximumBytes);
            physicalBytes = Math.addExact(physicalBytes, raw.length);
            if (!cumulative.tryCharge(object.resource().objectKey().length, raw.length)) {
                throw cumulative.incomplete();
            }
            if (!cumulative.beforeTimedWork()) {
                throw cumulative.incomplete();
            }
            return raw;
        }

        public void requireCurrent() {
            custody.requireLocalCurrent();
            if (failed || closed) {
                throw new IllegalStateException("archive receiving audit is not current");
            }
        }

        public long retainedBytes() {
            return Math.addExact(retained, inventory == null ? 0 : inventory.canonicalBytes().length);
        }

        public boolean ioExited() {
            return custody.ioExited();
        }

        @Override
        public void close() {
            closed = true;
            keys = null;
            custody.close();
        }

        @Override
        public boolean cleanupComplete() {
            return closed && custody.cleanupComplete();
        }

        @Override
        public CompletableFuture<Void> retryCleanup() {
            close();
            return custody.retryCleanup();
        }
    }
}
