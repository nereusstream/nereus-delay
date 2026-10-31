package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.TargetMigrationMaintenance;
import com.nereusstream.delay.ownership.TargetMigrationProcessDriver;
import com.nereusstream.delay.ownership.TargetMigrationSourceOpening;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.TargetMigrationArchiveEntry;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRequest;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRow;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationRetirementRecord;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.runtime.TargetMigrationArchivePreparation;
import com.nereusstream.delay.runtime.TargetMigrationArchiveStore;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Exact immutable receiving on the original checkpoint HTTP/credential owner and maintenance owned-I/O turn. */
public final class TargetMigrationArchiveCustody implements CheckpointInputFuture.Cleanup, AutoCloseable {
    /** Actual acquired new-owner physical fee/retention loan. It cannot declare a transfer completed. */
    public interface NewOwner extends CheckpointInputFuture.Cleanup, AutoCloseable {
        TargetQuotaIdentity owner();

        TargetMigrationObjectReference allocation();

        byte[] originalObligation();

        byte[] retention();

        void requireCurrent();

        void requireLocalCurrent();
        /** Reuses the same durable allocation for retries/UNKNOWN; no newly minted or twice-debited share. */
        void requireCapacity(
                TargetMigrationRoot root,
                TargetMigrationRetirementRecord retirement,
                long totalBytes,
                int totalObjects);

        void requireObject(CheckpointResource exact);
        /** Independent original Source/manifest/issuer-history qualification over all retained raw proof bytes. */
        void requireOriginalPacket(
                byte[] completePacket,
                java.util.List<TargetMigrationArchiveEntry> entries,
                TargetMigrationRoot root,
                TargetMigrationRetirementRecord retirement);

        void requireOriginalReplay(
                byte[] completePacket,
                TargetMigrationArchiveStore.AppliedTakeover takeover,
                TargetMigrationArchiveRow logical,
                com.nereusstream.delay.ownership.SourceReplayEntry actual,
                com.nereusstream.delay.ownership.SourceReplayOutcome outcome,
                long frozenFence);

        long retainedBytes();

        boolean ioExited();

        void requestStop();
        /** Returns this local loan only; durable receiver responsibility and physical shares survive. */
        @Override
        void close();
    }

    public record Limits(
            int maximumPacketBytes,
            int maximumReceiptBytes,
            int maximumBatches,
            long maximumRawBytes,
            long maximumRetainedBytes) {
        public Limits {
            if (maximumPacketBytes < 1
                    || maximumPacketBytes == Integer.MAX_VALUE
                    || maximumReceiptBytes < 1
                    || maximumReceiptBytes == Integer.MAX_VALUE
                    || maximumBatches < 1
                    || maximumBatches == Integer.MAX_VALUE
                    || maximumRawBytes < 1
                    || maximumRawBytes == Long.MAX_VALUE
                    || maximumRetainedBytes < 1
                    || maximumRetainedBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "archive custody requires its original finite object/total/retained envelope");
            }
        }
    }

    final TargetMigrationSourceOpening opening;
    final TargetMigrationRetirementStore.AppliedIntent intent;
    final TargetMigrationArchiveStore.AppliedTakeover takeover;
    final RenewableS3CompatibleCheckpointObjectStoreAdapter objects;
    final NewOwner owner;
    final OwnerCell ownerCell;
    final Limits limits;
    final byte[] checkpointId;
    private TargetMigrationArchiveReceived last;
    private int calls;
    private boolean stopping, closed;
    private CompletableFuture<Void> cleanup;

    private TargetMigrationArchiveCustody(
            TargetMigrationMaintenance.Opening opening,
            TargetMigrationRetirementStore.AppliedIntent intent,
            RenewableS3CompatibleCheckpointObjectStoreAdapter objects,
            NewOwner owner,
            Limits limits) {
        this.opening = Objects.requireNonNull(opening);
        this.intent = Objects.requireNonNull(intent);
        this.objects = Objects.requireNonNull(objects);
        this.takeover = null;
        this.owner = Objects.requireNonNull(owner);
        this.ownerCell = new OwnerCell(owner);
        this.limits = Objects.requireNonNull(limits);
        checkpointId = Arrays.copyOf(
                Bytes.sha256(
                        Bytes.utf8("nereus-delay-migration-archive-custody-v1\0"),
                        intent.intention().digest(),
                        intent.intentAt().canonicalBytes()),
                16);
    }

    private TargetMigrationArchiveCustody(
            TargetMigrationProcessDriver.Opening opening,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            RenewableS3CompatibleCheckpointObjectStoreAdapter objects,
            NewOwner owner,
            Limits limits) {
        this.opening = Objects.requireNonNull(opening);
        this.intent = null;
        this.takeover = Objects.requireNonNull(takeover);
        this.objects = Objects.requireNonNull(objects);
        this.owner = Objects.requireNonNull(owner);
        this.ownerCell = new OwnerCell(owner);
        this.limits = Objects.requireNonNull(limits);
        checkpointId = Arrays.copyOf(
                Bytes.sha256(
                        Bytes.utf8("nereus-delay-migration-archive-custody-v1\0"),
                        takeover.retirement().intention().digest(),
                        takeover.retirement().intentAt().canonicalBytes()),
                16);
    }

    public static TargetMigrationArchiveCustody open(
            TargetMigrationMaintenance.Opening opening,
            TargetMigrationRetirementStore.AppliedIntent intent,
            RenewableS3CompatibleCheckpointObjectStoreAdapter objects,
            NewOwner owner,
            Limits limits) {
        final var actual = new TargetMigrationArchiveCustody(opening, intent, objects, owner, limits);
        opening.retainArchiveCustody(actual);
        actual.requireCurrent();
        if (!owner.owner().equals(intent.migration().owner().identity())
                || owner.retainedBytes() < 0
                || owner.retainedBytes() > limits.maximumRetainedBytes()
                || limits.maximumRetainedBytes() > opening.maximumRetainedInputBytes()) {
            throw new IllegalStateException(
                    "archive receiver changed its actual new Root payer or held input allocation");
        }
        return actual;
    }
    /** A real applied T chooses this new domain before any old checkpoint/Store is opened. */
    public static TargetMigrationArchiveHeld.Cursor reopen(
            TargetMigrationProcessDriver.Opening opening,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            RenewableS3CompatibleCheckpointObjectStoreAdapter objects,
            NewOwner owner,
            Limits limits) {
        final var custody = new TargetMigrationArchiveCustody(opening, takeover, objects, owner, limits);
        opening.retainCleanup(custody);
        custody.requireCurrent();
        if (!owner.owner().equals(takeover.migration().owner().identity())
                || owner.retainedBytes() < 0
                || owner.retainedBytes() > limits.maximumRetainedBytes()
                || limits.maximumRetainedBytes() > opening.configuration().maximumRetainedBytes()) {
            throw new IllegalStateException("archive reopen changed the actual persistent new-owner allocation");
        }
        return TargetMigrationArchiveHeld.begin(
                custody,
                ((TargetMigrationArchiveRequest.Takeover) takeover.request().action()).receipt(),
                takeover);
    }

    public TargetMigrationArchiveHeld.Cursor audit(TargetMigrationArchiveReceived received) {
        if (received.custody != this) {
            throw new IllegalArgumentException("archive audit changed actual receiving owner");
        }
        return TargetMigrationArchiveHeld.begin(this, received);
    }

    public TargetMigrationArchiveReceived receive(TargetMigrationArchivePreparation.TransferMaterial material) {
        enter();
        try {
            material.requireCurrent();
            synchronized (this) {
                if (last != null && last.matchesMaterial(material)) {
                    last.requireMaterial(material);
                    return last;
                }
            }
            final var actual = TargetMigrationArchiveReceived.receive(this, material);
            synchronized (this) {
                last = actual;
            }
            if (retainedBytes() > limits.maximumRetainedBytes()) {
                throw new IllegalStateException("archive receiver exceeds its original held input bytes");
            }
            return actual;
        } finally {
            exit();
        }
    }

    synchronized void reserve(long packetBytes, long totalReservedBytes, int batches) {
        if (packetBytes < 1
                || packetBytes > limits.maximumPacketBytes()
                || totalReservedBytes > limits.maximumRawBytes()
                || batches < 1
                || batches > limits.maximumBatches()) {
            throw new IllegalStateException("archive raw packet exceeds the actual prepaid aggregate object envelope");
        }
        owner.requireCapacity(intent.migration(), intent.record(), totalReservedBytes, Math.multiplyExact(batches, 2));
    }

    CheckpointResource put(String name, byte[] bytes, int maximumBytes) {
        requireCurrent();
        final var result = objects.putMigrationObject(
                intent.migration().plan().newLineage(), checkpointId, name, bytes, maximumBytes);
        owner.requireObject(result);
        final var exact = objects.readMigrationObject(result, maximumBytes);
        if (!Arrays.equals(bytes, exact)) {
            throw new IllegalStateException("archive receiver did not retain the exact complete immutable content");
        }
        requireCurrent();
        return result;
    }

    public byte[] readExact(TargetMigrationObjectReference exact, int maximumBytes) {
        enter();
        try {
            owner.requireObject(exact.resource());
            final var bytes = objects.readMigrationObject(exact.resource(), maximumBytes);
            requireCurrent();
            return bytes;
        } finally {
            exit();
        }
    }

    public void requireCurrent() {
        opening.requireCurrent();
        if (intent != null) {
            intent.requireCurrent();
        } else {
            takeover.requireCurrent();
        }
        owner.requireCurrent();
        requireLocalCurrent();
    }

    public synchronized void requireLocalCurrent() {
        if (stopping || closed) {
            throw new IllegalStateException("archive receiving loan no longer admits new work");
        }
        opening.requireLocalCurrent();
        owner.requireLocalCurrent();
    }

    private synchronized void enter() {
        requireLocalCurrent();
        calls++;
    }

    private synchronized void exit() {
        calls--;
    }

    public synchronized long retainedBytes() {
        long bytes = Math.addExact(32L, ownerCell.parentRetainedBytes());
        return last == null ? bytes : Math.addExact(bytes, last.retainedBytes());
    }

    public synchronized boolean ioExited() {
        return calls == 0 && (ownerCell.runtimeOwned() || owner.ioExited()) && CheckpointInputFuture.ioExited(cleanup);
    }

    public synchronized void requestStop() {
        stopping = true;
        if (!ownerCell.runtimeOwned()) {
            owner.requestStop();
        }
    }

    @Override
    public void close() {
        requestStop();
        synchronized (this) {
            if (calls != 0) {
                return;
            }
            closed = true;
            ownerCell.releaseParent();
            if (!ownerCell.parentComplete() && CheckpointInputFuture.ioExited(cleanup)) {
                cleanup = Objects.requireNonNull(owner.retryCleanup());
            }
        }
    }

    @Override
    public synchronized boolean cleanupComplete() {
        return closed && ioExited() && ownerCell.parentComplete();
    }

    @Override
    public CompletableFuture<Void> retryCleanup() {
        close();
        return cleanupComplete()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(
                        new IllegalStateException("actual archive receiving loan remains retained"));
    }
    /** One original native/fee loan; handoff changes its local close owner without acquiring a second share. */
    static final class OwnerCell {
        final NewOwner owner;
        private boolean parentReleased, runtime, runtimeReleased, ownerClosed;

        OwnerCell(NewOwner owner) {
            this.owner = owner;
        }

        synchronized void acquireRuntime() {
            if (parentReleased || runtime) {
                throw new IllegalStateException(
                        "archive runtime loan already transferred or original acquisition exited");
            }
            owner.requireLocalCurrent();
            runtime = true;
        }

        synchronized boolean runtimeOwned() {
            return runtime && !runtimeReleased;
        }

        synchronized long parentRetainedBytes() {
            return runtimeOwned() ? 0 : owner.retainedBytes();
        }

        synchronized void releaseParent() {
            parentReleased = true;
            closeIfLast();
        }

        synchronized void releaseRuntime() {
            runtimeReleased = true;
            closeIfLast();
        }

        private void closeIfLast() {
            if (parentReleased && (!runtime || runtimeReleased) && !ownerClosed) {
                owner.requestStop();
                owner.close();
                ownerClosed = true;
            }
        }

        synchronized boolean parentComplete() {
            return parentReleased && (runtimeOwned() || ownerClosed && owner.cleanupComplete());
        }

        synchronized boolean runtimeComplete() {
            return runtimeReleased && (!parentReleased || ownerClosed && owner.cleanupComplete());
        }
    }
}
