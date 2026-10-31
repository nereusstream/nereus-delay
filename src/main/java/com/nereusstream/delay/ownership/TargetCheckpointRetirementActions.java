package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.ResourceKind;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt;
import com.nereusstream.delay.runtime.ResourceRetireIntentRecord;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import com.nereusstream.delay.store.CheckpointDeleteResult;
import com.nereusstream.delay.store.CheckpointReapingOwnerProof;
import com.nereusstream.delay.store.CheckpointReapingQuiescenceProof;
import com.nereusstream.delay.store.CheckpointReapingSweepCoordinator;
import com.nereusstream.delay.store.CheckpointReapingSweepResult;
import com.nereusstream.delay.store.CheckpointRetirementBarrier;
import com.nereusstream.delay.store.CheckpointRetirementResourceIdentity;
import com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend;
import com.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The existing checkpoint provider performs exact F2 actions on the caller's original WorkClass/owned-I/O turn. */
public final class TargetCheckpointRetirementActions {
    /** Original already acquired provider/Source/delete rights; missing capability must throw before DELETE. */
    public interface OriginalOwners {
        /** Acquires an operation sub-loan; a partial failure retains its exact Cleanup on the original opening. */
        DeleteHold published(
                TargetMigrationRetirementStore.AppliedIntent intent,
                CheckpointRetirementBarrier barrier,
                TargetMigrationRetirementInventory.Resource resource,
                CheckpointRetirementResourceIdentity identity);

        ReapingHold pending(
                TargetMigrationRetirementStore.AppliedIntent intent,
                CheckpointRetirementBarrier barrier,
                TargetMigrationRetirementInventory.Resource resource,
                CheckpointUploadIntent originalPending);
    }

    public interface Hold extends CheckpointInputFuture.Cleanup, AutoCloseable {
        void requireCurrent();
        /** Only additional bytes newly owned by this action; shared parent/client allocations are not charged again. */
        long newlyRetainedBytes();

        boolean ioExited();

        void requestStop();
        /** Local loan close never refunds physical shares or closes shared HTTP/credential/Source parents. */
        @Override
        void close();
    }

    public interface DeleteHold extends Hold {
        /**
         * Actual original APPLIED retire contract and first/POS/protection history, independently authenticated by its
         * original owner.
         */
        ResourceRetireIntentRecord originalRetire();

        void requireOriginalContract(
                TargetMigrationRetirementStore.AppliedIntent intent,
                TargetMigrationRetirementInventory.Resource resource,
                ResourceRetireIntentRecord exact);
        /** Full physical share/retention/provider/no-recreate eligibility, not a new fee grant or a refund. */
        void requireReleaseEligibility(
                TargetMigrationRetirementInventory.Resource resource, CheckpointDeleteResult actual);
    }

    public interface ReapingHold extends Hold {
        CheckpointReapingOwnerProof ownerProof();

        CheckpointReapingQuiescenceProof quiescence();

        int maximumVersions();

        void requireOriginalPending(
                TargetMigrationRetirementStore.AppliedIntent intent,
                TargetMigrationRetirementInventory.Resource resource,
                CheckpointUploadIntent exact);

        void requireReleaseEligibility(
                TargetMigrationRetirementInventory.Resource resource, CheckpointReapingSweepResult actual);
    }

    private final OxiaSyncCheckpointPublicationBackend catalog;
    private final RenewableS3CompatibleCheckpointObjectStoreAdapter provider;
    private final OriginalOwners owners;

    /** Borrows the real original adapter. It creates no client, executor, queue or replacement operation ledger. */
    public TargetCheckpointRetirementActions(
            OxiaSyncCheckpointPublicationBackend catalog,
            RenewableS3CompatibleCheckpointObjectStoreAdapter provider,
            OriginalOwners owners) {
        this.catalog = Objects.requireNonNull(catalog);
        this.provider = Objects.requireNonNull(provider);
        this.owners = Objects.requireNonNull(owners);
    }

    public TargetMigrationMaintenanceInputs.ResourceAction open(
            TargetMigrationMaintenance.Opening opening,
            TargetMigrationRetirementStore.AppliedIntent intent,
            TargetMigrationMaintenanceInputs.Barrier supplied,
            TargetMigrationRetirementInventory.Resource resource) {
        if (!(supplied instanceof CheckpointRetirementBarrier barrier)) {
            throw new IllegalArgumentException("checkpoint action requires the actual original Catalog CAS capability");
        }
        barrier.requireIntent(intent);
        barrier.requireResource(resource);
        barrier.requireDrained();
        if (resource.disposition() != TargetMigrationRetirementInventory.Disposition.RETIRE) {
            throw new IllegalStateException(
                    ("checkpoint with retained/transfer duties requires its real original custody "
                            + "owner, not physical deletion"));
        }
        final var identity = CheckpointRetirementResourceIdentity.decode(resource.exactIdentity());
        if (resource.kind() != TargetMigrationRetirementInventory.Kind.CHECKPOINT
                && !(identity.artifact()
                        && (resource.kind() == TargetMigrationRetirementInventory.Kind.HISTORY
                                || resource.kind() == TargetMigrationRetirementInventory.Kind.CUSTODY))) {
            throw new IllegalArgumentException("resource is outside the exact checkpoint/artifact provider action");
        }
        final var action = new Action(intent, barrier, resource, identity);
        opening.retainResourceAction(action);
        return action;
    }

    private final class Action implements TargetMigrationMaintenanceInputs.ResourceAction {
        private final TargetMigrationRetirementStore.AppliedIntent intent;
        private final CheckpointRetirementBarrier barrier;
        private final TargetMigrationRetirementInventory.Resource resource;
        private final CheckpointRetirementResourceIdentity identity;
        private final byte[] operationId;
        private Hold held;
        private byte[] originalContract;
        private TargetMigrationRetirementResourceReceipt result;
        private boolean stopping, closeRequested;
        private int running;
        private CompletableFuture<Void> cleanup;

        private Action(
                TargetMigrationRetirementStore.AppliedIntent intent,
                CheckpointRetirementBarrier barrier,
                TargetMigrationRetirementInventory.Resource resource,
                CheckpointRetirementResourceIdentity identity) {
            this.intent = intent;
            this.barrier = barrier;
            this.resource = resource;
            this.identity = identity;
            operationId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-f2-checkpoint-operation-v1\0"),
                    intent.intention().digest(),
                    resource.id(),
                    resource.digest());
        }

        @Override
        public void requireBound(
                TargetMigrationRetirementStore.AppliedIntent actual,
                TargetMigrationMaintenanceInputs.Barrier actualBarrier,
                TargetMigrationRetirementInventory.Resource exact) {
            if (actualBarrier != barrier
                    || !actual.intention().equals(intent.intention())
                    || !actual.intentAt().equals(intent.intentAt())
                    || !resource.equals(exact)) {
                throw new IllegalArgumentException(
                        "checkpoint action changed exact Source intent/resource/Catalog ownership");
            }
            barrier.requireIntent(actual);
            barrier.requireResource(exact);
        }

        @Override
        public void advance(Runnable current) {
            synchronized (this) {
                if (stopping) {
                    return;
                }
                if (result != null) {
                    return;
                }
                running++;
            }
            try {
                current.run();
                barrier.requireDrained();
                intent.requireCurrent();
                if (held == null) {
                    held = identity.pending() == null
                            ? Objects.requireNonNull(owners.published(intent, barrier, resource, identity))
                            : Objects.requireNonNull(owners.pending(intent, barrier, resource, identity.pending()));
                    held.requireCurrent();
                    if (held.newlyRetainedBytes() < 0) {
                        throw new IllegalStateException("negative original action byte reservation");
                    }
                    if (held instanceof DeleteHold deletion) {
                        final var original = Objects.requireNonNull(deletion.originalRetire());
                        deletion.requireOriginalContract(intent, resource, original);
                        if (original.resourceKind() != ResourceKind.CHECKPOINT
                                || original.appliedMutationSequence() == 0
                                || !Arrays.equals(
                                        original.resourceIdentity(),
                                        identity.published().exactResourceCanonicalBytes())
                                || !SourcePositionCodec.decode(original.appliedSourcePosition())
                                        .sameSourceIdentity(intent.intentAt().source())) {
                            throw new IllegalStateException(
                                    "published checkpoint has no matching actual original Source retire contract");
                        }
                        originalContract = original.encode();
                    } else if (held instanceof ReapingHold reaping) {
                        reaping.requireOriginalPending(intent, resource, identity.pending());
                        originalContract = identity.pending().canonicalBytes();
                        if (reaping.maximumVersions() < 1 || reaping.maximumVersions() == Integer.MAX_VALUE) {
                            throw new IllegalStateException(
                                    "pending checkpoint sweep requires an actual finite version bound");
                        }
                    } else {
                        throw new IllegalStateException("original owner supplied the wrong checkpoint action contract");
                    }
                    requireEvidenceCapacity();
                    current.run();
                    return;
                }
                held.requireCurrent();
                final byte[] evidence;
                if (held instanceof DeleteHold deletion) {
                    final var actual = identity.artifact()
                            ? provider.deleteMigrationObject(identity.published())
                            : provider.delete(catalog.retiringPublishedCheckpoint(barrier, identity.published()));
                    deletion.requireReleaseEligibility(resource, actual);
                    held.requireCurrent();
                    evidence = CanonicalProtobuf.message(out -> {
                        CanonicalProtobuf.uint32(out, 1, 1);
                        CanonicalProtobuf.uint32(out, 2, identity.artifact() ? 3 : 1);
                        CanonicalProtobuf.bytes(out, 3, operationId);
                        CanonicalProtobuf.bytes(out, 4, originalContract);
                        CanonicalProtobuf.bytes(out, 5, actual.resource().canonicalBytes());
                        CanonicalProtobuf.uint32(out, 6, actual.outcome().wireValue());
                        CanonicalProtobuf.bytes(out, 7, actual.providerRequestIdHash());
                        CanonicalProtobuf.bytes(out, 8, actual.responseHash());
                    });
                } else {
                    final var reaping = (ReapingHold) held;
                    reaping.requireOriginalPending(intent, resource, identity.pending());
                    final var actual = new CheckpointReapingSweepCoordinator(catalog, provider)
                            .reap(
                                    identity.pending(),
                                    catalog,
                                    reaping.ownerProof(),
                                    reaping.quiescence(),
                                    reaping.maximumVersions());
                    if (!actual.prefixSweep().emptyAfterSweep()) {
                        throw new IllegalStateException("original pending prefix is not empty");
                    }
                    reaping.requireReleaseEligibility(resource, actual);
                    held.requireCurrent();
                    evidence = CanonicalProtobuf.message(out -> {
                        CanonicalProtobuf.uint32(out, 1, 1);
                        CanonicalProtobuf.uint32(out, 2, 2);
                        CanonicalProtobuf.bytes(out, 3, operationId);
                        CanonicalProtobuf.bytes(out, 4, originalContract);
                        CanonicalProtobuf.bytes(out, 5, actual.reapingIntent().canonicalBytes());
                        CanonicalProtobuf.uint32(out, 6, actual.prefixSweep().listedVersionCount());
                        CanonicalProtobuf.uint32(out, 7, actual.prefixSweep().deletedVersionCount());
                        CanonicalProtobuf.bytes(out, 8, actual.prefixSweep().providerRequestIdHash());
                        CanonicalProtobuf.bytes(out, 9, actual.prefixSweep().responseHash());
                    });
                }
                current.run();
                barrier.requireDrained();
                result = new TargetMigrationRetirementResourceReceipt(
                        resource.id(),
                        resource.disposition(),
                        resource.digest(),
                        operationId,
                        resource.originalOwner(),
                        resource.heldUsage(),
                        evidence,
                        null,
                        List.of());
                result.requireResource(resource);
            } finally {
                synchronized (this) {
                    running--;
                }
            }
        }

        private void requireEvidenceCapacity() {
            final byte[] encoded;
            if (held instanceof DeleteHold) {
                encoded = CanonicalProtobuf.message(out -> {
                    CanonicalProtobuf.uint32(out, 1, 1);
                    CanonicalProtobuf.uint32(out, 2, identity.artifact() ? 3 : 1);
                    CanonicalProtobuf.bytes(out, 3, operationId);
                    CanonicalProtobuf.bytes(out, 4, originalContract);
                    CanonicalProtobuf.bytes(out, 5, identity.published().canonicalBytes());
                    CanonicalProtobuf.uint32(out, 6, 2);
                    CanonicalProtobuf.bytes(out, 7, new byte[32]);
                    CanonicalProtobuf.bytes(out, 8, new byte[32]);
                });
            } else {
                final var hold = (ReapingHold) held;
                final var p = identity.pending();
                final var reaping = new CheckpointUploadIntent(
                        p.shard(),
                        p.recoveryLineageId(),
                        p.checkpointId(),
                        p.owner(),
                        p.sourceStoreIncarnation(),
                        p.uploadToken(),
                        p.baseCatalogGeneration(),
                        p.parentCheckpointId(),
                        p.parentManifestSha256(),
                        p.objectStoreProfile(),
                        p.checkpointCreatedAt(),
                        p.uploadDeadlineEpochMs(),
                        CheckpointUploadState.REAPING,
                        Math.addExact(p.stateRevision(), 1),
                        null,
                        hold.quiescence().reapingEvidence());
                encoded = CanonicalProtobuf.message(out -> {
                    CanonicalProtobuf.uint32(out, 1, 1);
                    CanonicalProtobuf.uint32(out, 2, 2);
                    CanonicalProtobuf.bytes(out, 3, operationId);
                    CanonicalProtobuf.bytes(out, 4, originalContract);
                    CanonicalProtobuf.bytes(out, 5, reaping.canonicalBytes());
                    CanonicalProtobuf.uint32(out, 6, hold.maximumVersions());
                    CanonicalProtobuf.uint32(out, 7, hold.maximumVersions());
                    CanonicalProtobuf.bytes(out, 8, new byte[32]);
                    CanonicalProtobuf.bytes(out, 9, new byte[32]);
                });
            }
            if (encoded.length > TargetMigrationRetirementResourceReceipt.MAX_EVIDENCE_BYTES) {
                throw new IllegalStateException(
                        ("complete original checkpoint contract exceeds this confirmation operation's "
                                + "evidence capacity"));
            }
        }

        @Override
        public synchronized Optional<TargetMigrationRetirementResourceReceipt> terminalReceipt() {
            return running == 0 && (held == null || held.ioExited()) ? Optional.ofNullable(result) : Optional.empty();
        }

        @Override
        public synchronized long retainedBytes() {
            long bytes = operationId.length + (originalContract == null ? 0 : originalContract.length);
            if (result != null) {
                bytes = Math.addExact(bytes, result.canonicalBytes().length);
            }
            if (held != null) {
                bytes = Math.addExact(bytes, held.newlyRetainedBytes());
            }
            return bytes;
        }

        @Override
        public synchronized boolean ioExited() {
            return running == 0 && (held == null || held.ioExited()) && CheckpointInputFuture.ioExited(cleanup);
        }

        @Override
        public synchronized void requestStop() {
            stopping = true;
            if (held != null) {
                held.requestStop();
            }
        }

        @Override
        public void close() {
            requestStop();
            synchronized (this) {
                if (running != 0 || held == null) {
                    closeRequested = running == 0;
                    return;
                }
                if (!closeRequested) {
                    closeRequested = true;
                    held.close();
                }
                if (!held.cleanupComplete() && CheckpointInputFuture.ioExited(cleanup)) {
                    cleanup = Objects.requireNonNull(held.retryCleanup());
                }
            }
        }

        @Override
        public synchronized boolean cleanupComplete() {
            return closeRequested && ioExited() && (held == null || held.cleanupComplete());
        }

        @Override
        public CompletableFuture<Void> retryCleanup() {
            close();
            return cleanupComplete()
                    ? CompletableFuture.completedFuture(null)
                    : CompletableFuture.failedFuture(
                            new IllegalStateException("original checkpoint action remains retained"));
        }
    }
}
