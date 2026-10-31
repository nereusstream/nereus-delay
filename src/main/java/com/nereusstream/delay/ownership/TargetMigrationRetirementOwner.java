package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointRetirementAdmissions;
import com.nereusstream.delay.store.CheckpointRetirementBarrier;
import com.nereusstream.delay.store.CheckpointRetirementCut;
import com.nereusstream.delay.store.CheckpointRetirementResourceIdentity;
import com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend;
import java.util.List;
import java.util.Objects;

/** Actual Catalog and original provider composition used by the live F2 caller. */
public final class TargetMigrationRetirementOwner implements TargetMigrationMaintenanceInputs.Retirement {
    /** Original non-checkpoint components/receiver custodians, acquired by exact immutable resource identity. */
    public interface Resources {
        TargetMigrationMaintenanceInputs.ResourceAction open(
                TargetMigrationMaintenance.Opening opening,
                TargetMigrationRetirementStore.AppliedIntent intent,
                CheckpointRetirementBarrier barrier,
                TargetMigrationRetirementInventory.Resource exact,
                Runnable current);
        /** Raw complete noFuture/Pin/query/reader/retention/Source/fee material, independently authenticated. */
        TargetMigrationRetirementConfirmRequest.Closure closure(
                TargetMigrationRetirementStore.AppliedIntent intent,
                CheckpointRetirementBarrier barrier,
                List<TargetMigrationRetirementResourceReceipt> actual,
                BoundedReadBudget reads,
                Runnable current);
        /** Exact source-confirmed legacy child-loan return; shared parent clients and Inputs remain owned. */
        void releaseLegacySubLoan(TargetMigrationRetirementStore.AppliedConfirmation confirmation, Runnable current);
    }

    private final OxiaSyncCheckpointPublicationBackend catalog;
    private final CheckpointRetirementAdmissions admissions;
    private final TargetCheckpointRetirementActions checkpoints;
    private final Resources resources;
    private CheckpointRetirementCut cut;
    private CheckpointRetirementBarrier actualBarrier;

    public TargetMigrationRetirementOwner(
            OxiaSyncCheckpointPublicationBackend catalog,
            CheckpointRetirementAdmissions admissions,
            TargetCheckpointRetirementActions checkpoints,
            Resources resources) {
        this.catalog = Objects.requireNonNull(catalog);
        this.admissions = Objects.requireNonNull(admissions);
        this.checkpoints = Objects.requireNonNull(checkpoints);
        this.resources = Objects.requireNonNull(resources);
    }
    /** Capture the actual full member cut before signing; supplied resources still need original-owner authentication. */
    public TargetMigrationRetirementInventory inventory(
            TargetMigrationRoot actual,
            List<TargetMigrationRetirementInventory.Resource> exactResources,
            BoundedReadBudget reads,
            Runnable current) {
        current.run();
        if (cut == null) {
            cut = catalog.captureRetirementCut(actual, admissions, reads);
        }
        cut.requireMigration(actual);
        current.run();
        return new TargetMigrationRetirementInventory(cut.domain(), exactResources);
    }

    @Override
    public CheckpointRetirementBarrier closeAdmissions(
            TargetMigrationRetirementStore.AppliedIntent actual, Runnable current) {
        current.run();
        actual.requireCurrent();
        final var barrier =
                cut == null ? catalog.resumeRetirement(actual, admissions) : catalog.adoptRetirement(actual, cut);
        barrier.requireIntent(actual);
        current.run();
        actualBarrier = barrier;
        return barrier;
    }

    @Override
    public TargetMigrationMaintenanceInputs.ResourceAction openResource(
            TargetMigrationMaintenance.Opening opening,
            TargetMigrationRetirementStore.AppliedIntent actual,
            TargetMigrationMaintenanceInputs.Barrier held,
            TargetMigrationRetirementInventory.Resource exact,
            Runnable current) {
        final var barrier = requireBarrier(actual, held);
        current.run();
        if (exact.disposition() == TargetMigrationRetirementInventory.Disposition.RETIRE
                && (exact.kind() == TargetMigrationRetirementInventory.Kind.CHECKPOINT || checkpointArtifact(exact))) {
            return checkpoints.open(opening, actual, barrier, exact);
        }
        return Objects.requireNonNull(
                resources.open(opening, actual, barrier, exact, current), "actual original resource owner/action");
    }

    private static boolean checkpointArtifact(TargetMigrationRetirementInventory.Resource exact) {
        if (exact.kind() != TargetMigrationRetirementInventory.Kind.HISTORY
                && exact.kind() != TargetMigrationRetirementInventory.Kind.CUSTODY) {
            return false;
        }
        try {
            return CheckpointRetirementResourceIdentity.decode(exact.exactIdentity())
                    .artifact();
        } catch (IllegalArgumentException differentOriginalIdentity) {
            return false;
        }
    }

    @Override
    public TargetMigrationRetirementConfirmRequest.Closure closure(
            TargetMigrationRetirementStore.AppliedIntent actual,
            TargetMigrationMaintenanceInputs.Barrier held,
            List<TargetMigrationRetirementResourceReceipt> actualReceipts,
            BoundedReadBudget reads,
            Runnable current) {
        final var barrier = requireBarrier(actual, held);
        barrier.requireDrained();
        current.run();
        final var inventory = actual.intention().inventory().resources();
        if (actualReceipts.size() != inventory.size()) {
            throw new IllegalStateException("original resource coverage remains incomplete");
        }
        for (int i = 0; i < inventory.size(); i++) {
            actualReceipts.get(i).requireResource(inventory.get(i));
        }
        final var closure = Objects.requireNonNull(resources.closure(actual, barrier, actualReceipts, reads, current));
        barrier.requireDrained();
        current.run();
        return closure;
    }

    @Override
    public void adoptConfirmed(
            TargetMigrationRetirementStore.AppliedConfirmation actual,
            TargetMigrationMaintenanceInputs.Barrier held,
            Runnable current) {
        final var barrier = requireBarrier(actual.intent(), held);
        barrier.requireDrained();
        current.run();
        catalog.adoptRetired(actual, barrier);
        current.run();
    }

    @Override
    public void releaseLegacySubLoan(TargetMigrationRetirementStore.AppliedConfirmation actual, Runnable current) {
        actual.requireCurrent();
        current.run();
        resources.releaseLegacySubLoan(actual, current);
        current.run();
    }

    private CheckpointRetirementBarrier requireBarrier(
            TargetMigrationRetirementStore.AppliedIntent intent, TargetMigrationMaintenanceInputs.Barrier held) {
        if (!(held instanceof CheckpointRetirementBarrier actual) || actual != actualBarrier) {
            throw new IllegalArgumentException("actual original managed Catalog barrier required");
        }
        actual.requireIntent(intent);
        actual.requireCurrent();
        return actual;
    }
}
