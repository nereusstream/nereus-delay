package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt;
import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;

/** Actual Catalog CAS/read ownership, never a public completed-retirement callback or receipt DTO. */
public final class CheckpointRetirementBarrier
        implements com.nereusstream.delay.ownership.TargetMigrationMaintenanceInputs.Barrier {
    final OxiaSyncCheckpointPublicationBackend owner;
    final TargetMigrationRetirementStore.AppliedIntent intent;
    final CheckpointRetirementAdmissions admissions;
    final TargetMigrationCatalogBarrierReceipt receipt;

    CheckpointRetirementBarrier(
            OxiaSyncCheckpointPublicationBackend owner,
            TargetMigrationRetirementStore.AppliedIntent intent,
            CheckpointRetirementAdmissions admissions,
            TargetMigrationCatalogBarrierReceipt receipt) {
        this.owner = owner;
        this.intent = intent;
        this.admissions = admissions;
        this.receipt = receipt;
    }

    public TargetMigrationCatalogBarrierReceipt receipt() {
        requireCurrent();
        return receipt;
    }

    @Override
    public void requireIntent(TargetMigrationRetirementStore.AppliedIntent actual) {
        actual.requireCurrent();
        if (!actual.intention().equals(intent.intention()) || !actual.intentAt().equals(intent.intentAt())) {
            throw new IllegalArgumentException("Catalog barrier belongs to another actual Source retirement intent");
        }
        requireCurrent();
    }

    public void requireCurrent() {
        owner.requireRetirementBarrier(this, false);
    }

    public void requireDrained() {
        owner.requireRetirementBarrier(this, true);
    }

    public void requireResource(TargetMigrationRetirementInventory.Resource exact) {
        requireCurrent();
        if (!intent.intention().inventory().resource(exact.id()).equals(exact)) {
            throw new IllegalArgumentException(
                    "checkpoint retirement selected a resource outside the exact Source intent");
        }
    }
}
