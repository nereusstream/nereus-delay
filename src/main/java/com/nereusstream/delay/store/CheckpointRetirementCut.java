package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import java.util.Arrays;

/** Only an actual original managed Catalog read can create this complete, bounded old-domain cut. */
public final class CheckpointRetirementCut {
    final OxiaSyncCheckpointPublicationBackend owner;
    final TargetMigrationRoot migration;
    final TargetMigrationRetirementInventory.CatalogDomain domain;
    final CheckpointRetirementAdmissions admissions;

    CheckpointRetirementCut(
            OxiaSyncCheckpointPublicationBackend owner,
            TargetMigrationRoot migration,
            TargetMigrationRetirementInventory.CatalogDomain domain,
            CheckpointRetirementAdmissions admissions) {
        this.owner = owner;
        this.migration = migration;
        this.domain = domain;
        this.admissions = admissions;
    }

    public TargetMigrationRetirementInventory.CatalogDomain domain() {
        return domain;
    }

    public void requireCurrent() {
        owner.requireRetirementCut(this);
    }

    public void requireMigration(TargetMigrationRoot exact) {
        if (!Arrays.equals(exact.canonicalBytes(), migration.canonicalBytes())) {
            throw new IllegalArgumentException("Catalog retirement cut changed its original Source A");
        }
    }
}
