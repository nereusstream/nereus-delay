package com.nereusstream.delay.ownership;

/** Consumed by the actual independently authorized CONTROL producer at its original SDK handoff. */
public sealed interface TargetMigrationAppendPermit
        permits TargetMigrationProcessDriver.AppendPermit, TargetMigrationMaintenance.AppendPermit {
    default void beforeSdk(TargetMigrationControlWriter writer, TargetMigrationControlMutationFactory.Prepared exact) {
        beforeSdk(writer, exact, writer.copyContract());
    }

    void beforeSdk(
            TargetMigrationControlWriter writer,
            TargetMigrationControlMutationFactory.Prepared exact,
            TargetScopeExposureSource.CopyContract actualCopies);
}
