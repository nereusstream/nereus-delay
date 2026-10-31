package com.nereusstream.delay.ownership;

/** The original independently authorized CONTROL producer; it is never a normal Worker writer. */
public interface TargetMigrationControlWriter {
    void requireBinding(TargetMigrationSourceOpening original);

    TargetOwnedBackendGroup backend();

    TargetScopeExposureSource.CopyContract copyContract();

    /** The actual native send must consume the permit immediately before entering the SDK. */
    ShardLogMutationAppender.AppendOutcome append(
            TargetMigrationControlMutationFactory.Prepared exact, TargetMigrationAppendPermit permit);

    /** Includes native opening, sends, late callbacks and partially failed cleanup. */
    boolean operationsExited();

    /** Normal producer close, with the original handle retained until every callback has exited. */
    void requestStop();

    boolean cleanupComplete();
}
