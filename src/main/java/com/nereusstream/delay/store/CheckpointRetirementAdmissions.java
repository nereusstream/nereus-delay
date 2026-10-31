package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;

/** Original complete reader/writer/Pin/reference admission ownership; no snapshot can implement these rights. */
public interface CheckpointRetirementAdmissions extends AutoCloseable {
    /**
     * The original acquired epoch covers the entire exact old domain, including older protocols and in-flight admits.
     */
    void requirePrepared(TargetMigrationRoot migration, String namespace, String publicationKey);
    /** Raw original authenticated closure/history material, independently verified by the actual owner. */
    byte[] originalAdmissionClosure();
    /** Makes the original admission hold irreversible only for this actual APPLIED Source intent. */
    void adoptIntent(TargetMigrationRetirementStore.AppliedIntent intent);

    void requireCurrent();
    /** Complete old reader/query/reference/native callback exits; absence or an empty Pin is insufficient. */
    void requireDrained(TargetMigrationRetirementStore.AppliedIntent intent);
    /** Proves this exact original Pin admission attempt has exited and cannot later create a usable Pin. */
    void requirePinAttemptExited(RecoveryPin exact);
    /** Releases only this acquisition; durable Source intent and permanent old-domain closure survive. */
    @Override
    void close();
}
