package com.nereusstream.delay.store;

import java.nio.file.Path;

/** Test-only access to the package-local physical candidate primitive from real Worker fixtures. */
public final class TargetCheckpointCandidateTestBridge {
    private TargetCheckpointCandidateTestBridge() {}

    public static Path create(
            final ShardStore store,
            final Path path,
            final byte[] checkpointId,
            final byte[] lineage,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return store.createTargetCheckpointCandidate(
                path, checkpointId, lineage, physicalLimits, quotaLimits, ledgerLimits);
    }

    public static Path reuse(
            final ShardStore store,
            final Path path,
            final byte[] checkpointId,
            final byte[] lineage,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return store.reuseTargetCheckpointCandidate(
                path, checkpointId, lineage, physicalLimits, quotaLimits, ledgerLimits);
    }
}
