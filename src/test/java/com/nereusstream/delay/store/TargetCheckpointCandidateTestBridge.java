package com.nereusstream.delay.store;

import java.nio.file.Path;

/** Test-only access to the package-local physical candidate primitive from real Worker fixtures. */
public final class TargetCheckpointCandidateTestBridge {
    private TargetCheckpointCandidateTestBridge() {}

    public static Path create(
            final ShardStore store,
            final Path path,
            final byte[] checkpointId,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return store.createTargetCheckpointCandidate(path, checkpointId, physicalLimits, quotaLimits, ledgerLimits);
    }
}
