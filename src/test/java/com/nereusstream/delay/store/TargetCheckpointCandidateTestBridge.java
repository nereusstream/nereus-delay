package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
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

    public static Path createSemantic(ShardStore store, Path path, byte[] checkpointId, byte[] lineage,
            CheckpointManifestLimits physicalLimits, TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits, TargetCheckpointSemanticSnapshot snapshot,
            TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        return store.createTargetSemanticCheckpointCandidate(path, checkpointId, lineage, physicalLimits,
                quotaLimits, ledgerLimits, snapshot, semanticLimits);
    }

    public static ShardStore restoreTarget(ShardStoreConfig config, ShardId shard, SharedRocksDbResources resources,
            Path image, CheckpointManifest manifest, RecoveryCatalogAuthority catalog, RecoveryPin pin,
            CheckpointManifestLimits physicalLimits, ShardStore.TargetRestoreAuthentication authentication) {
        return ShardStore.restoreTargetFromCheckpoint(config, shard, resources, image, manifest, catalog, pin,
                physicalLimits, authentication);
    }

    /** Actual WorkClass/coordinator/permit/install path with an explicit local file-download fixture. */
    public static ShardStore restoreTargetThroughWorkClass(ShardStoreConfig config, ShardId shard,
            SharedRocksDbResources resources, Path image, CheckpointManifest manifest, RecoveryCatalogAuthority catalog,
            RecoveryPin pin, CheckpointManifestLimits physicalLimits,
            ShardStore.TargetRestoreAuthentication authentication,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry registry) {
        final var profile = new com.nereusstream.delay.protocol.ProfileRef(
                com.nereusstream.delay.protocol.Bytes.utf8("checkpoint-download-fixture"), 1,
                manifest.manifestSha256(), com.nereusstream.delay.protocol.ProfileKind.OBJECT_STORE);
        final var resource = new com.nereusstream.delay.protocol.CheckpointResource(manifest.recoveryLineageId(),
                manifest.checkpointId(), profile, new byte[] {1}, new byte[] {2}, new byte[] {3},
                manifest.canonicalJsonBytes().length, manifest.manifestSha256());
        final var coordinator = new CheckpointRestoreCoordinator(config, shard, resources, (request, target) -> {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    resources::acquireCheckpointDownloadPermit);
            try {
                java.nio.file.Files.createDirectories(target);
                for (var file : request.manifest().files()) {
                    final var copied = target.resolve(file.name());
                    java.nio.file.Files.createDirectories(copied.getParent());
                    java.nio.file.Files.copy(image.resolve(file.name()), copied);
                }
                return target;
            } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
        }, catalog, physicalLimits);
        final var executor = new CheckpointRestoreWorkClassExecutor(registry, coordinator);
        final var request = new CheckpointDownloadRequest(manifest, resource);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.submit(new CheckpointRestoreWorkClassExecutor.RestoreRequest(request, pin)));
        final var submitted = executor.submit(
                new CheckpointRestoreWorkClassExecutor.RestoreRequest(request, pin, authentication));
        org.junit.jupiter.api.Assertions.assertTrue(submitted.outcome().isEmpty());
        registry.runTurn(new com.nereusstream.delay.scheduler.SchedulerBudget(1, submitted.task().bytes(), 1_000_000));
        final var outcome = submitted.outcome().orElseThrow();
        if (outcome.failure() instanceof RuntimeException failure) { throw failure; }
        if (outcome.failure() != null) { throw new AssertionError(outcome.failure()); }
        return outcome.restored();
    }

    /** Explicit exact Catalog/RecoveryPin fixture; neither publishes format2 nor represents a production Catalog. */
    public static RecoveryCatalogAuthority restoreCatalogFixture(CheckpointManifest manifest,
            java.util.concurrent.atomic.AtomicReference<RecoveryPin> pin) {
        return new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(CheckpointManifest candidate, long generation) {
                throw new UnsupportedOperationException("fixture cannot publish");
            }

            @Override
            public RecoveryFloor advanceFloor(byte[] checkpoint, long generation, byte[] cursors) {
                throw new UnsupportedOperationException("fixture cannot advance Floor");
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(byte[] checkpoint) {
                return java.util.Arrays.equals(checkpoint, manifest.checkpointId())
                        ? java.util.Optional.of(manifest) : java.util.Optional.empty();
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(CheckpointManifest candidate) {
                if (!java.util.Arrays.equals(manifest.manifestSha256(), candidate.manifestSha256())) {
                    throw new IllegalStateException("fixture has no exact published candidate");
                }
            }

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(byte[] checkpoint,
                    long sequence, SourcePosition... positions) {
                throw new UnsupportedOperationException("fixture cannot authorize Floor coverage");
            }

            @Override
            public java.util.Optional<RecoveryPin> activeRecoveryPin() {
                return java.util.Optional.ofNullable(pin.get());
            }
        };
    }
}
