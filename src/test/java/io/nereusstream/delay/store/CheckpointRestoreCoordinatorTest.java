package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.EnumMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointRestoreCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void downloadsAndInstallsACompleteCheckpointBeforeCleaningProviderStaging() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 11);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("source"));
        final Path sourceCheckpoint = tempDir.resolve("source-checkpoint");
        final byte[] payload = Bytes.utf8("restore-coordinator-payload");
        final byte[] checkpointId = bytes(16, 1);
        final byte[] lineage = bytes(16, 2);
        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshotFor(shardId);
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), 7, null, 1_007);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = source.metadata().dbIdentity();
            sourceStoreIncarnation = source.metadata().storeIncarnationUuid();
            source.recordControlSnapshot(controlSnapshot);
            source.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64be(0));
                batch.putValue(ColumnFamily.META, 3, Bytes.utf8("restore-key"), payload);
            });
            source.createCheckpoint(sourceCheckpoint, checkpointId);
        }

        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(sourceCheckpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 3), bytes(8, 4), 9, bytes(32, 5));
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                new CheckpointManifest.CreatedAt(1_000, 1_001, "CERTIFIED_HOST_CLOCK", bytes(8, 6), 1, 2, 3,
                        bytes(32, 7), 0, null), shardId, dbIdentity, sourceStoreIncarnation, 1, 0,
                appliedPosition, controlSnapshot.snapshotDigest(), bytes(32, 8), List.of(), files);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, bytes(32, 9),
                ProfileKindV1.OBJECT_STORE);
        final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shardId), lineage, checkpointId, owner, uuidBytes(sourceStoreIncarnation),
                bytes(32, 10), 1, null, null, profile, evidence(1_000), 5_000,
                CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
        final CheckpointManifestLimits limits = new CheckpointManifestLimits(
                128, 64L * 1024 * 1024, 64L * 1024 * 1024, 4096, 4 * 1024 * 1024, 128, 4096);
        final Path objectRoot = tempDir.resolve("object-store");
        final CheckpointResourceV1 resource = new FilesystemCheckpointUploadAdapter(objectRoot, "container", limits)
                .upload(new CheckpointUploadRequest(pending, manifest, sourceCheckpoint,
                        manifest.canonicalJsonBytes()));
        final CheckpointDownloadRequest request = new CheckpointDownloadRequest(manifest, resource);

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            final FilesystemCheckpointDownloadAdapter filesystemDownloader =
                    new FilesystemCheckpointDownloadAdapter(objectRoot, limits);
            final CheckpointRestoreCoordinator coordinator = new CheckpointRestoreCoordinator(restoreConfig, shardId,
                    resources, (downloadRequest, target) -> {
                        assertThrows(IllegalStateException.class, resources::acquireCheckpointDownloadSlot);
                        return filesystemDownloader.download(downloadRequest, target);
                    }, null, limits);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final CheckpointRestoreWorkClassExecutor executor = new CheckpointRestoreWorkClassExecutor(
                    workClasses, coordinator);
            final CheckpointRestoreWorkClassExecutor.Submission submission = executor.submit(
                    new CheckpointRestoreWorkClassExecutor.RestoreRequest(request, null));
            assertTrue(submission.outcome().isEmpty());
            assertEquals(List.of(submission.task()), workClasses.runTurn(
                    new SchedulerBudget(1, submission.task().bytes(), 1_000_000)));
            final CheckpointRestoreWorkClassExecutor.RestoreOutcome outcome = submission.outcome().orElseThrow();
            assertTrue(outcome.failure() == null);
            try (ShardStore restored = outcome.restored()) {
                assertArrayEquals(payload, restored.getValue(ColumnFamily.META, Bytes.utf8("restore-key"), 3).payload());
                assertEquals(checkpointId.length, restored.runtimeMetadata().lastCheckpointId().length);
                assertTrue(Files.isDirectory(restored.dbPath()));
            }
            final Path downloadRoot = restoreConfig.rootPath().resolve("checkpoint-download-tmp");
            try (var entries = Files.list(downloadRoot)) {
                assertEquals(0, entries.count());
            }
        }
    }

    @Test
    void rejectsAProviderPathOutsideTheCoordinatorStagingBoundary() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 12);
        final CheckpointManifest manifest = minimalManifest(shardId);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, bytes(32, 30),
                ProfileKindV1.OBJECT_STORE);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 31), bytes(8, 32), 1, bytes(32, 33));
        final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(new ShardSubjectV1(shardId),
                manifest.recoveryLineageId(), manifest.checkpointId(), owner, uuidBytes(manifest.sourceStoreIncarnation()),
                bytes(32, 34), 1, null, null, profile, evidence(1), 100, CheckpointUploadStateV1.PENDING_UPLOAD,
                1, null, null);
        final CheckpointResourceV1 resource = new CheckpointResourceV1(manifest.recoveryLineageId(),
                manifest.checkpointId(), profile, Bytes.utf8("container"), Bytes.utf8("manifest"),
                Bytes.utf8("version"), manifest.canonicalJsonBytes().length, manifest.manifestSha256());
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("outside"));
        final ShardStoreConfig resourceConfig = ShardStoreConfig.defaults(tempDir.resolve("outside-resources"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(resourceConfig)) {
            final CheckpointRestoreCoordinator coordinator = new CheckpointRestoreCoordinator(
                    restoreConfig, shardId, resources, (request, target) -> tempDir.resolve("outside-return"), null,
                    CheckpointManifestLimits.unbounded());
            assertThrows(IllegalStateException.class,
                    () -> coordinator.restore(new CheckpointDownloadRequest(manifest, resource), null));
        }
    }

    @Test
    void restoreQueueRejectionDoesNotCallProviderOrCreateStaging() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 13);
        final CheckpointManifest manifest = minimalManifest(shardId);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, bytes(32, 70),
                ProfileKindV1.OBJECT_STORE);
        final CheckpointResourceV1 resource = new CheckpointResourceV1(manifest.recoveryLineageId(),
                manifest.checkpointId(), profile, Bytes.utf8("container"), Bytes.utf8("manifest"),
                Bytes.utf8("version"), manifest.canonicalJsonBytes().length, manifest.manifestSha256());
        final Path restoreRoot = tempDir.resolve("queue-rejected-restore");
        final ShardStoreConfig config = ShardStoreConfig.defaults(restoreRoot);
        final AtomicBoolean providerCalled = new AtomicBoolean();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final CheckpointRestoreCoordinator coordinator = new CheckpointRestoreCoordinator(config, shardId,
                    resources, (request, target) -> {
                        providerCalled.set(true);
                        return target;
                    }, null, CheckpointManifestLimits.unbounded());
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            workClasses.submit(new WorkClassTask(WorkClass.CHECKPOINT, "occupied", 1), () -> {
            });
            final CheckpointRestoreWorkClassExecutor executor = new CheckpointRestoreWorkClassExecutor(
                    workClasses, coordinator);
            assertThrows(IllegalStateException.class, () -> executor.submit(
                    new CheckpointRestoreWorkClassExecutor.RestoreRequest(
                            new CheckpointDownloadRequest(manifest, resource), null)));
            assertTrue(!providerCalled.get());
            assertTrue(!Files.exists(restoreRoot.resolve("checkpoint-download-tmp")));
        }
    }

    private static CheckpointManifest minimalManifest(final ShardId shardId) {
        final UUID sourceStore = UUID.randomUUID();
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 40), bytes(8, 41), 1, bytes(32, 42));
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 0,
                null, 1_000);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry(
                "CURRENT", 1, Bytes.sha256(Bytes.utf8("x")), Bytes.utf8("object/CURRENT"), Bytes.utf8("version"), null);
        return new CheckpointManifest(bytes(16, 43), bytes(16, 44), 0, null, null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                new CheckpointManifest.CreatedAt(1, 2, "CERTIFIED_HOST_CLOCK", bytes(8, 45), 1, 2, 3,
                        bytes(32, 46), 0, null), shardId, bytes(32, 47), sourceStore, 1, 0, position,
                bytes(32, 48), bytes(32, 49), List.of(), List.of(file));
    }

    private static CompatibleControlSnapshotV1 controlSnapshotFor(final ShardId shardId) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 50), 1, bytes(32, 51), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 52), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 60), 1, 2, 3,
                bytes(32, 61), 0, null);
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 1_000_000,
                    maxQueueRecords, 1_000_000, 1_000_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), new AtomicLong()::get);
    }
}
