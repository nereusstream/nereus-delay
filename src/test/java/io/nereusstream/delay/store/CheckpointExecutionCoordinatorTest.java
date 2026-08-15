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
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointExecutionCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void checkpointPipelineActionsAreNotPublicProductionApi() {
        assertNamedMethodsAreNotPublic(CheckpointExecutionCoordinator.class, "execute");
        assertNamedMethodsAreNotPublic(CheckpointPublicationCoordinator.class, "publish");
        assertNamedMethodsAreNotPublic(CheckpointUploadCoordinator.class, "upload");
        assertFalse(Arrays.stream(CheckpointWorkClassExecutor.ExecutionRequest.class.getRecordComponents())
                .anyMatch(component -> component.getName().equals("workClassBytes")));
    }

    @Test
    void executionRejectsPublicationBoundToAnotherWorkerResourceEnvelope() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final ShardStoreConfig storeConfig = ShardStoreConfig.defaults(tempDir.resolve("execution-store-resources"));
        final ShardStoreConfig foreignConfig = ShardStoreConfig.defaults(
                tempDir.resolve("execution-foreign-resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        try (SharedRocksDbResources storeResources = new SharedRocksDbResources(storeConfig);
             SharedRocksDbResources foreignResources = new SharedRocksDbResources(foreignConfig);
             ShardStore store = ShardStore.open(storeConfig, shard, storeResources)) {
            final CheckpointPublicationCoordinator foreignPublication = new CheckpointPublicationCoordinator(
                    foreignResources, new CheckpointUploadIntentStore(), new RecoveryCatalog());

            assertThrows(IllegalArgumentException.class,
                    () -> new CheckpointExecutionCoordinator(scheduler, store, foreignPublication));
            assertFalse(store.isCloseStarted());
        }
    }

    @Test
    void retriesSamePhysicalCheckpointAfterCatalogResponseLoss() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        scheduler.register(shard, 0);
        final CheckpointScheduler.ScheduledCheckpoint firstClaim = scheduler.claimDue(100, 1).get(0);
        final Path checkpointDirectory = tempDir.resolve("checkpoint");
        final byte[] lineage = bytes(16, 1);
        final byte[] checkpointId = bytes(16, 2);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 3), bytes(8, 4), 42, bytes(32, 5));
        final ProfileRefV1 objectStore = new ProfileRefV1(bytes(32, 6), 1, bytes(32, 7),
                ProfileKindV1.OBJECT_STORE);
        final UUID topicUuid = UUID.randomUUID();
        final KafkaSourcePosition parentPosition = new KafkaSourcePosition(shard, "cluster", topicUuid,
                0, null, 900);
        final KafkaSourcePosition childPosition = new KafkaSourcePosition(shard, "cluster", topicUuid,
                1, null, 901);
        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            store.recordControlSnapshot(controlSnapshot);
            // Seed the durable source barrier that a production Shard Log
            // apply would write in the same batch as the command mutation.
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), childPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(1));
            });

            final CheckpointManifest parent = parentManifest(store, shard, lineage, parentPosition, owner,
                    controlSnapshot);
            final ResponseLossCatalog catalog = new ResponseLossCatalog(new RecoveryCatalog());
            catalog.delegate.publish(parent, 0);
            final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard.routeIncarnation(), shard.partition()), lineage, checkpointId, owner,
                    uuidBytes(store.metadata().storeIncarnationUuid()), bytes(32, 8), 1,
                    parent.checkpointId(), parent.manifestSha256(), objectStore, evidence(900), 5_000,
                    CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
            final CheckpointUploadIntentStore intents = new CheckpointUploadIntentStore();
            intents.create(pending);
            final CheckpointPublicationCoordinator publication = new CheckpointPublicationCoordinator(resources,
                    intents, catalog);
            final CheckpointExecutionCoordinator coordinator = new CheckpointExecutionCoordinator(scheduler, store,
                    publication);
            final AtomicBoolean retryAdapterCalled = new AtomicBoolean();

            assertThrows(IllegalStateException.class, () -> coordinator.execute(firstClaim, checkpointDirectory,
                    pending, (directory, currentStore) -> childManifest(directory, currentStore, pending, parent,
                            owner, controlSnapshot), 1_000, () -> 100, request -> resource(request, pending,
                            objectStore)));
            assertTrue(Files.isDirectory(checkpointDirectory));
            assertFalse(scheduler.isInFlight(shard));

            final CheckpointScheduler.ScheduledCheckpoint retryClaim = scheduler.claimDue(200, 1).get(0);
            final CheckpointExecutionCoordinator.ExecutionResult result = coordinator.execute(retryClaim,
                    checkpointDirectory, pending, (directory, currentStore) -> childManifest(directory, currentStore,
                            pending, parent, owner, controlSnapshot), 5_000, () -> 200, request -> {
                                retryAdapterCalled.set(true);
                                throw new AssertionError("a published upload must not call the provider again");
                            });
            assertTrue(result.reusedExistingDirectory());
            assertEquals(CheckpointUploadStateV1.PUBLISHED, result.publication().uploadIntent().state());
            assertEquals(300, result.nextDueEpochMs());
            assertFalse(retryAdapterCalled.get());
            assertArrayEquals(checkpointId, result.manifest().checkpointId());
        }
    }

    @Test
    void executionFailureRemainsPrimaryWhenClaimCompletionAlsoFails() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("completion-failure-resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        scheduler.register(shard, 0);
        final CheckpointScheduler.ScheduledCheckpoint claim = scheduler.claimDue(100, 1).get(0);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 30), bytes(8, 31), 42, bytes(32, 32));
            final ProfileRefV1 objectStore = new ProfileRefV1(bytes(32, 33), 1, bytes(32, 34),
                    ProfileKindV1.OBJECT_STORE);
            final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard), bytes(16, 35), bytes(16, 36), owner,
                    uuidBytes(store.metadata().storeIncarnationUuid()), bytes(32, 37), 1,
                    null, null, objectStore, evidence(100), 5_000,
                    CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
            final CheckpointExecutionCoordinator coordinator = new CheckpointExecutionCoordinator(scheduler, store,
                    new CheckpointPublicationCoordinator(resources, new CheckpointUploadIntentStore(),
                            new RecoveryCatalog()));

            final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> coordinator.execute(claim, tempDir.resolve("completion-failure-checkpoint"), pending,
                            (directory, currentStore) -> {
                                throw new AssertionError("negative upload time must fail before manifest creation");
                            }, -1, () -> 99, request -> {
                                throw new AssertionError("negative upload time must fail before provider I/O");
                            }));

            assertEquals("uploadNowEpochMs must be non-negative", failure.getMessage());
            assertEquals(1, failure.getSuppressed().length);
            assertEquals("checkpoint completion precedes its claim due time",
                    failure.getSuppressed()[0].getMessage());
            assertTrue(scheduler.isInFlight(shard));
            assertEquals(200, scheduler.complete(claim, 100));
        }
    }

    @Test
    void checkpointWorkClassRejectsBeforeIoThenExecutesTheExactClaim() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("work-class-resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        scheduler.register(shard, 0);
        final CheckpointScheduler.ScheduledCheckpoint claim = scheduler.claimDue(100, 1).get(0);
        final Path checkpointDirectory = tempDir.resolve("work-class-checkpoint");
        final byte[] lineage = bytes(16, 40);
        final byte[] checkpointId = bytes(16, 41);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 42), bytes(8, 43), 44, bytes(32, 44));
        final ProfileRefV1 objectStore = new ProfileRefV1(bytes(32, 45), 1, bytes(32, 46),
                ProfileKindV1.OBJECT_STORE);
        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
        final UUID topicUuid = UUID.randomUUID();
        final KafkaSourcePosition parentPosition = new KafkaSourcePosition(shard, "cluster", topicUuid,
                0, null, 901);
        final KafkaSourcePosition applied = new KafkaSourcePosition(shard, "cluster", topicUuid,
                1, null, 902);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            store.recordControlSnapshot(controlSnapshot);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), applied.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(1));
            });
            final CheckpointManifest parent = parentManifest(store, shard, lineage, parentPosition, owner,
                    controlSnapshot);
            final RecoveryCatalog catalog = new RecoveryCatalog();
            catalog.publish(parent, 0);
            final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard), lineage, checkpointId, owner,
                    uuidBytes(store.metadata().storeIncarnationUuid()), bytes(32, 47), 1,
                    parent.checkpointId(), parent.manifestSha256(), objectStore, evidence(1_000), 5_000,
                    CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
            final CheckpointUploadIntentStore intents = new CheckpointUploadIntentStore();
            intents.create(pending);
            final CheckpointExecutionCoordinator coordinator = new CheckpointExecutionCoordinator(scheduler, store,
                    new CheckpointPublicationCoordinator(resources, intents, catalog));
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final CheckpointWorkClassExecutor executor = new CheckpointWorkClassExecutor(workClasses, coordinator);
            final AtomicBoolean adapterCalled = new AtomicBoolean();
            final CheckpointWorkClassExecutor.ExecutionRequest firstRequest =
                    new CheckpointWorkClassExecutor.ExecutionRequest(claim, checkpointDirectory, pending,
                            (directory, currentStore) -> childManifest(directory, currentStore, pending, parent,
                                    owner, controlSnapshot),
                            1_000, () -> 100, upload -> {
                                adapterCalled.set(true);
                                return resource(upload, pending, objectStore);
                            });

            workClasses.submit(new WorkClassTask(WorkClass.CHECKPOINT, "occupied", 8), () -> {
            });
            assertThrows(IllegalStateException.class, () -> executor.submit(firstRequest));
            assertFalse(adapterCalled.get());
            assertFalse(Files.exists(checkpointDirectory));
            assertTrue(scheduler.isInFlight(shard));
            assertEquals(1, workClasses.registeredActions());

            workClasses.runTurn(new SchedulerBudget(1, 8, 1_000));
            assertThrows(IllegalArgumentException.class,
                    () -> new CheckpointWorkClassExecutor.ExecutionRequest(
                            claim, checkpointDirectory, pending, (directory, currentStore) -> null,
                            -1, () -> 100, upload -> null));
            final CheckpointWorkClassExecutor.ExecutionRequest failingRequest =
                    new CheckpointWorkClassExecutor.ExecutionRequest(claim, checkpointDirectory, pending,
                            (directory, currentStore) -> {
                                throw new IllegalStateException("manifest factory failed");
                            }, 1_000, () -> 100, upload -> {
                                throw new AssertionError("manifest failure must precede provider I/O");
                            });
            final CheckpointWorkClassExecutor.Submission failed = executor.submit(failingRequest);
            assertTrue(failed.task().bytes() > 8);
            assertEquals(List.of(failed.task()),
                    workClasses.runTurn(new SchedulerBudget(1, failed.task().bytes(), 1_000)));
            final CheckpointWorkClassExecutor.AttemptOutcome failedOutcome = failed.outcome().orElseThrow();
            assertTrue(failedOutcome.result() == null);
            assertEquals("manifest factory failed", failedOutcome.failure().getMessage());
            assertFalse(scheduler.isInFlight(shard));
            assertEquals(0, workClasses.registeredActions());
            assertTrue(Files.isDirectory(checkpointDirectory));

            final CheckpointScheduler.ScheduledCheckpoint retryClaim = scheduler.claimDue(200, 1).get(0);
            final CheckpointWorkClassExecutor.ExecutionRequest retryRequest =
                    new CheckpointWorkClassExecutor.ExecutionRequest(retryClaim, checkpointDirectory, pending,
                            (directory, currentStore) -> childManifest(directory, currentStore, pending, parent,
                                    owner, controlSnapshot),
                            1_000, () -> 200, upload -> {
                                adapterCalled.set(true);
                                return resource(upload, pending, objectStore);
                            });
            final CheckpointWorkClassExecutor.Submission submitted = executor.submit(retryRequest);
            assertTrue(submitted.outcome().isEmpty());
            assertEquals(List.of(submitted.task()),
                    workClasses.runTurn(new SchedulerBudget(1, submitted.task().bytes(), 1_000)));

            final CheckpointWorkClassExecutor.AttemptOutcome outcome = submitted.outcome().orElseThrow();
            assertTrue(outcome.failure() == null);
            assertEquals(300, outcome.result().nextDueEpochMs());
            assertTrue(adapterCalled.get());
            assertTrue(Files.isDirectory(checkpointDirectory));
            assertFalse(scheduler.isInFlight(shard));
            assertEquals(0, workClasses.registeredActions());
        }
    }

    @Test
    void workerCheckpointRuntimeReleasesClaimWhenPrerequisiteChangesAfterQueueAdmission() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("runtime-resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final Path checkpointDirectory = tempDir.resolve("runtime-checkpoint");
        final byte[] lineage = bytes(16, 50);
        final byte[] checkpointId = bytes(16, 51);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 52), bytes(8, 53), 54, bytes(32, 55));
        final ProfileRefV1 objectStore = new ProfileRefV1(bytes(32, 56), 1, bytes(32, 57),
                ProfileKindV1.OBJECT_STORE);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard), lineage, checkpointId, owner,
                    uuidBytes(store.metadata().storeIncarnationUuid()), bytes(32, 58), 1,
                    null, null, objectStore, evidence(100), 5_000,
                    CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final AtomicInteger gateCalls = new AtomicInteger();
            final WorkerCheckpointRuntime runtime = new WorkerCheckpointRuntime(workClasses, scheduler, store,
                    new CheckpointPublicationCoordinator(resources, new CheckpointUploadIntentStore(),
                            new RecoveryCatalog()), request -> {
                                if (gateCalls.incrementAndGet() == 2) {
                                    throw new IllegalStateException("Owner session changed while queued");
                                }
                            });
            runtime.register(shard, 0);
            final CheckpointScheduler.ScheduledCheckpoint claim = runtime.claimDue(100, 1).get(0);
            final CheckpointWorkClassExecutor.ExecutionRequest request =
                    new CheckpointWorkClassExecutor.ExecutionRequest(claim, checkpointDirectory, pending,
                            (directory, currentStore) -> {
                                throw new AssertionError("prerequisite failure must precede checkpoint I/O");
                            }, 100, () -> 100, upload -> {
                                throw new AssertionError("prerequisite failure must precede provider I/O");
                            });

            final CheckpointWorkClassExecutor.Submission submitted = runtime.submit(request);
            assertEquals(1, gateCalls.get());
            assertEquals(List.of(submitted.task()), runtime.runTurn(new SchedulerBudget(1,
                    submitted.task().bytes(), 1_000)));
            assertEquals(2, gateCalls.get());
            assertEquals("Owner session changed while queued",
                    submitted.outcome().orElseThrow().failure().getMessage());
            assertFalse(Files.exists(checkpointDirectory));
            assertFalse(runtime.scheduler().isInFlight(shard));
            assertEquals(0, workClasses.registeredActions());

            assertEquals(1, runtime.claimDue(200, 1).size(),
                    "deferred prerequisite failure must leave the exact schedule retryable");
        }
    }

    @Test
    void checkpointPreflightFailureReleasesClaimForTheNextSchedule() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 22);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("preflight-release-resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final Path checkpointDirectory = tempDir.resolve("preflight-release-checkpoint");
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 60), bytes(8, 61), 62, bytes(32, 63));
        final ProfileRefV1 objectStore = new ProfileRefV1(bytes(32, 64), 1, bytes(32, 65),
                ProfileKindV1.OBJECT_STORE);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard), bytes(16, 66), bytes(16, 67), owner,
                    uuidBytes(store.metadata().storeIncarnationUuid()), bytes(32, 68), 1,
                    null, null, objectStore, evidence(100), 5_000,
                    CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final WorkerCheckpointRuntime runtime = new WorkerCheckpointRuntime(workClasses, scheduler, store,
                    new CheckpointPublicationCoordinator(resources, new CheckpointUploadIntentStore(),
                            new RecoveryCatalog()), request -> {
                                throw new IllegalStateException("Owner lease is no longer active");
                            });
            final ShardId otherShard = new ShardId(RouteIncarnation.random(), 23);
            assertThrows(IllegalArgumentException.class, () -> runtime.register(otherShard, 0));
            runtime.register(shard, 0);
            final CheckpointScheduler.ScheduledCheckpoint claim = runtime.claimDue(100, 1).get(0);
            final CheckpointWorkClassExecutor.ExecutionRequest request =
                    new CheckpointWorkClassExecutor.ExecutionRequest(claim, checkpointDirectory, pending,
                            (directory, currentStore) -> {
                                throw new AssertionError("preflight failure must precede checkpoint I/O");
                            }, 100, () -> 100, upload -> {
                                throw new AssertionError("preflight failure must precede provider I/O");
                            });

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> runtime.submit(request));
            assertEquals("Owner lease is no longer active", failure.getMessage());
            assertFalse(runtime.scheduler().isInFlight(shard));
            assertFalse(Files.exists(checkpointDirectory));
            assertEquals(1, runtime.claimDue(200, 1).size());
            assertEquals(0, workClasses.registeredActions());
        }
    }

    @Test
    void workerCheckpointClaimAndSubmitReleasesClaimWhenRequestFactoryFails() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 24);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("claim-submit-factory-resources"));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final WorkerCheckpointRuntime runtime = new WorkerCheckpointRuntime(
                    workClasses(1), scheduler, store,
                    new CheckpointPublicationCoordinator(resources, new CheckpointUploadIntentStore(),
                            new RecoveryCatalog()), request -> {
                    });
            runtime.register(shard, 0);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> runtime.claimDueAndSubmit(100, ignored -> {
                        throw new IllegalStateException("checkpoint request inputs unavailable");
                    }, () -> 100));
            assertEquals("checkpoint request inputs unavailable", failure.getMessage());
            assertFalse(scheduler.isInFlight(shard));
            assertEquals(1, runtime.claimDue(200, 1).size(),
                    "request construction failure must leave the exact schedule retryable");
        }
    }

    private static CheckpointManifest parentManifest(final ShardStore store, final ShardId shard,
                                                     final byte[] lineage, final SourcePosition position,
                                                     final OwnerIdentityV1 owner,
                                                     final CompatibleControlSnapshotV1 controlSnapshot) {
        return new CheckpointManifest(bytes(16, 9), lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                createdAt(890), shard, store.metadata().dbIdentity(), store.metadata().storeIncarnationUuid(), 1, 0,
                position, controlSnapshot.snapshotDigest(), bytes(32, 10), List.of(new CheckpointManifest.FileEntry(
                        "parent.sst", 1, bytes(32, 11), Bytes.utf8("parent/object"), Bytes.utf8("parent-version"),
                        null)));
    }

    private static CheckpointManifest childManifest(final Path directory, final ShardStore store,
                                                    final CheckpointUploadIntentV1 pending,
                                                    final CheckpointManifest parent, final OwnerIdentityV1 owner,
                                                    final CompatibleControlSnapshotV1 controlSnapshot) {
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(directory).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version/" + file.name()), null))
                .toList();
        return new CheckpointManifest(pending.checkpointId(), pending.recoveryLineageId(), 1,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(),
                        Bytes.hex(parent.manifestSha256())), null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                createdAt(1_000), store.shardId(), store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(), 1, store.shardMutationSequence(),
                store.appliedShardLogPosition(), controlSnapshot.snapshotDigest(), bytes(32, 12),
                store.runtimeMetadata().evidenceCursors(), files);
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, maxQueueRecords * 1_000_000L,
                    maxQueueRecords, maxQueueRecords * 1_000_000L, 1_000,
                    protectedClass ? 1 : 0, protectedClass ? 8 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 8_000_000), new AtomicLong()::get);
    }

    private static CheckpointResourceV1 resource(final CheckpointUploadRequest request,
                                                 final CheckpointUploadIntentV1 pending,
                                                 final ProfileRefV1 objectStore) {
        return new CheckpointResourceV1(pending.recoveryLineageId(), pending.checkpointId(), objectStore,
                bytes(4, 13), bytes(8, 14), bytes(8, 15), request.manifestBytes().length,
                request.manifest().manifestSha256());
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shard) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shard),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 16), 1, bytes(32, 17), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 18), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static CheckpointManifest.CreatedAt createdAt(final long time) {
        return new CheckpointManifest.CreatedAt(time, time + 1, "CERTIFIED_HOST_CLOCK", bytes(8, 19), 1, 1, 1,
                bytes(32, 20), 0, null);
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 21), 1, 1, 1,
                bytes(32, 22), 0, null);
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static void assertNamedMethodsAreNotPublic(final Class<?> type, final String methodName) {
        boolean found = false;
        for (var method : type.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                found = true;
                assertFalse(Modifier.isPublic(method.getModifiers()), method::toGenericString);
            }
        }
        assertTrue(found, () -> type.getName() + " has no declared method named " + methodName);
    }

    private static final class ResponseLossCatalog implements RecoveryCatalogAuthority {
        private final RecoveryCatalog delegate;
        private boolean dropNextPublicationResponse = true;

        private ResponseLossCatalog(final RecoveryCatalog delegate) {
            this.delegate = delegate;
        }

        @Override
        public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                                   final long expectedCatalogGeneration) {
            return delegate.publish(manifest, expectedCatalogGeneration);
        }

        @Override
        public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                          final byte[] evidenceCursorDigest) {
            return delegate.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursorDigest);
        }

        @Override
        public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
            return delegate.manifest(checkpointId);
        }

        @Override
        public Optional<RecoveryFloor> currentFloor() {
            return delegate.currentFloor();
        }

        @Override
        public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
            delegate.validatePublishedRestoreCandidate(candidate);
        }

        @Override
        public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                           final long requiredMutationSequence,
                                                                           final SourcePosition... requiredPositions) {
            return delegate.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions);
        }

        @Override
        public RecoveryCatalog.Publication publishUploadedCheckpoint(
                final CheckpointUploadIntentV1 publishedIntent, final CheckpointManifest manifest,
                final long expectedCatalogGeneration) {
            final RecoveryCatalog.Publication result = delegate.publishUploadedCheckpoint(publishedIntent, manifest,
                    expectedCatalogGeneration);
            if (dropNextPublicationResponse) {
                dropNextPublicationResponse = false;
                throw new IllegalStateException("catalog publication response lost");
            }
            return result;
        }
    }
}
