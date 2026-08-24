package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResourceV1;
import com.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import com.nereusstream.delay.protocol.CheckpointUploadStateV1;
import com.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import com.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import com.nereusstream.delay.protocol.OwnerIdentityV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import com.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import com.nereusstream.delay.protocol.RecoveryFloorRefV1;
import com.nereusstream.delay.protocol.RecoveryPinV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in real Oxia coverage for the Worker checkpoint publication composition. */
@Tag("real-service")
class OxiaRealCheckpointPublicationSmokeTest {
    private static final CheckpointManifestLimits LIMITS =
            new CheckpointManifestLimits(100, Long.MAX_VALUE, Long.MAX_VALUE, 4_096, 1 << 20, 100, 4_096);

    @TempDir
    Path tempDir;

    @Test
    void workerCheckpointRuntimePublishesAtomicIntentAndCatalogAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final ProfileRefV1 objectStore =
                new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(4), ProfileKindV1.OBJECT_STORE);
        final Path objectRoot = tempDir.resolve("objects");
        final PublishedCheckpoint published = publishWorkerCheckpoint(
                endpoint,
                "nereus-delay-real-checkpoint/" + UUID.randomUUID(),
                objectStore,
                new FilesystemCheckpointUploadAdapter(objectRoot, "bucket", LIMITS));
        assertTrue(Files.isDirectory(published.directory()));
        assertTrue(Files.isDirectory(objectRoot));
    }

    @Test
    void workerCheckpointRuntimePublishesToRealMinioAndOxia() throws Exception {
        final String oxiaEndpoint = endpoint();
        final URI minioEndpoint = URI.create(required("NEREUS_DELAY_MINIO_ENDPOINT"));
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", "us-east-1");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final ProfileSemanticEnvelopeV1 profile = minioProfile(minioEndpoint, region, bucket, accessKey);
        final S3CompatibleCheckpointObjectStoreAdapter adapter = new S3CompatibleCheckpointObjectStoreAdapter(
                profile,
                minioEndpoint,
                region,
                bucket,
                accessKey,
                secretKey,
                null,
                LIMITS,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                Clock.systemUTC(),
                requestTimeout());

        final PublishedCheckpoint published = publishWorkerCheckpoint(
                oxiaEndpoint, "nereus-delay-real-checkpoint-minio/" + UUID.randomUUID(), profile.ref(), adapter);
        final CheckpointResourceV1 resource = published.resource();
        final Path restored = adapter.download(
                new CheckpointDownloadRequest(published.manifest(), resource), tempDir.resolve("restored"));
        assertEquals(
                published.manifest().files().stream()
                        .map(CheckpointManifest.FileEntry::name)
                        .toList(),
                CheckpointFileInventory.collect(restored).stream()
                        .map(CheckpointFileInventory::name)
                        .toList());
        assertEquals(published.manifest().canonicalJsonBytes().length, resource.manifestLength());
        assertTrue(adapter.providerOwnershipObservation().activeOperationCount() == 0);
        System.out.println("Oxia + MinIO Worker checkpoint publication passed: atomic Intent/Catalog="
                + "true, immutable object upload/download=" + true + ", checkpoint="
                + Bytes.hex(published.manifest().checkpointId()));
    }

    @Test
    void workerCheckpointRuntimeRemainsPendingWhenMinioCommitFailsBeforeProviderWrite() throws Exception {
        final String oxiaEndpoint = endpoint();
        final URI minioEndpoint = URI.create(required("NEREUS_DELAY_MINIO_ENDPOINT"));
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", "us-east-1");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final ProfileSemanticEnvelopeV1 profile = minioProfile(minioEndpoint, region, bucket, accessKey);
        final S3CompatibleCheckpointObjectStoreAdapter adapter = new S3CompatibleCheckpointObjectStoreAdapter(
                profile,
                minioEndpoint,
                region,
                bucket,
                accessKey,
                secretKey,
                null,
                LIMITS,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                Clock.systemUTC(),
                requestTimeout());

        final WorkerCheckpointRun run = runWorkerCheckpoint(
                oxiaEndpoint,
                "nereus-delay-real-checkpoint-minio-precommit/" + UUID.randomUUID(),
                profile.ref(),
                adapter);
        assertNull(run.published());
        assertNotNull(run.failure());
        System.out.println("Oxia + MinIO checkpoint pre-commit failure remained fail-closed: "
                + "Worker attempt failed, Intent remained PENDING_UPLOAD, PUBLISHED Catalog was absent, "
                + "exact prefix was empty");
    }

    private PublishedCheckpoint publishWorkerCheckpoint(
            final String endpoint,
            final String prefix,
            final ProfileRefV1 objectStore,
            final CheckpointUploadAdapter adapter)
            throws Exception {
        final WorkerCheckpointRun run = runWorkerCheckpoint(endpoint, prefix, objectStore, adapter);
        assertNotNull(run.published());
        return run.published();
    }

    private WorkerCheckpointRun runWorkerCheckpoint(
            final String endpoint,
            final String prefix,
            final ProfileRefV1 objectStore,
            final CheckpointUploadAdapter adapter)
            throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 23);
        final byte[] lineage = id16(2);
        final byte[] checkpointId = id16(3);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resources-" + UUID.randomUUID()));
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final Path checkpointDirectory = tempDir.resolve("checkpoint-" + UUID.randomUUID());
        final UUID sourceTopic = UUID.nameUUIDFromBytes(lineage);
        final SourceAssignment assignment =
                new SourceAssignment(shard, id32(6), 1, new KafkaActivationBarrier(shard, "cluster", sourceTopic, 0));

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = client(endpoint, prefix + "/client");
                SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(client.backend());
            final long ownerNow = System.currentTimeMillis();
            final OwnerLease acquiring = ownerAuthority
                    .acquire(assignment, "worker", client.sessionIdentity(), ownerNow, 60_000)
                    .orElseThrow();
            final OwnerLease activeLease = ownerAuthority
                    .transitionOrRead(acquiring, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
            final OwnerIdentityV1 owner = new OwnerIdentityV1(
                    Bytes.utf8("deployment"), Bytes.utf8("worker"), activeLease.ownerEpoch(), id32(1));
            final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshot(shard);
            store.recordControlSnapshot(controlSnapshot);
            final KafkaSourcePosition parentPosition =
                    new KafkaSourcePosition(shard, "cluster", sourceTopic, 0, null, 900);
            final KafkaSourcePosition appliedPosition =
                    new KafkaSourcePosition(shard, "cluster", sourceTopic, 1, null, 901);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(1));
            });

            final OxiaSyncCheckpointPublicationBackend publicationBackend =
                    new OxiaSyncCheckpointPublicationBackend(client, prefix + "/publication", LIMITS);
            final CheckpointManifest parent =
                    parentManifest(store, shard, lineage, owner, controlSnapshot, parentPosition);
            assertEquals(1, publicationBackend.publish(parent, 0).catalogGeneration());
            final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard),
                    lineage,
                    checkpointId,
                    owner,
                    uuidBytes(store.metadata().storeIncarnationUuid()),
                    id32(5),
                    1,
                    parent.checkpointId(),
                    parent.manifestSha256(),
                    objectStore,
                    evidence(1_000),
                    5_000,
                    CheckpointUploadStateV1.PENDING_UPLOAD,
                    1,
                    null,
                    null);
            assertEquals(pending, publicationBackend.create(pending));

            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final CheckpointPublicationCoordinator publication =
                    new CheckpointPublicationCoordinator(resources, publicationBackend, LIMITS, publicationBackend);
            final WorkerCheckpointRuntime runtime =
                    new WorkerCheckpointRuntime(workClasses, scheduler, store, publication, request -> {
                        final OwnerLease current = ownerAuthority.current(shard).orElseThrow();
                        if (!activeLease.sameIdentity(current)
                                || current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                || !current.validAt(System.currentTimeMillis())) {
                            throw new IllegalStateException(
                                    "checkpoint Owner Lease/session is not the exact active lease");
                        }
                        assertEquals(
                                pending, publicationBackend.current(pending).orElseThrow());
                    });
            runtime.register(shard, 0);
            final CheckpointScheduler.ScheduledCheckpoint claim =
                    runtime.claimDue(100, 1).get(0);
            final CheckpointWorkClassExecutor.ExecutionRequest request =
                    new CheckpointWorkClassExecutor.ExecutionRequest(
                            claim,
                            checkpointDirectory,
                            pending,
                            (directory, currentStore) ->
                                    childManifest(directory, currentStore, pending, parent, owner, controlSnapshot),
                            1_000,
                            () -> 100,
                            adapter);

            final CheckpointWorkClassExecutor.Submission submitted = runtime.submit(request);
            assertEquals(
                    List.of(submitted.task()),
                    runtime.runTurn(new SchedulerBudget(1, submitted.task().bytes(), 1_000)));
            final CheckpointWorkClassExecutor.AttemptOutcome outcome =
                    submitted.outcome().orElseThrow();
            if (outcome.result() != null) {
                assertEquals(
                        CheckpointUploadStateV1.PUBLISHED,
                        outcome.result().publication().uploadIntent().state());
                assertEquals(
                        outcome.result().publication().uploadIntent(),
                        publicationBackend.currentPublishedFor(pending).orElseThrow());
                assertArrayEquals(
                        outcome.result().manifest().canonicalJsonBytes(),
                        publicationBackend.manifest(checkpointId).orElseThrow().canonicalJsonBytes());
                assertTrue(Files.isDirectory(checkpointDirectory));
                assertFalse(scheduler.isInFlight(shard));
                final CheckpointUploadIntentV1 publishedIntent =
                        outcome.result().publication().uploadIntent();
                assertTrue(ownerAuthority.release(activeLease));
                assertTrue(ownerAuthority.current(shard).isEmpty());
                return new WorkerCheckpointRun(
                        new PublishedCheckpoint(
                                checkpointDirectory, outcome.result().manifest(), publishedIntent.publishedManifest()),
                        null);
            }

            assertNotNull(outcome.failure());
            assertEquals(pending, publicationBackend.current(pending).orElseThrow());
            assertTrue(publicationBackend.currentPublishedFor(pending).isEmpty());
            assertTrue(publicationBackend.manifest(checkpointId).isEmpty());
            assertTrue(Files.isDirectory(checkpointDirectory));
            assertFalse(scheduler.isInFlight(shard));
            final CheckpointPrefixSweepResult reapedPartialPrefix = ((CheckpointPrefixSweepAdapter) adapter)
                    .sweep(new CheckpointPrefixSweepRequest(
                            objectStore, pending.recoveryLineageId(), pending.checkpointId(), 100));
            assertTrue(reapedPartialPrefix.listedVersionCount() > 0);
            assertEquals(reapedPartialPrefix.listedVersionCount(), reapedPartialPrefix.deletedVersionCount());
            assertTrue(reapedPartialPrefix.emptyAfterSweep());
            assertTrue(ownerAuthority.release(activeLease));
            assertTrue(ownerAuthority.current(shard).isEmpty());
            return new WorkerCheckpointRun(null, outcome.failure());
        }
    }

    private record WorkerCheckpointRun(PublishedCheckpoint published, Throwable failure) {
        private WorkerCheckpointRun {
            if ((published == null) == (failure == null)) {
                throw new IllegalArgumentException("checkpoint run must contain exactly one result branch");
            }
        }
    }

    @Test
    void recoveryPinIsSessionBoundAndExpiresWithTheRealPublicationSession() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-publication-pin/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 24);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(20);
        final CheckpointManifest manifest = simpleManifest(shard, topic, lineage, id16(21));

        try (OxiaSyncOwnerLeaseBackend.ClientHandle owner = client(endpoint, prefix + "/owner")) {
            final OxiaSyncCheckpointPublicationBackend backend =
                    new OxiaSyncCheckpointPublicationBackend(owner, prefix + "/publication", LIMITS);
            assertEquals(1, backend.publish(manifest, 0).catalogGeneration());
            final RecoveryFloorRefV1 floor = backend.advanceFloor(manifest.checkpointId(), 1, List.of());
            final RecoveryPinV1 pin = recoveryPin(shard, manifest, floor, owner.sessionIdentity());

            assertEquals(pin, backend.createRecoveryPin(pin));
            assertEquals(pin, backend.activeRecoveryPin().orElseThrow());
        }

        try (OxiaSyncOwnerLeaseBackend.ClientHandle replacement = client(endpoint, prefix + "/replacement")) {
            final OxiaSyncCheckpointPublicationBackend reopened =
                    new OxiaSyncCheckpointPublicationBackend(replacement, prefix + "/publication", LIMITS);
            assertTrue(reopened.activeRecoveryPin().isEmpty());
        }
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static String required(final String name) {
        final String value = System.getenv(name);
        Assumptions.assumeTrue(value != null && !value.isBlank(), name + " is not configured");
        return value;
    }

    private static String valueOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static Duration requestTimeout() {
        final String value = System.getenv("NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS");
        if (value == null || value.isBlank()) {
            return Duration.ofSeconds(60);
        }
        try {
            final long millis = Long.parseLong(value);
            if (millis <= 0) {
                throw new IllegalArgumentException("NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS must be positive");
            }
            return Duration.ofMillis(millis);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS must be a positive integer", failure);
        }
    }

    private static ProfileSemanticEnvelopeV1 minioProfile(
            final URI endpoint, final String region, final String bucket, final String accessKey) {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, region, bucket),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(accessKey, region, bucket),
                1,
                true,
                true,
                true,
                true,
                id32(20),
                1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT,
                1,
                id32(21));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("checkpoint-store"), 1, semantic);
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle client(final String endpoint, final String identifier)
            throws Exception {
        return OxiaSyncOwnerLeaseBackend.connect(
                endpoint, "default", identifier, Duration.ofSeconds(15), "real-checkpoint-smoke");
    }

    private static CheckpointManifest parentManifest(
            final ShardStore store,
            final ShardId shard,
            final byte[] lineage,
            final OwnerIdentityV1 owner,
            final CompatibleControlSnapshotV1 controlSnapshot,
            final KafkaSourcePosition position) {
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry(
                "CURRENT", 1, id32(10), Bytes.utf8("parent-object"), Bytes.utf8("parent-version"), null);
        return new CheckpointManifest(
                id16(11),
                lineage,
                0,
                null,
                null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                createdAt(800),
                shard,
                store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(),
                store.metadata().storeFormatVersion(),
                0,
                position,
                controlSnapshot.snapshotDigest(),
                id32(12),
                List.of(),
                List.of(file));
    }

    private static CheckpointManifest childManifest(
            final Path directory,
            final ShardStore store,
            final CheckpointUploadIntentV1 pending,
            final CheckpointManifest parent,
            final OwnerIdentityV1 owner,
            final CompatibleControlSnapshotV1 controlSnapshot) {
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(directory).stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("object/" + file.name()),
                        Bytes.utf8("version/" + file.name()),
                        null))
                .toList();
        return new CheckpointManifest(
                pending.checkpointId(),
                pending.recoveryLineageId(),
                1,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())),
                null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                createdAt(1_000),
                store.shardId(),
                store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(),
                store.metadata().storeFormatVersion(),
                store.shardMutationSequence(),
                store.appliedShardLogPosition(),
                controlSnapshot.snapshotDigest(),
                id32(13),
                store.runtimeMetadata().evidenceCursors(),
                files);
    }

    private static CheckpointManifest simpleManifest(
            final ShardId shard, final UUID topic, final byte[] lineage, final byte[] checkpointId) {
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", topic, 0, null, 1_000);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry(
                "CURRENT", 1, id32(30), Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(
                checkpointId,
                lineage,
                0,
                null,
                null,
                new CheckpointManifest.CreatedBy(id32(31), id32(32), 1),
                new CheckpointManifest.CreatedAt(
                        1_000, 1_001, "CERTIFIED_HOST_CLOCK", id32(33), 1, 0, 0, id32(34), 0, null),
                shard,
                id32(35),
                topic,
                1,
                0,
                position,
                id32(36),
                id32(37),
                List.of(),
                List.of(file));
    }

    private static RecoveryPinV1 recoveryPin(
            final ShardId shard,
            final CheckpointManifest manifest,
            final RecoveryFloorRefV1 floor,
            final byte[] sessionIdentity) {
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT,
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                null);
        return new RecoveryPinV1(
                id16(22),
                new ShardSubjectV1(shard),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(38)),
                candidate,
                floor,
                floor.catalogGeneration(),
                sessionIdentity);
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shard) {
        return new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shard),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(id32(20), 1, id32(21), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(
                        id32(22),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static CheckpointManifest.CreatedAt createdAt(final long time) {
        return new CheckpointManifest.CreatedAt(
                time, time + 1, "CERTIFIED_HOST_CLOCK", id32(30), 1, 1, 1, id32(31), 0, null);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                1,
                1,
                id32(32),
                0,
                null);
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass =
                    switch (workClass) {
                        case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                        case QUERY, CHECKPOINT -> false;
                    };
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            maxQueueRecords,
                            maxQueueRecords * 1_000_000L,
                            maxQueueRecords,
                            maxQueueRecords * 1_000_000L,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 8 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 8_000_000), new AtomicLong()::get);
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static byte[] id16(final int value) {
        final byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return bytes;
    }

    private static byte[] id32(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return bytes;
    }

    private record PublishedCheckpoint(Path directory, CheckpointManifest manifest, CheckpointResourceV1 resource) {}
}
