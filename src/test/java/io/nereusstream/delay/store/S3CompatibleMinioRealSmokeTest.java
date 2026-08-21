package io.nereusstream.delay.store;

import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.OwnerLeaseContext;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in real MinIO coverage for the S3-compatible checkpoint adapter. */
@Tag("real-service")
class S3CompatibleMinioRealSmokeTest {
    private static final String DEFAULT_REGION = "us-east-1";
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            10, 1 << 20, 1 << 20, 1024, 1 << 20, 10, 1024);

    @TempDir
    Path tempDir;

    @Test
    void immutableCheckpointUploadsIdempotentlyAndRestoresAgainstMinio() throws Exception {
        final String endpoint = required("NEREUS_DELAY_MINIO_ENDPOINT");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", DEFAULT_REGION);
        final URI endpointUri = URI.create(endpoint);
        final Fixture fixture = fixture(endpointUri, region, bucket, accessKey);
        final S3CompatibleCheckpointObjectStoreAdapter adapter = new S3CompatibleCheckpointObjectStoreAdapter(
                fixture.profile(), endpointUri, region, bucket, accessKey, secretKey, null, LIMITS);
        final CheckpointUploadRequest request = new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes());

        final CheckpointResourceV1 first = adapter.upload(request);
        final String providerVersion = new String(first.immutableVersion(), StandardCharsets.UTF_8);
        assertFalse(providerVersion.startsWith("sha256-"));
        System.out.println("MinIO checkpoint manifest provider version=" + providerVersion);
        assertEquals(first, adapter.upload(request));

        final Path restored = adapter.download(new CheckpointDownloadRequest(fixture.manifest(), first),
                tempDir.resolve("restored"));
        assertTrue(Files.isDirectory(restored));
        assertEquals("MANIFEST-1\n", Files.readString(restored.resolve("CURRENT")));
        assertEquals("sst-bytes", Files.readString(restored.resolve("000001.sst")));

        final CheckpointDeleteResult deleted = adapter.delete(new CheckpointDeleteRequest(fixture.manifest(), first));
        assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED, deleted.outcome());
        assertEquals(32, deleted.providerRequestIdHash().length);
        assertEquals(32, deleted.responseHash().length);
        assertThrows(IllegalStateException.class,
                () -> adapter.download(new CheckpointDownloadRequest(fixture.manifest(), first),
                        tempDir.resolve("deleted")));

        final CheckpointResourceV1 sweptResource = adapter.upload(request);
        final CheckpointUploadIntentStore reapingStore = new CheckpointUploadIntentStore();
        reapingStore.create(fixture.pending());
        final CheckpointReapingOwnerProof ownerProof = ownerProof(fixture.pending());
        final CheckpointReapingSweepResult reaping = new CheckpointReapingSweepCoordinator(
                reapingStore, adapter).reap(fixture.pending(), new RecoveryCatalog(), ownerProof,
                        quiescence(fixture.pending(), ownerProof), 100);
        assertEquals(3, reaping.prefixSweep().listedVersionCount());
        assertEquals(3, reaping.prefixSweep().deletedVersionCount());
        assertTrue(reaping.prefixSweep().emptyAfterSweep());
        assertThrows(IllegalStateException.class,
                () -> adapter.download(new CheckpointDownloadRequest(fixture.manifest(), sweptResource),
                        tempDir.resolve("swept")));
    }

    @Test
    void realMinioFiveHundredAfterCommitResolvesByExactReadback() throws Exception {
        final String endpoint = required("NEREUS_DELAY_MINIO_ENDPOINT");
        final String controlEndpoint = required("NEREUS_DELAY_MINIO_FAULT_CONTROL");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", DEFAULT_REGION);
        final Fixture fixture = fixture(URI.create(endpoint), region, bucket, accessKey);
        final Path stateDirectory = stateDirectoryOrNull();

        setFaultMode(controlEndpoint, "PUT_503_AFTER_COMMIT");
        try {
            persistBefore("object-store-5xx", fixture, "PUT_503_AFTER_COMMIT");
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture, accessKey, secretKey,
                    Duration.ofSeconds(5));
            final CheckpointResourceV1 resource = adapter.upload(new CheckpointUploadRequest(fixture.pending(),
                    fixture.manifest(), fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes()));

            final String providerVersion = new String(resource.immutableVersion(), StandardCharsets.UTF_8);
            assertFalse(providerVersion.startsWith("sha256-"));
            if (stateDirectory == null) {
                assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                        adapter.delete(new CheckpointDeleteRequest(fixture.manifest(), resource)).outcome());
            } else {
                persistPublished("object-store-5xx", fixture, resource, "PUT_503_AFTER_COMMIT");
            }
        } finally {
            setFaultMode(controlEndpoint, "NONE");
        }
    }

    @Test
    void realMinioFiveHundredBeforeCommitRemainsFailClosed() throws Exception {
        final String endpoint = required("NEREUS_DELAY_MINIO_ENDPOINT");
        final String controlEndpoint = required("NEREUS_DELAY_MINIO_FAULT_CONTROL");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", DEFAULT_REGION);
        final Fixture fixture = fixture(URI.create(endpoint), region, bucket, accessKey);

        setFaultMode(controlEndpoint, "PUT_503_BEFORE_COMMIT");
        try {
            persistBefore("storage-provider-fault", fixture, "PUT_503_BEFORE_COMMIT");
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture, accessKey, secretKey,
                    Duration.ofSeconds(5));
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> adapter.upload(new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                            fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes())));
            assertTrue(failure.getMessage().contains("HTTP 503"));
        } finally {
            setFaultMode(controlEndpoint, "NONE");
        }
    }

    @Test
    void realMinioTimeoutAfterCommitResolvesByExactReadback() throws Exception {
        final String endpoint = required("NEREUS_DELAY_MINIO_ENDPOINT");
        final String controlEndpoint = required("NEREUS_DELAY_MINIO_FAULT_CONTROL");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", DEFAULT_REGION);
        final Fixture fixture = fixture(URI.create(endpoint), region, bucket, accessKey);
        final Path stateDirectory = stateDirectoryOrNull();

        setFaultMode(controlEndpoint, "PUT_TIMEOUT_AFTER_COMMIT");
        try {
            persistBefore("object-store-timeout", fixture, "PUT_TIMEOUT_AFTER_COMMIT");
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture, accessKey, secretKey,
                    Duration.ofMillis(750));
            final CheckpointResourceV1 resource = adapter.upload(new CheckpointUploadRequest(fixture.pending(),
                    fixture.manifest(), fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes()));

            assertFalse(new String(resource.immutableVersion(), StandardCharsets.UTF_8).startsWith("sha256-"));
            if (stateDirectory == null) {
                assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                        adapter.delete(new CheckpointDeleteRequest(fixture.manifest(), resource)).outcome());
            } else {
                persistPublished("object-store-timeout", fixture, resource, "PUT_TIMEOUT_AFTER_COMMIT");
            }
        } finally {
            setFaultMode(controlEndpoint, "NONE");
        }
    }

    @Test
    void realMinioCredentialConfigurationDriftFailsClosed() throws Exception {
        final String endpoint = required("NEREUS_DELAY_MINIO_ENDPOINT");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", DEFAULT_REGION);
        final Fixture fixture = fixture(URI.create(endpoint), region, bucket, accessKey);
        persistBefore("config-drift", fixture, "CREDENTIAL_CONFIGURATION_DRIFT");
        final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture, accessKey,
                secretKey + "-drift", Duration.ofSeconds(5));

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> adapter.upload(new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                        fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes())));
        assertTrue(failure.getMessage().contains("HTTP 403"));
    }

    @Test
    void realMinioFaultRecoveryRunsInFreshProcess() throws Exception {
        final Path stateDirectory = requiredStateDirectory();
        recoverPublished("object-store-5xx");
        recoverPublished("object-store-timeout");
        recoverProviderFault("storage-provider-fault");
        recoverProviderFault("config-drift");
        assertTrue(Files.exists(stateDirectory.resolve("object-store-5xx/after.json")));
    }

    private static Path stateDirectoryOrNull() {
        final String value = System.getenv("NEREUS_DELAY_MINIO_FAULT_STATE_DUMP_DIR");
        return value == null || value.isBlank() ? null : Path.of(value).toAbsolutePath().normalize();
    }

    private static Path requiredStateDirectory() {
        final Path value = stateDirectoryOrNull();
        Assumptions.assumeTrue(value != null, "NEREUS_DELAY_MINIO_FAULT_STATE_DUMP_DIR is not configured");
        return value;
    }

    private static void persistBefore(final String cell, final Fixture fixture, final String fault) throws Exception {
        final Path stateDirectory = stateDirectoryOrNull();
        if (stateDirectory == null) {
            return;
        }
        final Path cellDirectory = stateDirectory.resolve(cell);
        final Path checkpointDirectory = cellDirectory.resolve("checkpoint");
        Files.createDirectories(checkpointDirectory);
        writeForced(cellDirectory.resolve("manifest.json"), fixture.manifest().canonicalJsonBytes());
        writeForced(cellDirectory.resolve("profile.bin"), fixture.profile().canonicalBytes());
        writeForced(cellDirectory.resolve("pending-intent.bin"), fixture.pending().canonicalBytes());
        for (CheckpointManifest.FileEntry file : fixture.manifest().files()) {
            final Path target = checkpointDirectory.resolve(file.name()).normalize();
            if (!target.startsWith(checkpointDirectory)) {
                throw new IllegalStateException("checkpoint file escaped durable state directory: " + file.name());
            }
            writeForced(target, Files.readAllBytes(fixture.checkpointDirectory().resolve(file.name())));
        }
        writeDump(cellDirectory.resolve("before.json"), cell, "BEFORE_FRESH_PROCESS_RECOVERY", fault,
                fixture.manifest(), fixture.pending(), null, false, "NO_RESOURCE_YET");
    }

    private static void persistPublished(final String cell, final Fixture fixture,
                                         final CheckpointResourceV1 resource, final String fault) throws Exception {
        final Path stateDirectory = requiredStateDirectory();
        final Path cellDirectory = stateDirectory.resolve(cell);
        final CheckpointUploadIntentV1 published = new CheckpointUploadIntentV1(
                fixture.pending().shard(), fixture.pending().recoveryLineageId(), fixture.pending().checkpointId(),
                fixture.pending().owner(), fixture.pending().sourceStoreIncarnation(), fixture.pending().uploadToken(),
                fixture.pending().baseCatalogGeneration(), fixture.pending().parentCheckpointId(),
                fixture.pending().parentManifestSha256(), fixture.pending().objectStoreProfile(),
                fixture.pending().checkpointCreatedAt(), fixture.pending().uploadDeadlineEpochMs(),
                CheckpointUploadStateV1.PUBLISHED, fixture.pending().stateRevision() + 1, resource, null);
        writeForced(cellDirectory.resolve("resource.bin"), resource.canonicalBytes());
        writeForced(cellDirectory.resolve("published-intent.bin"), published.canonicalBytes());
        writeDump(cellDirectory.resolve("before.json"), cell, "BEFORE_FRESH_PROCESS_RECOVERY", fault,
                fixture.manifest(), published, resource, true, "PUBLISHED_RESOURCE_DURABLE");
    }

    private void recoverPublished(final String cell) throws Exception {
        final Path cellDirectory = requiredStateDirectory().resolve(cell);
        final CheckpointManifest manifest = CheckpointManifest.decodeCanonicalJson(
                Files.readAllBytes(cellDirectory.resolve("manifest.json")), LIMITS);
        final ProfileSemanticEnvelopeV1 profile = ProfileSemanticEnvelopeV1.decode(
                Files.readAllBytes(cellDirectory.resolve("profile.bin")));
        final CheckpointUploadIntentV1 published = CheckpointUploadIntentV1.decode(
                Files.readAllBytes(cellDirectory.resolve("published-intent.bin")));
        final CheckpointResourceV1 resource = CheckpointResourceV1.decode(
                Files.readAllBytes(cellDirectory.resolve("resource.bin")));
        assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
        assertEquals(resource, published.publishedManifest());
        assertArrayEquals(manifest.manifestSha256(), resource.manifestSha256());
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(profile, accessKey, secretKey,
                Duration.ofSeconds(5));
        final Path restored = adapter.download(new CheckpointDownloadRequest(manifest, resource),
                tempDir.resolve(cell + "-restored"));
        assertEquals("MANIFEST-1\n", Files.readString(restored.resolve("CURRENT")));
        assertEquals("sst-bytes", Files.readString(restored.resolve("000001.sst")));
        assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                adapter.delete(new CheckpointDeleteRequest(manifest, resource)).outcome());
        final CheckpointPrefixSweepResult sweep = adapter.sweep(new CheckpointPrefixSweepRequest(
                profile.ref(), manifest.recoveryLineageId(), manifest.checkpointId(), 100));
        assertEquals(0, sweep.listedVersionCount());
        assertTrue(sweep.emptyAfterSweep());
        writeDump(cellDirectory.resolve("after.json"), cell, "RECOVERED_AFTER_FRESH_PROCESS", "NONE", manifest,
                published, resource, false, "DOWNLOAD_EXACT_READBACK_DELETE_EXACT_VERSION");
    }

    private void recoverProviderFault(final String cell) throws Exception {
        final Path cellDirectory = requiredStateDirectory().resolve(cell);
        final CheckpointManifest manifest = CheckpointManifest.decodeCanonicalJson(
                Files.readAllBytes(cellDirectory.resolve("manifest.json")), LIMITS);
        final ProfileSemanticEnvelopeV1 profile = ProfileSemanticEnvelopeV1.decode(
                Files.readAllBytes(cellDirectory.resolve("profile.bin")));
        final CheckpointUploadIntentV1 pending = CheckpointUploadIntentV1.decode(
                Files.readAllBytes(cellDirectory.resolve("pending-intent.bin")));
        assertEquals(CheckpointUploadStateV1.PENDING_UPLOAD, pending.state());
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(profile, accessKey, secretKey,
                Duration.ofSeconds(5));
        final CheckpointPrefixSweepResult sweep = adapter.sweep(new CheckpointPrefixSweepRequest(
                profile.ref(), manifest.recoveryLineageId(), manifest.checkpointId(), 100));
        assertTrue(sweep.emptyAfterSweep());
        writeDump(cellDirectory.resolve("after.json"), cell, "RECOVERED_AFTER_FRESH_PROCESS", "NONE", manifest,
                pending, null, false, "EXACT_PREFIX_SWEEP_AFTER_PRECOMMIT_FAILURE");
    }

    private static void writeDump(final Path path, final String cell, final String phase, final String fault,
                                  final CheckpointManifest manifest, final CheckpointUploadIntentV1 intent,
                                  final CheckpointResourceV1 resource, final boolean objectPresent,
                                  final String recoveryAction) throws Exception {
        final String resourceDigest = resource == null ? "" : Bytes.hex(resource.manifestSha256());
        final String json = "{\n"
                + "  \"schema\": \"nereus-delay-chaos-durable-state-dump-v1\",\n"
                + "  \"cell\": " + jsonString(cell) + ",\n"
                + "  \"phase\": " + jsonString(phase) + ",\n"
                + "  \"fault\": " + jsonString(fault) + ",\n"
                + "  \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + "  \"manifest_sha256\": " + jsonString(Bytes.hex(manifest.manifestSha256())) + ",\n"
                + "  \"manifest_bytes\": " + manifest.canonicalJsonBytes().length + ",\n"
                + "  \"intent_state\": " + jsonString(intent.state().name()) + ",\n"
                + "  \"resource_present\": " + resourcePresent(objectPresent) + ",\n"
                + "  \"resource_manifest_sha256\": " + jsonString(resourceDigest) + ",\n"
                + "  \"recovery_action\": " + jsonString(recoveryAction) + ",\n"
                + "  \"durable_store_read\": true,\n"
                + "  \"dump_forced\": true\n"
                + "}\n";
        writeForced(path, json.getBytes(StandardCharsets.UTF_8));
    }

    private static String resourcePresent(final boolean value) {
        return Boolean.toString(value);
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static void writeForced(final Path path, final byte[] bytes) throws Exception {
        final Path parent = path.getParent();
        Files.createDirectories(parent);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private static S3CompatibleCheckpointObjectStoreAdapter adapter(final Fixture fixture,
                                                                    final String accessKey,
                                                                    final String secretKey,
                                                                    final Duration timeout) {
        return adapter(fixture.profile(), accessKey, secretKey, timeout);
    }

    private static S3CompatibleCheckpointObjectStoreAdapter adapter(final ProfileSemanticEnvelopeV1 profile,
                                                                    final String accessKey,
                                                                    final String secretKey,
                                                                    final Duration timeout) {
        return new S3CompatibleCheckpointObjectStoreAdapter(profile,
                URI.create(required("NEREUS_DELAY_MINIO_ENDPOINT")),
                valueOrDefault("NEREUS_DELAY_MINIO_REGION", DEFAULT_REGION),
                required("NEREUS_DELAY_MINIO_BUCKET"), accessKey, secretKey, null, LIMITS,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                Clock.systemUTC(), timeout);
    }

    private static void setFaultMode(final String controlEndpoint, final String mode) throws Exception {
        final HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(controlEndpoint)).timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString(mode))
                        .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "MinIO fault proxy control failed: " + response.body());
    }

    private Fixture fixture(final URI endpoint, final String region, final String bucket,
                            final String accessKey) throws Exception {
        final Path directory = tempDir.resolve("checkpoint-" + UUID.randomUUID());
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("CURRENT"), "MANIFEST-1\n");
        Files.writeString(directory.resolve("000001.sst"), "sst-bytes");
        final ProfileSemanticEnvelopeV1 profile = profile(endpoint, region, bucket, accessKey);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] lineage = uuidBytes(UUID.randomUUID());
        final byte[] checkpoint = uuidBytes(UUID.randomUUID());
        final UUID storeIncarnation = UUID.randomUUID();
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 5), bytes(8, 6), 42, bytes(32, 7));
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(directory, LIMITS);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9,
                3, 1_000);
        final CheckpointManifest manifest = new CheckpointManifest(checkpoint, lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                new CheckpointManifest.CreatedAt(900, 1_000, "CERTIFIED_HOST_CLOCK", bytes(8, 8), 1, 2, 3,
                        bytes(32, 9), 0, null), shard, bytes(32, 10), storeIncarnation, 1, 7, position,
                bytes(32, 11), bytes(32, 12), List.of(), files);
        final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard), lineage, checkpoint, owner, uuidBytes(storeIncarnation), bytes(32, 13),
                1, null, null, profile.ref(), evidence(900), 5_000, CheckpointUploadStateV1.PENDING_UPLOAD,
                1, null, null);
        return new Fixture(directory, profile, manifest, pending);
    }

    private static ProfileSemanticEnvelopeV1 profile(final URI endpoint, final String region, final String bucket,
                                                     final String accessKey) {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, region, bucket),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                        accessKey, region, bucket),
                1, true, true, true, true, bytes(32, 20), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 21));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("checkpoint-store"), 1,
                semantic);
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

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
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
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 14), 1, 2, 3,
                bytes(32, 15), 0, null);
    }

    private static CheckpointReapingQuiescenceProof quiescence(final CheckpointUploadIntentV1 pending,
                                                               final CheckpointReapingOwnerProof ownerProof) {
        return new CheckpointReapingQuiescenceProof(pending.intentDigest(), evidence(5_000), evidence(10_000),
                evidence(7_000), evidence(7_000), 1_000, 500, 10, ownerProof.proofDigest(), bytes(32, 61));
    }

    private static CheckpointReapingOwnerProof ownerProof(final CheckpointUploadIntentV1 pending) {
        final OwnerLease lease = new OwnerLease(pending.shard().shardId(), "owner-proof", pending.owner().ownerEpoch(),
                bytes(32, 62), 20_000,
                new OwnerLeaseContext(bytes(32, 63), 1, bytes(32, 64)), ShardLifecycleState.ACTIVE_FOR_COMMANDS);
        return new CheckpointReapingOwnerProof(pending.intentDigest(), pending.owner(),
                pending.sourceStoreIncarnation(), lease,
                CheckpointReapingOwnerProof.Kind.EXACT_OWNER_EXPLICIT_ABANDON, null, evidence(7_000));
    }

    private record Fixture(Path checkpointDirectory, ProfileSemanticEnvelopeV1 profile,
                           CheckpointManifest manifest, CheckpointUploadIntentV1 pending) {
    }
}
