package io.nereusstream.delay.store;

import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OwnerLease;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in real Oxia + MinIO coverage for the bounded checkpoint REAPING handoff. */
@Tag("real-service")
class OxiaRealCheckpointReapingSmokeTest {
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            10, 1 << 20, 1 << 20, 1_024, 1 << 20, 10, 1_024);

    @TempDir
    Path tempDir;

    @Test
    void realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix() throws Exception {
        final String oxiaEndpoint = required("NEREUS_DELAY_OXIA_ENDPOINT");
        final URI minioEndpoint = URI.create(required("NEREUS_DELAY_MINIO_ENDPOINT"));
        final String region = valueOrDefault("NEREUS_DELAY_MINIO_REGION", "us-east-1");
        final String bucket = required("NEREUS_DELAY_MINIO_BUCKET");
        final String accessKey = required("NEREUS_DELAY_MINIO_ACCESS_KEY");
        final String secretKey = required("NEREUS_DELAY_MINIO_SECRET_KEY");
        final ProfileSemanticEnvelopeV1 profile = profile(minioEndpoint, region, bucket, accessKey);
        final S3CompatibleCheckpointObjectStoreAdapter uploader = adapter(profile, minioEndpoint, region, bucket,
                accessKey, secretKey);
        final S3CompatibleCheckpointObjectStoreAdapter reaper = adapter(profile, minioEndpoint, region, bucket,
                accessKey, secretKey);
        final long startedAt = System.currentTimeMillis();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 31);
        final byte[] lineage = uuidBytes(UUID.randomUUID());
        final byte[] checkpointId = uuidBytes(UUID.randomUUID());
        final UUID storeIncarnation = UUID.randomUUID();
        final Path checkpointDirectory = tempDir.resolve("checkpoint-" + UUID.randomUUID());
        Files.createDirectories(checkpointDirectory);
        Files.writeString(checkpointDirectory.resolve("CURRENT"), "REAPING-CURRENT\n");
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpointDirectory, LIMITS);
        final OwnerIdentityV1 ownerIdentity = new OwnerIdentityV1(Bytes.utf8("deployment"),
                Bytes.utf8("reaping-owner"), 1, id32(4));
        final CheckpointManifest.CreatedAt createdAt = new CheckpointManifest.CreatedAt(
                startedAt, startedAt + 1, "CERTIFIED_HOST_CLOCK", id32(5), 1, 2, 3, id32(6), 0, null);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version/" + file.name()), null))
                .toList();
        final KafkaSourcePosition sourcePosition = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(),
                9, 3, startedAt);
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(ownerIdentity.deploymentId(), ownerIdentity.workerRunId(),
                        ownerIdentity.ownerEpoch()), createdAt, shard, id32(7), storeIncarnation, 1, 7,
                sourcePosition, id32(8), id32(9), List.of(), files);
        final CheckpointUploadIntentV1 pending;

        final String authorityPrefix = "nereus-delay-real-reaping/" + UUID.randomUUID();
        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = OxiaSyncOwnerLeaseBackend.connect(
                oxiaEndpoint, "default", "reaping-" + UUID.randomUUID(), Duration.ofSeconds(15),
                authorityPrefix + "/owner")) {
            final SourceAssignment assignment = new SourceAssignment(shard, id32(10), 1,
                    new KafkaActivationBarrier(shard, "cluster", sourcePosition.nativeTopicUuid(), 0));
            final OxiaOwnerLeaseStore ownerAuthority = new OxiaOwnerLeaseStore(client.backend());
            final OwnerLease acquiring = ownerAuthority.acquire(assignment, "reaping-owner",
                    client.sessionIdentity(), startedAt, 60_000).orElseThrow();
            final OwnerLease active = ownerAuthority.transitionOrRead(acquiring,
                    ShardLifecycleState.ACTIVE_FOR_COMMANDS).orElseThrow();
            final OwnerIdentityV1 exactOwner = new OwnerIdentityV1(ownerIdentity.deploymentId(),
                    ownerIdentity.workerRunId(), active.ownerEpoch(), ownerIdentity.leaseFencingDigest());
            final CheckpointUploadIntentV1 requested = new CheckpointUploadIntentV1(
                    new ShardSubjectV1(shard), lineage, checkpointId, exactOwner, uuidBytes(storeIncarnation),
                    id32(11), 1, null, null, profile.ref(), evidence(startedAt), startedAt - 1,
                    CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
            final OxiaSyncCheckpointUploadIntentBackend intent =
                    new OxiaSyncCheckpointUploadIntentBackend(client, authorityPrefix + "/intent");
            final OxiaSyncRecoveryCatalogBackend catalog = new OxiaSyncRecoveryCatalogBackend(client,
                    authorityPrefix + "/catalog", LIMITS);
            final OxiaRecoveryCatalog catalogAuthority = new OxiaRecoveryCatalog(catalog);
            pending = intent.create(requested);
            uploader.upload(new CheckpointUploadRequest(pending, manifest, checkpointDirectory,
                    manifest.canonicalJsonBytes()));

            uploader.beginProviderQuiescence();
            final ObjectStoreProviderOwnershipTracker.Observation localProviderClosed =
                    uploader.requireProviderQuiescence();
            final long ownerObservedAt = System.currentTimeMillis();
            final CheckpointReapingOwnerProof ownerProof = CheckpointReapingOwnerProofIssuer.explicitOwnerAbandon(
                    pending, ownerAuthority, active, evidence(ownerObservedAt));
            assertTrue(ownerAuthority.current(shard).isEmpty());

            final long reapingAt = Math.max(ownerObservedAt, System.currentTimeMillis());
            final TrustedUtcIntervalEvidence reapingEvidence = evidence(reapingAt);
            final CheckpointReapingQuiescenceProof quiescence = quiescenceAfter(reapingEvidence, ownerProof,
                    localProviderClosed.observationDigest());
            final CheckpointReapingSweepResult result = new CheckpointReapingSweepCoordinator(intent, reaper)
                    .reap(pending, catalogAuthority, ownerProof, quiescence, 100);

            assertEquals(CheckpointUploadStateV1.REAPING, result.reapingIntent().state());
            assertEquals(result.reapingIntent(), intent.current(pending).orElseThrow());
            assertEquals(inventory.size() + 1, result.prefixSweep().listedVersionCount());
            assertEquals(result.prefixSweep().listedVersionCount(), result.prefixSweep().deletedVersionCount());
            assertTrue(result.prefixSweep().emptyAfterSweep());
            System.out.println("Oxia + MinIO checkpoint REAPING authority passed: real Owner abandonment="
                    + "true, real Intent PENDING_UPLOAD->REAPING=true, exact-version prefix sweep="
                    + result.prefixSweep().deletedVersionCount() + ", finalEmptyPrefix=true, "
                    + "localProviderOwnershipClosed=true");
        }
    }

    private S3CompatibleCheckpointObjectStoreAdapter adapter(final ProfileSemanticEnvelopeV1 profile,
                                                             final URI endpoint, final String region,
                                                             final String bucket, final String accessKey,
                                                             final String secretKey) {
        return new S3CompatibleCheckpointObjectStoreAdapter(profile, endpoint, region, bucket, accessKey,
                secretKey, null, LIMITS, null,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(), Clock.systemUTC(),
                requestTimeout(), 0);
    }

    private static ProfileSemanticEnvelopeV1 profile(final URI endpoint, final String region,
                                                     final String bucket, final String accessKey) {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, region, bucket),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                        accessKey, region, bucket),
                1, true, true, true, true, id32(20), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, id32(21));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("checkpoint-store"), 1,
                semantic);
    }

    private static CheckpointReapingQuiescenceProof quiescenceAfter(
            final TrustedUtcIntervalEvidence reapingEvidence,
            final CheckpointReapingOwnerProof ownerProof,
            final byte[] providerOwnershipDigest) throws InterruptedException {
        Thread.sleep(5);
        final long observedAt = System.currentTimeMillis();
        return new CheckpointReapingQuiescenceProof(
                ownerProof.pendingIntentDigest(), reapingEvidence, evidence(observedAt), evidence(observedAt),
                evidence(observedAt), 1, 0, 1, ownerProof.proofDigest(), providerOwnershipDigest);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                id32(30), 0, null);
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
            return Duration.ofSeconds(30);
        }
        try {
            final long millis = Long.parseLong(value);
            if (millis <= 0) {
                throw new IllegalArgumentException("NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS must be positive");
            }
            return Duration.ofMillis(millis);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS must be a positive integer",
                    failure);
        }
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static byte[] id32(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return bytes;
    }
}
