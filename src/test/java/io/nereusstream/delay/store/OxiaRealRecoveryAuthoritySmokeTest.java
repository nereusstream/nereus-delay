package io.nereusstream.delay.store;

import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryInstallPhaseV1;
import io.nereusstream.delay.protocol.RecoveryInstallStateV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in real Oxia coverage for the single-record recovery authorities. */
@Tag("real-service")
class OxiaRealRecoveryAuthoritySmokeTest {
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            100, Long.MAX_VALUE, Long.MAX_VALUE, 4_096, 1 << 20, 100, 4_096);

    @Test
    void recoveryCatalogCasAndLocalReuseValidationWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-recovery/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(1);
        final CheckpointManifest manifest = manifest(shard, topic, lineage, id16(2), 0, 10, 10, null);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = client(endpoint, prefix + "/client")) {
            final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(
                    client, prefix + "/catalog", LIMITS);
            assertEquals(1, backend.publish(manifest, 0).catalogGeneration());
            final var floor = backend.advanceFloor(manifest.checkpointId(), 1, List.<EvidenceCursorV1>of());
            assertEquals(2, floor.catalogGeneration());
            final var reopened = new OxiaSyncRecoveryCatalogBackend(client, prefix + "/catalog", LIMITS);
            assertArrayEquals(floor.checkpointId(), reopened.currentFloorRef().orElseThrow().checkpointId());

            final byte[] storeIncarnation = id16(3);
            final StoreRecoveryMetadata local = new StoreRecoveryMetadata(
                    new RecoveryCandidateRefV1(RecoveryCandidateKindV1.LOCAL_STORE, lineage,
                            manifest.checkpointId(), manifest.manifestSha256(), storeIncarnation), floor,
                    floor.catalogGeneration(), new RecoveryInstallStateV1(RecoveryInstallPhaseV1.OPEN,
                            storeIncarnation, manifest.checkpointId()));
            new OxiaRecoveryCatalog(reopened).validateLocalStoreRecovery(shard, local);
        }
    }

    @Test
    void recoveryPinIsSessionBoundAndExpiresWithTheRealOxiaSession() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-pin/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 13);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(10);
        final CheckpointManifest manifest = manifest(shard, topic, lineage, id16(11), 0, 10, 10, null);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle owner = client(endpoint, prefix + "/owner")) {
            final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(
                    owner, prefix + "/catalog", LIMITS);
            backend.publish(manifest, 0);
            final RecoveryFloorRefV1 floor = backend.advanceFloor(manifest.checkpointId(), 1, List.of());
            final RecoveryPinV1 pin = recoveryPin(shard, manifest, floor, owner.sessionIdentity());

            assertEquals(pin, backend.createRecoveryPin(pin));
            assertEquals(pin, backend.activeRecoveryPin().orElseThrow());
        }

        try (OxiaSyncOwnerLeaseBackend.ClientHandle replacement = client(endpoint, prefix + "/replacement")) {
            final OxiaSyncRecoveryCatalogBackend reopened = new OxiaSyncRecoveryCatalogBackend(
                    replacement, prefix + "/catalog", LIMITS);
            assertTrue(reopened.activeRecoveryPin().isEmpty());
        }
    }

    @Test
    void checkpointUploadIntentCasAndReopenWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-upload/" + UUID.randomUUID();
        final CheckpointUploadIntentV1 pending = pendingIntent(20);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = client(endpoint, prefix + "/client")) {
            final OxiaSyncCheckpointUploadIntentBackend backend =
                    new OxiaSyncCheckpointUploadIntentBackend(client, prefix + "/intent");
            assertEquals(pending, backend.create(pending));
            final CheckpointResourceV1 resource = resource(pending);
            final CheckpointUploadIntentV1 published = backend.publish(pending, resource);
            assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
            assertEquals(published, backend.currentPublishedFor(pending).orElseThrow());
            final CheckpointUploadIntentV1 reopened = backend.current(pending).orElseThrow();
            assertEquals(published, reopened);
        }
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle client(final String endpoint, final String identifier)
            throws Exception {
        return OxiaSyncOwnerLeaseBackend.connect(endpoint, "default", identifier, Duration.ofSeconds(15),
                "real-smoke");
    }

    private static CheckpointManifest manifest(final ShardId shard, final UUID topic, final byte[] lineage,
                                               final byte[] checkpointId, final long lineageGeneration,
                                               final long offset, final long mutationSequence,
                                               final CheckpointManifest.ParentCheckpoint parent) {
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", topic, offset,
                null, 1_000 + offset);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry("CURRENT", 1, id32(20),
                Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(checkpointId, lineage, lineageGeneration, parent, null,
                new CheckpointManifest.CreatedBy(id32(21), id32(22), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_001, "CERTIFIED_HOST_CLOCK", id32(23), 1,
                        offset, offset, id32(24), 0, null), shard, id32(25), UUID.randomUUID(), 1,
                mutationSequence, position, id32(26), id32(27), List.of(), List.of(file));
    }

    private static CheckpointUploadIntentV1 pendingIntent(final int seed) {
        final ShardId shard = new ShardId(new RouteIncarnation(id16(seed + 1)), seed);
        return new CheckpointUploadIntentV1(new ShardSubjectV1(shard), id16(seed + 2), id16(seed + 3),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(seed + 4)),
                id16(seed + 5), id32(seed + 6), 1, null, null,
                new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(seed + 7), ProfileKindV1.OBJECT_STORE),
                evidence(1_000), 4_000, CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
    }

    private static CheckpointResourceV1 resource(final CheckpointUploadIntentV1 pending) {
        return new CheckpointResourceV1(pending.recoveryLineageId(), pending.checkpointId(),
                pending.objectStoreProfile(), Bytes.utf8("bucket"), Bytes.utf8("checkpoint/manifest"),
                Bytes.utf8("version"), 10, id32(40));
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                id32(50), 0, null);
    }

    private static RecoveryPinV1 recoveryPin(final ShardId shard, final CheckpointManifest manifest,
                                             final RecoveryFloorRefV1 floor, final byte[] sessionIdentity) {
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, manifest.recoveryLineageId(), manifest.checkpointId(),
                manifest.manifestSha256(), null);
        return new RecoveryPinV1(id16(12), new ShardSubjectV1(shard),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(13)), candidate,
                floor, floor.catalogGeneration(), sessionIdentity);
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
}
