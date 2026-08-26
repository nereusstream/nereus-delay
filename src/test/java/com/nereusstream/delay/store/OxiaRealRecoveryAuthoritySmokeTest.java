package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryInstallPhase;
import com.nereusstream.delay.protocol.RecoveryInstallState;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in real Oxia coverage for the single-record recovery authorities. */
@Tag("real-service")
class OxiaRealRecoveryAuthoritySmokeTest {
    private static final CheckpointManifestLimits LIMITS =
            new CheckpointManifestLimits(100, Long.MAX_VALUE, Long.MAX_VALUE, 4_096, 1 << 20, 100, 4_096);

    @Test
    void recoveryCatalogCasAndLocalReuseValidationWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-recovery/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(1);
        final CheckpointManifest manifest = manifest(shard, topic, lineage, id16(2), 0, 10, 10, null);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = client(endpoint, prefix + "/client")) {
            final OxiaSyncRecoveryCatalogBackend backend =
                    new OxiaSyncRecoveryCatalogBackend(client, prefix + "/catalog", LIMITS);
            assertEquals(1, backend.publish(manifest, 0).catalogGeneration());
            final var floor = backend.advanceFloor(manifest.checkpointId(), 1, List.<EvidenceCursor>of());
            assertEquals(2, floor.catalogGeneration());
            final var reopened = new OxiaSyncRecoveryCatalogBackend(client, prefix + "/catalog", LIMITS);
            assertArrayEquals(
                    floor.checkpointId(),
                    reopened.currentFloorRef().orElseThrow().checkpointId());

            final byte[] storeIncarnation = id16(3);
            final StoreRecoveryMetadata local = new StoreRecoveryMetadata(
                    new RecoveryCandidateRef(
                            RecoveryCandidateKind.LOCAL_STORE,
                            lineage,
                            manifest.checkpointId(),
                            manifest.manifestSha256(),
                            storeIncarnation),
                    floor,
                    floor.catalogGeneration(),
                    new RecoveryInstallState(RecoveryInstallPhase.OPEN, storeIncarnation, manifest.checkpointId()));
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
            final OxiaSyncRecoveryCatalogBackend backend =
                    new OxiaSyncRecoveryCatalogBackend(owner, prefix + "/catalog", LIMITS);
            backend.publish(manifest, 0);
            final RecoveryFloorRef floor = backend.advanceFloor(manifest.checkpointId(), 1, List.of());
            final RecoveryPin pin = recoveryPin(shard, manifest, floor, owner.sessionIdentity());

            assertEquals(pin, backend.createRecoveryPin(pin));
            assertEquals(pin, backend.activeRecoveryPin().orElseThrow());
        }

        try (OxiaSyncOwnerLeaseBackend.ClientHandle replacement = client(endpoint, prefix + "/replacement")) {
            final OxiaSyncRecoveryCatalogBackend reopened =
                    new OxiaSyncRecoveryCatalogBackend(replacement, prefix + "/catalog", LIMITS);
            assertTrue(reopened.activeRecoveryPin().isEmpty());
        }
    }

    @Test
    void checkpointUploadIntentCasAndReopenWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-upload/" + UUID.randomUUID();
        final CheckpointUploadIntent pending = pendingIntent(20);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = client(endpoint, prefix + "/client")) {
            final OxiaSyncCheckpointUploadIntentBackend backend =
                    new OxiaSyncCheckpointUploadIntentBackend(client, prefix + "/intent");
            assertEquals(pending, backend.create(pending));
            final CheckpointResource resource = resource(pending);
            final CheckpointUploadIntent published = backend.publish(pending, resource);
            assertEquals(CheckpointUploadState.PUBLISHED, published.state());
            assertEquals(published, backend.currentPublishedFor(pending).orElseThrow());
            final CheckpointUploadIntent reopened = backend.current(pending).orElseThrow();
            assertEquals(published, reopened);
        }
    }

    @Test
    void freshProcessPhaseReopensDurableRecoveryAuthorities() throws Exception {
        final String phase = System.getenv("NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE");
        final String prefix = System.getenv("NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX");
        Assumptions.assumeTrue(
                phase != null && (phase.equals("WRITE") || phase.equals("READ")),
                "fresh-process authority phase is not configured");
        Assumptions.assumeTrue(prefix != null && !prefix.isBlank(), "fresh-process authority prefix is not configured");
        final String endpoint = endpoint();
        final ShardId shard = new ShardId(new RouteIncarnation(id16(121)), 120);
        final CheckpointManifest manifest = manifest(
                shard, UUID.fromString("12345678-1234-4abc-8def-1234567890ab"), id16(122), id16(123), 0, 10, 10, null);
        final CheckpointUploadIntent pending = pendingIntent(120);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = client(endpoint, prefix + "/recovery-client")) {
            final OxiaSyncRecoveryCatalogBackend catalog =
                    new OxiaSyncRecoveryCatalogBackend(client, prefix + "/catalog", LIMITS);
            final OxiaSyncCheckpointUploadIntentBackend intents =
                    new OxiaSyncCheckpointUploadIntentBackend(client, prefix + "/intent");
            if (phase.equals("WRITE")) {
                final var published = catalog.publish(manifest, 0);
                catalog.advanceFloor(manifest.checkpointId(), published.catalogGeneration(), List.of());
                assertEquals(pending, intents.create(pending));
                assertEquals(
                        CheckpointUploadState.PUBLISHED,
                        intents.publish(pending, resource(pending)).state());
                System.out.println("fresh-process recovery authority write phase passed");
            } else {
                assertArrayEquals(
                        manifest.checkpointId(),
                        catalog.currentFloorRef().orElseThrow().checkpointId());
                assertEquals(
                        CheckpointUploadState.PUBLISHED,
                        intents.current(pending).orElseThrow().state());
                System.out.println("fresh-process recovery authority read phase passed");
            }
        }
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle client(final String endpoint, final String identifier)
            throws Exception {
        return OxiaSyncOwnerLeaseBackend.connect(endpoint, "default", identifier, Duration.ofSeconds(15), "real-smoke");
    }

    private static CheckpointManifest manifest(
            final ShardId shard,
            final UUID topic,
            final byte[] lineage,
            final byte[] checkpointId,
            final long lineageGeneration,
            final long offset,
            final long mutationSequence,
            final CheckpointManifest.ParentCheckpoint parent) {
        final KafkaSourcePosition position =
                new KafkaSourcePosition(shard, "cluster", topic, offset, null, 1_000 + offset);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry(
                "CURRENT", 1, id32(20), Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(
                checkpointId,
                lineage,
                lineageGeneration,
                parent,
                null,
                new CheckpointManifest.CreatedBy(id32(21), id32(22), 1),
                new CheckpointManifest.CreatedAt(
                        1_000, 1_001, "CERTIFIED_HOST_CLOCK", id32(23), 1, offset, offset, id32(24), 0, null),
                shard,
                id32(25),
                UUID.randomUUID(),
                1,
                mutationSequence,
                position,
                id32(26),
                id32(27),
                List.of(),
                List.of(file));
    }

    private static CheckpointUploadIntent pendingIntent(final int seed) {
        final ShardId shard = new ShardId(new RouteIncarnation(id16(seed + 1)), seed);
        return new CheckpointUploadIntent(
                new ShardSubject(shard),
                id16(seed + 2),
                id16(seed + 3),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(seed + 4)),
                id16(seed + 5),
                id32(seed + 6),
                1,
                null,
                null,
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(seed + 7), ProfileKind.OBJECT_STORE),
                evidence(1_000),
                4_000,
                CheckpointUploadState.PENDING_UPLOAD,
                1,
                null,
                null);
    }

    private static CheckpointResource resource(final CheckpointUploadIntent pending) {
        return new CheckpointResource(
                pending.recoveryLineageId(),
                pending.checkpointId(),
                pending.objectStoreProfile(),
                Bytes.utf8("bucket"),
                Bytes.utf8("checkpoint/manifest"),
                Bytes.utf8("version"),
                10,
                id32(40));
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
                id32(50),
                0,
                null);
    }

    private static RecoveryPin recoveryPin(
            final ShardId shard,
            final CheckpointManifest manifest,
            final RecoveryFloorRef floor,
            final byte[] sessionIdentity) {
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT,
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                null);
        return new RecoveryPin(
                id16(12),
                new ShardSubject(shard),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(13)),
                candidate,
                floor,
                floor.catalogGeneration(),
                sessionIdentity);
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
