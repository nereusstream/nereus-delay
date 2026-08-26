package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
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
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncRecoveryCatalogBackendTest {
    private static final CheckpointManifestLimits LIMITS =
            new CheckpointManifestLimits(100, Long.MAX_VALUE, Long.MAX_VALUE, 4_096, 1 << 20, 100, 4_096);

    @Test
    void storesOneCanonicalSnapshotAndReopensItThroughRealCasSurface() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/shard", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final CheckpointManifest manifest = manifest(shard, id16(1), id16(2), 0, 10, 10, null);

        assertEquals(1, backend.publish(manifest, 0).catalogGeneration());
        assertEquals(1, records.putCount);
        assertArrayEquals(
                manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());

        final RecoveryFloor floor = backend.advanceFloor(manifest.checkpointId(), 1, id32(3));
        assertEquals(2, floor.catalogGeneration());
        final var typed = backend.advanceFloor(manifest.checkpointId(), 2, java.util.List.<EvidenceCursor>of());
        assertEquals(3, typed.catalogGeneration());

        final OxiaSyncRecoveryCatalogBackend reopened =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/shard", LIMITS);
        assertArrayEquals(
                floor.checkpointId(), reopened.currentFloor().orElseThrow().checkpointId());
        assertEquals(typed, reopened.currentFloorRef().orElseThrow());
        assertArrayEquals(
                manifest.canonicalJsonBytes(),
                reopened.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void exactRereadConvertsResponseLossIntoSuccess() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/lost", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CheckpointManifest manifest = manifest(shard, id16(4), id16(5), 0, 1, 1, null);

        records.failNextPutAfterCommit = true;
        assertEquals(manifest, backend.publish(manifest, 0).manifest());
        assertArrayEquals(
                manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void sessionFenceRejectsACommittedCatalogPublicationAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-catalog", LIMITS, () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        final ShardId shard = new ShardId(RouteIncarnation.random(), 14);
        final CheckpointManifest manifest = manifest(shard, id16(14), id16(15), 0, 1, 1, null);
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.publish(manifest, 0));

        final OxiaSyncRecoveryCatalogBackend reopened =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-catalog", LIMITS);
        assertArrayEquals(
                manifest.canonicalJsonBytes(),
                reopened.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void sessionFenceRejectsACommittedRecoveryPinAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-pin-create", LIMITS, () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        final ShardId shard = new ShardId(RouteIncarnation.random(), 15);
        final CheckpointManifest manifest = manifest(shard, id16(16), id16(17), 0, 1, 1, null);
        backend.publish(manifest, 0);
        final RecoveryFloorRef floor =
                backend.advanceFloor(manifest.checkpointId(), 1, java.util.List.<EvidenceCursor>of());
        final RecoveryPin pin = pin(shard, manifest, floor, fakeSessionIdentity(), 18, 19);
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.createRecoveryPin(pin));

        final OxiaSyncRecoveryCatalogBackend reopened =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-pin-create", LIMITS);
        assertEquals(pin, reopened.activeRecoveryPin().orElseThrow());
        reopened.releaseRecoveryPin(pin);
    }

    @Test
    void sessionFenceRejectsACommittedRecoveryPinReleaseAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend unbound =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-pin-release", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 16);
        final CheckpointManifest manifest = manifest(shard, id16(20), id16(21), 0, 1, 1, null);
        unbound.publish(manifest, 0);
        final RecoveryFloorRef floor =
                unbound.advanceFloor(manifest.checkpointId(), 1, java.util.List.<EvidenceCursor>of());
        final RecoveryPin pin = pin(shard, manifest, floor, fakeSessionIdentity(), 22, 23);
        unbound.createRecoveryPin(pin);

        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncRecoveryCatalogBackend fenced =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-pin-release", LIMITS, () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        records.afterDelete = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> fenced.releaseRecoveryPin(pin));

        final OxiaSyncRecoveryCatalogBackend reopened =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/fenced-pin-release", LIMITS);
        assertTrue(reopened.activeRecoveryPin().isEmpty());
    }

    @Test
    void responseLossWithWrongRereadRecordIdentityDoesNotBecomeSuccess() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/wrong-key", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final CheckpointManifest manifest = manifest(shard, id16(6), id16(7), 0, 1, 1, null);

        records.failNextPutAfterCommit = true;
        records.returnWrongKeyOnNextGet = true;
        assertThrows(IllegalStateException.class, () -> backend.publish(manifest, 0));
        assertArrayEquals(
                manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void malformedRemoteSnapshotFailsClosedBeforeAnyCatalogProjection() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/bad", LIMITS);
        records.putRaw("delay/bad/catalog", new byte[] {0x08, 0x02});

        assertThrows(IllegalArgumentException.class, backend::currentFloor);
    }

    @Test
    void catalogReadRejectsARecordWithoutAnOxiaVersion() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/no-version", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final CheckpointManifest manifest = manifest(shard, id16(8), id16(9), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot =
                new RecoveryCatalog.Snapshot(1, shard, java.util.List.of(manifest), Map.of(), null, null, null);
        records.putRawWithoutVersion(
                "delay/no-version/catalog", OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));

        assertThrows(IllegalStateException.class, backend::currentFloor);
    }

    @Test
    void directManifestReadRejectsCheckpointIdentityWithWrongWidth() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/manifest-width", LIMITS);

        assertThrows(IllegalArgumentException.class, () -> backend.manifest(new byte[15]));
        assertThrows(IllegalArgumentException.class, () -> backend.manifest(new byte[17]));
    }

    @Test
    void directFloorCoverageReadRejectsCandidateIdentityWithWrongWidth() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/coverage-width", LIMITS);

        assertThrows(IllegalArgumentException.class, () -> backend.proveFloorCoverage(new byte[15], 0));
        assertThrows(IllegalArgumentException.class, () -> backend.proveFloorCoverage(new byte[17], 0));
    }

    @Test
    void validatesLocalStoreRecoveryAgainstTheCurrentRemoteFloorSnapshot() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/recovery-reuse", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(30);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(31), 0, 1, 1, null);

        assertEquals(1, backend.publish(genesis, 0).catalogGeneration());
        final var floor = backend.advanceFloor(genesis.checkpointId(), 1, java.util.List.of());
        final byte[] storeIncarnation = id16(32);
        final StoreRecoveryMetadata local = new StoreRecoveryMetadata(
                new RecoveryCandidateRef(
                        RecoveryCandidateKind.LOCAL_STORE,
                        lineage,
                        genesis.checkpointId(),
                        genesis.manifestSha256(),
                        storeIncarnation),
                floor,
                floor.catalogGeneration(),
                new RecoveryInstallState(RecoveryInstallPhase.OPEN, storeIncarnation, genesis.checkpointId()));

        new OxiaRecoveryCatalog(backend).validateLocalStoreRecovery(shard, local);

        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(33),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        backend.publish(child, floor.catalogGeneration());
        backend.advanceFloor(child.checkpointId(), floor.catalogGeneration() + 1, java.util.List.of());
        assertThrows(IllegalStateException.class, () -> new OxiaRecoveryCatalog(backend)
                .validateLocalStoreRecovery(shard, local));
    }

    @Test
    void uploadIntentCasRemainsSeparateFromCatalogCas() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/strict", LIMITS);
        assertThrows(UnsupportedOperationException.class, () -> backend.publishUploadedCheckpoint(null, null, 0));
    }

    @Test
    void recoveryPinUsesAnEphemeralSingletonCasAndExactRereadRelease() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/pin", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final CheckpointManifest manifest = manifest(shard, id16(60), id16(61), 0, 1, 1, null);
        backend.publish(manifest, 0);
        final RecoveryFloorRef floor =
                backend.advanceFloor(manifest.checkpointId(), 1, java.util.List.<EvidenceCursor>of());
        final RecoveryPin pin = pin(shard, manifest, floor, fakeSessionIdentity(), 62, 63);

        records.failNextPutAfterCommit = true;
        assertEquals(pin, backend.createRecoveryPin(pin));
        assertEquals(pin, backend.activeRecoveryPin().orElseThrow());

        final OxiaSyncRecoveryCatalogBackend reopened =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/pin", LIMITS);
        assertEquals(pin, reopened.activeRecoveryPin().orElseThrow());
        final RecoveryPin conflicting = pin(shard, manifest, floor, fakeSessionIdentity(), 64, 65);
        assertThrows(IllegalStateException.class, () -> reopened.createRecoveryPin(conflicting));

        records.failNextDeleteAfterCommit = true;
        reopened.releaseRecoveryPin(pin);
        assertTrue(reopened.activeRecoveryPin().isEmpty());
        assertThrows(IllegalStateException.class, () -> reopened.releaseRecoveryPin(pin));
    }

    @Test
    void recoveryPinRequiresAnIdentityBearingCallerSession() {
        final FakeRecordClient records = new FakeRecordClient(null);
        final OxiaSyncRecoveryCatalogBackend catalogOnly =
                new OxiaSyncRecoveryCatalogBackend(records, "delay/catalog-only", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final CheckpointManifest manifest = manifest(shard, id16(66), id16(67), 0, 1, 1, null);
        catalogOnly.publish(manifest, 0);
        final RecoveryFloorRef floor =
                catalogOnly.advanceFloor(manifest.checkpointId(), 1, java.util.List.<EvidenceCursor>of());
        final RecoveryPin pin = pin(shard, manifest, floor, fakeSessionIdentity(), 68, 69);

        assertThrows(IllegalStateException.class, () -> catalogOnly.createRecoveryPin(pin));
        assertTrue(catalogOnly.activeRecoveryPin().isEmpty());
    }

    @Test
    void rejectsManifestCountAboveBoundBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final CheckpointManifest manifest = manifest(shard, id16(40), id16(41), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, shard, java.util.Collections.nCopies(100_001, manifest), Map.of(), null, null, null);

        assertThrows(IllegalStateException.class, () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsDuplicateManifestIdentityBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final CheckpointManifest manifest = manifest(shard, id16(45), id16(46), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, shard, java.util.List.of(manifest, manifest), Map.of(), null, null, null);

        assertThrows(IllegalStateException.class, () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsResourceCountAboveBoundBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final CheckpointManifest manifest = manifest(shard, id16(42), id16(43), 0, 1, 1, null);
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(44), ProfileKind.OBJECT_STORE);
        final CheckpointResource resource = new CheckpointResource(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                profile,
                Bytes.utf8("bucket"),
                Bytes.utf8("manifest"),
                Bytes.utf8("version"),
                manifest.canonicalJsonBytes().length,
                manifest.manifestSha256());
        final Map<String, CheckpointResource> resources = new HashMap<>();
        for (int index = 0; index <= 100_000; index++) {
            resources.put("resource-" + index, resource);
        }
        final RecoveryCatalog.Snapshot snapshot =
                new RecoveryCatalog.Snapshot(1, shard, java.util.List.of(manifest), resources, null, null, null);

        assertThrows(IllegalStateException.class, () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsResourceMapKeyThatDoesNotMatchCheckpointIdentityBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final CheckpointManifest manifest = manifest(shard, id16(47), id16(48), 0, 1, 1, null);
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(49), ProfileKind.OBJECT_STORE);
        final CheckpointResource resource = new CheckpointResource(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                profile,
                Bytes.utf8("bucket"),
                Bytes.utf8("manifest"),
                Bytes.utf8("version"),
                manifest.canonicalJsonBytes().length,
                manifest.manifestSha256());
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, shard, java.util.List.of(manifest), Map.of("alias", resource), null, null, null);

        assertThrows(IllegalStateException.class, () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsCatalogShardIdentityThatDoesNotMatchManifestBeforeEncodingSnapshot() {
        final ShardId manifestShard = new ShardId(RouteIncarnation.random(), 22);
        final ShardId catalogShard = new ShardId(RouteIncarnation.random(), 23);
        final CheckpointManifest manifest = manifest(manifestShard, id16(56), id16(57), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot =
                new RecoveryCatalog.Snapshot(1, catalogShard, java.util.List.of(manifest), Map.of(), null, null, null);

        assertThrows(IllegalArgumentException.class, () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsUnsupportedRecoveryPinBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        final CheckpointManifest manifest = manifest(shard, id16(50), id16(51), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(manifest, 0);
        final RecoveryFloor floor = catalog.advanceFloor(manifest.checkpointId(), 1, id32(55));
        final RecoveryFloorRef floorRef = new RecoveryFloorRef(
                floor.recoveryLineageId(),
                floor.checkpointId(),
                floor.manifestSha256(),
                floor.catalogGeneration(),
                floor.appliedSourcePosition(),
                floor.includedMutationSequence(),
                java.util.List.of());
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT,
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                null);
        final RecoveryPin pin = new RecoveryPin(
                id16(52),
                new ShardSubject(shard),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(53)),
                candidate,
                floorRef,
                floor.catalogGeneration(),
                id32(54));
        catalog.createRecoveryPin(pin);

        assertThrows(
                IllegalStateException.class, () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(catalog.snapshot()));
    }

    private static CheckpointManifest manifest(
            final ShardId shard,
            final byte[] lineage,
            final byte[] checkpointId,
            final long lineageGeneration,
            final long offset,
            final long mutationSequence,
            final CheckpointManifest.ParentCheckpoint parent) {
        return manifest(
                shard, UUID.randomUUID(), lineage, checkpointId, lineageGeneration, offset, mutationSequence, parent);
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
                java.util.List.of(),
                java.util.List.of(file));
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

    private static RecoveryPin pin(
            final ShardId shard,
            final CheckpointManifest manifest,
            final RecoveryFloorRef floor,
            final byte[] sessionIdentity,
            final int pinId,
            final int ownerId) {
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT,
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                null);
        return new RecoveryPin(
                id16(pinId),
                new ShardSubject(shard),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(ownerId)),
                candidate,
                floor,
                floor.catalogGeneration(),
                sessionIdentity);
    }

    private static byte[] fakeSessionIdentity() {
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-oxia-session-identity\0"),
                Bytes.u64be(101),
                Bytes.lp32(Bytes.utf8("fake-recovery-session")));
    }

    private static final class FakeRecordClient implements OxiaSyncRecoveryCatalogBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private final byte[] sessionIdentity;
        private long nextVersion = 1;
        private int putCount;
        private boolean failNextPutAfterCommit;
        private boolean failNextDeleteAfterCommit;
        private boolean returnWrongKeyOnNextGet;
        private Runnable afterPut = () -> {};
        private Runnable afterDelete = () -> {};

        private FakeRecordClient() {
            this(fakeSessionIdentity());
        }

        private FakeRecordClient(final byte[] sessionIdentity) {
            this.sessionIdentity = sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
        }

        @Override
        public byte[] sessionIdentity() {
            return sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
        }

        @Override
        public GetResult get(final String key) {
            final GetResult result = records.get(key);
            if (result != null && returnWrongKeyOnNextGet) {
                returnWrongKeyOnNextGet = false;
                return new GetResult(key + "/wrong", result.value(), result.version());
            }
            return result;
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final GetResult current = records.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (condition != null) {
                if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                    throw new KeyAlreadyExistsException(key);
                }
                if (condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                        && (current == null || current.version().versionId() != condition.versionId())) {
                    throw new UnexpectedVersionIdException(
                            key,
                            current == null
                                    ? OptionVersionId.KEY_NOT_EXISTS
                                    : current.version().versionId());
                }
            }
            final Version version =
                    new Version(nextVersion++, 0, 0, 1, Optional.of(101L), Optional.of("fake-recovery-session"));
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            putCount++;
            afterPut.run();
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            final GetResult current = records.get(key);
            if (current == null) {
                return false;
            }
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (condition != null
                    && (current.version() == null || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current.version() == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.version().versionId());
            }
            records.remove(key);
            afterDelete.run();
            if (failNextDeleteAfterCommit) {
                failNextDeleteAfterCommit = false;
                throw new IllegalStateException("simulated delete response loss");
            }
            return true;
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version =
                    new Version(nextVersion++, 0, 0, 1, Optional.of(101L), Optional.of("fake-recovery-session"));
            records.put(key, new GetResult(key, Bytes.copy(value), version));
        }

        private void putRawWithoutVersion(final String key, final byte[] value) {
            records.put(key, new GetResult(key, Bytes.copy(value), null));
        }
    }
}
