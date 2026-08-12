package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryInstallPhaseV1;
import io.nereusstream.delay.protocol.RecoveryInstallStateV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaSyncRecoveryCatalogBackendTest {
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            100, Long.MAX_VALUE, Long.MAX_VALUE, 4_096, 1 << 20, 100, 4_096);

    @Test
    void storesOneCanonicalSnapshotAndReopensItThroughRealCasSurface() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/shard",
                LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final CheckpointManifest manifest = manifest(shard, id16(1), id16(2), 0, 10, 10, null);

        assertEquals(1, backend.publish(manifest, 0).catalogGeneration());
        assertEquals(1, records.putCount);
        assertArrayEquals(manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());

        final RecoveryFloor floor = backend.advanceFloor(manifest.checkpointId(), 1, id32(3));
        assertEquals(2, floor.catalogGeneration());
        final var typed = backend.advanceFloor(manifest.checkpointId(), 2, java.util.List.<EvidenceCursorV1>of());
        assertEquals(3, typed.catalogGeneration());

        final OxiaSyncRecoveryCatalogBackend reopened = new OxiaSyncRecoveryCatalogBackend(records, "delay/shard",
                LIMITS);
        assertArrayEquals(floor.checkpointId(), reopened.currentFloor().orElseThrow().checkpointId());
        assertEquals(typed, reopened.currentFloorRef().orElseThrow());
        assertArrayEquals(manifest.canonicalJsonBytes(),
                reopened.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void exactRereadConvertsResponseLossIntoSuccess() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/lost",
                LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CheckpointManifest manifest = manifest(shard, id16(4), id16(5), 0, 1, 1, null);

        records.failNextPutAfterCommit = true;
        assertEquals(manifest, backend.publish(manifest, 0).manifest());
        assertArrayEquals(manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void responseLossWithWrongRereadRecordIdentityDoesNotBecomeSuccess() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/wrong-key",
                LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final CheckpointManifest manifest = manifest(shard, id16(6), id16(7), 0, 1, 1, null);

        records.failNextPutAfterCommit = true;
        records.returnWrongKeyOnNextGet = true;
        assertThrows(IllegalStateException.class, () -> backend.publish(manifest, 0));
        assertArrayEquals(manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
    }

    @Test
    void malformedRemoteSnapshotFailsClosedBeforeAnyCatalogProjection() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/bad",
                LIMITS);
        records.putRaw("delay/bad/catalog", new byte[]{0x08, 0x02});

        assertThrows(IllegalArgumentException.class, backend::currentFloor);
    }

    @Test
    void catalogReadRejectsARecordWithoutAnOxiaVersion() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records,
                "delay/no-version", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final CheckpointManifest manifest = manifest(shard, id16(8), id16(9), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(1, shard,
                java.util.List.of(manifest), Map.of(), null, null, null);
        records.putRawWithoutVersion("delay/no-version/catalog",
                OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));

        assertThrows(IllegalStateException.class, backend::currentFloor);
    }

    @Test
    void validatesLocalStoreRecoveryAgainstTheCurrentRemoteFloorSnapshot() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records,
                "delay/recovery-reuse", LIMITS);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(30);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(31), 0, 1, 1, null);

        assertEquals(1, backend.publish(genesis, 0).catalogGeneration());
        final var floor = backend.advanceFloor(genesis.checkpointId(), 1, java.util.List.of());
        final byte[] storeIncarnation = id16(32);
        final StoreRecoveryMetadata local = new StoreRecoveryMetadata(
                new RecoveryCandidateRefV1(RecoveryCandidateKindV1.LOCAL_STORE, lineage,
                        genesis.checkpointId(), genesis.manifestSha256(), storeIncarnation), floor,
                floor.catalogGeneration(), new RecoveryInstallStateV1(RecoveryInstallPhaseV1.OPEN,
                        storeIncarnation, genesis.checkpointId()));

        new OxiaRecoveryCatalog(backend).validateLocalStoreRecovery(shard, local);

        final CheckpointManifest child = manifest(shard, topic, lineage, id16(33), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(),
                        Bytes.hex(genesis.manifestSha256())));
        backend.publish(child, floor.catalogGeneration());
        backend.advanceFloor(child.checkpointId(), floor.catalogGeneration() + 1, java.util.List.of());
        assertThrows(IllegalStateException.class,
                () -> new OxiaRecoveryCatalog(backend).validateLocalStoreRecovery(shard, local));
    }

    @Test
    void uploadIntentAndPinTransactionAreNotPretendedToBeSingleRecordCas() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncRecoveryCatalogBackend backend = new OxiaSyncRecoveryCatalogBackend(records, "delay/strict",
                LIMITS);
        assertThrows(UnsupportedOperationException.class,
                () -> backend.publishUploadedCheckpoint(null, null, 0));
    }

    @Test
    void rejectsManifestCountAboveBoundBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final CheckpointManifest manifest = manifest(shard, id16(40), id16(41), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, shard, java.util.Collections.nCopies(100_001, manifest), Map.of(), null, null, null);

        assertThrows(IllegalStateException.class,
                () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsResourceCountAboveBoundBeforeEncodingSnapshot() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final CheckpointManifest manifest = manifest(shard, id16(42), id16(43), 0, 1, 1, null);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(44),
                ProfileKindV1.OBJECT_STORE);
        final CheckpointResourceV1 resource = new CheckpointResourceV1(manifest.recoveryLineageId(),
                manifest.checkpointId(), profile, Bytes.utf8("bucket"), Bytes.utf8("manifest"),
                Bytes.utf8("version"), manifest.canonicalJsonBytes().length, manifest.manifestSha256());
        final Map<String, CheckpointResourceV1> resources = new HashMap<>();
        for (int index = 0; index <= 100_000; index++) {
            resources.put("resource-" + index, resource);
        }
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, shard, java.util.List.of(manifest), resources, null, null, null);

        assertThrows(IllegalStateException.class,
                () -> OxiaSyncRecoveryCatalogBackend.encodeSnapshot(snapshot));
    }

    private static CheckpointManifest manifest(final ShardId shard, final byte[] lineage,
                                               final byte[] checkpointId, final long lineageGeneration,
                                               final long offset, final long mutationSequence,
                                               final CheckpointManifest.ParentCheckpoint parent) {
        return manifest(shard, UUID.randomUUID(), lineage, checkpointId, lineageGeneration, offset,
                mutationSequence, parent);
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
                mutationSequence, position, id32(26), id32(27), java.util.List.of(), java.util.List.of(file));
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

    private static final class FakeRecordClient implements OxiaSyncRecoveryCatalogBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private int putCount;
        private boolean failNextPutAfterCommit;
        private boolean returnWrongKeyOnNextGet;

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
                    .findFirst().orElse(null);
            if (condition != null) {
                if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                    throw new KeyAlreadyExistsException(key);
                }
                if (condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                        && (current == null || current.version().versionId() != condition.versionId())) {
                    throw new UnexpectedVersionIdException(key,
                            current == null ? OptionVersionId.KEY_NOT_EXISTS : current.version().versionId());
                }
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            putCount++;
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
        }

        private void putRawWithoutVersion(final String key, final byte[] value) {
            records.put(key, new GetResult(key, Bytes.copy(value), null));
        }
    }
}
